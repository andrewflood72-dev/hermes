"""Table Extractor — structured table extraction from PDF files.

Uses pdfplumber as the primary engine (most reliable for digital PDFs) and
falls back to Camelot-py when pdfplumber finds no tables (better for
scanned/image-heavy documents with lattice lines).

``ExtractedTable`` is the common output model regardless of which engine
produced the result.
"""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


class ExtractedTable(BaseModel):
    """Structured representation of a single table extracted from a PDF page."""

    page_number: int
    headers: list[str]
    rows: list[list[str]]
    confidence: float = Field(ge=0.0, le=1.0, default=0.8)
    raw_dataframe: Any = Field(default=None, exclude=True)  # pd.DataFrame

    model_config = {"arbitrary_types_allowed": True}


def _df_to_extracted_table(
    df: "pd.DataFrame", page_number: int, confidence: float = 0.8
) -> ExtractedTable:
    """Convert a pandas DataFrame to an ``ExtractedTable``.

    The first row or the DataFrame columns become the table headers.

    Args:
        df: DataFrame from pdfplumber or Camelot.
        page_number: 1-based page number the table came from.
        confidence: Initial confidence estimate.

    Returns:
        An ``ExtractedTable`` instance.
    """
    df = df.fillna("").astype(str)

    # If the columns are generic integers (0, 1, 2 …) the first data row may
    # actually be the header.
    if all(isinstance(c, int) for c in df.columns):
        if not df.empty:
            headers = list(df.iloc[0].values)
            rows = [list(row) for row in df.iloc[1:].values]
        else:
            headers = []
            rows = []
    else:
        headers = [str(c) for c in df.columns]
        rows = [list(row) for row in df.values]

    return ExtractedTable(
        page_number=page_number,
        headers=headers,
        rows=rows,
        confidence=confidence,
        raw_dataframe=df,
    )


class TableExtractor:
    """Extracts structured tables from PDF files using pdfplumber and Camelot.

    Tries pdfplumber first for all pages (or the specified page range).  If
    pdfplumber returns no tables on a page, attempts Camelot extraction on
    that page using its ``lattice`` flavor (best for ruled tables) followed
    by ``stream`` flavor (best for whitespace-delimited tables).
    """

    def extract_tables(
        self, file_path: str, pages: str = "all"
    ) -> list[ExtractedTable]:
        """Extract all tables from a PDF file.

        Args:
            file_path: Absolute path to the PDF.
            pages: Page range string.  ``"all"`` extracts all pages.
                   Camelot-style ranges such as ``"1-3,5"`` are also accepted
                   when falling back to Camelot.

        Returns:
            List of ``ExtractedTable`` objects sorted by page number.
        """
        tables: list[ExtractedTable] = []

        # ── pdfplumber pass ────────────────────────────────────────────────
        pdfplumber_tables = self._extract_with_pdfplumber(file_path, pages)
        tables.extend(pdfplumber_tables)

        # ── Camelot fallback for pages with no pdfplumber results ─────────
        if not pdfplumber_tables:
            logger.info(
                "pdfplumber found no tables in '%s'; trying Camelot", file_path
            )
            camelot_tables = self._extract_with_camelot(file_path, pages)
            tables.extend(camelot_tables)

        logger.info(
            "TableExtractor: %d tables extracted from '%s'",
            len(tables),
            file_path,
        )
        return sorted(tables, key=lambda t: t.page_number)

    # ── pdfplumber ─────────────────────────────────────────────────────────

    def _extract_with_pdfplumber(
        self, file_path: str, pages: str
    ) -> list[ExtractedTable]:
        """Extract tables using pdfplumber.

        Args:
            file_path: Path to the PDF file.
            pages: ``"all"`` or a pdfplumber-compatible page spec.

        Returns:
            List of ``ExtractedTable`` objects, one per extracted table.
        """
        try:
            import pdfplumber  # type: ignore[import]
        except ImportError:
            logger.warning("pdfplumber not installed; skipping pdfplumber pass")
            return []

        results: list[ExtractedTable] = []
        try:
            with pdfplumber.open(file_path) as pdf:
                page_iter = (
                    enumerate(pdf.pages, start=1)
                    if pages == "all"
                    else self._iter_pdfplumber_pages(pdf, pages)
                )
                for page_num, page in page_iter:
                    raw_tables = page.extract_tables()
                    for raw in raw_tables:
                        if not raw:
                            continue
                        df = pd.DataFrame(raw)
                        confidence = self._estimate_pdfplumber_confidence(df)
                        results.append(
                            _df_to_extracted_table(df, page_num, confidence)
                        )
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "pdfplumber extraction failed for '%s': %s", file_path, exc
            )
        return results

    @staticmethod
    def _iter_pdfplumber_pages(
        pdf: Any, pages_spec: str
    ):
        """Yield (page_number, page) tuples for a comma/range page spec."""
        page_nums: list[int] = []
        for part in pages_spec.split(","):
            part = part.strip()
            if "-" in part:
                start_s, end_s = part.split("-", 1)
                page_nums.extend(range(int(start_s), int(end_s) + 1))
            else:
                page_nums.append(int(part))
        for pn in page_nums:
            if 1 <= pn <= len(pdf.pages):
                yield pn, pdf.pages[pn - 1]

    @staticmethod
    def _estimate_pdfplumber_confidence(df: "pd.DataFrame") -> float:
        """Estimate table extraction quality from a pdfplumber-sourced DataFrame.

        Heuristics:
          - More columns → higher base confidence.
          - High proportion of non-empty cells → higher confidence.
          - At least one column that looks numeric → higher confidence.
        """
        if df.empty:
            return 0.3
        total_cells = df.size
        non_empty = int((df != "").sum().sum())
        fill_rate = non_empty / max(total_cells, 1)
        col_score = min(len(df.columns) / 5, 1.0)
        return round(0.5 * fill_rate + 0.3 * col_score + 0.2, 4)

    # ── Camelot ───────────────────────────────────────────────────────────

    def _extract_with_camelot(
        self, file_path: str, pages: str
    ) -> list[ExtractedTable]:
        """Extract tables using Camelot-py.

        Tries ``lattice`` flavor first (for tables with visible border lines),
        then ``stream`` flavor (for whitespace-delimited tables).

        Args:
            file_path: Path to the PDF file.
            pages: Camelot page spec string (``"all"``, ``"1"``, ``"1-3"``, …).

        Returns:
            List of ``ExtractedTable`` objects.
        """
        try:
            import camelot  # type: ignore[import]
        except ImportError:
            logger.warning("camelot-py not installed; skipping Camelot pass")
            return []

        results: list[ExtractedTable] = []

        for flavor in ("lattice", "stream"):
            try:
                table_list = camelot.read_pdf(
                    file_path,
                    pages=pages,
                    flavor=flavor,
                    suppress_stdout=True,
                )
                for ct in table_list:
                    df = ct.df
                    confidence = round(ct.accuracy / 100.0, 4)
                    results.append(
                        _df_to_extracted_table(
                            df, ct.page, confidence
                        )
                    )
                if results:
                    logger.info(
                        "Camelot (%s) found %d tables in '%s'",
                        flavor,
                        len(results),
                        file_path,
                    )
                    return results
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "Camelot %s extraction failed for '%s': %s",
                    flavor,
                    file_path,
                    exc,
                )

        return results
