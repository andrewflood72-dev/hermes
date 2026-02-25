"""Rate Parser — extracts base rates, rating factors, and territory definitions.

Parsing strategy (in order):
  1. Attempt structured table extraction with pdfplumber / Camelot.
  2. If no tables found or average confidence is below 0.70, fall back to
     Claude AI extraction from raw text.
  3. Classify each table as base_rate, factor, ilf, deductible, or territory.
  4. Store results in hermes_base_rates, hermes_rating_factors,
     hermes_territory_definitions.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any
from uuid import UUID

from hermes.config import settings
from hermes.extraction.ai_extractor import AIExtractor
from hermes.extraction.table_extractor import ExtractedTable, TableExtractor
from hermes.extraction.text_extractor import TextExtractor
from hermes.parsers.base_parser import BaseParser
from hermes.validation.confidence import ConfidenceScorer

logger = logging.getLogger(__name__)

# Keywords used to classify table types from header text.
_TABLE_TYPE_PATTERNS: dict[str, list[str]] = {
    "base_rate": [
        "base rate", "manual rate", "class rate", "basic rate",
        "class code", "classification",
    ],
    "ilf": [
        "increased limit", "ilf", "limit factor", "basic limits",
    ],
    "deductible": [
        "deductible", "ded factor", "deductible factor",
    ],
    "territory": [
        "territory", "zip code", "county", "geographic", "region",
    ],
    "factor": [
        "factor", "credit", "surcharge", "modifier", "multiplier",
        "schedule", "experience",
    ],
}


def _classify_table_type(headers: list[str]) -> str:
    """Guess the table type from its column headers."""
    header_text = " ".join(h.lower() for h in headers)
    for table_type, keywords in _TABLE_TYPE_PATTERNS.items():
        if any(kw in header_text for kw in keywords):
            return table_type
    return "factor"  # safe default


def _parse_numeric(value: str) -> float | None:
    """Strip currency symbols/commas and coerce to float, or return None."""
    cleaned = re.sub(r"[,$%\s]", "", str(value))
    try:
        return float(cleaned)
    except ValueError:
        return None


class RateParser(BaseParser):
    """Extracts rate tables, rating factors, and territory definitions from
    rate exhibit PDFs.

    Combines pdfplumber / Camelot structured extraction with Claude AI
    fallback to maximise coverage across the wide variety of rate exhibit
    layouts found in SERFF filings.
    """

    parser_type = "rate"
    parser_version = "1.0.0"

    def __init__(self) -> None:
        super().__init__()
        self._table_extractor = TableExtractor()
        self._text_extractor = TextExtractor()
        self._ai_extractor = AIExtractor()
        self._confidence_scorer = ConfidenceScorer()

    # ── Extraction ─────────────────────────────────────────────────────────

    async def _extract_content(self, file_path: str) -> dict[str, Any]:
        """Extract rates, factors, and territory data from a rate exhibit PDF.

        First tries pdfplumber/Camelot structured extraction.  Falls back to
        AI extraction for any section where table extraction fails or returns
        low-confidence results.

        Returns:
            Dict with keys:
              base_rates, rating_factors, territory_definitions,
              tables_extracted, factors_extracted, warnings.
        """
        results: dict[str, Any] = {
            "base_rates": [],
            "rating_factors": [],
            "territory_definitions": [],
            "tables_extracted": 0,
            "factors_extracted": 0,
            "warnings": [],
        }

        # ── Step 1: structured table extraction ───────────────────────────
        try:
            tables = self._table_extractor.extract_tables(file_path)
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("Table extractor failed: %s", exc)
            tables = []
            results["warnings"].append(f"Table extraction error: {exc}")

        high_confidence_tables: list[ExtractedTable] = []

        for table in tables:
            score = self._confidence_scorer.score_table_extraction(table)
            self.confidence_tracker.record(score)
            table_type = _classify_table_type(table.headers)

            if score >= 0.40:
                high_confidence_tables.append(table)
                self._process_table(table, table_type, results)
            else:
                results["warnings"].append(
                    f"Low-confidence table on page {table.page_number} "
                    f"(score={score:.3f}) — will attempt AI fallback"
                )

        # ── Step 2: AI fallback for entire document if tables are sparse ──
        if len(high_confidence_tables) == 0:
            self.logger.info(
                "No high-confidence tables found; falling back to AI extraction"
            )
            await self._ai_fallback_extract(file_path, results)
        else:
            # AI fallback for individual low-confidence pages only.
            high_conf_pages = {t.page_number for t in high_confidence_tables}
            low_conf_pages = [
                t.page_number
                for t in tables
                if t.page_number not in high_conf_pages
            ]
            if low_conf_pages:
                await self._ai_fallback_pages(
                    file_path, low_conf_pages, results
                )

        results["tables_extracted"] = len(results["base_rates"])
        results["factors_extracted"] = len(results["rating_factors"])
        return results

    def _process_table(
        self,
        table: ExtractedTable,
        table_type: str,
        results: dict[str, Any],
    ) -> None:
        """Parse a single ExtractedTable into structured records."""
        if table_type == "base_rate":
            self._parse_base_rate_table(table, results)
        elif table_type == "territory":
            self._parse_territory_table(table, results)
        else:
            self._parse_factor_table(table, table_type, results)

    def _parse_base_rate_table(
        self, table: ExtractedTable, results: dict[str, Any]
    ) -> None:
        """Parse a base rate table into hermes_base_rates records."""
        header_lower = [h.lower() for h in table.headers]

        # Locate columns by partial header match.
        col_map = _map_columns(
            header_lower,
            {
                "class_code": ["class", "code", "class code"],
                "class_description": ["description", "class desc", "class name", "occupation"],
                "territory": ["territory", "terr"],
                "base_rate": ["base rate", "rate", "manual rate"],
                "rate_per_unit": ["per", "unit", "basis", "exposure"],
                "minimum_premium": ["min prem", "minimum premium", "min"],
            },
        )

        score = self._confidence_scorer.score_table_extraction(table)

        for row in table.rows:
            if not row or all(c == "" for c in row):
                continue
            record: dict[str, Any] = {
                "source_page": table.page_number,
                "confidence": score,
            }
            for field, col_idx in col_map.items():
                if col_idx is not None and col_idx < len(row):
                    raw = str(row[col_idx]).strip()
                    if field in ("base_rate", "minimum_premium"):
                        record[field] = _parse_numeric(raw)
                    else:
                        record[field] = raw if raw else None
            if record.get("base_rate") is not None:
                results["base_rates"].append(record)

    def _parse_factor_table(
        self,
        table: ExtractedTable,
        factor_type: str,
        results: dict[str, Any],
    ) -> None:
        """Parse a factor / ILF / deductible table into hermes_rating_factors."""
        score = self._confidence_scorer.score_table_extraction(table)

        for row in table.rows:
            if not row or all(c == "" for c in row):
                continue
            if len(row) < 2:
                continue
            factor_key = str(row[0]).strip()
            factor_val = _parse_numeric(str(row[1]))
            if factor_key and factor_val is not None:
                results["rating_factors"].append(
                    {
                        "factor_type": factor_type,
                        "factor_key": factor_key,
                        "factor_value": factor_val,
                        "factor_description": (
                            str(row[2]).strip() if len(row) > 2 else None
                        ),
                        "source_page": table.page_number,
                        "confidence": score,
                    }
                )

    def _parse_territory_table(
        self, table: ExtractedTable, results: dict[str, Any]
    ) -> None:
        """Parse a territory definition table into hermes_territory_definitions."""
        header_lower = [h.lower() for h in table.headers]
        col_map = _map_columns(
            header_lower,
            {
                "territory_code": ["territory", "terr", "code"],
                "territory_name": ["name", "description", "counties"],
                "zip_codes": ["zip", "zip code", "postal"],
                "risk_tier": ["tier", "class", "grade"],
            },
        )
        score = self._confidence_scorer.score_table_extraction(table)

        for row in table.rows:
            if not row or all(c == "" for c in row):
                continue
            record: dict[str, Any] = {"source_page": table.page_number}
            for field, col_idx in col_map.items():
                if col_idx is not None and col_idx < len(row):
                    raw = str(row[col_idx]).strip()
                    record[field] = raw if raw else None
            if record.get("territory_code"):
                results["territory_definitions"].append(record)

        self.confidence_tracker.record(score)

    async def _ai_fallback_extract(
        self, file_path: str, results: dict[str, Any]
    ) -> None:
        """Use Claude AI to extract rates from the entire document."""
        pages = self._text_extractor.extract_text(file_path)
        full_text = "\n\n".join(p.text for p in pages)

        try:
            ai_rates = await self._ai_extractor.extract_rates_from_text(
                full_text
            )
            self._ai_calls_made += 1
            for rec in ai_rates:
                confidence = float(rec.pop("confidence", 0.75))
                self.confidence_tracker.record(confidence)
                rec["confidence"] = confidence
                rec_type = rec.pop("record_type", "base_rate")
                if rec_type == "base_rate":
                    results["base_rates"].append(rec)
                elif rec_type in ("factor", "ilf", "deductible", "territory_factor"):
                    results["rating_factors"].append(rec)
        except Exception as exc:  # noqa: BLE001
            self.logger.error("AI fallback extraction failed: %s", exc)
            results["warnings"].append(f"AI extraction failed: {exc}")

    async def _ai_fallback_pages(
        self,
        file_path: str,
        page_numbers: list[int],
        results: dict[str, Any],
    ) -> None:
        """Run AI extraction on specific low-confidence pages only."""
        for page_num in page_numbers:
            try:
                text = self._text_extractor.extract_text_by_page(
                    file_path, page_num
                )
                if not text.strip():
                    continue
                ai_rates = await self._ai_extractor.extract_rates_from_text(
                    text
                )
                self._ai_calls_made += 1
                for rec in ai_rates:
                    confidence = float(rec.pop("confidence", 0.70))
                    self.confidence_tracker.record(confidence)
                    rec["confidence"] = confidence
                    rec_type = rec.pop("record_type", "base_rate")
                    if rec_type == "base_rate":
                        results["base_rates"].append(rec)
                    else:
                        results["rating_factors"].append(rec)
            except Exception as exc:  # noqa: BLE001
                self.logger.warning(
                    "AI fallback failed for page %s: %s", page_num, exc
                )

    # ── Storage ────────────────────────────────────────────────────────────

    async def _store_results(
        self, document_id: UUID, results: dict[str, Any]
    ) -> None:
        """Persist extracted rates, factors, and territories to the database.

        Inserts into:
          - hermes_base_rates
          - hermes_rating_factors
          - hermes_territory_definitions

        Each row links back to the source document via ``source_document_id``.

        Args:
            document_id: UUID of the ``hermes_filing_documents`` row.
            results: Dict returned by ``_extract_content``.
        """
        def _clamp_numeric(val, max_abs=999999.0):
            """Clamp a numeric value to fit numeric(12,6) column."""
            if val is None:
                return None
            try:
                v = float(val)
                if v > max_abs:
                    return max_abs
                if v < -max_abs:
                    return -max_abs
                return v
            except (ValueError, TypeError):
                return None

        try:
            from hermes.db import get_connection  # type: ignore[import]

            async with get_connection() as conn:
                # Resolve rate_table_id from the document record.
                rate_table_id = await self._get_or_create_rate_table(
                    conn, document_id
                )

                # ── base rates ─────────────────────────────────────────
                for rate in results.get("base_rates", []):
                    confidence = float(rate.get("confidence", 1.0))
                    await conn.execute(
                        """
                        INSERT INTO hermes_base_rates (
                            rate_table_id, class_code, class_description,
                            territory, base_rate, rate_per_unit,
                            minimum_premium, source_page, confidence
                        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
                        ON CONFLICT DO NOTHING
                        """,
                        str(rate_table_id),
                        rate.get("class_code"),
                        rate.get("class_description"),
                        rate.get("territory"),
                        _clamp_numeric(rate.get("base_rate")),
                        _clamp_numeric(rate.get("rate_per_unit")),
                        _clamp_numeric(rate.get("minimum_premium")),
                        rate.get("source_page"),
                        confidence,
                    )
                    # Queue low-confidence fields for review.
                    if confidence < 0.7 and rate.get("base_rate") is not None:
                        self._queue_low_confidence(
                            document_id,
                            "base_rate",
                            rate.get("base_rate"),
                            confidence,
                            f"class_code={rate.get('class_code')} "
                            f"page={rate.get('source_page')}",
                        )

                # ── rating factors ─────────────────────────────────────
                for factor in results.get("rating_factors", []):
                    confidence = float(factor.get("confidence", 1.0))
                    clamped_val = _clamp_numeric(factor.get("factor_value"))
                    if clamped_val is None:
                        continue
                    await conn.execute(
                        """
                        INSERT INTO hermes_rating_factors (
                            rate_table_id, factor_type, factor_key,
                            factor_value, factor_description,
                            source_page, confidence
                        ) VALUES ($1,$2,$3,$4,$5,$6,$7)
                        ON CONFLICT DO NOTHING
                        """,
                        str(rate_table_id),
                        factor.get("factor_type", "factor"),
                        factor.get("factor_key"),
                        clamped_val,
                        factor.get("factor_description"),
                        factor.get("source_page"),
                        confidence,
                    )

                # ── territory definitions ──────────────────────────────
                for terr in results.get("territory_definitions", []):
                    await conn.execute(
                        """
                        INSERT INTO hermes_territory_definitions (
                            rate_table_id, territory_code, territory_name,
                            zip_codes, counties, risk_tier
                        ) VALUES ($1,$2,$3,$4::jsonb,$5::jsonb,$6)
                        ON CONFLICT DO NOTHING
                        """,
                        str(rate_table_id),
                        terr.get("territory_code"),
                        terr.get("territory_name"),
                        json.dumps([]),
                        json.dumps([]),
                        terr.get("risk_tier"),
                    )

        except Exception as exc:  # noqa: BLE001
            self.logger.error("Failed to store rate results: %s", exc)
            raise

    async def _get_or_create_rate_table(
        self, conn: Any, document_id: UUID
    ) -> UUID:
        """Return the rate_table_id associated with ``document_id``.

        Looks up the ``hermes_rate_tables`` row linked to the document.
        If none exists (e.g., this is the first parse), creates a minimal
        placeholder row that callers can update later.
        """
        row = await conn.fetchrow(
            """
            SELECT rt.id
            FROM hermes_rate_tables rt
            JOIN hermes_filing_documents fd ON fd.filing_id = rt.filing_id
            WHERE fd.id = $1
            LIMIT 1
            """,
            str(document_id),
        )
        if row:
            return UUID(str(row["id"]))

        # Create a placeholder rate table so FK constraints are satisfied.
        doc_row = await conn.fetchrow(
            """
            SELECT fd.filing_id, f.carrier_id, f.state, f.line_of_business,
                   f.effective_date
            FROM hermes_filing_documents fd
            JOIN hermes_filings f ON f.id = fd.filing_id
            WHERE fd.id = $1
            """,
            str(document_id),
        )
        if not doc_row:
            raise ValueError(
                f"hermes_filing_documents row not found for id={document_id}"
            )

        from datetime import date as _date
        effective = doc_row["effective_date"] or _date.today()
        new_id_row = await conn.fetchrow(
            """
            INSERT INTO hermes_rate_tables (
                filing_id, carrier_id, state, line,
                table_name, table_type, effective_date, source_document_id
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
            RETURNING id
            """,
            str(doc_row["filing_id"]),
            str(doc_row["carrier_id"]),
            doc_row["state"],
            doc_row["line_of_business"],
            "Auto-generated by RateParser",
            "base_rate",
            effective,
            str(document_id),
        )
        return UUID(str(new_id_row["id"]))


# ── Helpers ────────────────────────────────────────────────────────────────


def _map_columns(
    header_lower: list[str], field_keywords: dict[str, list[str]]
) -> dict[str, int | None]:
    """Map field names to column indices using keyword matching.

    Args:
        header_lower: List of lowercase column header strings.
        field_keywords: Dict mapping field names to possible header substrings.

    Returns:
        Dict mapping each field name to its column index, or None if not found.
    """
    mapping: dict[str, int | None] = {f: None for f in field_keywords}
    for field, keywords in field_keywords.items():
        for idx, header in enumerate(header_lower):
            if any(kw in header for kw in keywords):
                mapping[field] = idx
                break
    return mapping
