"""Text Extractor — full-text and per-page text extraction from PDF files.

Uses PyMuPDF (``fitz``) as the extraction engine for high-fidelity text
reconstruction including proper whitespace handling and reading-order sorting.

``PageText`` is the common output model for a single extracted page.
"""

from __future__ import annotations

import logging
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


class PageText(BaseModel):
    """Text content from a single PDF page."""

    page_number: int = Field(description="1-based page number")
    text: str = Field(description="Full extracted text for this page")
    word_count: int = Field(
        default=0, description="Approximate word count (whitespace-split)"
    )

    def model_post_init(self, __context: object) -> None:  # type: ignore[override]
        """Auto-calculate word_count after model initialisation."""
        if self.word_count == 0 and self.text:
            object.__setattr__(self, "word_count", len(self.text.split()))


class TextExtractor:
    """Extracts text from PDF files using PyMuPDF (fitz).

    PyMuPDF is preferred over pdfminer / pdfplumber for raw text extraction
    because it preserves reading order, handles multi-column layouts better,
    and is significantly faster on large documents.
    """

    def extract_text(self, file_path: str) -> list[PageText]:
        """Extract text from every page of a PDF.

        Args:
            file_path: Absolute path to the PDF file.

        Returns:
            Ordered list of ``PageText`` objects, one per page.  Returns an
            empty list (with a logged warning) if extraction fails.
        """
        try:
            import fitz  # PyMuPDF  # type: ignore[import]
        except ImportError:
            logger.error(
                "PyMuPDF (fitz) is not installed. "
                "Install it with: pip install pymupdf"
            )
            return []

        pages: list[PageText] = []
        try:
            doc = fitz.open(file_path)
            for page_index in range(len(doc)):
                page = doc[page_index]
                # ``sort=True`` preserves left-to-right, top-to-bottom reading
                # order which is essential for multi-column rate tables.
                text = page.get_text("text", sort=True)
                pages.append(
                    PageText(
                        page_number=page_index + 1,
                        text=text,
                    )
                )
            doc.close()
            logger.debug(
                "TextExtractor: extracted text from %d pages of '%s'",
                len(pages),
                file_path,
            )
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "Text extraction failed for '%s': %s", file_path, exc
            )

        return pages

    def extract_text_by_page(self, file_path: str, page_num: int) -> str:
        """Extract text from a single page of a PDF.

        Args:
            file_path: Absolute path to the PDF file.
            page_num: 0-based page index (consistent with PyMuPDF's internal
                      indexing; pass 0 for the first page).

        Returns:
            Plain-text string for the requested page, or an empty string if
            the page cannot be read.
        """
        try:
            import fitz  # PyMuPDF  # type: ignore[import]
        except ImportError:
            logger.error("PyMuPDF (fitz) is not installed.")
            return ""

        try:
            doc = fitz.open(file_path)
            if page_num < 0 or page_num >= len(doc):
                logger.warning(
                    "Page index %d is out of range for '%s' (%d pages)",
                    page_num,
                    file_path,
                    len(doc),
                )
                doc.close()
                return ""
            page = doc[page_num]
            text = page.get_text("text", sort=True)
            doc.close()
            return text
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "Single-page extraction failed for '%s' page %d: %s",
                file_path,
                page_num,
                exc,
            )
            return ""
