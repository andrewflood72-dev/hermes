"""Hermes Extraction Layer — low-level PDF content extractors.

Provides three complementary extractors:
  - ``TableExtractor``: structured table extraction via pdfplumber / Camelot
  - ``TextExtractor``: full-text extraction via PyMuPDF (fitz)
  - ``AIExtractor``: Claude AI-powered extraction for unstructured content
"""

from hermes.extraction.table_extractor import ExtractedTable, TableExtractor
from hermes.extraction.text_extractor import PageText, TextExtractor
from hermes.extraction.ai_extractor import AIExtractor

__all__ = [
    "AIExtractor",
    "ExtractedTable",
    "PageText",
    "TableExtractor",
    "TextExtractor",
]
