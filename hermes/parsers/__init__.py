"""Hermes Parsers — AI-powered structured data extraction from SERFF filing PDFs.

Each parser handles a specific document type and combines traditional PDF
extraction libraries with Claude AI to produce structured, confidence-scored
output ready for database insertion.
"""

from hermes.parsers.base_parser import BaseParser, ParseResult
from hermes.parsers.rate_parser import RateParser
from hermes.parsers.rule_parser import RuleParser
from hermes.parsers.form_parser import FormParser
from hermes.parsers.classifier import DocumentClassifier

__all__ = [
    "BaseParser",
    "ParseResult",
    "RateParser",
    "RuleParser",
    "FormParser",
    "DocumentClassifier",
]
