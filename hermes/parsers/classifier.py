"""Document Classifier — determines the document type for a SERFF filing PDF.

Classification is attempted in three stages, each more expensive than the last:
  1. Filename pattern matching (free, instantaneous).
  2. First-page keyword analysis (free, local).
  3. Claude AI classification (paid, remote fallback).

Returns one of the ``hermes_filing_documents.document_type`` enum values:
  rate_exhibit, actuarial_memo, rule_manual, policy_form, endorsement,
  application, schedule, supporting.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any

from anthropic import AsyncAnthropic

from hermes.config import settings
from hermes.extraction.text_extractor import TextExtractor

logger = logging.getLogger(__name__)

# ── Document type constants ────────────────────────────────────────────────

DOCUMENT_TYPES = (
    "rate_exhibit",
    "actuarial_memo",
    "rule_manual",
    "policy_form",
    "endorsement",
    "application",
    "schedule",
    "supporting",
)

# ── Filename pattern rules ─────────────────────────────────────────────────
# Ordered from most-specific to least-specific.  Each entry is
# (pattern_regex, document_type).

_FILENAME_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"\brate[_ ]?exhibit\b", re.IGNORECASE), "rate_exhibit"),
    (re.compile(r"\brate[_ ]?table\b", re.IGNORECASE), "rate_exhibit"),
    (re.compile(r"\bilf\b", re.IGNORECASE), "rate_exhibit"),
    (re.compile(r"\bactuarial[_ ]?memo\b", re.IGNORECASE), "actuarial_memo"),
    (re.compile(r"\bact[_ ]?memo\b", re.IGNORECASE), "actuarial_memo"),
    (re.compile(r"\brule[_ ]?manual\b", re.IGNORECASE), "rule_manual"),
    (re.compile(r"\brules?\b", re.IGNORECASE), "rule_manual"),
    (re.compile(r"\bendorsement\b", re.IGNORECASE), "endorsement"),
    (re.compile(r"\bendorse\b", re.IGNORECASE), "endorsement"),
    (re.compile(r"\bapplication\b", re.IGNORECASE), "application"),
    (re.compile(r"\bschedule\b", re.IGNORECASE), "schedule"),
    (re.compile(r"\brate\b", re.IGNORECASE), "rate_exhibit"),
    (re.compile(r"\bform\b", re.IGNORECASE), "policy_form"),
    (re.compile(r"\bpolicy\b", re.IGNORECASE), "policy_form"),
]

# ── First-page keyword rules ───────────────────────────────────────────────

_KEYWORD_RULES: list[tuple[list[str], str]] = [
    (
        ["rate exhibit", "base rate", "rating factor", "rate table",
         "territory factor", "increased limit", "ilf table"],
        "rate_exhibit",
    ),
    (
        ["actuarial memorandum", "actuarial memo", "loss ratio",
         "rate change", "credibility", "trend factor", "indicated change"],
        "actuarial_memo",
    ),
    (
        ["rule manual", "underwriting rule", "eligibility criteria",
         "ineligible class", "eligible class", "underwriting guideline"],
        "rule_manual",
    ),
    (
        ["endorsement", "amendatory", "this endorsement modifies",
         "in consideration of the premium"],
        "endorsement",
    ),
    (
        ["application for insurance", "applicant information",
         "to be completed by", "acord"],
        "application",
    ),
    (
        ["schedule of", "supplemental declarations",
         "schedule of locations", "schedule of operations"],
        "schedule",
    ),
    (
        ["coverage form", "policy declarations", "insuring agreement",
         "definitions", "conditions", "exclusions"],
        "policy_form",
    ),
]

# ── Claude prompt ──────────────────────────────────────────────────────────

_CLASSIFY_PROMPT = """\
You are an expert in commercial insurance regulatory filings. Based on the \
following text from the first page of a SERFF filing document, classify it \
into exactly one of these document types:

  rate_exhibit    — rate tables, base rates, rating factors, ILF tables
  actuarial_memo  — actuarial memoranda, loss ratio analysis, trend analysis
  rule_manual     — underwriting rules, eligibility criteria, guidelines
  policy_form     — policy coverage forms, main policy wording
  endorsement     — endorsements, amendatory endorsements, form modifications
  application     — insurance applications, ACORD forms
  schedule        — schedules of locations, operations, or values
  supporting      — anything else (financial statements, correspondence, etc.)

Reply with ONLY the document type string, nothing else.

FIRST PAGE TEXT:
\"\"\"
{first_page_text}
\"\"\"
"""


class DocumentClassifier:
    """Classifies SERFF filing PDFs into their document type.

    Uses a three-tier approach: filename patterns → keyword analysis →
    Claude AI fallback.  Only escalates to the next tier if the previous
    tier returns no confident result.
    """

    def __init__(self) -> None:
        self._text_extractor = TextExtractor()
        self._client = AsyncAnthropic(api_key=settings.anthropic_api_key)

    async def classify(self, file_path: str) -> str:
        """Classify the document at ``file_path`` and return its type string.

        Args:
            file_path: Absolute path to the PDF file.

        Returns:
            One of the ``DOCUMENT_TYPES`` strings.  Falls back to
            ``"supporting"`` if classification is inconclusive.
        """
        filename = Path(file_path).stem

        # ── Tier 1: filename pattern matching ─────────────────────────────
        result = self._classify_by_filename(filename)
        if result:
            logger.debug(
                "Classified '%s' as '%s' via filename pattern",
                filename,
                result,
            )
            return result

        # ── Tier 2: first-page keyword analysis ───────────────────────────
        try:
            first_page_text = self._text_extractor.extract_text_by_page(
                file_path, 0
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Could not extract text from '%s' for classification: %s",
                file_path,
                exc,
            )
            first_page_text = ""

        if first_page_text:
            result = self._classify_by_keywords(first_page_text)
            if result:
                logger.debug(
                    "Classified '%s' as '%s' via keyword analysis",
                    filename,
                    result,
                )
                return result

        # ── Tier 3: Claude AI classification ──────────────────────────────
        if first_page_text:
            try:
                result = await self._classify_by_ai(first_page_text)
                logger.debug(
                    "Classified '%s' as '%s' via Claude AI",
                    filename,
                    result,
                )
                return result
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "AI classification failed for '%s': %s",
                    filename,
                    exc,
                )

        logger.info(
            "Could not confidently classify '%s'; defaulting to 'supporting'",
            filename,
        )
        return "supporting"

    # ── Tier implementations ───────────────────────────────────────────────

    def _classify_by_filename(self, filename: str) -> str | None:
        """Return document type based on filename patterns, or None."""
        for pattern, doc_type in _FILENAME_PATTERNS:
            if pattern.search(filename):
                return doc_type
        return None

    def _classify_by_keywords(self, first_page_text: str) -> str | None:
        """Return document type based on first-page keyword matching, or None.

        Scoring: each keyword match increments the candidate type's score.
        The type with the highest score (minimum 2 keyword hits) wins.
        """
        text_lower = first_page_text.lower()
        scores: dict[str, int] = {t: 0 for t in DOCUMENT_TYPES}

        for keywords, doc_type in _KEYWORD_RULES:
            for kw in keywords:
                if kw in text_lower:
                    scores[doc_type] += 1

        best_type = max(scores, key=lambda t: scores[t])
        if scores[best_type] >= 2:
            return best_type
        if scores[best_type] == 1:
            # Single hit is enough for highly-specific keyword types.
            if best_type in ("actuarial_memo", "application"):
                return best_type
        return None

    async def _classify_by_ai(self, first_page_text: str) -> str:
        """Use Claude AI to classify the document from first-page text.

        Args:
            first_page_text: Text from the first page (truncated to 3000 chars).

        Returns:
            One of the ``DOCUMENT_TYPES`` strings.
        """
        prompt = _CLASSIFY_PROMPT.format(
            first_page_text=first_page_text[:3000]
        )

        response = await self._client.messages.create(
            model="claude-opus-4-6",
            max_tokens=20,
            messages=[{"role": "user", "content": prompt}],
        )

        raw = response.content[0].text.strip().lower()

        # Validate that the model returned a known type.
        for doc_type in DOCUMENT_TYPES:
            if doc_type in raw:
                return doc_type

        # If we can't match the response, fall back to keyword classification
        # with a lower threshold.
        logger.warning(
            "AI returned unrecognised document type '%s'; using 'supporting'",
            raw,
        )
        return "supporting"
