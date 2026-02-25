"""Form Parser — extracts policy form metadata and key provisions from
policy form and endorsement PDFs.

Strategy:
  1. Extract first-page text to identify form number, edition date, and name.
  2. Extract remaining pages and send to Claude AI to summarise provisions
     and classify each as coverage_grant, exclusion, condition, or definition.
  3. Persist to hermes_policy_forms and hermes_form_provisions.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any
from uuid import UUID

from anthropic import AsyncAnthropic

from hermes.config import settings
from hermes.extraction.text_extractor import TextExtractor
from hermes.parsers.base_parser import BaseParser
from hermes.validation.confidence import ConfidenceScorer

logger = logging.getLogger(__name__)

# ── Regex patterns for first-page metadata extraction ─────────────────────

_FORM_NUMBER_RE = re.compile(
    r"(?:form\s+(?:no\.?|number)?|form:)\s*([A-Z0-9][-A-Z0-9 ]{1,30})",
    re.IGNORECASE,
)
_EDITION_DATE_RE = re.compile(
    r"(?:ed(?:ition)?\.?\s*|rev(?:ised)?\.?\s*|dated?\s*)"
    r"(\d{1,2}[/-]\d{2,4}|\d{4}|[A-Z]{2,3}\s+\d{4})",
    re.IGNORECASE,
)
_FORM_TYPE_KEYWORDS: dict[str, list[str]] = {
    "endorsement": ["endorsement", "amendatory", "amendment"],
    "application": ["application", "acord"],
    "schedule": ["schedule", "supplemental dec"],
    "certificate": ["certificate"],
    "notice": ["notice", "advisory"],
    "declarations": ["declarations", "dec page"],
    "policy": ["policy", "coverage form", "commercial lines"],
}

_PROVISION_EXTRACTION_PROMPT = """\
You are an expert commercial insurance analyst. Analyse the following policy \
form text and extract all key provisions.

For each provision return a JSON object in this array:

[
  {{
    "provision_type": "<coverage_grant|exclusion|condition|definition>",
    "provision_key": "<short identifier, e.g. 'pollution_exclusion'>",
    "provision_text_summary": "<1-3 sentence AI summary>",
    "provision_text_full": "<verbatim clause text, truncated at 1000 chars>",
    "section_reference": "<section/paragraph label if visible, else null>",
    "is_coverage_broadening": <true|false|null>,
    "is_coverage_restricting": <true|false|null>,
    "iso_comparison_notes": "<notes on how this differs from ISO standard, or null>",
    "confidence": <0.0–1.0>
  }}
]

Return ONLY a valid JSON array, no markdown fences.

FORM TEXT:
\"\"\"
{form_text}
\"\"\"
"""

_MAX_FORM_CHARS = 12_000  # trim to avoid exceeding context limits


def _detect_form_type(first_page_text: str, form_name: str | None) -> str:
    """Guess form type from first-page text and name."""
    combined = f"{first_page_text} {form_name or ''}".lower()
    for form_type, keywords in _FORM_TYPE_KEYWORDS.items():
        if any(kw in combined for kw in keywords):
            return form_type
    return "policy"


class FormParser(BaseParser):
    """Parses policy forms and endorsements to extract metadata and provisions.

    Uses regex on the first page for structural metadata (form number, edition
    date, form type) then leverages Claude AI to summarise and classify the
    substantive form provisions.
    """

    parser_type = "form"
    parser_version = "1.0.0"

    def __init__(self) -> None:
        super().__init__()
        self._text_extractor = TextExtractor()
        self._confidence_scorer = ConfidenceScorer()
        self._client = AsyncAnthropic(api_key=settings.anthropic_api_key)

    # ── Extraction ─────────────────────────────────────────────────────────

    async def _extract_content(self, file_path: str) -> dict[str, Any]:
        """Extract form metadata from first page; provisions from full text.

        Returns:
            Dict with keys: form_metadata, provisions, forms_extracted,
            tables_extracted, rules_extracted, factors_extracted, warnings.
        """
        results: dict[str, Any] = {
            "form_metadata": {},
            "provisions": [],
            "forms_extracted": 0,
            "tables_extracted": 0,
            "rules_extracted": 0,
            "factors_extracted": 0,
            "warnings": [],
        }

        pages = self._text_extractor.extract_text(file_path)
        if not pages:
            results["warnings"].append("No pages extracted from PDF")
            return results

        # ── First-page metadata extraction ────────────────────────────────
        first_page_text = pages[0].text
        metadata = self._extract_first_page_metadata(first_page_text)
        results["form_metadata"] = metadata
        self.logger.info(
            "Form metadata extracted: number=%s edition=%s type=%s",
            metadata.get("form_number"),
            metadata.get("form_edition_date"),
            metadata.get("form_type"),
        )

        # ── Provision extraction via Claude AI ────────────────────────────
        full_text = "\n\n".join(p.text for p in pages)
        truncated_text = full_text[:_MAX_FORM_CHARS]

        try:
            provisions = await self._extract_provisions(truncated_text)
            results["provisions"] = provisions
            self._ai_calls_made += 1
        except Exception as exc:  # noqa: BLE001
            msg = f"Provision extraction failed: {exc}"
            self.logger.error(msg)
            results["warnings"].append(msg)

        results["forms_extracted"] = 1  # one form per document
        return results

    def _extract_first_page_metadata(self, first_page_text: str) -> dict[str, Any]:
        """Parse form number, edition date, name, and form type from page 1.

        Args:
            first_page_text: Raw text from the first page of the form PDF.

        Returns:
            Dict with form_number, form_edition_date, form_name, form_type,
            and is_manuscript keys.
        """
        form_number: str | None = None
        form_number_match = _FORM_NUMBER_RE.search(first_page_text)
        if form_number_match:
            form_number = form_number_match.group(1).strip()

        edition_date: str | None = None
        edition_match = _EDITION_DATE_RE.search(first_page_text)
        if edition_match:
            edition_date = edition_match.group(1).strip()

        # Attempt to derive form name from first non-empty line after the form
        # number, falling back to the first substantive line of the page.
        form_name: str | None = None
        lines = [l.strip() for l in first_page_text.splitlines() if l.strip()]
        for line in lines[:10]:
            if len(line) > 10 and not re.search(r"^\d", line):
                form_name = line[:500]
                break

        form_type = _detect_form_type(first_page_text, form_name)

        # Manuscript detection — absence of ISO-style form number suggests
        # carrier-proprietary manuscript form.
        is_manuscript = form_number is None or not re.search(
            r"^[A-Z]{2,4}\s*\d{4,6}", form_number or ""
        )

        return {
            "form_number": form_number,
            "form_edition_date": edition_date,
            "form_name": form_name,
            "form_type": form_type,
            "is_manuscript": is_manuscript,
        }

    async def _extract_provisions(self, form_text: str) -> list[dict[str, Any]]:
        """Send form text to Claude and parse the returned provision list.

        Args:
            form_text: Truncated full-document text.

        Returns:
            List of provision dicts matching the hermes_form_provisions schema.
        """
        prompt = _PROVISION_EXTRACTION_PROMPT.format(form_text=form_text)

        response = await self._client.messages.create(
            model="claude-opus-4-6",
            max_tokens=4096,
            messages=[{"role": "user", "content": prompt}],
        )
        if response.usage:
            self._ai_tokens_used += (
                response.usage.input_tokens + response.usage.output_tokens
            )

        raw_text = response.content[0].text.strip()

        # Strip markdown fences if present.
        if raw_text.startswith("```"):
            raw_text = re.sub(r"^```(?:json)?\s*", "", raw_text)
            raw_text = re.sub(r"\s*```$", "", raw_text)

        try:
            parsed = json.loads(raw_text)
            if not isinstance(parsed, list):
                self.logger.warning(
                    "Provision extraction returned non-list; wrapping in list"
                )
                parsed = [parsed] if isinstance(parsed, dict) else []
        except json.JSONDecodeError as exc:
            self.logger.warning(
                "Could not parse provision JSON: %s — preview: %.200s",
                exc,
                raw_text,
            )
            return []

        # Score and validate each provision.
        validated: list[dict[str, Any]] = []
        for item in parsed:
            if not isinstance(item, dict):
                continue
            score = self._confidence_scorer.score_form_extraction(item)
            self.confidence_tracker.record(score)
            item["confidence"] = score
            validated.append(item)

        return validated

    # ── Storage ────────────────────────────────────────────────────────────

    async def _store_results(
        self, document_id: UUID, results: dict[str, Any]
    ) -> None:
        """Persist form metadata and provisions to the database.

        Inserts into:
          - hermes_policy_forms (one row per document)
          - hermes_form_provisions (one row per extracted provision)

        Args:
            document_id: UUID of the source ``hermes_filing_documents`` row.
            results: Dict returned by ``_extract_content``.
        """
        try:
            from hermes.db import get_connection  # type: ignore[import]

            async with get_connection() as conn:
                context = await self._resolve_filing_context(conn, document_id)
                meta = results.get("form_metadata", {})

                form_confidence = self._confidence_scorer.score_form_extraction(meta)
                self.confidence_tracker.record(form_confidence)

                form_row = await conn.fetchrow(
                    """
                    INSERT INTO hermes_policy_forms (
                        filing_id, carrier_id, state, line,
                        form_number, form_name, form_edition_date,
                        form_type, is_manuscript,
                        source_document_id, effective_date, confidence
                    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
                    ON CONFLICT DO NOTHING
                    RETURNING id
                    """,
                    str(context["filing_id"]),
                    str(context["carrier_id"]),
                    context["state"],
                    context["line"],
                    meta.get("form_number", "UNKNOWN"),
                    meta.get("form_name"),
                    meta.get("form_edition_date"),
                    meta.get("form_type", "policy"),
                    bool(meta.get("is_manuscript", False)),
                    str(document_id),
                    context["effective_date"],
                    form_confidence,
                )

                if not form_row:
                    # ON CONFLICT path — fetch existing form id.
                    form_row = await conn.fetchrow(
                        """
                        SELECT id FROM hermes_policy_forms
                        WHERE source_document_id = $1
                        LIMIT 1
                        """,
                        str(document_id),
                    )

                if not form_row:
                    self.logger.warning(
                        "Could not create or retrieve policy form row for "
                        "document_id=%s",
                        document_id,
                    )
                    return

                form_id = form_row["id"]

                # ── provisions ─────────────────────────────────────────
                for provision in results.get("provisions", []):
                    prov_confidence = float(provision.get("confidence", 0.75))
                    await conn.execute(
                        """
                        INSERT INTO hermes_form_provisions (
                            form_id, provision_type, provision_key,
                            provision_text_summary, provision_text_full,
                            section_reference, is_coverage_broadening,
                            is_coverage_restricting, iso_comparison_notes,
                            confidence
                        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
                        """,
                        str(form_id),
                        provision.get("provision_type", "condition"),
                        provision.get("provision_key"),
                        provision.get("provision_text_summary", ""),
                        provision.get("provision_text_full"),
                        provision.get("section_reference"),
                        provision.get("is_coverage_broadening"),
                        provision.get("is_coverage_restricting"),
                        provision.get("iso_comparison_notes"),
                        prov_confidence,
                    )
                    if prov_confidence < 0.7:
                        self._queue_low_confidence(
                            document_id,
                            "provision_type",
                            provision.get("provision_type"),
                            prov_confidence,
                            str(provision.get("provision_text_summary", ""))[:500],
                        )

        except Exception as exc:  # noqa: BLE001
            self.logger.error("Failed to store form results: %s", exc)
            raise

    async def _resolve_filing_context(
        self, conn: Any, document_id: UUID
    ) -> dict[str, Any]:
        """Fetch filing metadata needed for FK columns."""
        row = await conn.fetchrow(
            """
            SELECT f.id AS filing_id, f.carrier_id, f.state,
                   f.line_of_business AS line, f.effective_date
            FROM hermes_filing_documents fd
            JOIN hermes_filings f ON f.id = fd.filing_id
            WHERE fd.id = $1
            """,
            str(document_id),
        )
        if not row:
            raise ValueError(
                f"No filing context found for document_id={document_id}"
            )
        return dict(row)
