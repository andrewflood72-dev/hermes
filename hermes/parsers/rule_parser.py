"""Rule Parser — extracts underwriting rules, eligibility criteria,
coverage options, credits/surcharges, and exclusions from rule manual PDFs.

Strategy:
  1. Extract full text with PyMuPDF.
  2. Chunk the text into logical sections (by detected headings).
  3. Send each chunk to Claude (claude-opus-4-6) with a structured JSON
     extraction prompt aligned to the schema defined in spec section 6.3.
  4. Persist results to hermes_underwriting_rules, hermes_eligibility_criteria,
     hermes_coverage_options, hermes_credits_surcharges, hermes_exclusions.
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

# ── Claude prompt (spec section 6.3) ──────────────────────────────────────

RULE_EXTRACTION_PROMPT = """\
You are an expert commercial insurance analyst specialising in SERFF regulatory \
filings. Your task is to extract structured underwriting rules and eligibility \
criteria from the following excerpt of an insurance rule manual.

Return ONLY a valid JSON object with this exact structure (no markdown fences):

{
  "rules": [
    {
      "rule_type": "<eligibility|rating|territory|classification|general>",
      "rule_category": "<string — e.g. eligible_class, ineligible_class, \
min_years_business, max_loss_ratio, territory_restriction, construction_type>",
      "rule_text": "<verbatim or paraphrased rule text>",
      "section_reference": "<page/section reference if visible, else null>",
      "confidence": <0.0–1.0>
    }
  ],
  "eligibility_criteria": [
    {
      "criterion_type": "<eligible_class|ineligible_class|min_years_business|\
max_loss_ratio|territory_restriction|construction_type|min_employees|\
max_employees|revenue_range|operations_restriction>",
      "criterion_value": "<string value or JSON array for IN/NOT_IN>",
      "criterion_operator": "<equals|gt|lt|gte|lte|in|not_in|between|\
contains|not_contains>",
      "criterion_unit": "<years|percent|dollars|null>",
      "is_hard_rule": <true|false>,
      "description": "<human-readable explanation>",
      "confidence": <0.0–1.0>
    }
  ],
  "coverage_options": [
    {
      "coverage_type": "<occurrence|claims-made|aggregate|per-project|blanket>",
      "limit_min": <number or null>,
      "limit_max": <number or null>,
      "default_limit": <number or null>,
      "deductible_options": [<list of numbers>],
      "confidence": <0.0–1.0>
    }
  ],
  "credits_surcharges": [
    {
      "credit_type": "<protective_safeguard|loss_free|sprinkler|alarm|\
new_venture|claims_free|safety_program|fleet_size|experience_mod|\
schedule_rating>",
      "credit_or_surcharge": "<credit|surcharge>",
      "range_min": <number or null>,
      "range_max": <number or null>,
      "description": "<string>",
      "confidence": <0.0–1.0>
    }
  ],
  "exclusions": [
    {
      "exclusion_type": "<standard|non_standard|endorsement|absolute>",
      "exclusion_text": "<verbatim exclusion language>",
      "exclusion_summary": "<one-sentence summary>",
      "exclusion_category": "<pollution|cyber|epl|professional|terrorism|\
mold|lead|asbestos|nuclear|communicable_disease|other>",
      "is_optional": <true|false>,
      "confidence": <0.0–1.0>
    }
  ]
}

Rules:
- Use null (not "null") for missing numeric fields.
- Omit keys entirely only if completely irrelevant to the section.
- Set confidence based on how unambiguously the rule is stated.
- If a section contains no relevant items for a category, return an empty array.

TEXT TO EXTRACT FROM:
\"\"\"
{section_text}
\"\"\"
"""

# ── Section chunking ───────────────────────────────────────────────────────

# Headings that typically signal a new rule section in insurance manuals.
_SECTION_HEADING_RE = re.compile(
    r"^\s{0,4}(?:SECTION|RULE|ARTICLE|PART|CHAPTER|\d+[\.\)]\s+[A-Z])"
    r"|^[A-Z][A-Z\s]{4,}(?:RULES?|CRITERIA|ELIGIBILITY|EXCLUSIONS?|"
    r"CREDITS?|SURCHARGES?|COVERAGE)\s*$",
    re.MULTILINE,
)

_MAX_CHUNK_CHARS = 8_000  # stay well inside Claude's context window per call


def _chunk_text(full_text: str) -> list[str]:
    """Split rule manual text into sections for AI extraction.

    Attempts heading-based splitting first; falls back to fixed-size chunks
    if no headings are detected.

    Args:
        full_text: Concatenated full-document text.

    Returns:
        List of text chunks, each within ``_MAX_CHUNK_CHARS`` characters.
    """
    matches = list(_SECTION_HEADING_RE.finditer(full_text))
    if len(matches) < 2:
        # No clear headings — split by character count.
        return [
            full_text[i: i + _MAX_CHUNK_CHARS]
            for i in range(0, len(full_text), _MAX_CHUNK_CHARS)
        ]

    chunks: list[str] = []
    boundaries = [m.start() for m in matches] + [len(full_text)]
    for start, end in zip(boundaries, boundaries[1:]):
        section = full_text[start:end].strip()
        if not section:
            continue
        # Sub-chunk if the section is too large.
        for i in range(0, len(section), _MAX_CHUNK_CHARS):
            chunks.append(section[i: i + _MAX_CHUNK_CHARS])
    return chunks


class RuleParser(BaseParser):
    """Extracts underwriting rules and eligibility criteria from rule manuals.

    Sends each text chunk to Claude with the spec section 6.3 prompt and
    aggregates results across all chunks into the five rule tables.
    """

    parser_type = "rule"
    parser_version = "1.0.0"

    def __init__(self) -> None:
        super().__init__()
        self._text_extractor = TextExtractor()
        self._confidence_scorer = ConfidenceScorer()
        self._client = AsyncAnthropic(api_key=settings.anthropic_api_key)

    # ── Extraction ─────────────────────────────────────────────────────────

    async def _extract_content(self, file_path: str) -> dict[str, Any]:
        """Chunk the rule manual and extract structured data via Claude.

        Returns:
            Dict with keys: rules, eligibility_criteria, coverage_options,
            credits_surcharges, exclusions, rules_extracted, warnings.
        """
        results: dict[str, Any] = {
            "rules": [],
            "eligibility_criteria": [],
            "coverage_options": [],
            "credits_surcharges": [],
            "exclusions": [],
            "rules_extracted": 0,
            "tables_extracted": 0,
            "forms_extracted": 0,
            "factors_extracted": 0,
            "warnings": [],
        }

        pages = self._text_extractor.extract_text(file_path)
        full_text = "\n\n".join(p.text for p in pages)

        if not full_text.strip():
            results["warnings"].append("No text could be extracted from PDF")
            return results

        chunks = _chunk_text(full_text)
        self.logger.info(
            "Rule manual split into %d chunks for AI extraction", len(chunks)
        )

        for idx, chunk in enumerate(chunks, start=1):
            try:
                extracted = await self._extract_chunk(chunk, idx)
                self._merge_chunk_results(extracted, results)
            except Exception as exc:  # noqa: BLE001
                msg = f"Chunk {idx} extraction failed: {exc}"
                self.logger.warning(msg)
                results["warnings"].append(msg)

        results["rules_extracted"] = len(results["rules"])
        return results

    async def _extract_chunk(
        self, section_text: str, chunk_index: int
    ) -> dict[str, Any]:
        """Send a single text chunk to Claude and parse the JSON response.

        Args:
            section_text: The text to analyse.
            chunk_index: 1-based index used in logging.

        Returns:
            Parsed dict matching the RULE_EXTRACTION_PROMPT schema.
        """
        prompt = RULE_EXTRACTION_PROMPT.format(section_text=section_text)

        response = await self._client.messages.create(
            model="claude-opus-4-6",
            max_tokens=4096,
            messages=[{"role": "user", "content": prompt}],
        )
        self._ai_calls_made += 1
        if response.usage:
            self._ai_tokens_used += (
                response.usage.input_tokens + response.usage.output_tokens
            )

        raw_text = response.content[0].text.strip()

        # Strip accidental markdown fences.
        if raw_text.startswith("```"):
            raw_text = re.sub(r"^```(?:json)?\s*", "", raw_text)
            raw_text = re.sub(r"\s*```$", "", raw_text)

        try:
            return json.loads(raw_text)
        except json.JSONDecodeError as exc:
            self.logger.warning(
                "Chunk %d JSON parse error: %s — response preview: %.200s",
                chunk_index,
                exc,
                raw_text,
            )
            return {}

    def _merge_chunk_results(
        self, chunk: dict[str, Any], results: dict[str, Any]
    ) -> None:
        """Merge a single chunk's extracted data into the aggregate results."""
        for key in (
            "rules",
            "eligibility_criteria",
            "coverage_options",
            "credits_surcharges",
            "exclusions",
        ):
            items = chunk.get(key, [])
            if not isinstance(items, list):
                continue
            for item in items:
                if not isinstance(item, dict):
                    continue
                confidence = float(item.get("confidence", 0.75))
                self.confidence_tracker.record(confidence)
                score = self._confidence_scorer.score_rule_extraction(item)
                item["confidence"] = score
                results[key].append(item)

    # ── Storage ────────────────────────────────────────────────────────────

    async def _store_results(
        self, document_id: UUID, results: dict[str, Any]
    ) -> None:
        """Persist extracted rules to the five underwriting rule tables.

        Inserts into:
          - hermes_underwriting_rules
          - hermes_eligibility_criteria
          - hermes_coverage_options
          - hermes_credits_surcharges
          - hermes_exclusions

        Args:
            document_id: UUID of the source ``hermes_filing_documents`` row.
            results: Dict returned by ``_extract_content``.
        """
        try:
            from hermes.db import get_connection  # type: ignore[import]

            async with get_connection() as conn:
                context = await self._resolve_filing_context(conn, document_id)

                # ── underwriting rules ─────────────────────────────────
                for rule in results.get("rules", []):
                    confidence = float(rule.get("confidence", 0.75))
                    rule_row = await conn.fetchrow(
                        """
                        INSERT INTO hermes_underwriting_rules (
                            filing_id, carrier_id, state, line,
                            rule_type, rule_category, rule_text,
                            rule_structured, section_reference,
                            effective_date, source_document_id, confidence
                        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9,$10,$11,$12)
                        RETURNING id
                        """,
                        str(context["filing_id"]),
                        str(context["carrier_id"]),
                        context["state"],
                        context["line"],
                        rule.get("rule_type", "general"),
                        rule.get("rule_category"),
                        rule.get("rule_text", ""),
                        json.dumps(rule),
                        rule.get("section_reference"),
                        context["effective_date"],
                        str(document_id),
                        confidence,
                    )
                    rule_id = rule_row["id"]

                    # ── eligibility criteria linked to this rule ───────
                    for criterion in results.get("eligibility_criteria", []):
                        crit_confidence = float(criterion.get("confidence", 0.75))
                        await conn.execute(
                            """
                            INSERT INTO hermes_eligibility_criteria (
                                rule_id, criterion_type, criterion_value,
                                criterion_operator, criterion_unit,
                                is_hard_rule, description, confidence
                            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                            ON CONFLICT DO NOTHING
                            """,
                            str(rule_id),
                            criterion.get("criterion_type", "eligible_class"),
                            str(criterion.get("criterion_value", "")),
                            criterion.get("criterion_operator", "equals"),
                            criterion.get("criterion_unit"),
                            bool(criterion.get("is_hard_rule", True)),
                            criterion.get("description"),
                            crit_confidence,
                        )
                        if crit_confidence < 0.7:
                            self._queue_low_confidence(
                                document_id,
                                "criterion_value",
                                criterion.get("criterion_value"),
                                crit_confidence,
                                str(criterion.get("description", "")),
                            )

                # ── coverage options ───────────────────────────────────
                for cov in results.get("coverage_options", []):
                    await conn.execute(
                        """
                        INSERT INTO hermes_coverage_options (
                            filing_id, carrier_id, state, line,
                            coverage_type, limit_min, limit_max,
                            default_limit, deductible_options,
                            source_document_id, effective_date, confidence
                        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,$10,$11,$12)
                        ON CONFLICT DO NOTHING
                        """,
                        str(context["filing_id"]),
                        str(context["carrier_id"]),
                        context["state"],
                        context["line"],
                        cov.get("coverage_type", "occurrence"),
                        cov.get("limit_min"),
                        cov.get("limit_max"),
                        cov.get("default_limit"),
                        json.dumps(cov.get("deductible_options", [])),
                        str(document_id),
                        context["effective_date"],
                        float(cov.get("confidence", 0.75)),
                    )

                # ── credits / surcharges ───────────────────────────────
                for cs in results.get("credits_surcharges", []):
                    await conn.execute(
                        """
                        INSERT INTO hermes_credits_surcharges (
                            filing_id, carrier_id, state, line,
                            credit_type, credit_or_surcharge,
                            range_min, range_max, description,
                            effective_date, confidence
                        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
                        ON CONFLICT DO NOTHING
                        """,
                        str(context["filing_id"]),
                        str(context["carrier_id"]),
                        context["state"],
                        context["line"],
                        cs.get("credit_type", "schedule_rating"),
                        cs.get("credit_or_surcharge", "credit"),
                        cs.get("range_min"),
                        cs.get("range_max"),
                        cs.get("description"),
                        context["effective_date"],
                        float(cs.get("confidence", 0.75)),
                    )

                # ── exclusions ─────────────────────────────────────────
                for excl in results.get("exclusions", []):
                    await conn.execute(
                        """
                        INSERT INTO hermes_exclusions (
                            filing_id, carrier_id, state, line,
                            exclusion_type, exclusion_text,
                            exclusion_summary, exclusion_category,
                            is_optional, source_document_id,
                            effective_date, confidence
                        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
                        ON CONFLICT DO NOTHING
                        """,
                        str(context["filing_id"]),
                        str(context["carrier_id"]),
                        context["state"],
                        context["line"],
                        excl.get("exclusion_type", "standard"),
                        excl.get("exclusion_text", ""),
                        excl.get("exclusion_summary"),
                        excl.get("exclusion_category", "other"),
                        bool(excl.get("is_optional", False)),
                        str(document_id),
                        context["effective_date"],
                        float(excl.get("confidence", 0.75)),
                    )

        except Exception as exc:  # noqa: BLE001
            self.logger.error("Failed to store rule results: %s", exc)
            raise

    async def _resolve_filing_context(
        self, conn: Any, document_id: UUID
    ) -> dict[str, Any]:
        """Fetch filing metadata needed as FK values for rule table inserts."""
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
