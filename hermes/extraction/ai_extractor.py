"""AI Extractor — Claude-powered extraction for unstructured PDF content.

Wraps the Anthropic AsyncAnthropic client with:
  - Structured prompts for rate tables, rule text, form provisions, and
    document classification.
  - Automatic retry with exponential back-off via tenacity.
  - Token usage tracking for cost monitoring.

All methods are ``async`` and safe to call from asyncio event loops.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any

from anthropic import AsyncAnthropic, APIError, RateLimitError, APIConnectionError
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
    before_sleep_log,
)

from hermes.config import settings

logger = logging.getLogger(__name__)

# ── Retry configuration ────────────────────────────────────────────────────

_RETRY_EXCEPTIONS = (RateLimitError, APIConnectionError)

_retry_policy = dict(
    retry=retry_if_exception_type(_RETRY_EXCEPTIONS),
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=2, min=2, max=60),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)

# ── Cost constants (USD per million tokens, as of Claude Opus 4.6) ─────────

_INPUT_COST_PER_M_TOKENS = 15.00
_OUTPUT_COST_PER_M_TOKENS = 75.00

# ── Prompts ────────────────────────────────────────────────────────────────

_RATE_EXTRACTION_PROMPT = """\
You are an expert commercial insurance analyst. Extract all rate table data \
from the following text taken from an insurance rate exhibit filing.

For each rate or factor found, return a JSON object in this array:

[
  {{
    "record_type": "<base_rate|factor|ilf|deductible|territory_factor>",
    "class_code": "<string or null>",
    "class_description": "<string or null>",
    "territory": "<string or null>",
    "base_rate": <number or null>,
    "rate_per_unit": "<string description e.g. per $100 payroll, or null>",
    "minimum_premium": <number or null>,
    "factor_type": "<territory|ilf|deductible|schedule_credit|other or null>",
    "factor_key": "<lookup key e.g. territory code, limit amount, or null>",
    "factor_value": <number or null>,
    "factor_description": "<string or null>",
    "confidence": <0.0–1.0>
  }}
]

Rules:
- Use null (not "null" string) for missing numeric fields.
- Only include rows where at least one numeric value is present.
- Return ONLY a valid JSON array, no markdown, no explanation.

TEXT:
\"\"\"
{text}
\"\"\"
"""

_RULE_EXTRACTION_PROMPT = """\
You are an expert commercial insurance analyst specialising in SERFF regulatory \
filings. Your task is to extract structured underwriting rules and eligibility \
criteria from the following excerpt of an insurance rule manual.

Return ONLY a valid JSON object with this exact structure (no markdown fences):

{{
  "rules": [
    {{
      "rule_type": "<eligibility|rating|territory|classification|general>",
      "rule_category": "<string>",
      "rule_text": "<verbatim or paraphrased rule text>",
      "section_reference": "<page/section reference if visible, else null>",
      "confidence": <0.0–1.0>
    }}
  ],
  "eligibility_criteria": [
    {{
      "criterion_type": "<eligible_class|ineligible_class|min_years_business|\
max_loss_ratio|territory_restriction|construction_type|min_employees|\
max_employees|revenue_range|operations_restriction>",
      "criterion_value": "<string value>",
      "criterion_operator": "<equals|gt|lt|gte|lte|in|not_in|between|contains|not_contains>",
      "criterion_unit": "<years|percent|dollars|null>",
      "is_hard_rule": <true|false>,
      "description": "<human-readable explanation>",
      "confidence": <0.0–1.0>
    }}
  ],
  "coverage_options": [],
  "credits_surcharges": [],
  "exclusions": []
}}

TEXT TO EXTRACT FROM:
\"\"\"
{text}
\"\"\"
"""

_FORM_PROVISION_PROMPT = """\
You are an expert commercial insurance analyst. Extract all key provisions \
from the following policy form text.

Return a JSON array where each element is:

{{
  "provision_type": "<coverage_grant|exclusion|condition|definition>",
  "provision_key": "<short snake_case identifier>",
  "provision_text_summary": "<1-2 sentence AI summary>",
  "provision_text_full": "<verbatim text, max 800 chars>",
  "section_reference": "<section label or null>",
  "is_coverage_broadening": <true|false|null>,
  "is_coverage_restricting": <true|false|null>,
  "iso_comparison_notes": "<how this differs from ISO, or null>",
  "confidence": <0.0–1.0>
}}

Return ONLY a valid JSON array, no markdown fences.

FORM TEXT:
\"\"\"
{text}
\"\"\"
"""

_CLASSIFY_PROMPT = """\
Classify the following insurance document text into one of:
rate_exhibit, actuarial_memo, rule_manual, policy_form, endorsement,
application, schedule, supporting.

Reply with ONLY the document type string.

TEXT:
\"\"\"
{text}
\"\"\"
"""

_VALID_DOC_TYPES = frozenset(
    [
        "rate_exhibit",
        "actuarial_memo",
        "rule_manual",
        "policy_form",
        "endorsement",
        "application",
        "schedule",
        "supporting",
    ]
)


def _strip_fences(text: str) -> str:
    """Remove markdown code fences from a model response."""
    text = text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
    return text.strip()


class AIExtractor:
    """Claude AI-powered extractor for rate tables, rules, forms, and classification.

    Uses ``claude-opus-4-6`` for all extraction calls and tracks cumulative
    token usage for cost reporting.  All public methods implement tenacity
    retry logic to handle rate limits and transient API errors.
    """

    def __init__(self) -> None:
        self._client = AsyncAnthropic(api_key=settings.anthropic_api_key)
        self.total_input_tokens: int = 0
        self.total_output_tokens: int = 0
        self.total_calls: int = 0

    # ── Rate extraction ────────────────────────────────────────────────────

    @retry(**_retry_policy)
    async def extract_rates_from_text(self, text: str) -> list[dict[str, Any]]:
        """Send rate exhibit text to Claude for structured rate extraction.

        Args:
            text: Raw text from one or more pages of a rate exhibit PDF.

        Returns:
            List of rate/factor record dicts.  Each dict includes a
            ``record_type`` key to disambiguate base rates from factors.
        """
        prompt = _RATE_EXTRACTION_PROMPT.format(text=text[:10_000])

        response = await self._client.messages.create(
            model="claude-opus-4-6",
            max_tokens=4096,
            messages=[{"role": "user", "content": prompt}],
        )
        self._track_usage(response)

        raw = _strip_fences(response.content[0].text)
        try:
            result = json.loads(raw)
            return result if isinstance(result, list) else []
        except json.JSONDecodeError as exc:
            logger.warning("Rate extraction JSON parse error: %s", exc)
            return []

    # ── Rule extraction ────────────────────────────────────────────────────

    @retry(**_retry_policy)
    async def extract_rules_from_text(self, text: str) -> list[dict[str, Any]]:
        """Send rule manual text to Claude for structured rule extraction.

        Uses the spec section 6.3 prompt structure that requests rules,
        eligibility_criteria, coverage_options, credits_surcharges, and
        exclusions in a single call.

        Args:
            text: Section text from a rule manual PDF.

        Returns:
            Flat list of extracted rule dicts (merged from all categories).
        """
        prompt = _RULE_EXTRACTION_PROMPT.format(text=text[:10_000])

        response = await self._client.messages.create(
            model="claude-opus-4-6",
            max_tokens=4096,
            messages=[{"role": "user", "content": prompt}],
        )
        self._track_usage(response)

        raw = _strip_fences(response.content[0].text)
        try:
            parsed = json.loads(raw)
            if not isinstance(parsed, dict):
                return []
            # Flatten all categories into a single list with a category tag.
            results: list[dict[str, Any]] = []
            for category in (
                "rules",
                "eligibility_criteria",
                "coverage_options",
                "credits_surcharges",
                "exclusions",
            ):
                for item in parsed.get(category, []):
                    if isinstance(item, dict):
                        item["_category"] = category
                        results.append(item)
            return results
        except json.JSONDecodeError as exc:
            logger.warning("Rule extraction JSON parse error: %s", exc)
            return []

    # ── Form provision extraction ──────────────────────────────────────────

    @retry(**_retry_policy)
    async def extract_form_provisions(self, text: str) -> list[dict[str, Any]]:
        """Extract policy form provisions from text using Claude AI.

        Args:
            text: Full or truncated text from a policy form PDF.

        Returns:
            List of provision dicts with type, summary, full text, and
            coverage classification.
        """
        prompt = _FORM_PROVISION_PROMPT.format(text=text[:12_000])

        response = await self._client.messages.create(
            model="claude-opus-4-6",
            max_tokens=4096,
            messages=[{"role": "user", "content": prompt}],
        )
        self._track_usage(response)

        raw = _strip_fences(response.content[0].text)
        try:
            result = json.loads(raw)
            return result if isinstance(result, list) else []
        except json.JSONDecodeError as exc:
            logger.warning("Form provision JSON parse error: %s", exc)
            return []

    # ── Document classification ────────────────────────────────────────────

    @retry(**_retry_policy)
    async def classify_document(self, first_page_text: str) -> str:
        """Classify a document by its first-page text.

        Args:
            first_page_text: Text from the first page of the PDF
                             (automatically truncated to 3000 characters).

        Returns:
            One of the valid ``hermes_filing_documents.document_type`` values.
            Falls back to ``"supporting"`` if the model response is ambiguous.
        """
        prompt = _CLASSIFY_PROMPT.format(text=first_page_text[:3_000])

        response = await self._client.messages.create(
            model="claude-opus-4-6",
            max_tokens=20,
            messages=[{"role": "user", "content": prompt}],
        )
        self._track_usage(response)

        raw = response.content[0].text.strip().lower()
        for doc_type in _VALID_DOC_TYPES:
            if doc_type in raw:
                return doc_type

        logger.warning(
            "Claude returned unrecognised document type '%s'; defaulting to 'supporting'",
            raw,
        )
        return "supporting"

    # ── Usage tracking ─────────────────────────────────────────────────────

    def _track_usage(self, response: Any) -> None:
        """Accumulate token counts and estimated USD cost from a response.

        Args:
            response: An Anthropic ``Message`` response object.
        """
        self.total_calls += 1
        if not response.usage:
            return

        input_tokens: int = getattr(response.usage, "input_tokens", 0)
        output_tokens: int = getattr(response.usage, "output_tokens", 0)
        self.total_input_tokens += input_tokens
        self.total_output_tokens += output_tokens

        cost = (
            input_tokens * _INPUT_COST_PER_M_TOKENS / 1_000_000
            + output_tokens * _OUTPUT_COST_PER_M_TOKENS / 1_000_000
        )
        logger.debug(
            "AI call #%d: in=%d out=%d tokens | est. cost=$%.4f",
            self.total_calls,
            input_tokens,
            output_tokens,
            cost,
        )

    @property
    def total_cost_usd(self) -> float:
        """Estimated total USD cost across all calls made by this instance."""
        return (
            self.total_input_tokens * _INPUT_COST_PER_M_TOKENS / 1_000_000
            + self.total_output_tokens * _OUTPUT_COST_PER_M_TOKENS / 1_000_000
        )
