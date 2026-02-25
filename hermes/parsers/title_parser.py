"""Title Insurance Rate Parser — extracts tiered rates, simultaneous issue
schedules, reissue credits, and ALTA endorsement pricing from SERFF filings.

Parsing strategy:
  1. Attempt structured table extraction with pdfplumber.
  2. Classify each table as: owner_rate, lender_rate, simultaneous,
     reissue, endorsement.
  3. If no tables found or confidence below 0.70, fall back to Claude AI
     extraction with a title-specific 10-task prompt.
  4. Store results via TitleRateCardLoader.
"""

from __future__ import annotations

import json
import logging
import re
from datetime import date
from typing import Any
from uuid import UUID

from hermes.config import settings
from hermes.extraction.ai_extractor import AIExtractor
from hermes.extraction.table_extractor import TableExtractor
from hermes.extraction.text_extractor import TextExtractor
from hermes.parsers.base_parser import BaseParser
from hermes.validation.confidence import ConfidenceScorer

logger = logging.getLogger(__name__)

# Title-specific table type classification patterns
_TITLE_TABLE_PATTERNS: dict[str, list[str]] = {
    "owner_rate": [
        "owner", "owner's policy", "owner rate", "purchase price",
        "coverage amount", "liability amount",
    ],
    "lender_rate": [
        "lender", "loan policy", "mortgagee", "loan amount",
        "lender's policy", "lender rate",
    ],
    "simultaneous": [
        "simultaneous", "simultaneous issue", "combo", "combined",
        "when issued with", "concurrent", "additional charge",
    ],
    "reissue": [
        "reissue", "refinance", "prior policy", "years since",
        "reissue credit", "short-term rate",
    ],
    "endorsement": [
        "endorsement", "alta", "endorsement code", "endorsement fee",
        "schedule of endorsements", "endorsement charge",
    ],
}

# Claude AI extraction prompt for title-specific documents
_TITLE_AI_PROMPT = """\
You are an expert in title insurance rate filings. Extract structured rate data
from this SERFF filing document.  Complete ALL applicable extraction tasks:

1. OWNER RATES: Extract tiered owner's policy rate schedule.
   Format: [{{"coverage_min": 0, "coverage_max": 100000, "rate_per_thousand": 5.75, "flat_fee": 0, "minimum_premium": 200}}]

2. LENDER RATES: Extract tiered lender's policy rate schedule.
   Same format as owner rates.

3. SIMULTANEOUS ISSUE: Extract simultaneous issue discount schedule.
   Format: [{{"loan_min": 0, "loan_max": 500000, "discount_rate_per_thousand": 2.50, "discount_pct": 0, "flat_fee": 0}}]

4. REISSUE CREDITS: Extract refinance reissue credit schedule.
   Format: [{{"years_since_min": 0, "years_since_max": 3, "credit_pct": 30}}]

5. ENDORSEMENTS: Extract ALTA endorsement pricing.
   Format: [{{"endorsement_code": "ALTA 8.1", "endorsement_name": "Environmental Protection Lien", "flat_fee": 25, "rate_per_thousand": 0, "pct_of_base": 0}}]

6. EFFECTIVE DATE: Extract the filing effective date (YYYY-MM-DD format).

7. STATE: Extract the two-letter state code this filing applies to.

8. CARRIER NAME: Extract the underwriter/carrier name.

9. IS PROMULGATED: Is this a promulgated (state-set) rate? true/false.

10. NOTES: Any relevant filing notes, conditions, or restrictions.

Return a JSON object with keys: owner_rates, lender_rates, simultaneous,
reissue_credits, endorsements, effective_date, state, carrier_name,
is_promulgated, notes.  Use null for sections not found in the document.

DOCUMENT TEXT:
\"\"\"
{document_text}
\"\"\"
"""


def _classify_title_table(headers: list[str], first_row_text: str = "") -> str:
    """Classify a table type from its column headers and content."""
    combined = " ".join(h.lower() for h in headers) + " " + first_row_text.lower()
    for table_type, keywords in _TITLE_TABLE_PATTERNS.items():
        if any(kw in combined for kw in keywords):
            return table_type
    return "owner_rate"  # safe default for title filings


def _parse_numeric(value: str) -> float | None:
    """Strip currency symbols/commas and coerce to float, or return None."""
    cleaned = re.sub(r"[,$%\s]", "", str(value))
    try:
        return float(cleaned)
    except ValueError:
        return None


class TitleParser(BaseParser):
    """Extracts title insurance rate data from SERFF filing PDFs.

    Combines pdfplumber structured extraction with Claude AI fallback
    to handle the variety of rate exhibit layouts in title filings.
    """

    parser_type = "title_rate"
    parser_version = "1.0.0"

    def __init__(self) -> None:
        super().__init__()
        self._table_extractor = TableExtractor()
        self._text_extractor = TextExtractor()
        self._ai_extractor = AIExtractor()
        self._confidence_scorer = ConfidenceScorer()

    async def _extract_content(self, file_path: str) -> dict[str, Any]:
        """Extract title rate data from a PDF file."""
        extracted: dict[str, Any] = {
            "tables_extracted": 0,
            "owner_rates": [],
            "lender_rates": [],
            "simultaneous": [],
            "reissue_credits": [],
            "endorsements": [],
            "warnings": [],
            "metadata": {},
        }

        # ── Tier 1: Structured table extraction ─────────────────────
        try:
            tables = self._table_extractor.extract_tables(file_path)
            for table in tables:
                table_type = _classify_title_table(
                    table.headers,
                    " ".join(str(c) for c in table.rows[0]) if table.rows else "",
                )
                parsed = self._parse_title_table(table_type, table)
                if parsed:
                    extracted[table_type + "s" if not table_type.endswith("s") else table_type].extend(parsed)
                    extracted["tables_extracted"] += 1
                    self.confidence_tracker.record(0.85)

        except Exception as exc:
            extracted["warnings"].append(f"Table extraction failed: {exc}")
            self.logger.warning("Table extraction failed for %s: %s", file_path, exc)

        # ── Tier 2: AI fallback if insufficient data ────────────────
        has_rates = bool(extracted["owner_rates"] or extracted["lender_rates"])
        if not has_rates or self.confidence_tracker.average < 0.70:
            try:
                ai_result = await self._ai_extract_title_data(file_path)
                if ai_result:
                    self._merge_ai_results(extracted, ai_result)
                    self._ai_calls_made += 1
            except Exception as exc:
                extracted["warnings"].append(f"AI extraction failed: {exc}")
                self.logger.warning("AI extraction failed for %s: %s", file_path, exc)

        return extracted

    def _parse_title_table(
        self, table_type: str, table: Any
    ) -> list[dict[str, Any]]:
        """Convert a raw extracted table into structured rate records."""
        records: list[dict[str, Any]] = []

        if table_type in ("owner_rate", "lender_rate"):
            for row in table.rows:
                if len(row) < 2:
                    continue
                cov_range = str(row[0])
                rate_val = _parse_numeric(str(row[-1]))
                if rate_val is None:
                    continue

                # Parse coverage range (e.g. "$0 - $100,000" or "100,001 - 500,000")
                parts = re.findall(r"[\d,]+", cov_range)
                if len(parts) >= 2:
                    cov_min = _parse_numeric(parts[0]) or 0
                    cov_max = _parse_numeric(parts[1]) or 0
                else:
                    continue

                records.append({
                    "coverage_min": cov_min,
                    "coverage_max": cov_max,
                    "rate_per_thousand": rate_val,
                    "flat_fee": 0,
                    "minimum_premium": 0,
                })

        elif table_type == "simultaneous":
            for row in table.rows:
                if len(row) < 2:
                    continue
                range_val = str(row[0])
                disc_val = _parse_numeric(str(row[-1]))
                if disc_val is None:
                    continue

                parts = re.findall(r"[\d,]+", range_val)
                if len(parts) >= 2:
                    loan_min = _parse_numeric(parts[0]) or 0
                    loan_max = _parse_numeric(parts[1]) or 0
                else:
                    continue

                records.append({
                    "loan_min": loan_min,
                    "loan_max": loan_max,
                    "discount_rate_per_thousand": disc_val,
                    "discount_pct": 0,
                    "flat_fee": 0,
                })

        elif table_type == "reissue":
            for row in table.rows:
                if len(row) < 2:
                    continue
                years_val = str(row[0])
                credit_val = _parse_numeric(str(row[-1]))
                if credit_val is None:
                    continue

                parts = re.findall(r"[\d.]+", years_val)
                if len(parts) >= 2:
                    yr_min = float(parts[0])
                    yr_max = float(parts[1])
                elif len(parts) == 1:
                    yr_min = 0
                    yr_max = float(parts[0])
                else:
                    continue

                records.append({
                    "years_since_min": yr_min,
                    "years_since_max": yr_max,
                    "credit_pct": credit_val,
                })

        elif table_type == "endorsement":
            for row in table.rows:
                if len(row) < 3:
                    continue
                code = str(row[0]).strip()
                name = str(row[1]).strip()
                fee_val = _parse_numeric(str(row[-1]))

                if not code or fee_val is None:
                    continue

                records.append({
                    "endorsement_code": code,
                    "endorsement_name": name,
                    "flat_fee": fee_val,
                    "rate_per_thousand": 0,
                    "pct_of_base": 0,
                })

        return records

    async def _ai_extract_title_data(self, file_path: str) -> dict[str, Any] | None:
        """Use Claude AI to extract title rate data from the full document text."""
        from anthropic import AsyncAnthropic

        try:
            full_text = self._text_extractor.extract_all_text(file_path)
            if not full_text or len(full_text.strip()) < 100:
                return None

            # Truncate to fit context
            doc_text = full_text[:15000]

            client = AsyncAnthropic(api_key=settings.anthropic_api_key)
            response = await client.messages.create(
                model="claude-sonnet-4-5-20250514",
                max_tokens=4096,
                messages=[{
                    "role": "user",
                    "content": _TITLE_AI_PROMPT.format(document_text=doc_text),
                }],
            )

            raw = response.content[0].text.strip()

            # Extract JSON from the response
            json_match = re.search(r"\{[\s\S]*\}", raw)
            if json_match:
                result = json.loads(json_match.group())
                self.confidence_tracker.record(0.80)
                self._ai_tokens_used += response.usage.input_tokens + response.usage.output_tokens
                return result

        except Exception as exc:
            self.logger.warning("AI title extraction failed: %s", exc)

        return None

    def _merge_ai_results(
        self, extracted: dict[str, Any], ai_result: dict[str, Any]
    ) -> None:
        """Merge AI extraction results into the extracted dict,
        preferring existing structured data when available."""
        for key in ("owner_rates", "lender_rates", "simultaneous",
                     "reissue_credits", "endorsements"):
            ai_data = ai_result.get(key)
            if ai_data and isinstance(ai_data, list) and not extracted.get(key):
                extracted[key] = ai_data

        # Metadata
        for mkey in ("effective_date", "state", "carrier_name",
                      "is_promulgated", "notes"):
            val = ai_result.get(mkey)
            if val is not None:
                extracted["metadata"][mkey] = val

    async def _store_results(
        self, document_id: UUID, results: dict[str, Any]
    ) -> None:
        """Persist extracted title rate data via TitleRateCardLoader."""
        from hermes.title.rate_card_loader import TitleRateCardLoader

        metadata = results.get("metadata", {})
        state = metadata.get("state", "XX")
        carrier_name = metadata.get("carrier_name")
        eff_date_str = metadata.get("effective_date")
        is_promulgated = metadata.get("is_promulgated", False)

        effective_date = date.today()
        if eff_date_str:
            try:
                effective_date = date.fromisoformat(eff_date_str)
            except (ValueError, TypeError):
                pass

        if not carrier_name:
            self.logger.warning(
                "No carrier name found in parsed title data for doc=%s", document_id
            )
            return

        # Look up carrier NAIC from name
        from hermes.db import async_session
        from sqlalchemy import text as sa_text

        async with async_session() as session:
            result = await session.execute(
                sa_text(
                    "SELECT naic_code FROM hermes_carriers "
                    "WHERE legal_name ILIKE :name OR legal_name ILIKE :name_pct "
                    "LIMIT 1"
                ),
                {"name": carrier_name, "name_pct": f"%{carrier_name}%"},
            )
            row = result.fetchone()

        if not row:
            self.logger.warning(
                "Carrier '%s' not found in DB for doc=%s", carrier_name, document_id
            )
            return

        carrier_naic = row[0]
        loader = TitleRateCardLoader()

        # Load owner rates
        if results.get("owner_rates"):
            try:
                await loader.load_rate_card(
                    carrier_naic=carrier_naic,
                    policy_type="owner",
                    state=state,
                    effective_date=effective_date,
                    rates=results["owner_rates"],
                    simultaneous=results.get("simultaneous"),
                    reissue_credits=results.get("reissue_credits"),
                    endorsements=results.get("endorsements"),
                    is_promulgated=is_promulgated,
                    source="serff",
                    notes=f"Parsed from document {document_id}",
                )
            except Exception as exc:
                self.logger.error("Failed to store owner rates: %s", exc)

        # Load lender rates
        if results.get("lender_rates"):
            try:
                await loader.load_rate_card(
                    carrier_naic=carrier_naic,
                    policy_type="lender",
                    state=state,
                    effective_date=effective_date,
                    rates=results["lender_rates"],
                    is_promulgated=is_promulgated,
                    source="serff",
                    notes=f"Parsed from document {document_id}",
                )
            except Exception as exc:
                self.logger.error("Failed to store lender rates: %s", exc)
