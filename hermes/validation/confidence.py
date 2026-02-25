"""Confidence Scorer — assigns confidence scores to extracted data.

Scores are in the range [0.0, 1.0] and are interpreted as:
  > 0.90  — High confidence: accept automatically.
  0.70–0.90 — Medium confidence: accept with a note.
  < 0.70  — Needs review: queue for human review via hermes_parse_review_queue.

Scores combine rule-based heuristics (completeness, numeric validity, key
field presence) into a single composite float.
"""

from __future__ import annotations

import logging
import re
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from hermes.extraction.table_extractor import ExtractedTable

logger = logging.getLogger(__name__)

# ── Threshold constants ────────────────────────────────────────────────────

HIGH_CONFIDENCE = 0.90
MEDIUM_CONFIDENCE = 0.70

# Regex to detect whether a string is primarily numeric.
_NUMERIC_RE = re.compile(r"^[\d,.$%\s\-]+$")


class ConfidenceScorer:
    """Computes confidence scores for table, rule, and form extractions.

    Each ``score_*`` method returns a float in [0.0, 1.0].  The scoring is
    additive: component scores are weighted and summed to produce the final
    value, which is clipped to [0.0, 1.0].
    """

    # ── Table extraction confidence ────────────────────────────────────────

    def score_table_extraction(self, table: "ExtractedTable") -> float:
        """Score the quality of a table extracted from a PDF.

        Heuristics:
          - Header clarity: non-empty, non-numeric header strings.
          - Cell completeness: fraction of non-empty cells.
          - Numeric consistency: at least one column that looks like numbers.
          - Minimum row count: single-row tables are likely false positives.

        Args:
            table: An ``ExtractedTable`` instance.

        Returns:
            Confidence score in [0.0, 1.0].
        """
        score = 0.0

        # ── Header quality (weight 0.25) ───────────────────────────────────
        if table.headers:
            non_empty_headers = [h for h in table.headers if h and h.strip()]
            descriptive_headers = [
                h for h in non_empty_headers
                if not _NUMERIC_RE.match(h.strip())
            ]
            header_score = len(descriptive_headers) / max(len(table.headers), 1)
            score += 0.25 * header_score
        else:
            score += 0.0  # No headers is a strong signal of poor extraction.

        # ── Cell completeness (weight 0.30) ───────────────────────────────
        if table.rows:
            total_cells = sum(len(row) for row in table.rows)
            non_empty_cells = sum(
                1
                for row in table.rows
                for cell in row
                if cell and str(cell).strip()
            )
            fill_rate = non_empty_cells / max(total_cells, 1)
            score += 0.30 * fill_rate
        else:
            # Empty rows → very low score.
            score += 0.0

        # ── Numeric consistency (weight 0.25) ─────────────────────────────
        # At least one column should contain numeric-looking values.
        numeric_columns = 0
        if table.rows and table.headers:
            for col_idx in range(len(table.headers)):
                col_values = [
                    row[col_idx]
                    for row in table.rows
                    if col_idx < len(row)
                ]
                numeric_count = sum(
                    1 for v in col_values
                    if v and _NUMERIC_RE.match(str(v).strip())
                )
                if numeric_count / max(len(col_values), 1) > 0.5:
                    numeric_columns += 1
            numeric_ratio = min(numeric_columns / max(len(table.headers), 1), 1.0)
            score += 0.25 * numeric_ratio

        # ── Row count (weight 0.20) ────────────────────────────────────────
        row_count = len(table.rows)
        if row_count >= 10:
            score += 0.20
        elif row_count >= 3:
            score += 0.10
        elif row_count >= 1:
            score += 0.05
        # 0 rows → no addition.

        final = round(min(max(score, 0.0), 1.0), 4)
        self._log_score("table", final, {"rows": row_count, "cols": len(table.headers)})
        return final

    # ── Rule extraction confidence ─────────────────────────────────────────

    def score_rule_extraction(self, rule: dict[str, Any]) -> float:
        """Score the completeness of a single extracted rule record.

        Heuristics:
          - Presence of ``rule_type`` (or ``criterion_type``): 0.30
          - Non-empty text field (``rule_text``, ``criterion_value``, etc.): 0.35
          - Presence of ``section_reference`` or ``description``: 0.15
          - Explicit ``confidence`` field from the AI model itself: 0.20

        Args:
            rule: A single rule or eligibility criterion dict from AI extraction.

        Returns:
            Composite confidence score in [0.0, 1.0].
        """
        score = 0.0

        # Type field.
        type_key = rule.get("rule_type") or rule.get("criterion_type") or rule.get("credit_type")
        if type_key:
            score += 0.30

        # Primary text field.
        text_value = (
            rule.get("rule_text")
            or rule.get("criterion_value")
            or rule.get("exclusion_text")
            or rule.get("description")
            or rule.get("provision_text_summary")
        )
        if text_value and len(str(text_value).strip()) > 10:
            score += 0.35

        # Contextual / reference field.
        context = rule.get("section_reference") or rule.get("description")
        if context:
            score += 0.15

        # AI's own confidence estimate.
        ai_confidence = rule.get("confidence")
        if ai_confidence is not None:
            try:
                score += 0.20 * float(ai_confidence)
            except (TypeError, ValueError):
                pass

        final = round(min(max(score, 0.0), 1.0), 4)
        self._log_score("rule", final, {"type": type_key})
        return final

    # ── Form extraction confidence ─────────────────────────────────────────

    def score_form_extraction(self, form: dict[str, Any]) -> float:
        """Score the quality of a form metadata or provision extraction.

        Heuristics:
          - ``form_number`` present: 0.25
          - ``form_edition_date`` present: 0.20
          - ``form_type`` or ``provision_type`` present: 0.20
          - At least one substantive text field present: 0.20
          - AI ``confidence`` field: 0.15

        Args:
            form: A form metadata dict or provision dict from AI extraction.

        Returns:
            Composite confidence score in [0.0, 1.0].
        """
        score = 0.0

        # Form number (most reliable identifier).
        if form.get("form_number"):
            score += 0.25

        # Edition date.
        if form.get("form_edition_date"):
            score += 0.20

        # Type classification.
        type_val = form.get("form_type") or form.get("provision_type")
        if type_val:
            score += 0.20

        # Substantive text.
        text_val = (
            form.get("form_name")
            or form.get("provision_text_summary")
            or form.get("provision_text_full")
        )
        if text_val and len(str(text_val).strip()) > 10:
            score += 0.20

        # AI confidence.
        ai_confidence = form.get("confidence")
        if ai_confidence is not None:
            try:
                score += 0.15 * float(ai_confidence)
            except (TypeError, ValueError):
                pass

        final = round(min(max(score, 0.0), 1.0), 4)
        self._log_score("form", final, {"form_number": form.get("form_number")})
        return final

    # ── Helpers ────────────────────────────────────────────────────────────

    @staticmethod
    def _log_score(
        extraction_type: str, score: float, context: dict[str, Any]
    ) -> None:
        """Log a confidence score at DEBUG level with contextual info."""
        tier = (
            "HIGH"
            if score > HIGH_CONFIDENCE
            else ("MEDIUM" if score >= MEDIUM_CONFIDENCE else "LOW—needs review")
        )
        logger.debug(
            "Confidence[%s] score=%.4f tier=%s context=%s",
            extraction_type,
            score,
            tier,
            context,
        )

    @staticmethod
    def tier(score: float) -> str:
        """Return the human-readable confidence tier for a score.

        Args:
            score: A confidence float in [0.0, 1.0].

        Returns:
            ``"high"``, ``"medium"``, or ``"low"``.
        """
        if score > HIGH_CONFIDENCE:
            return "high"
        if score >= MEDIUM_CONFIDENCE:
            return "medium"
        return "low"
