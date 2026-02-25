"""Mozart-Hermes Bridge — integrates Atlas risk scoring with Hermes carrier
matching to produce actionable placement recommendations.

The :class:`MozartHermesBridge` is the primary integration point called by the
Mozart orchestration engine when it needs a carrier placement recommendation for
a commercial lines submission.

Placement strategy selection:

- **single_carrier** — one carrier covers all requested lines within limit
  appetite; confidence ≥ 0.70.
- **split_placement** — no single carrier covers all lines at the requested
  limits; the best carrier per line is recommended.
- **excess_layering** — risk premium or limits exceed single-carrier appetite;
  primary + excess layers recommended.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field

from hermes.matching.engine import CarrierMatchResult, MatchingEngine

logger = logging.getLogger("hermes.bridge")


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------


class PlacementRecommendation(BaseModel):
    """Full placement recommendation returned to the Mozart orchestrator.

    Attributes
    ----------
    entity_name:
        Legal name of the insured entity.
    state:
        State of domicile.
    lines:
        Lines of business covered by this recommendation.
    recommended_carriers:
        Ranked carrier match results (top candidates).
    total_estimated_premium:
        Aggregated premium estimates per line; keys are line names.
    placement_strategy:
        One of ``"single_carrier"``, ``"split_placement"``,
        ``"excess_layering"``.
    confidence:
        Overall confidence in the recommendation (0.0-1.0).
    market_conditions:
        Summary of current market conditions per line.
    generated_at:
        UTC timestamp when the recommendation was generated.
    """

    entity_name: str
    state: str
    lines: list[str]
    recommended_carriers: list[CarrierMatchResult]
    total_estimated_premium: dict[str, Any] = Field(default_factory=dict)
    placement_strategy: str = Field(
        default="single_carrier",
        description="single_carrier | split_placement | excess_layering",
    )
    confidence: float = Field(default=0.0, ge=0.0, le=1.0)
    market_conditions: dict[str, Any] = Field(default_factory=dict)
    generated_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    model_config = {"arbitrary_types_allowed": True}


# ---------------------------------------------------------------------------
# MozartHermesBridge
# ---------------------------------------------------------------------------


class MozartHermesBridge:
    """Bridges the Mozart orchestration engine with Hermes carrier matching.

    Mozart supplies Atlas risk scores and submission data; this bridge merges
    them into a :class:`~hermes.api.schemas.RiskProfileInput`-compatible dict,
    runs the full matching pipeline, and returns a structured
    :class:`PlacementRecommendation`.

    Parameters
    ----------
    matching_engine:
        Optional pre-built :class:`MatchingEngine`.  When omitted, a new
        engine is instantiated.
    """

    # Maximum number of recommended carriers to surface in the response
    _MAX_RECOMMENDED = 5

    # Threshold for single-carrier strategy confidence
    _SINGLE_CARRIER_CONFIDENCE_THRESHOLD = 0.70

    # Threshold above which we flag excess layering
    _EXCESS_PREMIUM_THRESHOLD = 500_000  # USD

    def __init__(self, matching_engine: MatchingEngine | None = None) -> None:
        self._engine = matching_engine or MatchingEngine()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def match_and_place(
        self,
        atlas_risk_profile: dict,
        submission_data: dict,
    ) -> PlacementRecommendation:
        """Generate a carrier placement recommendation from Atlas + submission data.

        This is the primary entry point for the Mozart orchestrator.

        The method:

        1. Merges *atlas_risk_profile* and *submission_data* into a unified
           risk dict.
        2. Calls :meth:`MatchingEngine.match` to obtain ranked carrier results.
        3. Determines the optimal placement strategy.
        4. Fetches market condition summaries for each requested line.
        5. Returns a :class:`PlacementRecommendation`.

        Parameters
        ----------
        atlas_risk_profile:
            Risk scores and enriched attributes produced by the Atlas platform.
            Expected keys include ``entity_name``, ``naics_code``, ``state``,
            ``zip_code``, ``years_in_business``, ``annual_revenue``,
            ``employee_count``, ``loss_ratio_3yr``, ``experience_mod``,
            plus any Atlas-computed scores (``atlas_risk_score``,
            ``atlas_complexity_score``, etc.).
        submission_data:
            Submission data from the Mozart workflow.  Expected keys include
            ``coverage_lines`` (list[str]), ``requested_limits`` (dict),
            ``construction_type`` (optional str).

        Returns
        -------
        PlacementRecommendation
        """
        entity_name: str = (
            atlas_risk_profile.get("entity_name")
            or submission_data.get("entity_name")
            or "Unknown Entity"
        )
        state: str = (
            atlas_risk_profile.get("state")
            or submission_data.get("state")
            or ""
        ).upper()
        coverage_lines: list[str] = submission_data.get("coverage_lines") or []

        if not state:
            logger.error("No state provided in atlas_risk_profile or submission_data")
            return self._empty_recommendation(entity_name, state, coverage_lines)

        if not coverage_lines:
            logger.warning("No coverage_lines in submission_data for entity=%s", entity_name)
            return self._empty_recommendation(entity_name, state, coverage_lines)

        # Build unified risk dict
        risk_dict = self._build_risk_dict(atlas_risk_profile, submission_data)

        logger.info(
            "MozartHermesBridge: matching entity=%s state=%s lines=%s",
            entity_name,
            state,
            coverage_lines,
        )

        # Run matching pipeline
        matches = await self._engine.match(
            risk_profile=risk_dict,
            state=state,
            lines=coverage_lines,
        )

        # Top candidates
        top_carriers = matches[: self._MAX_RECOMMENDED]

        # Compute premium aggregates per line
        total_premium = self._aggregate_premiums(matches, coverage_lines)

        # Determine placement strategy
        strategy = self._determine_strategy(top_carriers, coverage_lines, total_premium)

        # Compute overall confidence
        confidence = self._compute_confidence(top_carriers)

        # Market conditions per line
        market_conditions: dict[str, Any] = {}
        for line in coverage_lines:
            overview = await self._engine.get_market_overview(state=state, line=line)
            market_conditions[line] = {
                "trend": overview.get("market_trend"),
                "avg_rate_change": overview.get("avg_rate_change_pct"),
                "filing_count": overview.get("filing_count"),
            }

        logger.info(
            "PlacementRecommendation: strategy=%s confidence=%.2f carriers=%d",
            strategy,
            confidence,
            len(top_carriers),
        )

        return PlacementRecommendation(
            entity_name=entity_name,
            state=state,
            lines=coverage_lines,
            recommended_carriers=top_carriers,
            total_estimated_premium=total_premium,
            placement_strategy=strategy,
            confidence=confidence,
            market_conditions=market_conditions,
        )

    # ------------------------------------------------------------------
    # Strategy determination
    # ------------------------------------------------------------------

    def _determine_strategy(
        self,
        top_carriers: list[CarrierMatchResult],
        coverage_lines: list[str],
        total_premium: dict[str, Any],
    ) -> str:
        """Select the placement strategy based on market results.

        Rules (in priority order):

        1. If total estimated premium exceeds the excess-layer threshold →
           ``"excess_layering"``.
        2. If a single carrier covers all lines with adequate confidence →
           ``"single_carrier"``.
        3. Otherwise → ``"split_placement"``.

        Parameters
        ----------
        top_carriers:
            Ranked list of top carrier matches.
        coverage_lines:
            All requested lines.
        total_premium:
            Aggregated premium estimates by line.

        Returns
        -------
        str
            Strategy identifier.
        """
        total_est = total_premium.get("total_estimated", 0.0) or 0.0

        if total_est > self._EXCESS_PREMIUM_THRESHOLD:
            logger.debug(
                "Total premium $%.0f exceeds threshold; recommending excess layering",
                total_est,
            )
            return "excess_layering"

        if not top_carriers:
            return "split_placement"

        # Check if any single carrier covers all requested lines
        lines_set = set(coverage_lines)
        for carrier in top_carriers:
            # A carrier "covers" a line if it has a passing/conditional match for it
            carrier_lines = {
                m.line
                for m in top_carriers
                if m.carrier_id == carrier.carrier_id
                and m.eligibility.status in ("pass", "conditional")
            }
            if lines_set.issubset(carrier_lines):
                if carrier.placement_probability >= self._SINGLE_CARRIER_CONFIDENCE_THRESHOLD:
                    logger.debug(
                        "Carrier %s covers all lines; recommending single_carrier",
                        carrier.carrier_name,
                    )
                    return "single_carrier"

        return "split_placement"

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _build_risk_dict(
        atlas_risk_profile: dict,
        submission_data: dict,
    ) -> dict:
        """Merge Atlas profile and submission data into a unified risk dict.

        Submission data keys take precedence over Atlas profile keys for
        fields that appear in both.

        Parameters
        ----------
        atlas_risk_profile:
            Atlas-enriched risk attributes.
        submission_data:
            Submission-specific data from Mozart.

        Returns
        -------
        dict
            Merged risk dict ready for :class:`MatchingEngine`.
        """
        risk = {**atlas_risk_profile}  # start with Atlas data
        # Overlay submission-specific keys
        for key in (
            "coverage_lines",
            "requested_limits",
            "construction_type",
            "entity_name",
        ):
            if key in submission_data:
                risk[key] = submission_data[key]
        # Surface atlas_risk_scores as a nested dict for downstream logging
        risk.setdefault("atlas_risk_scores", {
            k: v for k, v in atlas_risk_profile.items()
            if k.startswith("atlas_")
        })
        return risk

    @staticmethod
    def _aggregate_premiums(
        matches: list[CarrierMatchResult],
        coverage_lines: list[str],
    ) -> dict[str, Any]:
        """Aggregate estimated premiums across lines and carriers.

        For each line, reports the range (min/max) of estimated premiums among
        eligible carriers and the total as the sum of minimum premiums.

        Parameters
        ----------
        matches:
            All eligible carrier matches.
        coverage_lines:
            Requested lines of business.

        Returns
        -------
        dict
            Keys: line names + ``"total_estimated"``.
        """
        aggregates: dict[str, Any] = {}
        grand_total = 0.0

        for line in coverage_lines:
            line_matches = [
                m for m in matches
                if m.line == line and m.premium.final_estimated > 0
            ]
            if not line_matches:
                aggregates[line] = {"min": None, "max": None, "carrier_count": 0}
                continue

            premiums = [m.premium.final_estimated for m in line_matches]
            min_p = min(premiums)
            max_p = max(premiums)
            grand_total += min_p  # conservative: use minimum

            aggregates[line] = {
                "min_estimated": round(min_p, 2),
                "max_estimated": round(max_p, 2),
                "carrier_count": len(line_matches),
            }

        aggregates["total_estimated"] = round(grand_total, 2)
        return aggregates

    @staticmethod
    def _compute_confidence(top_carriers: list[CarrierMatchResult]) -> float:
        """Compute overall placement confidence from the top carrier results.

        Takes the average of the top three placement probabilities, weighted
        by position (rank 1 = 3x, rank 2 = 2x, rank 3 = 1x).

        Parameters
        ----------
        top_carriers:
            Top-ranked carrier matches.

        Returns
        -------
        float
            Confidence between 0.0 and 1.0.
        """
        if not top_carriers:
            return 0.0

        weights = [3.0, 2.0, 1.0]
        weighted_sum = 0.0
        weight_total = 0.0

        for i, carrier in enumerate(top_carriers[:3]):
            w = weights[i] if i < len(weights) else 1.0
            weighted_sum += carrier.placement_probability * w
            weight_total += w

        if weight_total == 0:
            return 0.0

        return round(min(1.0, weighted_sum / weight_total), 4)

    @staticmethod
    def _empty_recommendation(
        entity_name: str,
        state: str,
        lines: list[str],
    ) -> PlacementRecommendation:
        """Return an empty recommendation when matching cannot proceed."""
        return PlacementRecommendation(
            entity_name=entity_name,
            state=state,
            lines=lines,
            recommended_carriers=[],
            total_estimated_premium={},
            placement_strategy="split_placement",
            confidence=0.0,
            market_conditions={},
        )
