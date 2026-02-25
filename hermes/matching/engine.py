"""Matching engine — the main orchestrator for the Hermes carrier-risk
matching pipeline.

:class:`MatchingEngine` coordinates the three evaluation stages (eligibility →
appetite → premium) across all carriers with active appetite profiles for the
requested state and lines, then delegates to :class:`CarrierRanker` to produce
a sorted, scored result set.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import date, datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from hermes.config import settings
from hermes.matching.eligibility import EligibilityFilter, EligibilityResult
from hermes.matching.appetite import AppetiteScorer, AppetiteResult
from hermes.matching.premium import PremiumEstimator, PremiumEstimate
from hermes.matching.ranker import CarrierRanker

logger = logging.getLogger("hermes.matching.engine")


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------


class CarrierMatchResult(BaseModel):
    """Full matching result for a single carrier/state/line combination.

    Attributes
    ----------
    carrier_id:
        UUID of the carrier from ``hermes_carriers``.
    carrier_name:
        Legal name of the carrier.
    naic_code:
        NAIC code string.
    state:
        Two-letter state code.
    line:
        Line of business.
    eligibility:
        Result of the eligibility check.
    appetite:
        Result of the appetite scoring.
    premium:
        Estimated premium breakdown.
    competitiveness_rank:
        Rank assigned by :class:`CarrierRanker` (1 = best).
    composite_score:
        Weighted composite score (0-100 range, higher is better).
    coverage_highlights:
        Notable coverage features extracted from ``hermes_coverage_options``.
    recent_signals:
        Appetite signals from the past 90 days.
    filing_references:
        Key filing references (SERFF tracking numbers, dates).
    placement_probability:
        Estimated probability (0.0-1.0) of successful placement.
    """

    carrier_id: UUID
    carrier_name: str
    naic_code: str
    state: str
    line: str
    eligibility: EligibilityResult
    appetite: AppetiteResult
    premium: PremiumEstimate
    competitiveness_rank: int = Field(default=0)
    composite_score: float = Field(default=0.0)
    coverage_highlights: list[dict] = Field(default_factory=list)
    recent_signals: list[dict] = Field(default_factory=list)
    filing_references: list[dict] = Field(default_factory=list)
    placement_probability: float = Field(default=0.0)

    model_config = {"arbitrary_types_allowed": True}


# ---------------------------------------------------------------------------
# MatchingEngine
# ---------------------------------------------------------------------------


class MatchingEngine:
    """Orchestrates the full carrier-risk matching pipeline.

    Usage::

        engine = MatchingEngine()
        results = await engine.match(risk_profile, state="TX", lines=["Commercial Auto"])

    Parameters
    ----------
    db_engine:
        Optional pre-built SQLAlchemy async engine shared across all components.
    """

    def __init__(self, db_engine: AsyncEngine | None = None) -> None:
        self._engine = db_engine
        self.eligibility_filter = EligibilityFilter(engine=db_engine)
        self.appetite_scorer = AppetiteScorer(engine=db_engine)
        self.premium_estimator = PremiumEstimator(engine=db_engine)
        self.ranker = CarrierRanker()
        logger.info("MatchingEngine initialised")

    # ------------------------------------------------------------------
    # Engine access
    # ------------------------------------------------------------------

    async def _get_engine(self) -> AsyncEngine:
        """Return (creating lazily) the shared async SQLAlchemy engine."""
        if self._engine is None:
            self._engine = create_async_engine(
                settings.database_url,
                pool_size=10,
                max_overflow=20,
                echo=False,
            )
            # Share the engine with sub-components
            self.eligibility_filter._engine = self._engine
            self.appetite_scorer._engine = self._engine
            self.premium_estimator._engine = self._engine
        return self._engine

    async def close(self) -> None:
        """Dispose the database engine pool."""
        if self._engine is not None:
            await self._engine.dispose()
            logger.info("MatchingEngine database engine disposed")

    # ------------------------------------------------------------------
    # Primary matching API
    # ------------------------------------------------------------------

    async def match(
        self,
        risk_profile: dict,
        state: str,
        lines: list[str],
    ) -> list[CarrierMatchResult]:
        """Run the full matching pipeline for the given risk profile.

        For each line of business:

        1. Discover all carriers with an active appetite profile for the
           state/line from ``hermes_appetite_profiles``.
        2. For each carrier run eligibility → appetite → premium in parallel.
        3. Filter out ``"fail"`` eligibility results (keep ``"conditional"``).
        4. Combine results across all lines.
        5. Rank and return sorted :class:`CarrierMatchResult` list.

        Parameters
        ----------
        risk_profile:
            Dict of risk attributes (naics_code, state, zip_code, etc.).
        state:
            Two-letter state code.
        lines:
            List of lines of business to match against.

        Returns
        -------
        list[CarrierMatchResult]
            Ranked carrier matches; failed eligibility excluded.
        """
        await self._get_engine()
        all_results: list[CarrierMatchResult] = []

        for line in lines:
            logger.info("Matching state=%s line=%s", state, line)
            carriers = await self._get_active_carriers(state, line)
            logger.info(
                "Found %d active carriers for state=%s line=%s",
                len(carriers),
                state,
                line,
            )

            if not carriers:
                continue

            # Process all carriers concurrently per line
            tasks = [
                self._evaluate_carrier(carrier, state, line, risk_profile)
                for carrier in carriers
            ]
            line_results: list[CarrierMatchResult | None] = await asyncio.gather(
                *tasks, return_exceptions=False
            )

            for result in line_results:
                if result is None:
                    continue
                if result.eligibility.status == "fail":
                    logger.debug(
                        "Carrier %s failed eligibility for %s/%s; excluded",
                        result.carrier_name,
                        state,
                        line,
                    )
                    continue
                all_results.append(result)

        # Rank across all lines
        ranked = self.ranker.rank_carriers(all_results)
        logger.info(
            "Matching complete: %d eligible carriers across %d lines",
            len(ranked),
            len(lines),
        )
        return ranked

    async def get_market_overview(self, state: str, line: str) -> dict:
        """Return aggregate market intelligence statistics for a state/line.

        Queries the most recent ``hermes_market_intelligence`` row for the
        state/line combination and returns it as a plain dict.

        Parameters
        ----------
        state:
            Two-letter state code.
        line:
            Line of business.

        Returns
        -------
        dict
            Market intelligence summary; empty dict if no data found.
        """
        await self._get_engine()
        query = text(
            """
            SELECT
                id,
                state,
                line,
                period_start,
                period_end,
                avg_rate_change_pct,
                median_rate_change_pct,
                filing_count,
                rate_increase_count,
                rate_decrease_count,
                new_entrant_count,
                withdrawal_count,
                new_entrants,
                withdrawals,
                top_appetite_shifts,
                market_trend,
                summary,
                computed_at
            FROM hermes_market_intelligence
            WHERE state = :state AND line = :line
            ORDER BY period_end DESC
            LIMIT 1
            """
        )
        engine = await self._get_engine()
        async with engine.connect() as conn:
            result = await conn.execute(query, {"state": state, "line": line})
            row = result.mappings().first()

        if row is None:
            logger.info("No market intelligence found for state=%s line=%s", state, line)
            return {"state": state, "line": line, "data": None}

        overview = dict(row)
        # Serialise dates
        for k, v in overview.items():
            if isinstance(v, (date, datetime)):
                overview[k] = v.isoformat()
        return overview

    # ------------------------------------------------------------------
    # Carrier evaluation pipeline
    # ------------------------------------------------------------------

    async def _evaluate_carrier(
        self,
        carrier: dict,
        state: str,
        line: str,
        risk_profile: dict,
    ) -> CarrierMatchResult | None:
        """Run the full evaluation pipeline for a single carrier.

        Catches and logs exceptions so one failed carrier does not abort the
        entire match.

        Parameters
        ----------
        carrier:
            Carrier dict with ``id``, ``naic_code``, ``legal_name``.
        state:
            Two-letter state code.
        line:
            Line of business.
        risk_profile:
            Risk attributes dict.

        Returns
        -------
        CarrierMatchResult | None
            None on unhandled exception.
        """
        carrier_id = UUID(str(carrier["id"]))
        carrier_name: str = carrier.get("legal_name", "Unknown")
        naic_code: str = carrier.get("naic_code", "")

        try:
            # Stage 1: Eligibility
            eligibility = await self.eligibility_filter.check_eligibility(
                risk_profile=risk_profile,
                carrier_id=carrier_id,
                state=state,
                line=line,
            )

            # Stage 2: Appetite (run regardless of eligibility for data completeness)
            appetite = await self.appetite_scorer.score_appetite(
                carrier_id=carrier_id,
                state=state,
                line=line,
                risk_profile=risk_profile,
            )

            # Stage 3: Premium (only if not failed eligibility)
            if eligibility.status != "fail":
                premium = await self.premium_estimator.estimate_premium(
                    carrier_id=carrier_id,
                    state=state,
                    line=line,
                    risk_profile=risk_profile,
                )
            else:
                premium = PremiumEstimate(
                    notes=["Premium not estimated due to eligibility failure."]
                )

            # Load supplemental data
            coverage_highlights = await self._load_coverage_highlights(carrier_id, state, line)
            filing_references = await self._load_filing_references(carrier_id, state, line)

            return CarrierMatchResult(
                carrier_id=carrier_id,
                carrier_name=carrier_name,
                naic_code=naic_code,
                state=state,
                line=line,
                eligibility=eligibility,
                appetite=appetite,
                premium=premium,
                coverage_highlights=coverage_highlights,
                recent_signals=appetite.recent_signals,
                filing_references=filing_references,
            )

        except Exception as exc:
            logger.exception(
                "Error evaluating carrier=%s state=%s line=%s: %s",
                carrier_name,
                state,
                line,
                exc,
            )
            return None

    # ------------------------------------------------------------------
    # Database access
    # ------------------------------------------------------------------

    async def _get_active_carriers(self, state: str, line: str) -> list[dict]:
        """Return all carriers with a current appetite profile for state/line.

        Joins ``hermes_appetite_profiles`` (is_current = TRUE) with
        ``hermes_carriers`` (status = 'active').

        Parameters
        ----------
        state:
            Two-letter state code.
        line:
            Line of business.

        Returns
        -------
        list[dict]
            Each dict has ``id``, ``naic_code``, ``legal_name``,
            ``am_best_rating``.
        """
        query = text(
            """
            SELECT DISTINCT
                c.id,
                c.naic_code,
                c.legal_name,
                c.am_best_rating,
                c.am_best_outlook
            FROM hermes_carriers c
            JOIN hermes_appetite_profiles ap ON ap.carrier_id = c.id
            WHERE
                ap.state      = :state
                AND ap.line   = :line
                AND ap.is_current = TRUE
                AND c.status  = 'active'
            ORDER BY c.legal_name
            """
        )
        engine = await self._get_engine()
        async with engine.connect() as conn:
            result = await conn.execute(query, {"state": state, "line": line})
            rows = result.mappings().all()
        return [dict(row) for row in rows]

    async def _load_coverage_highlights(
        self, carrier_id: UUID, state: str, line: str
    ) -> list[dict]:
        """Load notable coverage features from ``hermes_coverage_options``."""
        query = text(
            """
            SELECT
                coverage_type,
                limit_min,
                limit_max,
                default_limit,
                default_deductible,
                sublimits
            FROM hermes_coverage_options
            WHERE
                carrier_id = :carrier_id
                AND state   = :state
                AND line    = :line
                AND is_current = TRUE
            ORDER BY coverage_type
            LIMIT 10
            """
        )
        engine = await self._get_engine()
        async with engine.connect() as conn:
            result = await conn.execute(
                query,
                {"carrier_id": str(carrier_id), "state": state, "line": line},
            )
            rows = result.mappings().all()
        return [_row_to_dict(row) for row in rows]

    async def _load_filing_references(
        self, carrier_id: UUID, state: str, line: str
    ) -> list[dict]:
        """Load recent filing references from ``hermes_filings``."""
        query = text(
            """
            SELECT
                serff_tracking_number,
                filing_type,
                status,
                effective_date,
                filed_date,
                overall_rate_change_pct,
                filing_description
            FROM hermes_filings
            WHERE
                carrier_id      = :carrier_id
                AND state       = :state
                AND line_of_business = :line
                AND status IN ('approved', 'pending')
            ORDER BY effective_date DESC NULLS LAST
            LIMIT 5
            """
        )
        engine = await self._get_engine()
        async with engine.connect() as conn:
            result = await conn.execute(
                query,
                {"carrier_id": str(carrier_id), "state": state, "line": line},
            )
            rows = result.mappings().all()
        return [_row_to_dict(row) for row in rows]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _row_to_dict(row: Any) -> dict:
    """Convert a SQLAlchemy mapping row to a plain dict, serialising dates."""
    d = dict(row)
    for k, v in d.items():
        if isinstance(v, (date, datetime)):
            d[k] = v.isoformat()
    return d
