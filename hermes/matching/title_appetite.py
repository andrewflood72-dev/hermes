"""Title-specific appetite scorer — computes a 0-100 appetite score for title
carriers based on market presence, rate recency, competitive positioning, and
product breadth.

Unlike P&C appetite (which uses NAICS class fit and territory preferences),
title appetite focuses on rate card coverage, update freshness, and pricing
competitiveness against peers.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from typing import Any
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from hermes.config import settings
from hermes.matching.appetite import AppetiteResult

logger = logging.getLogger("hermes.matching.title_appetite")


class TitleAppetiteScorer:
    """Scores a title carrier's appetite for a given state.

    Four scoring components (sum to 100):

    - **Market presence** (30 pts): rate card count for state, multi-state presence
    - **Rate recency** (25 pts): how recently rate cards were updated
    - **Competitive positioning** (25 pts): avg rate_per_thousand vs peers
    - **Product breadth** (20 pts): simultaneous issue, reissue credits, endorsements

    Parameters
    ----------
    engine:
        Optional pre-built SQLAlchemy async engine.
    """

    def __init__(self, engine: AsyncEngine | None = None) -> None:
        self._engine = engine

    async def _get_engine(self) -> AsyncEngine:
        if self._engine is None:
            self._engine = create_async_engine(
                settings.database_url,
                pool_size=5,
                max_overflow=10,
                echo=False,
            )
        return self._engine

    async def score_appetite(
        self,
        carrier_id: UUID,
        state: str,
        risk_profile: dict,
    ) -> AppetiteResult:
        """Compute a 0-100 appetite score for a title carrier in this state.

        Parameters
        ----------
        carrier_id:
            UUID of the carrier.
        state:
            Two-letter state code.
        risk_profile:
            Title risk attributes (not heavily used for appetite but available).

        Returns
        -------
        AppetiteResult
        """
        notes: list[str] = []
        components: dict[str, float] = {}

        # 1. Market presence (0-30)
        presence = await self._score_market_presence(carrier_id, state)
        components["market_presence"] = presence
        if presence >= 25:
            notes.append("Strong market presence in this state.")
        elif presence < 10:
            notes.append("Limited market presence in this state.")

        # 2. Rate recency (0-25)
        recency = await self._score_rate_recency(carrier_id, state)
        components["rate_recency"] = recency
        if recency < 10:
            notes.append("Rate cards may be stale (> 2 years since last update).")

        # 3. Competitive positioning (0-25)
        competitive = await self._score_competitive_positioning(carrier_id, state)
        components["competitive_positioning"] = competitive
        if competitive >= 20:
            notes.append("Rates are competitively positioned vs. peers.")
        elif competitive < 10:
            notes.append("Rates are above average for this state.")

        # 4. Product breadth (0-20)
        breadth = await self._score_product_breadth(carrier_id, state)
        components["product_breadth"] = breadth
        if breadth >= 16:
            notes.append("Full product suite available (simultaneous, reissue, endorsements).")

        composite = sum(components.values())
        composite = min(100.0, max(0.0, round(composite, 2)))

        logger.info(
            "Title appetite score for carrier=%s state=%s: %.1f",
            carrier_id, state, composite,
        )

        return AppetiteResult(
            score=composite,
            components=components,
            recent_signals=[],
            notes=notes,
        )

    # ------------------------------------------------------------------
    # Scoring components
    # ------------------------------------------------------------------

    async def _score_market_presence(self, carrier_id: UUID, state: str) -> float:
        """Score market presence (0-30).

        - Rate card count for this state (up to 15 pts)
        - Multi-state presence (up to 15 pts)
        """
        engine = await self._get_engine()

        # Rate cards for this state
        state_query = text("""
            SELECT COUNT(*) FROM hermes_title_rate_cards
            WHERE carrier_id = :cid AND state = :state AND is_current = TRUE
        """)
        async with engine.connect() as conn:
            result = await conn.execute(
                state_query, {"cid": str(carrier_id), "state": state}
            )
            state_count = result.scalar() or 0

        # Multi-state presence (distinct states with current rate cards)
        multi_query = text("""
            SELECT COUNT(DISTINCT state) FROM hermes_title_rate_cards
            WHERE carrier_id = :cid AND is_current = TRUE
        """)
        async with engine.connect() as conn:
            result = await conn.execute(multi_query, {"cid": str(carrier_id)})
            total_states = result.scalar() or 0

        # State card score: 1 card = 5, 2 = 10, 3+ = 15
        if state_count >= 3:
            state_score = 15.0
        elif state_count == 2:
            state_score = 10.0
        elif state_count == 1:
            state_score = 5.0
        else:
            state_score = 0.0

        # Multi-state score: 1 state = 3, 5+ = 8, 10+ = 12, 20+ = 15
        if total_states >= 20:
            multi_score = 15.0
        elif total_states >= 10:
            multi_score = 12.0
        elif total_states >= 5:
            multi_score = 8.0
        elif total_states >= 1:
            multi_score = 3.0
        else:
            multi_score = 0.0

        return min(30.0, state_score + multi_score)

    async def _score_rate_recency(self, carrier_id: UUID, state: str) -> float:
        """Score rate recency (0-25).

        Most recent rate card effective_date determines freshness.
        """
        engine = await self._get_engine()
        query = text("""
            SELECT MAX(effective_date) AS latest
            FROM hermes_title_rate_cards
            WHERE carrier_id = :cid AND state = :state AND is_current = TRUE
        """)
        async with engine.connect() as conn:
            result = await conn.execute(
                query, {"cid": str(carrier_id), "state": state}
            )
            row = result.mappings().first()

        if not row or row["latest"] is None:
            return 5.0

        latest = row["latest"]
        if isinstance(latest, str):
            try:
                latest = date.fromisoformat(latest)
            except ValueError:
                return 5.0

        days_since = (date.today() - latest).days

        if days_since <= 180:
            return 25.0
        elif days_since <= 365:
            return 20.0
        elif days_since <= 730:
            return 13.0
        elif days_since <= 1095:
            return 8.0
        else:
            return 3.0

    async def _score_competitive_positioning(
        self, carrier_id: UUID, state: str
    ) -> float:
        """Score competitive positioning (0-25).

        Compares this carrier's average rate_per_thousand against the state average
        across all carriers.  Lower rates → higher score.
        """
        engine = await self._get_engine()

        # This carrier's avg rate
        carrier_query = text("""
            SELECT AVG(r.rate_per_thousand) AS avg_rate
            FROM hermes_title_rates r
            JOIN hermes_title_rate_cards rc ON rc.id = r.rate_card_id
            WHERE rc.carrier_id = :cid
              AND rc.state = :state
              AND rc.is_current = TRUE
        """)

        # All carriers' avg rate for this state
        all_query = text("""
            SELECT AVG(r.rate_per_thousand) AS avg_rate
            FROM hermes_title_rates r
            JOIN hermes_title_rate_cards rc ON rc.id = r.rate_card_id
            WHERE rc.state = :state
              AND rc.is_current = TRUE
        """)

        async with engine.connect() as conn:
            carrier_result = await conn.execute(
                carrier_query, {"cid": str(carrier_id), "state": state}
            )
            carrier_row = carrier_result.mappings().first()

            all_result = await conn.execute(all_query, {"state": state})
            all_row = all_result.mappings().first()

        carrier_avg = float(carrier_row["avg_rate"]) if carrier_row and carrier_row["avg_rate"] else None
        state_avg = float(all_row["avg_rate"]) if all_row and all_row["avg_rate"] else None

        if carrier_avg is None or state_avg is None or state_avg == 0:
            return 12.5  # neutral

        # Ratio < 1.0 means carrier is cheaper than average
        ratio = carrier_avg / state_avg

        if ratio <= 0.85:
            return 25.0
        elif ratio <= 0.95:
            return 22.0
        elif ratio <= 1.05:
            return 18.0  # roughly average
        elif ratio <= 1.15:
            return 12.0
        elif ratio <= 1.30:
            return 7.0
        else:
            return 3.0

    async def _score_product_breadth(self, carrier_id: UUID, state: str) -> float:
        """Score product breadth (0-20).

        Checks for:
        - Simultaneous issue schedules (up to 8 pts)
        - Reissue credit data (up to 6 pts)
        - Endorsement catalog (up to 6 pts)
        """
        engine = await self._get_engine()
        score = 0.0

        # Simultaneous issue
        simul_query = text("""
            SELECT COUNT(*) FROM hermes_title_simultaneous_issue si
            JOIN hermes_title_rate_cards rc ON rc.id = si.rate_card_id
            WHERE rc.carrier_id = :cid AND rc.state = :state AND rc.is_current = TRUE
        """)
        async with engine.connect() as conn:
            result = await conn.execute(
                simul_query, {"cid": str(carrier_id), "state": state}
            )
            simul_count = result.scalar() or 0
        if simul_count > 0:
            score += 8.0

        # Reissue credits
        reissue_query = text("""
            SELECT COUNT(*) FROM hermes_title_reissue_credits rc2
            JOIN hermes_title_rate_cards rc ON rc.id = rc2.rate_card_id
            WHERE rc.carrier_id = :cid AND rc.state = :state AND rc.is_current = TRUE
        """)
        async with engine.connect() as conn:
            result = await conn.execute(
                reissue_query, {"cid": str(carrier_id), "state": state}
            )
            reissue_count = result.scalar() or 0
        if reissue_count > 0:
            score += 6.0

        # Endorsements
        endorse_query = text("""
            SELECT COUNT(DISTINCT e.endorsement_code)
            FROM hermes_title_endorsements e
            JOIN hermes_title_rate_cards rc ON rc.id = e.rate_card_id
            WHERE rc.carrier_id = :cid AND rc.state = :state AND rc.is_current = TRUE
        """)
        async with engine.connect() as conn:
            result = await conn.execute(
                endorse_query, {"cid": str(carrier_id), "state": state}
            )
            endorse_count = result.scalar() or 0
        if endorse_count >= 5:
            score += 6.0
        elif endorse_count >= 1:
            score += 3.0

        return min(20.0, score)
