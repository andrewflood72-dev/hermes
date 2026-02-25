"""Title-specific eligibility filter — checks whether a title carrier is eligible
to underwrite a transaction for a given state.

Title eligibility is simpler than P&C: a carrier is eligible if it has a current
rate card for the requested state.  Additional conditional notes are raised for
refinance transactions without reissue credit data or endorsement availability.
"""

from __future__ import annotations

import logging
from typing import Any
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from hermes.config import settings
from hermes.matching.eligibility import EligibilityResult

logger = logging.getLogger("hermes.matching.title_eligibility")


class TitleEligibilityFilter:
    """Checks title carrier eligibility for a state and risk profile.

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

    async def check_eligibility(
        self,
        carrier_id: UUID,
        state: str,
        risk_profile: dict,
    ) -> EligibilityResult:
        """Evaluate whether a title carrier is eligible for this transaction.

        Checks:
        1. Carrier has at least one ``is_current = TRUE`` rate card for the state.
        2. Carrier status is active.
        3. If ``is_refinance`` and no reissue credit data → conditional note.
        4. If endorsements requested but not all available → conditional note.

        Parameters
        ----------
        carrier_id:
            UUID of the carrier.
        state:
            Two-letter state code.
        risk_profile:
            Title risk attributes dict.  Expected keys: ``is_refinance``,
            ``years_since_prior_policy``, ``endorsements``.

        Returns
        -------
        EligibilityResult
        """
        failed: list[str] = []
        conditional: list[str] = []
        criteria_checked = 0

        # Check 1: Current rate card exists for state
        criteria_checked += 1
        has_rate_card = await self._has_current_rate_card(carrier_id, state)
        if not has_rate_card:
            failed.append(f"No current rate card for state {state}.")
            return EligibilityResult(
                status="fail",
                failed_criteria=failed,
                conditional_notes=conditional,
                criteria_checked=criteria_checked,
            )

        # Check 2: Refinance reissue credit availability
        is_refinance = risk_profile.get("is_refinance", False)
        years_since = risk_profile.get("years_since_prior_policy")
        if is_refinance:
            criteria_checked += 1
            if years_since is None:
                conditional.append(
                    "Refinance requested but years_since_prior_policy not provided; "
                    "reissue credit cannot be computed."
                )
            else:
                has_reissue = await self._has_reissue_credit(carrier_id, state)
                if not has_reissue:
                    conditional.append(
                        "Carrier has no reissue credit schedule for this state; "
                        "refinance discount may not be available."
                    )

        # Check 3: Endorsement availability
        endorsements = risk_profile.get("endorsements") or []
        if endorsements:
            criteria_checked += 1
            available = await self._get_available_endorsements(carrier_id, state)
            missing = [e for e in endorsements if e not in available]
            if missing:
                conditional.append(
                    f"Endorsements not available from this carrier: {', '.join(missing)}"
                )

        if failed:
            status = "fail"
        elif conditional:
            status = "conditional"
        else:
            status = "pass"

        return EligibilityResult(
            status=status,
            failed_criteria=failed,
            conditional_notes=conditional,
            criteria_checked=criteria_checked,
        )

    # ------------------------------------------------------------------
    # Database queries
    # ------------------------------------------------------------------

    async def _has_current_rate_card(self, carrier_id: UUID, state: str) -> bool:
        """Return True if carrier has at least one current rate card for state."""
        engine = await self._get_engine()
        query = text("""
            SELECT COUNT(*) FROM hermes_title_rate_cards
            WHERE carrier_id = :carrier_id
              AND state = :state
              AND is_current = TRUE
        """)
        async with engine.connect() as conn:
            result = await conn.execute(
                query, {"carrier_id": str(carrier_id), "state": state}
            )
            count = result.scalar() or 0
        return count > 0

    async def _has_reissue_credit(self, carrier_id: UUID, state: str) -> bool:
        """Return True if carrier has reissue credit data for state."""
        engine = await self._get_engine()
        query = text("""
            SELECT COUNT(*) FROM hermes_title_reissue_credits rc2
            JOIN hermes_title_rate_cards rc ON rc.id = rc2.rate_card_id
            WHERE rc.carrier_id = :carrier_id
              AND rc.state = :state
              AND rc.is_current = TRUE
        """)
        async with engine.connect() as conn:
            result = await conn.execute(
                query, {"carrier_id": str(carrier_id), "state": state}
            )
            count = result.scalar() or 0
        return count > 0

    async def _get_available_endorsements(
        self, carrier_id: UUID, state: str
    ) -> set[str]:
        """Return the set of endorsement codes available from this carrier/state."""
        engine = await self._get_engine()
        query = text("""
            SELECT DISTINCT e.endorsement_code
            FROM hermes_title_endorsements e
            JOIN hermes_title_rate_cards rc ON rc.id = e.rate_card_id
            WHERE rc.carrier_id = :carrier_id
              AND rc.state = :state
              AND rc.is_current = TRUE
        """)
        async with engine.connect() as conn:
            result = await conn.execute(
                query, {"carrier_id": str(carrier_id), "state": state}
            )
            rows = result.scalars().all()
        return set(rows)
