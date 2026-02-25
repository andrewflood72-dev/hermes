"""Hermes Change Detector — Appetite and rate shift detection from SERFF filings.

Compares newly parsed filing data against existing appetite profiles to surface
significant market changes: rate movements, class code additions/removals,
new state entries, filing withdrawals, and territory expansions.

Each detected shift is persisted as a ``hermes_appetite_signals`` row and
returned as an :class:`AppetiteShift` Pydantic model for downstream alerting.
"""

from __future__ import annotations

import logging
import uuid
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Optional
from uuid import UUID

from pydantic import BaseModel, Field
from sqlalchemy import select, and_, text
from sqlalchemy.ext.asyncio import AsyncSession

from hermes.config import settings
from hermes.db import (
    async_session,
    Carrier,
    Filing,
    AppetiteProfile,
    AppetiteSignal,
    CarrierRanking,
    BaseRate,
    RateTable,
    ClassCodeMapping,
    TerritoryDefinition,
)

logger = logging.getLogger("hermes.monitoring.change_detector")

# ---------------------------------------------------------------------------
# Pydantic model
# ---------------------------------------------------------------------------

RATE_DECREASE_THRESHOLD = Decimal("-5.0")   # > 5% decrease is a signal
RATE_INCREASE_THRESHOLD = Decimal("10.0")   # > 10% increase is a signal


class AppetiteShift(BaseModel):
    """A detected change in a carrier's appetite or rate posture.

    Attributes:
        carrier_id: UUID of the carrier in ``hermes_carriers``.
        carrier_name: Human-readable carrier legal name.
        state: Two-letter state code where the shift occurred.
        line: Line of business affected.
        signal_type: Category of shift (rate_decrease, rate_increase, etc.).
        signal_strength: Severity on a 1–10 scale (10 = most impactful).
        description: Human-readable summary of what changed.
        source_filing_id: UUID of the SERFF filing that triggered the shift.
        old_value: The prior value (rate pct, class list, etc.) as a string.
        new_value: The new value after the filing's effect as a string.
    """

    carrier_id: UUID
    carrier_name: str
    state: str
    line: str
    signal_type: str
    signal_strength: int = Field(ge=1, le=10)
    description: str
    source_filing_id: Optional[UUID] = None
    old_value: Optional[str] = None
    new_value: Optional[str] = None


# ---------------------------------------------------------------------------
# ChangeDetector
# ---------------------------------------------------------------------------


class ChangeDetector:
    """Detects appetite and rate shifts from newly parsed SERFF filings.

    Uses the SQLAlchemy async session from ``hermes.db`` to compare fresh
    filing data against the current ``hermes_appetite_profiles`` snapshot and
    emit ``hermes_appetite_signals`` rows for each meaningful change found.

    Typical usage::

        detector = ChangeDetector()
        shifts = await detector.detect_all_shifts(since_date=date.today())
    """

    # Signal type constants
    RATE_DECREASE = "rate_decrease"
    RATE_INCREASE = "rate_increase"
    NEW_CLASS_CODES = "expanded_classes"
    REMOVED_CLASS_CODES = "contracted_classes"
    NEW_STATE_FILING = "new_state_entry"
    FILING_WITHDRAWAL = "filing_withdrawal"
    TERRITORY_EXPANSION = "territory_expansion"

    def __init__(self) -> None:
        self._session_factory = async_session

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def detect_shifts(
        self, carrier_id: UUID, state: str, line: str
    ) -> list[AppetiteShift]:
        """Detect appetite shifts for a specific carrier / state / line combination.

        Compares the current ``hermes_filings`` data against the stored
        ``hermes_appetite_profiles`` snapshot and identifies:

        - Rate decreases greater than 5%
        - Rate increases greater than 10%
        - New class codes added to eligible_classes
        - Class codes removed from eligible_classes
        - Brand-new state filings (no prior profile exists)
        - Filing withdrawals (status = 'withdrawn')
        - Territory expansions (new territories in rate tables)

        Args:
            carrier_id: UUID of the carrier to analyse.
            state: Two-letter state code.
            line: Line of business string.

        Returns:
            List of :class:`AppetiteShift` instances, one per detected change.
            Empty list if no meaningful changes are found.
        """
        shifts: list[AppetiteShift] = []

        async with self._session_factory() as session:
            # Fetch carrier record
            carrier = await self._get_carrier(session, carrier_id)
            if not carrier:
                logger.warning("Carrier not found: %s", carrier_id)
                return shifts

            # Fetch existing profile
            profile = await self._get_profile(session, carrier_id, state, line)

            # Fetch latest approved filing for this carrier/state/line
            latest_filing = await self._get_latest_filing(
                session, carrier_id, state, line
            )

            if not latest_filing:
                logger.debug(
                    "No filings found for carrier=%s state=%s line=%s",
                    carrier_id,
                    state,
                    line,
                )
                return shifts

            # --- New state entry (no prior profile) ---
            if profile is None:
                shift = AppetiteShift(
                    carrier_id=carrier_id,
                    carrier_name=carrier.legal_name,
                    state=state,
                    line=line,
                    signal_type=self.NEW_STATE_FILING,
                    signal_strength=8,
                    description=(
                        f"{carrier.legal_name} has filed in {state} for {line} "
                        f"— no prior appetite profile existed."
                    ),
                    source_filing_id=latest_filing.id,
                    old_value=None,
                    new_value="new_state_entry",
                )
                shifts.append(shift)
                await self._persist_signal(session, shift, profile)
                await session.commit()
                return shifts

            # --- Filing withdrawal detection ---
            withdrawal_shifts = await self._detect_withdrawals(
                session, carrier, state, line, profile
            )
            shifts.extend(withdrawal_shifts)

            # --- Rate change detection ---
            rate_shifts = await self._detect_rate_changes(
                session, carrier, state, line, profile, latest_filing
            )
            shifts.extend(rate_shifts)

            # --- Class code changes ---
            class_shifts = await self._detect_class_code_changes(
                session, carrier, state, line, profile, latest_filing
            )
            shifts.extend(class_shifts)

            # --- Territory expansion ---
            territory_shifts = await self._detect_territory_expansions(
                session, carrier, state, line, profile, latest_filing
            )
            shifts.extend(territory_shifts)

            # Persist all signals
            for shift in shifts:
                await self._persist_signal(session, shift, profile)

            await session.commit()

        logger.info(
            "Detected %d shifts for carrier=%s state=%s line=%s",
            len(shifts),
            carrier_id,
            state,
            line,
        )
        return shifts

    async def detect_all_shifts(self, since_date: date) -> list[AppetiteShift]:
        """Run shift detection across all carriers/states where filings changed.

        Queries ``hermes_filings`` for any filing updated or created since
        ``since_date``, then runs :meth:`detect_shifts` for each unique
        carrier / state / line combination found.

        Args:
            since_date: Only consider filings created or updated on or after
                this date.

        Returns:
            Aggregated list of :class:`AppetiteShift` instances across all
            carriers and markets.
        """
        all_shifts: list[AppetiteShift] = []

        async with self._session_factory() as session:
            # Find all distinct (carrier_id, state, line) tuples with recent filings
            stmt = text(
                """
                SELECT DISTINCT
                    f.carrier_id,
                    f.state,
                    f.line_of_business AS line
                FROM hermes_filings f
                JOIN hermes_filing_documents fd ON fd.filing_id = f.id
                WHERE
                    fd.parsed_flag = TRUE
                    AND f.carrier_id IS NOT NULL
                    AND f.updated_at >= :since_date
                ORDER BY f.state, f.line_of_business
                """
            )
            result = await session.execute(stmt, {"since_date": since_date})
            combos = result.fetchall()

        logger.info(
            "Running shift detection for %d carrier/state/line combinations since %s",
            len(combos),
            since_date,
        )

        for row in combos:
            try:
                shifts = await self.detect_shifts(
                    carrier_id=row.carrier_id,
                    state=row.state,
                    line=row.line,
                )
                all_shifts.extend(shifts)
            except Exception as exc:
                logger.error(
                    "Error detecting shifts for carrier=%s state=%s line=%s: %s",
                    row.carrier_id,
                    row.state,
                    row.line,
                    exc,
                )

        logger.info("Total shifts detected: %d", len(all_shifts))
        return all_shifts

    async def recompute_appetite(
        self, carrier_id: UUID, state: str, line: str
    ) -> None:
        """Recompute the appetite profile for a carrier / state / line from scratch.

        Recalculates:
        - ``eligible_classes`` from current rate table class code mappings
        - ``ineligible_classes`` from underwriting rules
        - ``last_rate_change_pct`` from the latest filing's overall_rate_change_pct
        - ``rate_competitiveness_index`` by comparing rates to peer carriers
        - ``carrier_rankings`` for each class code covered

        Args:
            carrier_id: UUID of the carrier whose profile should be refreshed.
            state: Two-letter state code.
            line: Line of business string.
        """
        logger.info(
            "Recomputing appetite profile: carrier=%s state=%s line=%s",
            carrier_id,
            state,
            line,
        )

        async with self._session_factory() as session:
            carrier = await self._get_carrier(session, carrier_id)
            if not carrier:
                logger.warning("Carrier not found for recompute: %s", carrier_id)
                return

            # Gather eligible classes from rate tables
            eligible_classes = await self._gather_eligible_classes(
                session, carrier_id, state, line
            )

            # Gather ineligible classes from underwriting rules
            ineligible_classes = await self._gather_ineligible_classes(
                session, carrier_id, state, line
            )

            # Get latest rate change
            latest_filing = await self._get_latest_filing(
                session, carrier_id, state, line
            )
            last_rate_change_pct = None
            last_rate_change_date = None
            source_filing_count = 0

            if latest_filing:
                last_rate_change_pct = latest_filing.overall_rate_change_pct
                last_rate_change_date = latest_filing.effective_date

            # Count source filings
            count_stmt = text(
                """
                SELECT COUNT(*) AS cnt
                FROM hermes_filings
                WHERE carrier_id = :carrier_id
                  AND state = :state
                  AND line_of_business = :line
                  AND status IN ('approved', 'effective')
                """
            )
            count_result = await session.execute(
                count_stmt,
                {"carrier_id": str(carrier_id), "state": state, "line": line},
            )
            row = count_result.fetchone()
            source_filing_count = row.cnt if row else 0

            # Compute rate competitiveness index vs peers
            rate_competitiveness_index = await self._compute_competitiveness(
                session, carrier_id, state, line
            )

            # Upsert appetite profile
            now = datetime.now(timezone.utc)
            upsert_stmt = text(
                """
                INSERT INTO hermes_appetite_profiles (
                    id, carrier_id, state, line,
                    eligible_classes, ineligible_classes,
                    last_rate_change_pct, last_rate_change_date,
                    rate_competitiveness_index, source_filing_count,
                    computed_at, is_current, created_at, updated_at
                ) VALUES (
                    :id, :carrier_id, :state, :line,
                    :eligible_classes::jsonb, :ineligible_classes::jsonb,
                    :last_rate_change_pct, :last_rate_change_date,
                    :rate_competitiveness_index, :source_filing_count,
                    :computed_at, TRUE, :computed_at, :computed_at
                )
                ON CONFLICT (carrier_id, state, line) DO UPDATE SET
                    eligible_classes = EXCLUDED.eligible_classes,
                    ineligible_classes = EXCLUDED.ineligible_classes,
                    last_rate_change_pct = EXCLUDED.last_rate_change_pct,
                    last_rate_change_date = EXCLUDED.last_rate_change_date,
                    rate_competitiveness_index = EXCLUDED.rate_competitiveness_index,
                    source_filing_count = EXCLUDED.source_filing_count,
                    computed_at = EXCLUDED.computed_at,
                    updated_at = EXCLUDED.updated_at
                """
            )

            import json

            await session.execute(
                upsert_stmt,
                {
                    "id": str(uuid.uuid4()),
                    "carrier_id": str(carrier_id),
                    "state": state,
                    "line": line,
                    "eligible_classes": json.dumps(eligible_classes),
                    "ineligible_classes": json.dumps(ineligible_classes),
                    "last_rate_change_pct": (
                        float(last_rate_change_pct) if last_rate_change_pct else None
                    ),
                    "last_rate_change_date": last_rate_change_date,
                    "rate_competitiveness_index": rate_competitiveness_index,
                    "source_filing_count": source_filing_count,
                    "computed_at": now,
                },
            )

            # Recompute carrier rankings for each class code
            await self._recompute_rankings(
                session, carrier_id, state, line, eligible_classes
            )

            await session.commit()

        logger.info(
            "Appetite profile recomputed: carrier=%s state=%s line=%s "
            "eligible_classes=%d",
            carrier_id,
            state,
            line,
            len(eligible_classes),
        )

    # ------------------------------------------------------------------
    # Private helpers — detection
    # ------------------------------------------------------------------

    async def _detect_rate_changes(
        self,
        session: AsyncSession,
        carrier: Any,
        state: str,
        line: str,
        profile: Any,
        latest_filing: Any,
    ) -> list[AppetiteShift]:
        """Detect significant rate increases or decreases from the latest filing."""
        shifts: list[AppetiteShift] = []

        if not latest_filing.overall_rate_change_pct:
            return shifts

        new_rate = Decimal(str(latest_filing.overall_rate_change_pct))
        old_rate = profile.last_rate_change_pct

        if new_rate <= RATE_DECREASE_THRESHOLD:
            strength = min(10, int(abs(new_rate) / 2))
            shift = AppetiteShift(
                carrier_id=carrier.id,
                carrier_name=carrier.legal_name,
                state=state,
                line=line,
                signal_type=self.RATE_DECREASE,
                signal_strength=max(1, strength),
                description=(
                    f"{carrier.legal_name} filed a {new_rate:+.2f}% rate decrease "
                    f"in {state} for {line}."
                ),
                source_filing_id=latest_filing.id,
                old_value=str(old_rate) if old_rate is not None else None,
                new_value=str(new_rate),
            )
            shifts.append(shift)

        elif new_rate >= RATE_INCREASE_THRESHOLD:
            strength = min(10, int(new_rate / 3))
            shift = AppetiteShift(
                carrier_id=carrier.id,
                carrier_name=carrier.legal_name,
                state=state,
                line=line,
                signal_type=self.RATE_INCREASE,
                signal_strength=max(1, strength),
                description=(
                    f"{carrier.legal_name} filed a {new_rate:+.2f}% rate increase "
                    f"in {state} for {line}."
                ),
                source_filing_id=latest_filing.id,
                old_value=str(old_rate) if old_rate is not None else None,
                new_value=str(new_rate),
            )
            shifts.append(shift)

        return shifts

    async def _detect_class_code_changes(
        self,
        session: AsyncSession,
        carrier: Any,
        state: str,
        line: str,
        profile: Any,
        latest_filing: Any,
    ) -> list[AppetiteShift]:
        """Compare current class codes in rate tables against stored eligible_classes."""
        shifts: list[AppetiteShift] = []

        current_classes = await self._gather_eligible_classes(
            session, carrier.id, state, line
        )
        current_set = set(current_classes)
        prior_set = set(profile.eligible_classes or [])

        added = current_set - prior_set
        removed = prior_set - current_set

        if added:
            shift = AppetiteShift(
                carrier_id=carrier.id,
                carrier_name=carrier.legal_name,
                state=state,
                line=line,
                signal_type=self.NEW_CLASS_CODES,
                signal_strength=min(10, max(1, len(added))),
                description=(
                    f"{carrier.legal_name} added {len(added)} new class code(s) "
                    f"in {state} for {line}: {', '.join(sorted(added)[:10])}"
                ),
                source_filing_id=latest_filing.id,
                old_value=str(sorted(prior_set)),
                new_value=str(sorted(added)),
            )
            shifts.append(shift)

        if removed:
            shift = AppetiteShift(
                carrier_id=carrier.id,
                carrier_name=carrier.legal_name,
                state=state,
                line=line,
                signal_type=self.REMOVED_CLASS_CODES,
                signal_strength=min(10, max(1, len(removed) + 2)),
                description=(
                    f"{carrier.legal_name} removed {len(removed)} class code(s) "
                    f"in {state} for {line}: {', '.join(sorted(removed)[:10])}"
                ),
                source_filing_id=latest_filing.id,
                old_value=str(sorted(removed)),
                new_value=str(sorted(current_set)),
            )
            shifts.append(shift)

        return shifts

    async def _detect_withdrawals(
        self,
        session: AsyncSession,
        carrier: Any,
        state: str,
        line: str,
        profile: Any,
    ) -> list[AppetiteShift]:
        """Detect withdrawn filings that indicate a market exit signal."""
        shifts: list[AppetiteShift] = []

        stmt = text(
            """
            SELECT id, serff_tracking_number, effective_date
            FROM hermes_filings
            WHERE carrier_id = :carrier_id
              AND state = :state
              AND line_of_business = :line
              AND status = 'withdrawn'
              AND updated_at >= NOW() - INTERVAL '7 days'
            ORDER BY updated_at DESC
            LIMIT 5
            """
        )
        result = await session.execute(
            stmt,
            {
                "carrier_id": str(carrier.id),
                "state": state,
                "line": line,
            },
        )
        withdrawn = result.fetchall()

        if withdrawn:
            shift = AppetiteShift(
                carrier_id=carrier.id,
                carrier_name=carrier.legal_name,
                state=state,
                line=line,
                signal_type=self.FILING_WITHDRAWAL,
                signal_strength=min(10, max(5, len(withdrawn) + 3)),
                description=(
                    f"{carrier.legal_name} withdrew {len(withdrawn)} filing(s) "
                    f"in {state} for {line}. "
                    f"SERFF#: {', '.join(r.serff_tracking_number for r in withdrawn[:3])}"
                ),
                source_filing_id=withdrawn[0].id,
                old_value="active",
                new_value="withdrawn",
            )
            shifts.append(shift)

        return shifts

    async def _detect_territory_expansions(
        self,
        session: AsyncSession,
        carrier: Any,
        state: str,
        line: str,
        profile: Any,
        latest_filing: Any,
    ) -> list[AppetiteShift]:
        """Detect new territories added in the latest rate table."""
        shifts: list[AppetiteShift] = []

        prior_territories = set(
            (profile.territory_preferences or {}).keys()
        )

        # Get territories from the latest rate table
        stmt = text(
            """
            SELECT DISTINCT td.territory_code
            FROM hermes_territory_definitions td
            JOIN hermes_rate_tables rt ON rt.id = td.rate_table_id
            WHERE rt.filing_id = :filing_id
            """
        )
        result = await session.execute(stmt, {"filing_id": str(latest_filing.id)})
        current_territories = {row.territory_code for row in result.fetchall()}

        new_territories = current_territories - prior_territories
        if new_territories:
            shift = AppetiteShift(
                carrier_id=carrier.id,
                carrier_name=carrier.legal_name,
                state=state,
                line=line,
                signal_type=self.TERRITORY_EXPANSION,
                signal_strength=min(10, max(3, len(new_territories) + 2)),
                description=(
                    f"{carrier.legal_name} expanded into {len(new_territories)} "
                    f"new territory/territories in {state} for {line}: "
                    f"{', '.join(sorted(new_territories)[:10])}"
                ),
                source_filing_id=latest_filing.id,
                old_value=str(sorted(prior_territories)),
                new_value=str(sorted(new_territories)),
            )
            shifts.append(shift)

        return shifts

    # ------------------------------------------------------------------
    # Private helpers — data access
    # ------------------------------------------------------------------

    async def _get_carrier(self, session: AsyncSession, carrier_id: UUID) -> Any:
        """Fetch a Carrier ORM record by primary key."""
        stmt = select(Carrier).where(Carrier.id == carrier_id)
        result = await session.execute(stmt)
        return result.scalar_one_or_none()

    async def _get_profile(
        self, session: AsyncSession, carrier_id: UUID, state: str, line: str
    ) -> Any:
        """Fetch the current AppetiteProfile for a carrier/state/line."""
        stmt = select(AppetiteProfile).where(
            and_(
                AppetiteProfile.carrier_id == carrier_id,
                AppetiteProfile.state == state,
                AppetiteProfile.line == line,
                AppetiteProfile.is_current == True,  # noqa: E712
            )
        )
        result = await session.execute(stmt)
        return result.scalar_one_or_none()

    async def _get_latest_filing(
        self, session: AsyncSession, carrier_id: UUID, state: str, line: str
    ) -> Any:
        """Fetch the most recently approved filing for a carrier/state/line."""
        stmt = text(
            """
            SELECT *
            FROM hermes_filings
            WHERE carrier_id = :carrier_id
              AND state = :state
              AND line_of_business = :line
              AND status IN ('approved', 'effective')
            ORDER BY effective_date DESC NULLS LAST, filed_date DESC NULLS LAST
            LIMIT 1
            """
        )
        result = await session.execute(
            stmt,
            {
                "carrier_id": str(carrier_id),
                "state": state,
                "line": line,
            },
        )
        return result.fetchone()

    async def _gather_eligible_classes(
        self, session: AsyncSession, carrier_id: UUID, state: str, line: str
    ) -> list[str]:
        """Collect all unique carrier class codes from active rate tables."""
        stmt = text(
            """
            SELECT DISTINCT ccm.carrier_class_code
            FROM hermes_class_code_mappings ccm
            JOIN hermes_rate_tables rt ON rt.id = ccm.rate_table_id
            WHERE rt.carrier_id = :carrier_id
              AND rt.state = :state
              AND rt.line = :line
              AND rt.is_current = TRUE
              AND ccm.eligibility_status = 'eligible'
            ORDER BY ccm.carrier_class_code
            """
        )
        result = await session.execute(
            stmt,
            {"carrier_id": str(carrier_id), "state": state, "line": line},
        )
        return [row.carrier_class_code for row in result.fetchall()]

    async def _gather_ineligible_classes(
        self, session: AsyncSession, carrier_id: UUID, state: str, line: str
    ) -> list[str]:
        """Collect class codes marked ineligible in underwriting rules."""
        stmt = text(
            """
            SELECT DISTINCT ccm.carrier_class_code
            FROM hermes_class_code_mappings ccm
            JOIN hermes_rate_tables rt ON rt.id = ccm.rate_table_id
            WHERE rt.carrier_id = :carrier_id
              AND rt.state = :state
              AND rt.line = :line
              AND rt.is_current = TRUE
              AND ccm.eligibility_status = 'ineligible'
            ORDER BY ccm.carrier_class_code
            """
        )
        result = await session.execute(
            stmt,
            {"carrier_id": str(carrier_id), "state": state, "line": line},
        )
        return [row.carrier_class_code for row in result.fetchall()]

    async def _compute_competitiveness(
        self, session: AsyncSession, carrier_id: UUID, state: str, line: str
    ) -> Optional[float]:
        """Compute rate competitiveness index (0-100) relative to peer carriers.

        Compares average base rates for this carrier against the market average
        for the same state/line.  Lower rates = higher competitiveness index.
        Returns None if insufficient data.
        """
        # Get this carrier's average base rate
        own_stmt = text(
            """
            SELECT AVG(br.base_rate) AS avg_rate
            FROM hermes_base_rates br
            JOIN hermes_rate_tables rt ON rt.id = br.rate_table_id
            WHERE rt.carrier_id = :carrier_id
              AND rt.state = :state
              AND rt.line = :line
              AND rt.is_current = TRUE
            """
        )
        own_result = await session.execute(
            own_stmt,
            {"carrier_id": str(carrier_id), "state": state, "line": line},
        )
        own_row = own_result.fetchone()
        own_avg = own_row.avg_rate if own_row and own_row.avg_rate else None

        if own_avg is None:
            return None

        # Get market average base rate
        market_stmt = text(
            """
            SELECT AVG(br.base_rate) AS market_avg
            FROM hermes_base_rates br
            JOIN hermes_rate_tables rt ON rt.id = br.rate_table_id
            WHERE rt.state = :state
              AND rt.line = :line
              AND rt.is_current = TRUE
            """
        )
        market_result = await session.execute(
            market_stmt, {"state": state, "line": line}
        )
        market_row = market_result.fetchone()
        market_avg = market_row.market_avg if market_row and market_row.market_avg else None

        if market_avg is None or float(market_avg) == 0:
            return None

        # Lower rate vs market = higher index.  Capped at 0-100.
        ratio = float(own_avg) / float(market_avg)
        index = max(0.0, min(100.0, (2.0 - ratio) * 50.0))
        return round(index, 2)

    async def _recompute_rankings(
        self,
        session: AsyncSession,
        carrier_id: UUID,
        state: str,
        line: str,
        class_codes: list[str],
    ) -> None:
        """Update hermes_carrier_rankings for each class code.

        Fetches all carriers active in the same state/line and ranks them
        by their estimated_premium_index (lower = better) for each class code.
        """
        if not class_codes:
            return

        now = datetime.now(timezone.utc)

        for class_code in class_codes[:50]:  # cap to avoid excessive writes
            # Get the estimated premium index for this carrier/class
            idx_stmt = text(
                """
                SELECT AVG(br.base_rate) AS avg_rate
                FROM hermes_base_rates br
                JOIN hermes_rate_tables rt ON rt.id = br.rate_table_id
                WHERE rt.carrier_id = :carrier_id
                  AND rt.state = :state
                  AND rt.line = :line
                  AND rt.is_current = TRUE
                  AND br.class_code = :class_code
                """
            )
            idx_result = await session.execute(
                idx_stmt,
                {
                    "carrier_id": str(carrier_id),
                    "state": state,
                    "line": line,
                    "class_code": class_code,
                },
            )
            idx_row = idx_result.fetchone()
            premium_index = (
                float(idx_row.avg_rate) if idx_row and idx_row.avg_rate else None
            )

            # Determine rank among all carriers for this state/line/class
            rank_stmt = text(
                """
                SELECT COUNT(*) + 1 AS rank
                FROM hermes_carrier_rankings
                WHERE state = :state
                  AND line = :line
                  AND class_code = :class_code
                  AND estimated_premium_index < :own_index
                """
            )
            rank = 1
            if premium_index is not None:
                rank_result = await session.execute(
                    rank_stmt,
                    {
                        "state": state,
                        "line": line,
                        "class_code": class_code,
                        "own_index": premium_index,
                    },
                )
                rank_row = rank_result.fetchone()
                rank = rank_row.rank if rank_row else 1

            upsert_stmt = text(
                """
                INSERT INTO hermes_carrier_rankings (
                    id, state, line, class_code, carrier_id,
                    rank, estimated_premium_index, computed_at, updated_at
                ) VALUES (
                    :id, :state, :line, :class_code, :carrier_id,
                    :rank, :premium_index, :now, :now
                )
                ON CONFLICT (state, line, class_code, carrier_id) DO UPDATE SET
                    rank = EXCLUDED.rank,
                    estimated_premium_index = EXCLUDED.estimated_premium_index,
                    updated_at = EXCLUDED.updated_at
                """
            )
            await session.execute(
                upsert_stmt,
                {
                    "id": str(uuid.uuid4()),
                    "state": state,
                    "line": line,
                    "class_code": class_code,
                    "carrier_id": str(carrier_id),
                    "rank": rank,
                    "premium_index": premium_index,
                    "now": now,
                },
            )

    async def _persist_signal(
        self, session: AsyncSession, shift: AppetiteShift, profile: Any
    ) -> None:
        """Write an AppetiteSignal row to ``hermes_appetite_signals``.

        If no profile exists yet (new state entry), the profile_id is set to
        a placeholder that is resolved after the upsert in
        :meth:`recompute_appetite`.

        Args:
            session: Active SQLAlchemy async session.
            shift: The :class:`AppetiteShift` to persist.
            profile: The ``AppetiteProfile`` ORM instance, or None.
        """
        if profile is None:
            # We cannot FK to a profile that doesn't exist yet.
            # Skip DB insert; the signal will be captured after profile creation.
            logger.debug(
                "Skipping signal persist (no profile): carrier=%s state=%s line=%s",
                shift.carrier_id,
                shift.state,
                shift.line,
            )
            return

        stmt = text(
            """
            INSERT INTO hermes_appetite_signals (
                id, profile_id, carrier_id, state, line,
                signal_type, signal_strength, signal_date,
                signal_description, source_filing_id,
                confidence, acknowledged, created_at
            ) VALUES (
                :id, :profile_id, :carrier_id, :state, :line,
                :signal_type, :signal_strength, :signal_date,
                :signal_description, :source_filing_id,
                1.0, FALSE, NOW()
            )
            """
        )
        await session.execute(
            stmt,
            {
                "id": str(uuid.uuid4()),
                "profile_id": str(profile.id),
                "carrier_id": str(shift.carrier_id),
                "state": shift.state,
                "line": shift.line,
                "signal_type": shift.signal_type,
                "signal_strength": shift.signal_strength,
                "signal_date": date.today(),
                "signal_description": shift.description,
                "source_filing_id": (
                    str(shift.source_filing_id) if shift.source_filing_id else None
                ),
            },
        )
