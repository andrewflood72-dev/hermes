"""Hermes Market Report Generator — Structured market intelligence for state/line.

Analyses SERFF filing data over a configurable rolling window to produce
:class:`MarketReport` snapshots covering rate trends, new entrants, withdrawals,
and overall market direction.  Reports are persisted to
``hermes_market_intelligence`` for historical trending.
"""

from __future__ import annotations

import logging
import statistics
import uuid
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Optional
from uuid import UUID

from pydantic import BaseModel, Field
from sqlalchemy import text

from hermes.config import settings
from hermes.db import async_session

logger = logging.getLogger("hermes.monitoring.market_report")

# ---------------------------------------------------------------------------
# Pydantic model
# ---------------------------------------------------------------------------


class MarketReport(BaseModel):
    """A structured market intelligence report for a state/line/period.

    Attributes:
        state: Two-letter state code.
        line: Line of business.
        period_start: First day of the analysis window.
        period_end: Last day of the analysis window (inclusive).
        avg_rate_change: Average overall_rate_change_pct across all filings.
        median_rate_change: Median of the same distribution.
        filing_count: Total filings in the period.
        rate_increases: Count of filings with positive rate changes.
        rate_decreases: Count of filings with negative rate changes.
        new_entrants: List of carrier names filing in this state/line for the
            first time (no prior filings exist before period_start).
        withdrawals: List of carrier names that withdrew filings in the period.
        market_trend: Classification: hardening / softening / stable / mixed.
        top_signals: Up to 10 notable appetite shift signals in the period.
        summary: Narrative paragraph summarising the market situation.
    """

    state: str
    line: str
    period_start: date
    period_end: date
    avg_rate_change: Optional[float] = None
    median_rate_change: Optional[float] = None
    filing_count: int = 0
    rate_increases: int = 0
    rate_decreases: int = 0
    new_entrants: list[str] = Field(default_factory=list)
    withdrawals: list[str] = Field(default_factory=list)
    market_trend: str = "stable"
    top_signals: list[dict] = Field(default_factory=list)
    summary: str = ""


# ---------------------------------------------------------------------------
# MarketReportGenerator
# ---------------------------------------------------------------------------


class MarketReportGenerator:
    """Generates and persists structured market intelligence reports.

    Reports are computed from ``hermes_filings`` and ``hermes_appetite_signals``
    data and stored in ``hermes_market_intelligence`` for downstream consumption
    by the Hermes API and the CLI ``market-report`` command.

    Typical usage::

        generator = MarketReportGenerator()
        report = await generator.generate_report("TX", "Commercial Auto", period_days=30)
        trend = await generator.get_trend("TX", "Commercial Auto")
    """

    def __init__(self) -> None:
        self._session_factory = async_session

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def generate_report(
        self, state: str, line: str, period_days: int = 30
    ) -> MarketReport:
        """Compute a market intelligence report and persist it.

        Analyses all SERFF filings and appetite signals for the given
        ``state`` / ``line`` combination over the past ``period_days`` days.

        Computed metrics:
        - Average and median overall_rate_change_pct
        - Count of rate increases vs decreases
        - New market entrants (carriers with no prior filings before window)
        - Market withdrawals (carriers with 'withdrawn' filings in window)
        - Market trend classification
        - Top 10 appetite signals by signal_strength

        The result is upserted to ``hermes_market_intelligence``.

        Args:
            state: Two-letter state code (e.g. ``"TX"``).
            line: Line of business string (e.g. ``"Commercial Auto"``).
            period_days: Number of days to look back.  Defaults to 30.

        Returns:
            Populated :class:`MarketReport` instance.
        """
        period_end = date.today()
        period_start = period_end - timedelta(days=period_days)

        logger.info(
            "Generating market report: state=%s line=%s period=%s to %s",
            state,
            line,
            period_start,
            period_end,
        )

        async with self._session_factory() as session:
            # --- Filing statistics ---
            filings_stmt = text(
                """
                SELECT
                    overall_rate_change_pct,
                    status,
                    carrier_id,
                    carrier_name_filed
                FROM hermes_filings
                WHERE state = :state
                  AND line_of_business = :line
                  AND filed_date BETWEEN :period_start AND :period_end
                """
            )
            filings_result = await session.execute(
                filings_stmt,
                {"state": state, "line": line, "period_start": period_start, "period_end": period_end},
            )
            filings = filings_result.fetchall()

            filing_count = len(filings)
            rate_changes: list[float] = []
            rate_increases = 0
            rate_decreases = 0
            withdrawn_carrier_ids: set[str] = set()

            for f in filings:
                if f.overall_rate_change_pct is not None:
                    pct = float(f.overall_rate_change_pct)
                    rate_changes.append(pct)
                    if pct > 0:
                        rate_increases += 1
                    elif pct < 0:
                        rate_decreases += 1
                if f.status == "withdrawn" and f.carrier_id:
                    withdrawn_carrier_ids.add(str(f.carrier_id))

            avg_rate_change = (
                round(sum(rate_changes) / len(rate_changes), 4)
                if rate_changes
                else None
            )
            median_rate_change = (
                round(statistics.median(rate_changes), 4) if len(rate_changes) > 1 else avg_rate_change
            )

            # --- New entrants ---
            new_entrants = await self._find_new_entrants(
                session, state, line, period_start, period_end
            )

            # --- Withdrawals ---
            withdrawals = await self._find_withdrawals(
                session, state, line, period_start, period_end
            )

            # --- Market trend classification ---
            market_trend = self._classify_trend(
                avg_rate_change=avg_rate_change,
                rate_increases=rate_increases,
                rate_decreases=rate_decreases,
                new_entrants_count=len(new_entrants),
                withdrawal_count=len(withdrawals),
            )

            # --- Top appetite signals ---
            top_signals = await self._fetch_top_signals(
                session, state, line, period_start, period_end
            )

            # --- Summary narrative ---
            summary = self._generate_summary(
                state=state,
                line=line,
                period_start=period_start,
                period_end=period_end,
                filing_count=filing_count,
                avg_rate_change=avg_rate_change,
                market_trend=market_trend,
                new_entrants=new_entrants,
                withdrawals=withdrawals,
                rate_increases=rate_increases,
                rate_decreases=rate_decreases,
            )

            report = MarketReport(
                state=state,
                line=line,
                period_start=period_start,
                period_end=period_end,
                avg_rate_change=avg_rate_change,
                median_rate_change=median_rate_change,
                filing_count=filing_count,
                rate_increases=rate_increases,
                rate_decreases=rate_decreases,
                new_entrants=new_entrants,
                withdrawals=withdrawals,
                market_trend=market_trend,
                top_signals=top_signals,
                summary=summary,
            )

            # Persist to hermes_market_intelligence
            await self._persist_report(session, report)
            await session.commit()

        logger.info(
            "Market report complete: state=%s line=%s trend=%s filings=%d",
            state,
            line,
            market_trend,
            filing_count,
        )
        return report

    async def get_trend(self, state: str, line: str) -> str:
        """Return a quick market trend classification without a full report.

        Looks up the most recent ``hermes_market_intelligence`` record for the
        state/line.  Falls back to computing a fresh 30-day trend if none exists.

        Args:
            state: Two-letter state code.
            line: Line of business string.

        Returns:
            One of: ``"hardening"``, ``"softening"``, ``"stable"``, ``"mixed"``.
        """
        async with self._session_factory() as session:
            stmt = text(
                """
                SELECT market_trend
                FROM hermes_market_intelligence
                WHERE state = :state AND line = :line
                ORDER BY computed_at DESC
                LIMIT 1
                """
            )
            result = await session.execute(stmt, {"state": state, "line": line})
            row = result.fetchone()

        if row and row.market_trend:
            return row.market_trend

        # No stored report — generate a quick estimate from raw filings
        async with self._session_factory() as session:
            period_start = date.today() - timedelta(days=30)
            quick_stmt = text(
                """
                SELECT
                    AVG(NULLIF(overall_rate_change_pct, 0)) AS avg_rate,
                    SUM(CASE WHEN overall_rate_change_pct > 0 THEN 1 ELSE 0 END) AS increases,
                    SUM(CASE WHEN overall_rate_change_pct < 0 THEN 1 ELSE 0 END) AS decreases
                FROM hermes_filings
                WHERE state = :state
                  AND line_of_business = :line
                  AND filed_date >= :period_start
                """
            )
            result = await session.execute(
                quick_stmt, {"state": state, "line": line, "period_start": period_start}
            )
            row = result.fetchone()

        avg = float(row.avg_rate) if row and row.avg_rate else 0.0
        increases = int(row.increases or 0) if row else 0
        decreases = int(row.decreases or 0) if row else 0

        return self._classify_trend(
            avg_rate_change=avg if avg != 0.0 else None,
            rate_increases=increases,
            rate_decreases=decreases,
            new_entrants_count=0,
            withdrawal_count=0,
        )

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    async def _find_new_entrants(
        self,
        session,
        state: str,
        line: str,
        period_start: date,
        period_end: date,
    ) -> list[str]:
        """Find carriers that filed in this state/line for the first time."""
        stmt = text(
            """
            SELECT DISTINCT
                COALESCE(c.legal_name, f.carrier_name_filed) AS carrier_name
            FROM hermes_filings f
            LEFT JOIN hermes_carriers c ON c.id = f.carrier_id
            WHERE f.state = :state
              AND f.line_of_business = :line
              AND f.filed_date BETWEEN :period_start AND :period_end
              AND f.status NOT IN ('withdrawn', 'disapproved')
              AND f.carrier_id NOT IN (
                  SELECT DISTINCT carrier_id
                  FROM hermes_filings
                  WHERE state = :state
                    AND line_of_business = :line
                    AND filed_date < :period_start
                    AND carrier_id IS NOT NULL
              )
            ORDER BY carrier_name
            """
        )
        result = await session.execute(
            stmt,
            {
                "state": state,
                "line": line,
                "period_start": period_start,
                "period_end": period_end,
            },
        )
        return [row.carrier_name for row in result.fetchall() if row.carrier_name]

    async def _find_withdrawals(
        self,
        session,
        state: str,
        line: str,
        period_start: date,
        period_end: date,
    ) -> list[str]:
        """Find carriers that withdrew filings in the period."""
        stmt = text(
            """
            SELECT DISTINCT
                COALESCE(c.legal_name, f.carrier_name_filed) AS carrier_name
            FROM hermes_filings f
            LEFT JOIN hermes_carriers c ON c.id = f.carrier_id
            WHERE f.state = :state
              AND f.line_of_business = :line
              AND f.status = 'withdrawn'
              AND f.updated_at::date BETWEEN :period_start AND :period_end
            ORDER BY carrier_name
            """
        )
        result = await session.execute(
            stmt,
            {
                "state": state,
                "line": line,
                "period_start": period_start,
                "period_end": period_end,
            },
        )
        return [row.carrier_name for row in result.fetchall() if row.carrier_name]

    async def _fetch_top_signals(
        self,
        session,
        state: str,
        line: str,
        period_start: date,
        period_end: date,
    ) -> list[dict]:
        """Fetch top 10 appetite signals in the period by signal_strength."""
        stmt = text(
            """
            SELECT
                s.signal_type,
                s.signal_strength,
                s.signal_description,
                s.signal_date,
                c.legal_name AS carrier_name
            FROM hermes_appetite_signals s
            JOIN hermes_carriers c ON c.id = s.carrier_id
            WHERE s.state = :state
              AND s.line = :line
              AND s.signal_date BETWEEN :period_start AND :period_end
            ORDER BY s.signal_strength DESC, s.signal_date DESC
            LIMIT 10
            """
        )
        result = await session.execute(
            stmt,
            {
                "state": state,
                "line": line,
                "period_start": period_start,
                "period_end": period_end,
            },
        )
        return [
            {
                "signal_type": row.signal_type,
                "signal_strength": float(row.signal_strength),
                "description": row.signal_description,
                "signal_date": str(row.signal_date),
                "carrier_name": row.carrier_name,
            }
            for row in result.fetchall()
        ]

    def _classify_trend(
        self,
        avg_rate_change: Optional[float],
        rate_increases: int,
        rate_decreases: int,
        new_entrants_count: int,
        withdrawal_count: int,
    ) -> str:
        """Classify overall market direction.

        Rules:
        - hardening: avg rate change > 5% OR withdrawals > new entrants by 2+
        - softening: avg rate change < -5% OR new entrants > withdrawals by 2+
        - mixed: both increases and decreases roughly equal (neither > 60% of total)
        - stable: small rate changes, balanced filing activity

        Returns:
            One of: ``"hardening"``, ``"softening"``, ``"stable"``, ``"mixed"``.
        """
        total = rate_increases + rate_decreases

        if avg_rate_change is not None and avg_rate_change > 5.0:
            return "hardening"
        if avg_rate_change is not None and avg_rate_change < -5.0:
            return "softening"
        if withdrawal_count >= new_entrants_count + 2:
            return "hardening"
        if new_entrants_count >= withdrawal_count + 2:
            return "softening"
        if total > 0:
            increase_pct = rate_increases / total
            if increase_pct > 0.6:
                return "hardening"
            if increase_pct < 0.4:
                return "softening"
            if 0.35 <= increase_pct <= 0.65 and total >= 5:
                return "mixed"
        return "stable"

    def _generate_summary(
        self,
        state: str,
        line: str,
        period_start: date,
        period_end: date,
        filing_count: int,
        avg_rate_change: Optional[float],
        market_trend: str,
        new_entrants: list[str],
        withdrawals: list[str],
        rate_increases: int,
        rate_decreases: int,
    ) -> str:
        """Generate a narrative market summary paragraph.

        Args:
            All computed report fields.

        Returns:
            A 3–5 sentence plain-text summary suitable for display in reports
            and digest emails.
        """
        trend_desc = {
            "hardening": "hardening (rates rising, capacity tightening)",
            "softening": "softening (rates declining, capacity expanding)",
            "stable": "stable with limited directional pressure",
            "mixed": "mixed with divergent carrier strategies",
        }.get(market_trend, market_trend)

        rate_desc = ""
        if avg_rate_change is not None:
            direction = "increase" if avg_rate_change > 0 else "decrease"
            rate_desc = (
                f" The average rate {direction} was {abs(avg_rate_change):.2f}%,"
                f" with {rate_increases} carriers raising rates and"
                f" {rate_decreases} reducing rates."
            )

        entrant_desc = ""
        if new_entrants:
            names = ", ".join(new_entrants[:3])
            suffix = f" and {len(new_entrants) - 3} others" if len(new_entrants) > 3 else ""
            entrant_desc = f" New market entrants include: {names}{suffix}."

        withdrawal_desc = ""
        if withdrawals:
            names = ", ".join(withdrawals[:3])
            suffix = f" and {len(withdrawals) - 3} others" if len(withdrawals) > 3 else ""
            withdrawal_desc = f" Market withdrawals noted for: {names}{suffix}."

        return (
            f"The {state} {line} market for the period "
            f"{period_start.isoformat()} to {period_end.isoformat()} is {trend_desc}. "
            f"A total of {filing_count} SERFF filing(s) were analysed."
            f"{rate_desc}{entrant_desc}{withdrawal_desc}"
        )

    async def _persist_report(self, session, report: MarketReport) -> None:
        """Upsert the market report to ``hermes_market_intelligence``.

        Args:
            session: Active SQLAlchemy async session.
            report: Completed :class:`MarketReport` to persist.
        """
        import json

        stmt = text(
            """
            INSERT INTO hermes_market_intelligence (
                id, state, line,
                period_start, period_end,
                avg_rate_change_pct, median_rate_change_pct,
                filing_count, rate_increase_count, rate_decrease_count,
                new_entrant_count, withdrawal_count,
                new_entrants, withdrawals,
                top_appetite_shifts,
                market_trend, summary, computed_at
            ) VALUES (
                :id, :state, :line,
                :period_start, :period_end,
                :avg_rate_change, :median_rate_change,
                :filing_count, :rate_increases, :rate_decreases,
                :new_entrant_count, :withdrawal_count,
                :new_entrants::jsonb, :withdrawals::jsonb,
                :top_signals::jsonb,
                :market_trend, :summary, NOW()
            )
            ON CONFLICT DO NOTHING
            """
        )
        await session.execute(
            stmt,
            {
                "id": str(uuid.uuid4()),
                "state": report.state,
                "line": report.line,
                "period_start": report.period_start,
                "period_end": report.period_end,
                "avg_rate_change": report.avg_rate_change,
                "median_rate_change": report.median_rate_change,
                "filing_count": report.filing_count,
                "rate_increases": report.rate_increases,
                "rate_decreases": report.rate_decreases,
                "new_entrant_count": len(report.new_entrants),
                "withdrawal_count": len(report.withdrawals),
                "new_entrants": json.dumps(report.new_entrants),
                "withdrawals": json.dumps(report.withdrawals),
                "top_signals": json.dumps(report.top_signals),
                "market_trend": report.market_trend,
                "summary": report.summary,
            },
        )
        logger.debug(
            "Persisted market report: state=%s line=%s trend=%s",
            report.state,
            report.line,
            report.market_trend,
        )
