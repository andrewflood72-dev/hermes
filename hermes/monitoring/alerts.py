"""Hermes Alert Manager — Actionable alerts for appetite shifts and market events.

When :class:`ChangeDetector` surfaces an :class:`AppetiteShift`, the
:class:`AlertManager` evaluates whether active submissions in the pipeline
(visible via the Mozart integration schema) should be re-evaluated, and
generates structured :class:`Alert` objects for downstream consumption.

Alerts are persisted to ``hermes_appetite_signals`` (acknowledged field) and
can be queried, acknowledged, and digested via this manager.
"""

from __future__ import annotations

import logging
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Literal, Optional
from uuid import UUID

from pydantic import BaseModel, Field
from sqlalchemy import text

from hermes.config import settings
from hermes.db import async_session
from hermes.monitoring.change_detector import AppetiteShift

logger = logging.getLogger("hermes.monitoring.alerts")

# ---------------------------------------------------------------------------
# Pydantic model
# ---------------------------------------------------------------------------

AlertType = Literal["appetite_shift", "rate_change", "market_entry", "market_exit"]
AlertSeverity = Literal["high", "medium", "low"]


class Alert(BaseModel):
    """A structured actionable alert surfaced by the monitoring system.

    Attributes:
        id: Unique UUID for this alert (mirrors ``hermes_appetite_signals.id``).
        alert_type: One of appetite_shift, rate_change, market_entry, market_exit.
        severity: high / medium / low, derived from signal_strength.
        carrier_name: Human-readable carrier legal name.
        state: Two-letter state code affected.
        line: Line of business affected.
        description: Free-text summary of the underlying shift.
        action_recommended: Suggested action for the broker/underwriter.
        created_at: UTC timestamp when the signal was created.
        acknowledged: Whether a user has acknowledged this alert.
    """

    id: UUID
    alert_type: AlertType
    severity: AlertSeverity
    carrier_name: str
    state: str
    line: str
    description: str
    action_recommended: str
    created_at: datetime
    acknowledged: bool = False


# ---------------------------------------------------------------------------
# Helper maps
# ---------------------------------------------------------------------------

_SIGNAL_TYPE_TO_ALERT_TYPE: dict[str, AlertType] = {
    "rate_decrease": "rate_change",
    "rate_increase": "rate_change",
    "expanded_classes": "appetite_shift",
    "contracted_classes": "appetite_shift",
    "new_state_entry": "market_entry",
    "filing_withdrawal": "market_exit",
    "territory_expansion": "appetite_shift",
}

_SIGNAL_TYPE_TO_ACTION: dict[str, str] = {
    "rate_decrease": (
        "Re-market active submissions for this carrier/state/line — "
        "rates may now be more competitive than quoted."
    ),
    "rate_increase": (
        "Review active quotes; new rates may be uncompetitive. "
        "Consider re-marketing to alternative carriers."
    ),
    "expanded_classes": (
        "New class codes now eligible — review pending declines that may qualify."
    ),
    "contracted_classes": (
        "Some class codes no longer eligible — check active submissions for impacted classes."
    ),
    "new_state_entry": (
        "New carrier option available in this state — add to future submissions."
    ),
    "filing_withdrawal": (
        "Carrier withdrawing from market — identify and re-market affected renewals immediately."
    ),
    "territory_expansion": (
        "Carrier expanded territory appetite — risks in newly covered areas can now be submitted."
    ),
}


def _strength_to_severity(signal_strength: int) -> AlertSeverity:
    """Convert a 1–10 signal strength score to a severity label."""
    if signal_strength >= 7:
        return "high"
    if signal_strength >= 4:
        return "medium"
    return "low"


def _signal_type_to_alert_type(signal_type: str) -> AlertType:
    """Map a hermes signal_type string to an AlertType literal."""
    return _SIGNAL_TYPE_TO_ALERT_TYPE.get(signal_type, "appetite_shift")


# ---------------------------------------------------------------------------
# AlertManager
# ---------------------------------------------------------------------------


class AlertManager:
    """Generates, stores, and serves alerts derived from appetite shifts.

    Alerts are backed by the ``hermes_appetite_signals`` table — specifically
    the ``acknowledged`` flag and the signal metadata.  No separate alerts
    table is required.

    Typical usage::

        manager = AlertManager()
        alerts = await manager.check_submission_impacts(shifts)
        digest = await manager.generate_daily_digest()
    """

    def __init__(self) -> None:
        self._session_factory = async_session

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def check_submission_impacts(
        self, shifts: list[AppetiteShift]
    ) -> list[Alert]:
        """Evaluate appetite shifts against the active submission pipeline.

        For each shift, the manager checks whether there are open submissions
        (in the Mozart integration view ``mozart_submissions_open``) that
        match the affected carrier / state / line.  A high-priority alert is
        generated for any submission potentially impacted.

        If the Mozart integration tables are not available, alerts are still
        generated for all shifts — they will simply not be submission-specific.

        Args:
            shifts: List of :class:`AppetiteShift` instances from
                :class:`ChangeDetector`.

        Returns:
            List of :class:`Alert` instances ready for display or forwarding.
        """
        alerts: list[Alert] = []
        if not shifts:
            return alerts

        async with self._session_factory() as session:
            for shift in shifts:
                # Check for active submissions impacted by this shift
                impacted_count = await self._count_impacted_submissions(
                    session, shift
                )

                # Boost severity if live submissions are impacted
                effective_strength = shift.signal_strength
                if impacted_count > 0:
                    effective_strength = min(10, shift.signal_strength + 2)

                severity = _strength_to_severity(effective_strength)
                alert_type = _signal_type_to_alert_type(shift.signal_type)

                base_action = _SIGNAL_TYPE_TO_ACTION.get(
                    shift.signal_type,
                    "Review and assess impact on active book of business.",
                )
                action = base_action
                if impacted_count > 0:
                    action = (
                        f"{impacted_count} active submission(s) potentially impacted. "
                        f"{base_action}"
                    )

                alert = Alert(
                    id=uuid.uuid4(),
                    alert_type=alert_type,
                    severity=severity,
                    carrier_name=shift.carrier_name,
                    state=shift.state,
                    line=shift.line,
                    description=shift.description,
                    action_recommended=action,
                    created_at=datetime.now(timezone.utc),
                    acknowledged=False,
                )
                alerts.append(alert)

        logger.info(
            "Generated %d alerts from %d shifts", len(alerts), len(shifts)
        )
        return alerts

    async def get_unread_alerts(self) -> list[Alert]:
        """Retrieve all unacknowledged appetite signals as Alert objects.

        Queries ``hermes_appetite_signals`` for rows where
        ``acknowledged = FALSE``, ordered by signal_strength descending.

        Returns:
            List of :class:`Alert` instances, highest severity first.
        """
        async with self._session_factory() as session:
            stmt = text(
                """
                SELECT
                    s.id,
                    s.signal_type,
                    s.signal_strength,
                    s.signal_description,
                    s.state,
                    s.line,
                    s.created_at,
                    s.acknowledged,
                    c.legal_name AS carrier_name
                FROM hermes_appetite_signals s
                JOIN hermes_carriers c ON c.id = s.carrier_id
                WHERE s.acknowledged = FALSE
                ORDER BY s.signal_strength DESC, s.created_at DESC
                LIMIT 500
                """
            )
            result = await session.execute(stmt)
            rows = result.fetchall()

        alerts = []
        for row in rows:
            alert_type = _signal_type_to_alert_type(row.signal_type)
            severity = _strength_to_severity(int(row.signal_strength))
            action = _SIGNAL_TYPE_TO_ACTION.get(
                row.signal_type,
                "Review and assess impact on active book of business.",
            )
            alerts.append(
                Alert(
                    id=row.id,
                    alert_type=alert_type,
                    severity=severity,
                    carrier_name=row.carrier_name,
                    state=row.state,
                    line=row.line,
                    description=row.signal_description or "",
                    action_recommended=action,
                    created_at=row.created_at,
                    acknowledged=row.acknowledged,
                )
            )

        logger.debug("Retrieved %d unread alerts", len(alerts))
        return alerts

    async def acknowledge_alert(self, alert_id: UUID) -> None:
        """Mark a single alert (appetite signal) as acknowledged.

        Sets ``hermes_appetite_signals.acknowledged = TRUE`` for the given ID.

        Args:
            alert_id: UUID of the ``hermes_appetite_signals`` row.

        Raises:
            ValueError: If no signal with the given ID exists.
        """
        async with self._session_factory() as session:
            stmt = text(
                """
                UPDATE hermes_appetite_signals
                SET acknowledged = TRUE
                WHERE id = :alert_id
                RETURNING id
                """
            )
            result = await session.execute(stmt, {"alert_id": str(alert_id)})
            updated = result.fetchone()
            await session.commit()

        if not updated:
            raise ValueError(f"No alert found with id={alert_id}")

        logger.info("Alert acknowledged: %s", alert_id)

    async def generate_daily_digest(self) -> str:
        """Generate a plain-text daily digest of appetite shifts.

        Summarises all signals created in the last 24 hours grouped by
        severity (high → medium → low) and signal type.  Intended to be
        emailed or posted to a Slack channel via the Mozart notification
        pipeline.

        Returns:
            A formatted multi-line string digest.  Returns an informational
            message if no signals were detected in the last 24 hours.
        """
        cutoff = datetime.now(timezone.utc) - timedelta(hours=24)

        async with self._session_factory() as session:
            stmt = text(
                """
                SELECT
                    s.id,
                    s.signal_type,
                    s.signal_strength,
                    s.signal_description,
                    s.state,
                    s.line,
                    s.created_at,
                    c.legal_name AS carrier_name
                FROM hermes_appetite_signals s
                JOIN hermes_carriers c ON c.id = s.carrier_id
                WHERE s.created_at >= :cutoff
                ORDER BY s.signal_strength DESC, s.created_at DESC
                """
            )
            result = await session.execute(stmt, {"cutoff": cutoff})
            rows = result.fetchall()

        if not rows:
            return (
                f"Hermes Daily Digest — {date.today().isoformat()}\n"
                "No appetite shifts detected in the last 24 hours.\n"
            )

        high: list[str] = []
        medium: list[str] = []
        low: list[str] = []

        for row in rows:
            severity = _strength_to_severity(int(row.signal_strength))
            action = _SIGNAL_TYPE_TO_ACTION.get(
                row.signal_type,
                "Review and assess impact.",
            )
            entry = (
                f"  [{row.signal_type.upper()}] "
                f"{row.carrier_name} | {row.state} | {row.line}\n"
                f"  {row.signal_description}\n"
                f"  Action: {action}\n"
            )
            if severity == "high":
                high.append(entry)
            elif severity == "medium":
                medium.append(entry)
            else:
                low.append(entry)

        lines: list[str] = [
            f"Hermes Daily Digest — {date.today().isoformat()}",
            f"Total signals detected in last 24h: {len(rows)}",
            "=" * 60,
        ]

        if high:
            lines.append(f"\nHIGH SEVERITY ({len(high)} signals):")
            lines.append("-" * 40)
            lines.extend(high)

        if medium:
            lines.append(f"\nMEDIUM SEVERITY ({len(medium)} signals):")
            lines.append("-" * 40)
            lines.extend(medium)

        if low:
            lines.append(f"\nLOW SEVERITY ({len(low)} signals):")
            lines.append("-" * 40)
            lines.extend(low)

        lines.append("\n" + "=" * 60)
        lines.append("End of digest. Visit Hermes dashboard for full details.")

        digest = "\n".join(lines)
        logger.info(
            "Daily digest generated: %d signals (%d high / %d med / %d low)",
            len(rows),
            len(high),
            len(medium),
            len(low),
        )
        return digest

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    async def _count_impacted_submissions(
        self, session, shift: AppetiteShift
    ) -> int:
        """Count open Mozart submissions that match the shifted carrier/state/line.

        Attempts to query the Mozart integration view.  If the view does not
        exist (Mozart not connected), returns 0 gracefully.

        Args:
            session: Active SQLAlchemy async session.
            shift: The :class:`AppetiteShift` being evaluated.

        Returns:
            Number of active submissions potentially impacted, or 0.
        """
        try:
            stmt = text(
                """
                SELECT COUNT(*) AS cnt
                FROM mozart_submissions_open
                WHERE state = :state
                  AND line_of_business = :line
                  AND carrier_id = :carrier_id
                  AND status IN ('draft', 'submitted', 'quoted', 'bound_pending')
                """
            )
            result = await session.execute(
                stmt,
                {
                    "state": shift.state,
                    "line": shift.line,
                    "carrier_id": str(shift.carrier_id),
                },
            )
            row = result.fetchone()
            return int(row.cnt) if row else 0
        except Exception:
            # Mozart integration view not available — non-fatal
            return 0
