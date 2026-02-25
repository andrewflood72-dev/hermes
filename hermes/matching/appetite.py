"""Appetite scorer — synthesises a 0-100 appetite score for a carrier/state/line
combination by combining rate filing recency, rate-change direction, class code
fit, territory alignment, and recent appetite signals.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from hermes.config import settings

logger = logging.getLogger("hermes.matching.appetite")


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------


class AppetiteResult(BaseModel):
    """Appetite scoring result for one carrier/state/line.

    Attributes
    ----------
    score:
        Composite appetite score from 0 (no appetite) to 100 (maximum appetite).
    components:
        Breakdown of the composite score into individual sub-scores.
    recent_signals:
        List of recent :class:`hermes_appetite_signals` records for context.
    notes:
        Explanatory notes about the scoring.
    """

    score: float = Field(..., ge=0.0, le=100.0, description="Composite 0-100 appetite score")
    components: dict[str, float] = Field(
        default_factory=dict,
        description="Individual score components: recency, rate_direction, class_fit, territory, signal",
    )
    recent_signals: list[dict] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# AppetiteScorer
# ---------------------------------------------------------------------------


class AppetiteScorer:
    """Scores a carrier's appetite for a given risk profile.

    The composite score weights five components:

    - **recency_score** (20 pts) — how recently a rate filing was made
    - **rate_direction_score** (20 pts) — rate decreases signal volume-seeking appetite
    - **class_fit_score** (30 pts) — how well the risk's class code aligns with the
      carrier's preferred/eligible classes
    - **territory_score** (15 pts) — territory preference alignment
    - **signal_score** (15 pts) — recent appetite signals (expansions, new filings, etc.)

    Parameters
    ----------
    engine:
        Optional pre-built SQLAlchemy async engine.
    """

    # Component weights — must sum to 100
    _WEIGHTS = {
        "recency_score": 20.0,
        "rate_direction_score": 20.0,
        "class_fit_score": 30.0,
        "territory_score": 15.0,
        "signal_score": 15.0,
    }

    # Signal types that increase appetite
    _POSITIVE_SIGNALS = {
        "rate_decrease",
        "new_filing",
        "expanded_classes",
        "territory_expansion",
        "new_endorsement",
        "new_state_entry",
    }
    # Signal types that decrease appetite
    _NEGATIVE_SIGNALS = {
        "rate_increase",
        "contracted_classes",
        "territory_contraction",
        "filing_withdrawal",
        "market_exit",
    }

    def __init__(self, engine: AsyncEngine | None = None) -> None:
        self._engine = engine

    # ------------------------------------------------------------------
    # Engine access
    # ------------------------------------------------------------------

    async def _get_engine(self) -> AsyncEngine:
        """Return (creating lazily) the shared async SQLAlchemy engine."""
        if self._engine is None:
            self._engine = create_async_engine(
                settings.database_url,
                pool_size=5,
                max_overflow=10,
                echo=False,
            )
        return self._engine

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def score_appetite(
        self,
        carrier_id: UUID,
        state: str,
        line: str,
        risk_profile: dict,
    ) -> AppetiteResult:
        """Compute a 0-100 appetite score for the carrier/state/line/risk combination.

        Loads the appetite profile from ``hermes_appetite_profiles`` and recent
        signals from ``hermes_appetite_signals``, then scores each component.

        Parameters
        ----------
        carrier_id:
            UUID of the carrier.
        state:
            Two-letter state code.
        line:
            Line of business.
        risk_profile:
            Risk attributes; uses ``naics_code``, ``zip_code``, etc.

        Returns
        -------
        AppetiteResult
        """
        profile = await self._load_profile(carrier_id, state, line)
        signals = await self.get_recent_signals(carrier_id, state, line, days=90)

        notes: list[str] = []
        components: dict[str, float] = {}

        if profile is None:
            logger.warning(
                "No appetite profile found for carrier=%s state=%s line=%s",
                carrier_id,
                state,
                line,
            )
            notes.append("No appetite profile found; using conservative defaults.")
            # Return a minimal score
            return AppetiteResult(
                score=25.0,
                components={k: 5.0 for k in self._WEIGHTS},
                recent_signals=signals,
                notes=notes,
            )

        # 1. Recency score
        recency = self._score_recency(profile)
        components["recency_score"] = recency
        if recency < 10:
            notes.append("Rate filings are stale (> 2 years old); appetite confidence reduced.")

        # 2. Rate direction score
        rate_dir = self._score_rate_direction(profile)
        components["rate_direction_score"] = rate_dir
        last_change = profile.get("last_rate_change_pct")
        if last_change is not None:
            direction = "decrease" if float(last_change) < 0 else "increase"
            notes.append(
                f"Last rate change was {float(last_change):+.1f}% ({direction}); "
                f"signals {'volume-seeking' if direction == 'decrease' else 'restriction'}."
            )

        # 3. Class fit score
        class_fit = self._score_class_fit(profile, risk_profile.get("naics_code"))
        components["class_fit_score"] = class_fit
        if class_fit >= 25:
            notes.append("Risk class code is in carrier's preferred classes.")
        elif class_fit >= 15:
            notes.append("Risk class code is in carrier's eligible classes.")
        else:
            notes.append("Risk class code not found in preferred/eligible classes.")

        # 4. Territory score
        territory = self._score_territory(profile, risk_profile.get("zip_code"), state)
        components["territory_score"] = territory

        # 5. Signal score
        signal = self._score_signals(signals)
        components["signal_score"] = signal
        if signals:
            notes.append(
                f"{len(signals)} appetite signal(s) in the past 90 days detected."
            )

        # Composite score
        composite = sum(
            components.get(k, 0.0) * (self._WEIGHTS[k] / max_score)
            for k, max_score in [
                ("recency_score", 20.0),
                ("rate_direction_score", 20.0),
                ("class_fit_score", 30.0),
                ("territory_score", 15.0),
                ("signal_score", 15.0),
            ]
        )
        # Scale to 0-100
        composite = min(100.0, max(0.0, composite))

        # Blend with stored appetite_score (1-10 scale → 0-100)
        stored_score = float(profile.get("appetite_score") or 5.0)
        stored_normalized = (stored_score / 10.0) * 100.0
        # Weight: 70% computed, 30% stored profile score
        final_score = round(0.70 * composite + 0.30 * stored_normalized, 2)

        logger.info(
            "Appetite score for carrier=%s state=%s line=%s: %.1f",
            carrier_id,
            state,
            line,
            final_score,
        )

        return AppetiteResult(
            score=final_score,
            components=components,
            recent_signals=signals,
            notes=notes,
        )

    async def get_recent_signals(
        self,
        carrier_id: UUID,
        state: str,
        line: str,
        days: int = 90,
    ) -> list[dict]:
        """Return recent appetite signals for a carrier/state/line combination.

        Parameters
        ----------
        carrier_id:
            UUID of the carrier.
        state:
            Two-letter state code.
        line:
            Line of business.
        days:
            Look-back window in days (default 90).

        Returns
        -------
        list[dict]
            Each dict is a row from ``hermes_appetite_signals``.
        """
        cutoff = date.today() - timedelta(days=days)
        query = text(
            """
            SELECT
                id,
                signal_type,
                signal_strength,
                signal_date,
                signal_description,
                confidence
            FROM hermes_appetite_signals
            WHERE
                carrier_id = :carrier_id
                AND state = :state
                AND line  = :line
                AND signal_date >= :cutoff
            ORDER BY signal_date DESC
            LIMIT 50
            """
        )
        engine = await self._get_engine()
        async with engine.connect() as conn:
            result = await conn.execute(
                query,
                {
                    "carrier_id": str(carrier_id),
                    "state": state,
                    "line": line,
                    "cutoff": cutoff,
                },
            )
            rows = result.mappings().all()
        return [_row_to_dict(row) for row in rows]

    # ------------------------------------------------------------------
    # Scoring components
    # ------------------------------------------------------------------

    def _score_recency(self, profile: dict) -> float:
        """Score filing recency (0-20).

        More recent filings indicate an active, engaged carrier.
        """
        last_change_date = profile.get("last_rate_change_date")
        if last_change_date is None:
            return 5.0  # no data → low confidence

        if isinstance(last_change_date, str):
            try:
                last_change_date = date.fromisoformat(last_change_date)
            except ValueError:
                return 5.0

        days_since = (date.today() - last_change_date).days

        if days_since <= 90:
            return 20.0
        elif days_since <= 180:
            return 17.0
        elif days_since <= 365:
            return 13.0
        elif days_since <= 730:
            return 8.0
        else:
            return 3.0

    def _score_rate_direction(self, profile: dict) -> float:
        """Score based on rate change direction (0-20).

        Rate decreases → carrier is seeking volume → higher appetite.
        Rate increases → carrier is restricting → lower appetite.
        """
        last_change = profile.get("last_rate_change_pct")
        if last_change is None:
            return 10.0  # neutral when unknown

        pct = float(last_change)

        if pct <= -5.0:
            return 20.0   # aggressive decrease — very high appetite
        elif pct <= -2.0:
            return 17.0
        elif pct < 0.0:
            return 14.0
        elif pct == 0.0:
            return 10.0   # flat — neutral
        elif pct <= 3.0:
            return 7.0
        elif pct <= 7.0:
            return 4.0
        else:
            return 1.0    # large increase — restricting

    def _score_class_fit(self, profile: dict, naics_code: Any) -> float:
        """Score class code alignment (0-30).

        Preferred class → 30, eligible class → 18, not listed → 5.
        """
        if naics_code is None:
            return 10.0

        naics_str = str(naics_code).strip()

        preferred = profile.get("preferred_classes") or []
        eligible = profile.get("eligible_classes") or []
        ineligible = profile.get("ineligible_classes") or []

        if _naics_in_list(naics_str, ineligible):
            return 0.0
        if _naics_in_list(naics_str, preferred):
            return 30.0
        if _naics_in_list(naics_str, eligible):
            return 18.0
        return 5.0

    def _score_territory(self, profile: dict, zip_code: Any, state: str) -> float:
        """Score territory alignment (0-15).

        Uses ``territory_preferences`` JSONB from the profile.
        """
        prefs: dict = profile.get("territory_preferences") or {}
        if not prefs:
            return 10.0  # no data — neutral

        # Attempt a direct state-level preference lookup
        state_pref = prefs.get(state)
        if state_pref is not None:
            try:
                pref_score = float(state_pref)
                # Normalise the 0-10 preference score to 0-15
                return min(15.0, max(0.0, pref_score * 1.5))
            except (TypeError, ValueError):
                pass

        return 8.0  # state present but no preference entry → slightly below neutral

    def _score_signals(self, signals: list[dict]) -> float:
        """Score recent appetite signals (0-15).

        Positive signals increase the score; negative signals decrease it.
        Signal strength (1-10) is factored in.
        """
        if not signals:
            return 8.0  # neutral when no signals

        net = 0.0
        for sig in signals:
            signal_type = sig.get("signal_type", "")
            strength = float(sig.get("signal_strength") or 5.0)
            if signal_type in self._POSITIVE_SIGNALS:
                net += strength
            elif signal_type in self._NEGATIVE_SIGNALS:
                net -= strength

        # Normalise: max possible net per signal = 10
        max_net = len(signals) * 10.0
        if max_net == 0:
            return 8.0

        ratio = net / max_net  # -1.0 to +1.0
        # Map to 0-15 range, centred at 8
        score = 8.0 + ratio * 7.0
        return min(15.0, max(0.0, score))

    # ------------------------------------------------------------------
    # Database access
    # ------------------------------------------------------------------

    async def _load_profile(
        self, carrier_id: UUID, state: str, line: str
    ) -> dict | None:
        """Load the current appetite profile row for carrier/state/line."""
        query = text(
            """
            SELECT
                id,
                appetite_score,
                eligible_classes,
                ineligible_classes,
                preferred_classes,
                territory_preferences,
                limit_range_min,
                limit_range_max,
                rate_competitiveness_index,
                last_rate_change_pct,
                last_rate_change_date,
                filing_frequency_score,
                years_active_in_state,
                market_share_estimate,
                source_filing_count,
                computed_at
            FROM hermes_appetite_profiles
            WHERE
                carrier_id = :carrier_id
                AND state   = :state
                AND line    = :line
                AND is_current = TRUE
            LIMIT 1
            """
        )
        engine = await self._get_engine()
        async with engine.connect() as conn:
            result = await conn.execute(
                query,
                {"carrier_id": str(carrier_id), "state": state, "line": line},
            )
            row = result.mappings().first()
        return dict(row) if row else None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _naics_in_list(naics_code: str, code_list: list) -> bool:
    """Check whether *naics_code* matches any entry via exact or prefix match."""
    for entry in code_list:
        entry_str = str(entry).strip()
        if naics_code == entry_str or naics_code.startswith(entry_str) or entry_str.startswith(naics_code):
            return True
    return False


def _row_to_dict(row: Any) -> dict:
    """Convert a SQLAlchemy mapping row to a plain dict, serialising dates."""
    d = dict(row)
    for k, v in d.items():
        if isinstance(v, (date, datetime)):
            d[k] = v.isoformat()
    return d
