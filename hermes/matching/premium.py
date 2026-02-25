"""Premium estimator — builds an estimated premium for a carrier/state/line
combination by loading rate tables and applying rating factors.

The estimation pipeline is:

    estimated = base_rate
                × territory_factor
                × class_factor
                × experience_mod
                × ilf_factor
                × deductible_factor
                × (1 - schedule_credits)
                × exposure_units
"""

from __future__ import annotations

import logging
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from hermes.config import settings

logger = logging.getLogger("hermes.matching.premium")

# Default factor values used when the database has no record
_DEFAULT_TERRITORY_FACTOR = 1.0
_DEFAULT_CLASS_FACTOR = 1.0
_DEFAULT_EXPERIENCE_MOD = 1.0
_DEFAULT_ILF_FACTOR = 1.0
_DEFAULT_DEDUCTIBLE_FACTOR = 1.0
_DEFAULT_SCHEDULE_CREDITS = 0.0

# Exposure basis per line keyword → risk profile key
_EXPOSURE_FIELD_MAP: dict[str, str] = {
    "payroll": "annual_revenue",   # proxy if dedicated payroll field absent
    "revenue": "annual_revenue",
    "receipts": "annual_revenue",
    "area": "annual_revenue",      # fallback
    "units": "employee_count",
    "employees": "employee_count",
}


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------


class PremiumEstimate(BaseModel):
    """Estimated premium breakdown for one carrier/state/line.

    Attributes
    ----------
    base_premium:
        Base rate × exposure units before any modification factors.
    territory_factor:
        Territory rating factor applied.
    class_factor:
        Class code rating factor applied.
    experience_mod:
        Experience modification factor applied.
    schedule_credits:
        Fractional schedule credit/debit applied (e.g. 0.10 = 10% credit).
    ilf_factor:
        Increased limits factor for the requested limit.
    deductible_factor:
        Deductible credit factor applied.
    final_estimated:
        The fully loaded estimated annual premium.
    confidence:
        Confidence from 0.0 (highly estimated) to 1.0 (fully table-sourced).
    components:
        Dict with intermediate calculation steps.
    notes:
        Human-readable notes about assumptions and missing data.
    """

    base_premium: float = Field(default=0.0)
    territory_factor: float = Field(default=_DEFAULT_TERRITORY_FACTOR)
    class_factor: float = Field(default=_DEFAULT_CLASS_FACTOR)
    experience_mod: float = Field(default=_DEFAULT_EXPERIENCE_MOD)
    schedule_credits: float = Field(default=_DEFAULT_SCHEDULE_CREDITS)
    ilf_factor: float = Field(default=_DEFAULT_ILF_FACTOR)
    deductible_factor: float = Field(default=_DEFAULT_DEDUCTIBLE_FACTOR)
    final_estimated: float = Field(default=0.0)
    confidence: float = Field(default=0.0, ge=0.0, le=1.0)
    components: dict[str, Any] = Field(default_factory=dict)
    notes: list[str] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# PremiumEstimator
# ---------------------------------------------------------------------------


class PremiumEstimator:
    """Estimates premiums by loading carrier rate tables from the database.

    Parameters
    ----------
    engine:
        Optional pre-built SQLAlchemy async engine.
    """

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

    async def estimate_premium(
        self,
        carrier_id: UUID,
        state: str,
        line: str,
        risk_profile: dict,
    ) -> PremiumEstimate:
        """Estimate the annual premium for a carrier/state/line/risk combination.

        Loads the current rate table, finds a base rate for the risk's class
        code and territory, then applies all available rating factors.

        Parameters
        ----------
        carrier_id:
            UUID of the carrier.
        state:
            Two-letter state code.
        line:
            Line of business.
        risk_profile:
            Risk attributes; uses ``naics_code``, ``zip_code``,
            ``experience_mod``, ``annual_revenue``, ``employee_count``,
            ``requested_limits``, ``construction_type``, etc.

        Returns
        -------
        PremiumEstimate
        """
        notes: list[str] = []
        factors_found = 0
        factors_possible = 6  # territory, class, exp_mod, ilf, deductible, schedule

        # Load current rate table
        rate_table = await self._load_rate_table(carrier_id, state, line)
        if rate_table is None:
            notes.append("No current rate table found; estimate is highly approximate.")
            return PremiumEstimate(
                final_estimated=0.0,
                confidence=0.0,
                notes=notes,
            )

        rate_table_id: UUID = rate_table["id"]
        naics_code: str | None = risk_profile.get("naics_code")
        zip_code: str | None = risk_profile.get("zip_code")

        # Resolve territory code from zip
        territory_code = await self._lookup_territory(rate_table_id, zip_code)

        # Load base rate
        base_rate_row = await self._load_base_rate(rate_table_id, naics_code, territory_code)
        if base_rate_row is None:
            # Try without territory
            base_rate_row = await self._load_base_rate(rate_table_id, naics_code, territory=None)

        if base_rate_row is None:
            notes.append(
                f"No base rate found for class_code={naics_code!r} territory={territory_code!r}; "
                "cannot produce estimate."
            )
            return PremiumEstimate(
                confidence=0.0,
                notes=notes,
            )

        base_rate = float(base_rate_row["base_rate"])
        exposure_basis: str = base_rate_row.get("exposure_basis") or "revenue"
        notes.append(
            f"Base rate: {base_rate:.6f} per unit ({exposure_basis}), "
            f"class={base_rate_row.get('class_code')!r}, territory={territory_code!r}."
        )

        # Compute exposure units
        exposure_units = self._compute_exposure(exposure_basis, risk_profile)
        base_premium = base_rate * exposure_units

        # --- Territory factor ---
        territory_factor = await self._get_factor_value(rate_table_id, "territory", territory_code or "")
        if territory_factor != _DEFAULT_TERRITORY_FACTOR:
            factors_found += 1
        else:
            notes.append("Territory factor not found; using 1.0.")

        # --- Class factor ---
        class_factor = await self._get_factor_value(rate_table_id, "class", naics_code or "")
        if class_factor != _DEFAULT_CLASS_FACTOR:
            factors_found += 1
        else:
            notes.append("Class code factor not found; using 1.0.")

        # --- Experience mod ---
        risk_exp_mod = risk_profile.get("experience_mod")
        if risk_exp_mod is not None:
            try:
                experience_mod = float(risk_exp_mod)
                factors_found += 1
                notes.append(f"Experience mod from risk profile: {experience_mod:.3f}.")
            except (TypeError, ValueError):
                experience_mod = _DEFAULT_EXPERIENCE_MOD
                notes.append("Invalid experience_mod in risk profile; using 1.0.")
        else:
            experience_mod = _DEFAULT_EXPERIENCE_MOD
            notes.append("Experience mod not provided; using 1.0.")

        # --- ILF (increased limits factor) ---
        requested_limits: dict = risk_profile.get("requested_limits") or {}
        limit_key = _extract_limit_key(requested_limits)
        ilf_factor = await self._get_factor_value(rate_table_id, "ilf", limit_key)
        if ilf_factor != _DEFAULT_ILF_FACTOR:
            factors_found += 1
        else:
            notes.append(f"ILF not found for limit={limit_key!r}; using 1.0.")

        # --- Deductible factor ---
        deductible_key = str(requested_limits.get("deductible", "0"))
        deductible_factor = await self._get_factor_value(rate_table_id, "deductible", deductible_key)
        if deductible_factor != _DEFAULT_DEDUCTIBLE_FACTOR:
            factors_found += 1
        else:
            notes.append("Deductible factor not found; using 1.0.")

        # --- Schedule credits ---
        schedule_credits = _DEFAULT_SCHEDULE_CREDITS
        notes.append("Schedule credits/debits not applied (not in submission).")

        # Final premium calculation
        final_estimated = (
            base_premium
            * territory_factor
            * class_factor
            * experience_mod
            * ilf_factor
            * deductible_factor
            * (1.0 - schedule_credits)
        )

        confidence = round(factors_found / factors_possible, 3)

        logger.info(
            "Premium estimate for carrier=%s state=%s line=%s: $%.2f (confidence=%.2f)",
            carrier_id,
            state,
            line,
            final_estimated,
            confidence,
        )

        return PremiumEstimate(
            base_premium=round(base_premium, 2),
            territory_factor=territory_factor,
            class_factor=class_factor,
            experience_mod=experience_mod,
            schedule_credits=schedule_credits,
            ilf_factor=ilf_factor,
            deductible_factor=deductible_factor,
            final_estimated=round(final_estimated, 2),
            confidence=confidence,
            components={
                "base_rate": base_rate,
                "exposure_units": exposure_units,
                "exposure_basis": exposure_basis,
                "territory_code": territory_code,
                "class_code": naics_code,
                "limit_key": limit_key,
                "factors_found": factors_found,
                "factors_possible": factors_possible,
            },
            notes=notes,
        )

    # ------------------------------------------------------------------
    # Territory resolution
    # ------------------------------------------------------------------

    async def _lookup_territory(
        self, rate_table_id: UUID, zip_code: str | None
    ) -> str | None:
        """Map a zip code to a territory code using ``hermes_territory_definitions``.

        Searches the JSONB ``zip_codes`` array for the given zip.  Returns
        ``None`` if no match is found.

        Parameters
        ----------
        rate_table_id:
            UUID of the rate table.
        zip_code:
            5-digit zip code string or ``None``.

        Returns
        -------
        str | None
            Territory code, or ``None`` when not found.
        """
        if not zip_code:
            return None

        query = text(
            """
            SELECT territory_code
            FROM hermes_territory_definitions
            WHERE
                rate_table_id = :rate_table_id
                AND zip_codes @> :zip_json::jsonb
            LIMIT 1
            """
        )
        engine = await self._get_engine()
        async with engine.connect() as conn:
            result = await conn.execute(
                query,
                {
                    "rate_table_id": str(rate_table_id),
                    "zip_json": f'["{zip_code}"]',
                },
            )
            row = result.first()
        return row[0] if row else None

    # ------------------------------------------------------------------
    # Factor lookup
    # ------------------------------------------------------------------

    async def _get_factor_value(
        self,
        rate_table_id: UUID,
        factor_type: str,
        factor_key: str,
    ) -> float:
        """Look up a single rating factor from ``hermes_rating_factors``.

        Parameters
        ----------
        rate_table_id:
            UUID of the rate table.
        factor_type:
            Factor category (e.g. ``"territory"``, ``"ilf"``, ``"deductible"``).
        factor_key:
            Lookup key for this factor type (e.g. territory code, limit amount).

        Returns
        -------
        float
            The factor value, or the appropriate default if not found.
        """
        if not factor_key:
            return 1.0

        query = text(
            """
            SELECT factor_value
            FROM hermes_rating_factors
            WHERE
                rate_table_id = :rate_table_id
                AND factor_type = :factor_type
                AND factor_key  = :factor_key
            LIMIT 1
            """
        )
        engine = await self._get_engine()
        async with engine.connect() as conn:
            result = await conn.execute(
                query,
                {
                    "rate_table_id": str(rate_table_id),
                    "factor_type": factor_type,
                    "factor_key": factor_key,
                },
            )
            row = result.first()

        if row:
            return float(row[0])

        # Try a prefix match for class codes (e.g. look up "236" for "236118")
        if factor_type in ("class",) and len(factor_key) > 3:
            return await self._get_factor_prefix(rate_table_id, factor_type, factor_key)

        return 1.0

    async def _get_factor_prefix(
        self,
        rate_table_id: UUID,
        factor_type: str,
        factor_key: str,
    ) -> float:
        """Attempt a prefix-match factor lookup (for class codes)."""
        query = text(
            """
            SELECT factor_value, factor_key
            FROM hermes_rating_factors
            WHERE
                rate_table_id = :rate_table_id
                AND factor_type = :factor_type
                AND :factor_key LIKE factor_key || '%'
            ORDER BY length(factor_key) DESC
            LIMIT 1
            """
        )
        engine = await self._get_engine()
        async with engine.connect() as conn:
            result = await conn.execute(
                query,
                {
                    "rate_table_id": str(rate_table_id),
                    "factor_type": factor_type,
                    "factor_key": factor_key,
                },
            )
            row = result.first()
        return float(row[0]) if row else 1.0

    # ------------------------------------------------------------------
    # Database access helpers
    # ------------------------------------------------------------------

    async def _load_rate_table(
        self, carrier_id: UUID, state: str, line: str
    ) -> dict | None:
        """Load the current rate table row for carrier/state/line."""
        query = text(
            """
            SELECT
                id,
                table_name,
                table_type,
                effective_date,
                extraction_confidence
            FROM hermes_rate_tables
            WHERE
                carrier_id = :carrier_id
                AND state   = :state
                AND line    = :line
                AND is_current = TRUE
            ORDER BY effective_date DESC
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

    async def _load_base_rate(
        self,
        rate_table_id: UUID,
        class_code: str | None,
        territory: str | None,
    ) -> dict | None:
        """Load a base rate row for a class code (and optionally territory)."""
        if class_code is None:
            return None

        if territory:
            query = text(
                """
                SELECT
                    id, class_code, territory, base_rate,
                    rate_per_unit, minimum_premium, maximum_premium,
                    exposure_basis, confidence
                FROM hermes_base_rates
                WHERE
                    rate_table_id = :rate_table_id
                    AND (class_code = :class_code
                         OR :class_code LIKE class_code || '%'
                         OR class_code LIKE :class_code || '%')
                    AND territory = :territory
                ORDER BY length(class_code) DESC
                LIMIT 1
                """
            )
            params: dict = {
                "rate_table_id": str(rate_table_id),
                "class_code": class_code,
                "territory": territory,
            }
        else:
            query = text(
                """
                SELECT
                    id, class_code, territory, base_rate,
                    rate_per_unit, minimum_premium, maximum_premium,
                    exposure_basis, confidence
                FROM hermes_base_rates
                WHERE
                    rate_table_id = :rate_table_id
                    AND (class_code = :class_code
                         OR :class_code LIKE class_code || '%'
                         OR class_code LIKE :class_code || '%')
                ORDER BY
                    CASE WHEN territory IS NULL THEN 1 ELSE 0 END,
                    length(class_code) DESC
                LIMIT 1
                """
            )
            params = {
                "rate_table_id": str(rate_table_id),
                "class_code": class_code,
            }

        engine = await self._get_engine()
        async with engine.connect() as conn:
            result = await conn.execute(query, params)
            row = result.mappings().first()
        return dict(row) if row else None

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _compute_exposure(self, exposure_basis: str, risk_profile: dict) -> float:
        """Derive the exposure unit count from the risk profile.

        Maps the ``exposure_basis`` string to the appropriate risk profile field
        and normalises to the standard rate unit (per $100 revenue, per employee,
        etc.).

        Parameters
        ----------
        exposure_basis:
            Exposure basis string from the base rate row (e.g. ``"revenue"``,
            ``"payroll"``, ``"units"``).
        risk_profile:
            The full risk profile dict.

        Returns
        -------
        float
            Exposure units to multiply against the base rate.
        """
        basis_lower = (exposure_basis or "revenue").lower()
        field = _EXPOSURE_FIELD_MAP.get(basis_lower, "annual_revenue")
        raw = risk_profile.get(field)

        if raw is None:
            logger.debug("No exposure field '%s' in risk_profile; defaulting to 1.0", field)
            return 1.0

        try:
            value = float(raw)
        except (TypeError, ValueError):
            return 1.0

        # Normalise to per-$100 or per-unit depending on basis
        if basis_lower in ("revenue", "payroll", "receipts", "area"):
            return value / 100.0  # base rate is typically per $100
        return value  # unit-based (employees, vehicles, etc.)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _extract_limit_key(requested_limits: dict) -> str:
    """Extract a string limit key suitable for ILF lookups.

    Prefers ``occurrence`` limit; falls back to the first numeric value found.
    """
    for key in ("occurrence", "per_occurrence", "csl", "aggregate"):
        val = requested_limits.get(key)
        if val is not None:
            return str(val)
    # Fall back: first numeric value
    for val in requested_limits.values():
        try:
            return str(int(float(val)))
        except (TypeError, ValueError):
            continue
    return ""
