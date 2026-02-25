"""Eligibility filter — evaluates whether a risk profile meets a carrier's
underwriting criteria for a given state and line of business.

The :class:`EligibilityFilter` queries ``hermes_eligibility_criteria`` (joined
through ``hermes_underwriting_rules``) and checks each active hard and soft
criterion against the submitted risk profile.  Results are returned as an
:class:`EligibilityResult` with granular pass/fail/conditional status.
"""

from __future__ import annotations

import json
import logging
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from hermes.config import settings

logger = logging.getLogger("hermes.matching.eligibility")


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------


class EligibilityResult(BaseModel):
    """Result of an eligibility check for one carrier/state/line combination.

    Attributes
    ----------
    status:
        ``"pass"`` — all hard criteria satisfied.
        ``"fail"`` — one or more hard criteria failed.
        ``"conditional"`` — all hard criteria passed but soft criteria raised notes.
    failed_criteria:
        Human-readable descriptions of each criterion that failed.
    conditional_notes:
        Human-readable notes for soft criteria that were flagged.
    criteria_checked:
        Total number of criteria evaluated.
    """

    status: str = Field(..., description="pass | fail | conditional")
    failed_criteria: list[str] = Field(default_factory=list)
    conditional_notes: list[str] = Field(default_factory=list)
    criteria_checked: int = Field(default=0)


# ---------------------------------------------------------------------------
# EligibilityFilter
# ---------------------------------------------------------------------------


class EligibilityFilter:
    """Checks a risk profile against carrier eligibility criteria stored in
    the Hermes database.

    Parameters
    ----------
    engine:
        Optional pre-built SQLAlchemy async engine.  When omitted, one is
        created from :data:`hermes.config.settings`.
    """

    # Criterion types that map directly to risk-profile keys
    _FIELD_MAP: dict[str, str] = {
        "eligible_class": "naics_code",
        "ineligible_class": "naics_code",
        "min_years_business": "years_in_business",
        "max_loss_ratio": "loss_ratio_3yr",
        "territory_restriction": "state",
        "construction_type": "construction_type",
        "min_employees": "employee_count",
        "max_employees": "employee_count",
        "revenue_range": "annual_revenue",
        "operations_restriction": "naics_code",
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

    async def check_eligibility(
        self,
        risk_profile: dict,
        carrier_id: UUID,
        state: str,
        line: str,
    ) -> EligibilityResult:
        """Evaluate a risk profile against all active eligibility criteria for
        a given carrier/state/line combination.

        The method loads every row from ``hermes_eligibility_criteria`` that is
        linked to an active (``is_current = TRUE``) underwriting rule for the
        carrier, state, and line.  Each criterion is evaluated individually via
        :meth:`_check_criterion`.

        Hard criteria (``is_hard_rule = TRUE``) produce a ``"fail"`` status on
        failure.  Soft criteria produce ``"conditional"`` notes.

        Parameters
        ----------
        risk_profile:
            Dictionary containing risk attributes.  Expected keys include
            ``naics_code``, ``years_in_business``, ``loss_ratio_3yr``,
            ``state``, ``construction_type``, ``employee_count``,
            ``annual_revenue``, etc.
        carrier_id:
            UUID of the carrier record in ``hermes_carriers``.
        state:
            Two-letter state code.
        line:
            Line of business string (e.g. ``"Commercial Auto"``).

        Returns
        -------
        EligibilityResult
        """
        criteria = await self._load_criteria(carrier_id, state, line)
        logger.debug(
            "Loaded %d eligibility criteria for carrier=%s state=%s line=%s",
            len(criteria),
            carrier_id,
            state,
            line,
        )

        failed: list[str] = []
        conditional: list[str] = []

        for criterion in criteria:
            criterion_type: str = criterion["criterion_type"]
            is_hard: bool = criterion["is_hard_rule"]
            description: str = criterion.get("description") or criterion_type

            # Resolve the risk value for this criterion type
            risk_field = self._FIELD_MAP.get(criterion_type)
            if risk_field is None:
                logger.debug("No field mapping for criterion_type=%s; skipping", criterion_type)
                continue

            risk_value = risk_profile.get(risk_field)

            # Special handling: property lines require construction_type check
            if criterion_type == "construction_type" and not _is_property_line(line):
                logger.debug("Skipping construction_type criterion for non-property line=%s", line)
                continue

            passed = self._check_criterion(criterion, risk_value)
            logger.debug(
                "Criterion %s (%s) risk_value=%r -> %s",
                criterion_type,
                criterion["criterion_operator"],
                risk_value,
                "PASS" if passed else "FAIL",
            )

            if not passed:
                msg = _build_failure_message(criterion, risk_value)
                if is_hard:
                    failed.append(msg)
                else:
                    conditional.append(msg)

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
            criteria_checked=len(criteria),
        )

    # ------------------------------------------------------------------
    # Criterion evaluation
    # ------------------------------------------------------------------

    def _check_criterion(self, criterion: dict, risk_value: Any) -> bool:
        """Evaluate a single eligibility criterion against the risk value.

        Supported operators (stored in ``criterion_operator``):

        - ``equals`` — exact match (string comparison)
        - ``gt`` / ``gte`` — numeric greater-than / greater-than-or-equal
        - ``lt`` / ``lte`` — numeric less-than / less-than-or-equal
        - ``in`` — risk value is in the allowed set
        - ``not_in`` — risk value is NOT in the forbidden set
        - ``between`` — risk value is within [low, high] inclusive
        - ``contains`` — criterion_value is a substring of risk_value
        - ``not_contains`` — criterion_value is NOT a substring of risk_value

        Parameters
        ----------
        criterion:
            A row dict from ``hermes_eligibility_criteria``.
        risk_value:
            The value from the risk profile for the criterion's field.

        Returns
        -------
        bool
            ``True`` if the criterion is satisfied.
        """
        operator: str = criterion.get("criterion_operator", "equals").lower()
        raw_value: str = criterion.get("criterion_value", "")
        criterion_type: str = criterion.get("criterion_type", "")

        # --- eligible_class: risk's NAICS must be IN the eligible set
        if criterion_type == "eligible_class":
            allowed = _parse_json_list(raw_value)
            if not allowed:
                return True  # no restriction defined
            return _naics_in_list(risk_value, allowed)

        # --- ineligible_class: risk's NAICS must NOT be in the ineligible set
        if criterion_type == "ineligible_class":
            forbidden = _parse_json_list(raw_value)
            if not forbidden:
                return True
            return not _naics_in_list(risk_value, forbidden)

        if risk_value is None:
            # Cannot evaluate without a risk value — treat as conditional fail
            logger.debug("risk_value is None for operator=%s; returning False", operator)
            return False

        if operator == "equals":
            return str(risk_value).strip().lower() == raw_value.strip().lower()

        if operator in ("gt", "gte", "lt", "lte"):
            return _numeric_compare(operator, risk_value, raw_value)

        if operator == "in":
            allowed = _parse_json_list(raw_value)
            return str(risk_value).strip() in [str(v).strip() for v in allowed]

        if operator == "not_in":
            forbidden = _parse_json_list(raw_value)
            return str(risk_value).strip() not in [str(v).strip() for v in forbidden]

        if operator == "between":
            return _between(risk_value, raw_value)

        if operator == "contains":
            return raw_value.lower() in str(risk_value).lower()

        if operator == "not_contains":
            return raw_value.lower() not in str(risk_value).lower()

        logger.warning("Unknown operator '%s'; treating as pass", operator)
        return True

    # ------------------------------------------------------------------
    # Database access
    # ------------------------------------------------------------------

    async def _load_criteria(
        self, carrier_id: UUID, state: str, line: str
    ) -> list[dict]:
        """Load all active eligibility criteria for the carrier/state/line.

        Joins ``hermes_underwriting_rules`` (``is_current = TRUE``) with
        ``hermes_eligibility_criteria`` and returns all matching rows.

        Parameters
        ----------
        carrier_id:
            UUID of the carrier.
        state:
            Two-letter state code.
        line:
            Line of business.

        Returns
        -------
        list[dict]
            Each dict is a merged row with both rule and criterion fields.
        """
        query = text(
            """
            SELECT
                ec.id,
                ec.criterion_type,
                ec.criterion_value,
                ec.criterion_operator,
                ec.criterion_unit,
                ec.is_hard_rule,
                ec.description,
                ec.confidence,
                ur.state,
                ur.line
            FROM hermes_eligibility_criteria ec
            JOIN hermes_underwriting_rules ur ON ur.id = ec.rule_id
            WHERE
                ur.carrier_id = :carrier_id
                AND ur.state   = :state
                AND ur.line    = :line
                AND ur.is_current = TRUE
            ORDER BY ec.is_hard_rule DESC, ec.criterion_type
            """
        )
        engine = await self._get_engine()
        async with engine.connect() as conn:
            result = await conn.execute(
                query,
                {"carrier_id": str(carrier_id), "state": state, "line": line},
            )
            rows = result.mappings().all()
        return [dict(row) for row in rows]


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _is_property_line(line: str) -> bool:
    """Return True if the line of business is property-related."""
    return any(kw in line.lower() for kw in ("property", "commercial property", "bop", "fire"))


def _parse_json_list(raw: str) -> list:
    """Parse a criterion_value that may be a JSON array or a comma-separated string."""
    raw = raw.strip()
    if raw.startswith("["):
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            pass
    # Fall back to comma-separated
    return [v.strip() for v in raw.split(",") if v.strip()]


def _naics_in_list(naics_code: Any, code_list: list) -> bool:
    """Check whether *naics_code* matches any entry in *code_list*.

    Supports both exact matches and prefix matching (e.g. ``"236"`` matches
    ``"236118"``).
    """
    if naics_code is None:
        return False
    naics_str = str(naics_code).strip()
    for entry in code_list:
        entry_str = str(entry).strip()
        if naics_str == entry_str or naics_str.startswith(entry_str) or entry_str.startswith(naics_str):
            return True
    return False


def _numeric_compare(operator: str, risk_value: Any, raw_criterion: str) -> bool:
    """Perform a numeric comparison between *risk_value* and *raw_criterion*."""
    try:
        risk_num = float(risk_value)
        crit_num = float(raw_criterion.strip())
    except (TypeError, ValueError):
        logger.warning(
            "Cannot convert to float for numeric comparison: risk=%r criterion=%r",
            risk_value,
            raw_criterion,
        )
        return False

    if operator == "gt":
        return risk_num > crit_num
    if operator == "gte":
        return risk_num >= crit_num
    if operator == "lt":
        return risk_num < crit_num
    if operator == "lte":
        return risk_num <= crit_num
    return False


def _between(risk_value: Any, raw_criterion: str) -> bool:
    """Check whether *risk_value* falls between two values in *raw_criterion*.

    *raw_criterion* should be formatted as ``"low,high"`` or a JSON array
    ``[low, high]``.
    """
    parts = _parse_json_list(raw_criterion)
    if len(parts) != 2:
        logger.warning("between operator requires exactly 2 values; got %r", parts)
        return False
    try:
        low = float(parts[0])
        high = float(parts[1])
        risk_num = float(risk_value)
        return low <= risk_num <= high
    except (TypeError, ValueError):
        return False


def _build_failure_message(criterion: dict, risk_value: Any) -> str:
    """Build a human-readable failure explanation for a criterion."""
    criterion_type = criterion.get("criterion_type", "unknown")
    operator = criterion.get("criterion_operator", "equals")
    criterion_value = criterion.get("criterion_value", "")
    description = criterion.get("description")
    unit = criterion.get("criterion_unit", "")

    if description:
        return f"{description} (got {risk_value!r})"

    unit_str = f" {unit}" if unit else ""
    return (
        f"Criterion '{criterion_type}' failed: "
        f"expected {operator} {criterion_value!r}{unit_str}, "
        f"got {risk_value!r}"
    )
