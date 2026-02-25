"""Hermes PMI Pricing Engine — multi-carrier PMI rate comparison.

Queries all 6 US PMI carriers' rate cards, applies JSONB-based adjustments,
calculates premiums, and returns ranked results.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from hermes.config import settings
from hermes.pmi.schemas import (
    PMICarrierQuote,
    PMIQuickQuoteRequest,
    PMIQuoteRequest,
    PMIQuoteResponse,
)

logger = logging.getLogger("hermes.pmi.engine")

# ---------------------------------------------------------------------------
# GSE minimum coverage requirements
# ---------------------------------------------------------------------------
# Fannie Mae / Freddie Mac required MI coverage by LTV band.

GSE_COVERAGE_MINIMUMS: dict[tuple[float, float], float] = {
    (80.01, 85.00): 6.0,
    (85.01, 90.00): 25.0,
    (90.01, 95.00): 30.0,
    (95.01, 97.00): 35.0,
}


def _gse_required_coverage(ltv: float) -> float:
    """Return the GSE minimum coverage % for the given LTV."""
    for (lo, hi), cov in GSE_COVERAGE_MINIMUMS.items():
        if lo <= ltv <= hi:
            return cov
    # LTV <= 80 — no PMI required (return 0); above 97 not eligible
    return 0.0


class HermesPMIEngine:
    """Core PMI pricing engine following MatchingEngine patterns."""

    def __init__(self, db_engine: AsyncEngine | None = None) -> None:
        self._engine = db_engine
        logger.info("HermesPMIEngine initialised")

    async def _get_engine(self) -> AsyncEngine:
        if self._engine is None:
            self._engine = create_async_engine(
                settings.database_url, pool_size=10, max_overflow=20, echo=False
            )
        return self._engine

    async def close(self) -> None:
        if self._engine is not None:
            await self._engine.dispose()
            logger.info("HermesPMIEngine database engine disposed")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def price_loan(self, request: PMIQuoteRequest) -> PMIQuoteResponse:
        """Full pricing pipeline: LTV → coverage → query carriers → rates → adjustments → premiums."""
        t0 = time.monotonic()
        request_id = uuid.uuid4()

        ltv = (request.loan_amount / request.property_value) * 100
        coverage = request.coverage_pct or _gse_required_coverage(ltv)

        if ltv <= 80:
            return PMIQuoteResponse(
                quotes=[],
                loan_amount=request.loan_amount,
                property_value=request.property_value,
                ltv=round(ltv, 2),
                fico_score=request.fico_score,
                coverage_pct=coverage,
                carriers_quoted=0,
                processing_time_ms=round((time.monotonic() - t0) * 1000, 1),
                request_id=request_id,
            )

        # Determine which premium types to quote
        premium_types = (
            [request.premium_type] if request.premium_type else ["monthly", "single"]
        )

        # Get active PMI carriers with current rate cards
        carriers = await self._get_pmi_carriers(
            state=request.state,
            premium_types=premium_types,
            carrier_ids=request.carrier_ids,
        )

        # Price across all carriers concurrently
        tasks = [
            self._price_carrier(
                carrier=c,
                ltv=ltv,
                fico=request.fico_score,
                coverage=coverage,
                loan_amount=request.loan_amount,
                premium_types=premium_types,
                loan_params=request.model_dump(),
            )
            for c in carriers
        ]
        nested_results: list[list[PMICarrierQuote]] = await asyncio.gather(
            *tasks, return_exceptions=False
        )
        quotes = [q for sublist in nested_results for q in sublist]

        # Sort by annual premium (lowest first)
        quotes.sort(key=lambda q: q.annual_premium)

        best_monthly = next((q for q in quotes if q.premium_type == "monthly"), None)
        best_annual = quotes[0] if quotes else None

        elapsed_ms = round((time.monotonic() - t0) * 1000, 1)

        response = PMIQuoteResponse(
            quotes=quotes,
            best_monthly=best_monthly,
            best_annual=best_annual,
            loan_amount=request.loan_amount,
            property_value=request.property_value,
            ltv=round(ltv, 2),
            fico_score=request.fico_score,
            coverage_pct=coverage,
            carriers_quoted=len(carriers),
            processing_time_ms=elapsed_ms,
            request_id=request_id,
        )

        # Log the quote (fire-and-forget)
        asyncio.create_task(
            self._log_quote(request, response, elapsed_ms)
        )

        return response

    async def quick_quote(self, request: PMIQuickQuoteRequest) -> PMIQuoteResponse:
        """Minimal-input quote — uses defaults for DTI, property type, etc."""
        full_request = PMIQuoteRequest(
            loan_amount=request.loan_amount,
            property_value=request.property_value,
            fico_score=request.fico_score,
        )
        return await self.price_loan(full_request)

    async def compare_carriers(
        self,
        ltv: float,
        fico: int,
        coverage: float,
        loan_amount: float = 100_000,
    ) -> list[PMICarrierQuote]:
        """Cross-carrier comparison for a specific LTV/FICO/coverage point."""
        carriers = await self._get_pmi_carriers()
        tasks = [
            self._price_carrier(
                carrier=c,
                ltv=ltv,
                fico=fico,
                coverage=coverage,
                loan_amount=loan_amount,
                premium_types=["monthly"],
                loan_params={},
            )
            for c in carriers
        ]
        nested = await asyncio.gather(*tasks, return_exceptions=False)
        quotes = [q for sublist in nested for q in sublist]
        quotes.sort(key=lambda q: q.annual_premium)
        return quotes

    async def get_market_grid(self) -> list[dict[str, Any]]:
        """Build full LTV×FICO comparison grid across all carriers."""
        ltv_buckets = [
            ("80.01-85", 82.5),
            ("85.01-90", 87.5),
            ("90.01-95", 92.5),
            ("95.01-97", 96.0),
        ]
        fico_buckets = [
            ("760+", 780),
            ("740-759", 750),
            ("720-739", 730),
            ("700-719", 710),
            ("680-699", 690),
            ("660-679", 670),
            ("640-659", 650),
            ("620-639", 630),
        ]

        entries: list[dict[str, Any]] = []
        carriers = await self._get_pmi_carriers()

        for carrier in carriers:
            for ltv_label, ltv_val in ltv_buckets:
                cov = _gse_required_coverage(ltv_val)
                for fico_label, fico_val in fico_buckets:
                    rate = await self._lookup_base_rate(
                        carrier_id=carrier["id"],
                        ltv=ltv_val,
                        fico=fico_val,
                        coverage=cov,
                        premium_type="monthly",
                    )
                    if rate is not None:
                        monthly_per_100k = round(float(rate) / 12 * 1000, 2)
                        entries.append({
                            "carrier_name": carrier["legal_name"],
                            "carrier_id": str(carrier["id"]),
                            "ltv_bucket": ltv_label,
                            "fico_bucket": fico_label,
                            "coverage_pct": cov,
                            "rate_pct": float(rate),
                            "monthly_per_100k": monthly_per_100k,
                        })

        return entries

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _get_pmi_carriers(
        self,
        state: str | None = None,
        premium_types: list[str] | None = None,
        carrier_ids: list[uuid.UUID] | None = None,
    ) -> list[dict[str, Any]]:
        """Return active PMI carriers that have current rate cards."""
        engine = await self._get_engine()

        # Build dynamic WHERE clauses
        conditions = ["c.status = 'active'", "rc.is_current = TRUE"]
        params: dict[str, Any] = {}

        if state:
            conditions.append("(rc.state = :state OR rc.state IS NULL)")
            params["state"] = state

        if premium_types:
            conditions.append("rc.premium_type = ANY(:ptypes)")
            params["ptypes"] = premium_types

        if carrier_ids:
            conditions.append("c.id = ANY(:cids)")
            params["cids"] = [str(cid) for cid in carrier_ids]

        where = " AND ".join(conditions)

        query = text(f"""
            SELECT DISTINCT
                c.id, c.naic_code, c.legal_name, c.am_best_rating
            FROM hermes_carriers c
            JOIN hermes_pmi_rate_cards rc ON rc.carrier_id = c.id
            WHERE {where}
            ORDER BY c.legal_name
        """)

        async with engine.connect() as conn:
            result = await conn.execute(query, params)
            rows = result.mappings().all()

        return [dict(row) for row in rows]

    async def _price_carrier(
        self,
        carrier: dict[str, Any],
        ltv: float,
        fico: int,
        coverage: float,
        loan_amount: float,
        premium_types: list[str],
        loan_params: dict[str, Any],
    ) -> list[PMICarrierQuote]:
        """Price a single carrier across requested premium types."""
        quotes: list[PMICarrierQuote] = []
        carrier_id = carrier["id"]

        for ptype in premium_types:
            try:
                base_rate = await self._lookup_base_rate(
                    carrier_id=carrier_id,
                    ltv=ltv,
                    fico=fico,
                    coverage=coverage,
                    premium_type=ptype,
                )
                if base_rate is None:
                    continue

                adjustments = await self._apply_adjustments(
                    carrier_id=carrier_id,
                    premium_type=ptype,
                    loan_params=loan_params,
                )

                adjusted_rate = float(base_rate)
                applied: list[dict[str, Any]] = []
                for adj in adjustments:
                    method = adj["method"]
                    value = float(adj["value"])
                    before = adjusted_rate
                    if method == "additive":
                        adjusted_rate += value
                    elif method == "multiplicative":
                        adjusted_rate *= value
                    elif method == "override":
                        adjusted_rate = value
                    applied.append({
                        "name": adj["name"],
                        "method": method,
                        "value": value,
                        "rate_before": round(before, 4),
                        "rate_after": round(adjusted_rate, 4),
                    })

                monthly, annual, single = self._calculate_premiums(
                    rate_pct=adjusted_rate,
                    loan_amount=loan_amount,
                    premium_type=ptype,
                )

                # Get rate card metadata
                rc_meta = await self._get_rate_card_meta(carrier_id, ptype)

                quotes.append(PMICarrierQuote(
                    carrier_id=carrier_id,
                    carrier_name=carrier["legal_name"],
                    naic_code=carrier["naic_code"],
                    am_best_rating=carrier.get("am_best_rating"),
                    premium_type=ptype,
                    base_rate_pct=round(float(base_rate), 4),
                    adjusted_rate_pct=round(adjusted_rate, 4),
                    monthly_premium=round(monthly, 2),
                    annual_premium=round(annual, 2),
                    single_premium=round(single, 2) if single else None,
                    coverage_pct=coverage,
                    ltv=round(ltv, 2),
                    adjustments_applied=applied,
                    rate_card_source=rc_meta.get("source", "manual"),
                    rate_card_effective=rc_meta.get("effective_date"),
                ))

            except Exception as exc:
                logger.exception(
                    "Error pricing carrier=%s ptype=%s: %s",
                    carrier["legal_name"], ptype, exc,
                )

        return quotes

    async def _lookup_base_rate(
        self,
        carrier_id: Any,
        ltv: float,
        fico: int,
        coverage: float,
        premium_type: str,
        state: str | None = None,
    ) -> Decimal | None:
        """Look up the base rate from the rate grid.

        Checks for state-specific card first, falls back to nationwide.
        """
        engine = await self._get_engine()

        query = text("""
            SELECT r.rate_pct
            FROM hermes_pmi_rates r
            JOIN hermes_pmi_rate_cards rc ON rc.id = r.rate_card_id
            WHERE rc.carrier_id = :carrier_id
              AND rc.premium_type = :ptype
              AND rc.is_current = TRUE
              AND :ltv BETWEEN r.ltv_min AND r.ltv_max
              AND :fico BETWEEN r.fico_min AND r.fico_max
              AND r.coverage_pct = :coverage
            ORDER BY
                CASE WHEN rc.state IS NOT NULL THEN 0 ELSE 1 END,
                rc.effective_date DESC
            LIMIT 1
        """)

        async with engine.connect() as conn:
            result = await conn.execute(query, {
                "carrier_id": carrier_id,
                "ptype": premium_type,
                "ltv": Decimal(str(ltv)),
                "fico": fico,
                "coverage": Decimal(str(coverage)),
            })
            row = result.mappings().first()

        return row["rate_pct"] if row else None

    async def _apply_adjustments(
        self,
        carrier_id: Any,
        premium_type: str,
        loan_params: dict[str, Any],
    ) -> list[dict[str, Any]]:
        """Evaluate JSONB conditions and return matching adjustments."""
        if not loan_params:
            return []

        engine = await self._get_engine()

        query = text("""
            SELECT a.name, a.condition, a.adjustment_method, a.adjustment_value
            FROM hermes_pmi_adjustments a
            JOIN hermes_pmi_rate_cards rc ON rc.id = a.rate_card_id
            WHERE rc.carrier_id = :carrier_id
              AND rc.premium_type = :ptype
              AND rc.is_current = TRUE
            ORDER BY a.name
        """)

        async with engine.connect() as conn:
            result = await conn.execute(query, {
                "carrier_id": carrier_id,
                "ptype": premium_type,
            })
            rows = result.mappings().all()

        matched: list[dict[str, Any]] = []
        for row in rows:
            condition = row["condition"] if isinstance(row["condition"], dict) else {}
            if self._evaluate_condition(condition, loan_params):
                matched.append({
                    "name": row["name"],
                    "method": row["adjustment_method"],
                    "value": row["adjustment_value"],
                })

        return matched

    @staticmethod
    def _evaluate_condition(condition: dict[str, Any], params: dict[str, Any]) -> bool:
        """Check whether a JSONB condition matches the loan parameters.

        Supported condition keys:
        - field_min / field_max: range checks (e.g. dti_min=43, dti_max=50)
        - field_eq: exact match (e.g. property_type_eq="condo")
        - field_in: list membership (e.g. occupancy_in=["secondary","investment"])
        """
        if not condition:
            return False

        for key, value in condition.items():
            if key.endswith("_min"):
                field = key[:-4]  # e.g. "dti_min" → "dti"
                param_val = params.get(field)
                if param_val is None or float(param_val) < float(value):
                    return False

            elif key.endswith("_max"):
                field = key[:-4]
                param_val = params.get(field)
                if param_val is None or float(param_val) > float(value):
                    return False

            elif key.endswith("_eq"):
                field = key[:-3]
                param_val = params.get(field)
                if param_val is None or str(param_val) != str(value):
                    return False

            elif key.endswith("_in"):
                field = key[:-3]
                param_val = params.get(field)
                if param_val is None or str(param_val) not in [str(v) for v in value]:
                    return False

        return True

    @staticmethod
    def _calculate_premiums(
        rate_pct: float,
        loan_amount: float,
        premium_type: str,
    ) -> tuple[float, float, float | None]:
        """Calculate monthly, annual, and (if applicable) single premiums.

        Parameters
        ----------
        rate_pct : annual rate as percentage (e.g. 0.52 means 0.52%)
        loan_amount : loan amount in USD
        premium_type : monthly, single, split, lender_paid

        Returns
        -------
        (monthly_premium, annual_premium, single_premium_or_None)
        """
        rate_decimal = rate_pct / 100.0
        annual = rate_decimal * loan_amount
        monthly = annual / 12

        single = None
        if premium_type == "single":
            # Single premiums are typically ~2.5-4x the annual rate
            single = rate_decimal * loan_amount * 3.0
        elif premium_type == "split":
            # Split: upfront portion (~1.5x annual) + reduced monthly
            single = rate_decimal * loan_amount * 1.5
            monthly = monthly * 0.5

        return monthly, annual, single

    async def _get_rate_card_meta(
        self, carrier_id: Any, premium_type: str
    ) -> dict[str, Any]:
        """Return source and effective_date for the current rate card."""
        engine = await self._get_engine()
        query = text("""
            SELECT source, effective_date
            FROM hermes_pmi_rate_cards
            WHERE carrier_id = :cid AND premium_type = :ptype AND is_current = TRUE
            ORDER BY effective_date DESC
            LIMIT 1
        """)
        async with engine.connect() as conn:
            result = await conn.execute(query, {"cid": carrier_id, "ptype": premium_type})
            row = result.mappings().first()
        if row:
            eff = row["effective_date"]
            return {
                "source": row["source"],
                "effective_date": eff.isoformat() if isinstance(eff, (date, datetime)) else eff,
            }
        return {}

    async def _log_quote(
        self,
        request: PMIQuoteRequest,
        response: PMIQuoteResponse,
        elapsed_ms: float,
    ) -> None:
        """Insert an audit trail record into hermes_pmi_quote_log."""
        try:
            engine = await self._get_engine()
            best_carrier_id = None
            best_rate = None
            if response.quotes:
                best = response.quotes[0]
                best_carrier_id = best.carrier_id
                best_rate = Decimal(str(best.adjusted_rate_pct))

            async with engine.begin() as conn:
                await conn.execute(
                    text("""
                        INSERT INTO hermes_pmi_quote_log
                            (request_data, response_data, carriers_quoted,
                             best_rate, best_carrier_id, processing_time_ms, source)
                        VALUES
                            (CAST(:req AS jsonb), CAST(:resp AS jsonb), :count,
                             :best_rate, :best_cid, :elapsed, 'api')
                    """),
                    {
                        "req": json.dumps(request.model_dump(), default=str),
                        "resp": json.dumps({
                            "carriers_quoted": response.carriers_quoted,
                            "quotes_returned": len(response.quotes),
                            "best_annual": response.quotes[0].annual_premium if response.quotes else None,
                        }, default=str),
                        "count": response.carriers_quoted,
                        "best_rate": best_rate,
                        "best_cid": best_carrier_id,
                        "elapsed": Decimal(str(elapsed_ms)),
                    },
                )
        except Exception as exc:
            logger.warning("Failed to log PMI quote: %s", exc)
