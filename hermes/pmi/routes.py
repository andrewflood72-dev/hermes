"""FastAPI routes for the Hermes PMI pricing engine.

All endpoints are prefixed with ``/v1/pmi`` and require the
``X-API-Key`` header for authentication.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException, Query, status
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from hermes.pmi.engine import HermesPMIEngine
from hermes.pmi.schemas import (
    PMICarrierInfo,
    PMICarrierQuote,
    PMICarrierRatesResponse,
    PMIMarketGridEntry,
    PMIMarketGridResponse,
    PMIQuickQuoteRequest,
    PMIQuoteRequest,
    PMIQuoteResponse,
    PMIRateGridEntry,
    PMIStateRulesResponse,
)

logger = logging.getLogger("hermes.pmi.routes")

router = APIRouter(prefix="/v1/pmi", tags=["PMI"])

# ---------------------------------------------------------------------------
# Module-level state — set by the main app lifespan handler
# ---------------------------------------------------------------------------

_pmi_engine: HermesPMIEngine | None = None
_db_engine: AsyncEngine | None = None


def set_pmi_engine(engine: HermesPMIEngine) -> None:
    """Called by the main app to inject the initialised PMI engine."""
    global _pmi_engine
    _pmi_engine = engine


def set_db_engine(engine: AsyncEngine) -> None:
    """Called by the main app to inject the database engine."""
    global _db_engine
    _db_engine = engine


def _get_pmi_engine() -> HermesPMIEngine:
    if _pmi_engine is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="PMI engine not initialised.",
        )
    return _pmi_engine


def _get_db_engine() -> AsyncEngine:
    if _db_engine is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database engine not initialised.",
        )
    return _db_engine


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.post(
    "/quote",
    response_model=PMIQuoteResponse,
    summary="Full multi-carrier PMI pricing",
)
async def pmi_quote(body: PMIQuoteRequest) -> PMIQuoteResponse:
    """Price a loan across all PMI carriers and return ranked quotes."""
    engine = _get_pmi_engine()
    return await engine.price_loan(body)


@router.post(
    "/quick-quote",
    response_model=PMIQuoteResponse,
    summary="Quick PMI estimate with minimal inputs",
)
async def pmi_quick_quote(body: PMIQuickQuoteRequest) -> PMIQuoteResponse:
    """Estimated PMI range using only loan amount, property value, and FICO."""
    engine = _get_pmi_engine()
    return await engine.quick_quote(body)


@router.get(
    "/carriers",
    response_model=list[PMICarrierInfo],
    summary="List PMI carriers and rate card status",
)
async def list_pmi_carriers() -> list[PMICarrierInfo]:
    """Return all 6 PMI carriers with their current rate card availability."""
    db = _get_db_engine()

    query = text("""
        SELECT
            c.id AS carrier_id,
            c.legal_name,
            c.naic_code,
            c.am_best_rating,
            COUNT(DISTINCT rc.id) AS rate_cards_loaded,
            ARRAY_AGG(DISTINCT rc.premium_type) FILTER (WHERE rc.premium_type IS NOT NULL)
                AS premium_types
        FROM hermes_carriers c
        LEFT JOIN hermes_pmi_rate_cards rc
            ON rc.carrier_id = c.id AND rc.is_current = TRUE
        WHERE c.naic_code IN ('50501', '50502', '50503', '50504', '50505', '50506')
        GROUP BY c.id, c.legal_name, c.naic_code, c.am_best_rating
        ORDER BY c.legal_name
    """)

    async with db.connect() as conn:
        result = await conn.execute(query)
        rows = result.mappings().all()

    return [
        PMICarrierInfo(
            carrier_id=row["carrier_id"],
            legal_name=row["legal_name"],
            naic_code=row["naic_code"],
            am_best_rating=row["am_best_rating"],
            rate_cards_loaded=row["rate_cards_loaded"],
            premium_types_available=row["premium_types"] or [],
        )
        for row in rows
    ]


@router.get(
    "/carriers/{naic_code}/rates",
    response_model=PMICarrierRatesResponse,
    summary="Carrier rate grid",
)
async def get_carrier_rates(
    naic_code: str,
    premium_type: str = Query(default="monthly", description="Premium type to show"),
) -> PMICarrierRatesResponse:
    """Return the full rate grid for a specific carrier and premium type."""
    db = _get_db_engine()

    # Get carrier
    carrier_q = text("""
        SELECT id, legal_name FROM hermes_carriers WHERE naic_code = :naic
    """)
    async with db.connect() as conn:
        carrier_row = (await conn.execute(carrier_q, {"naic": naic_code})).mappings().first()

    if not carrier_row:
        raise HTTPException(status_code=404, detail=f"Carrier {naic_code} not found")

    # Get rates
    rates_q = text("""
        SELECT
            r.ltv_min, r.ltv_max, r.fico_min, r.fico_max,
            r.coverage_pct, r.rate_pct,
            rc.effective_date
        FROM hermes_pmi_rates r
        JOIN hermes_pmi_rate_cards rc ON rc.id = r.rate_card_id
        WHERE rc.carrier_id = :cid
          AND rc.premium_type = :ptype
          AND rc.is_current = TRUE
        ORDER BY r.ltv_min, r.fico_min DESC, r.coverage_pct
    """)

    async with db.connect() as conn:
        result = await conn.execute(rates_q, {
            "cid": carrier_row["id"],
            "ptype": premium_type,
        })
        rows = result.mappings().all()

    rates = []
    eff_date = None
    for row in rows:
        if eff_date is None:
            eff_date = row.get("effective_date")
        rate_pct = float(row["rate_pct"])
        rates.append(PMIRateGridEntry(
            ltv_range=f"{row['ltv_min']}-{row['ltv_max']}",
            fico_range=f"{row['fico_min']}-{row['fico_max']}",
            coverage_pct=float(row["coverage_pct"]),
            rate_pct=rate_pct,
            monthly_per_100k=round(rate_pct / 100 / 12 * 100_000, 2),
        ))

    return PMICarrierRatesResponse(
        carrier_id=carrier_row["id"],
        carrier_name=carrier_row["legal_name"],
        premium_type=premium_type,
        effective_date=eff_date,
        rates=rates,
    )


@router.get(
    "/comparison",
    response_model=list[PMICarrierQuote],
    summary="Cross-carrier comparison for specific profile",
)
async def compare_carriers(
    ltv: float = Query(..., ge=80.01, le=97, description="LTV ratio"),
    fico: int = Query(..., ge=300, le=850, description="FICO score"),
    coverage: float = Query(..., ge=1, le=50, description="Coverage %"),
    loan_amount: float = Query(default=100_000, gt=0, description="Loan amount for premium calc"),
) -> list[PMICarrierQuote]:
    """Compare PMI rates across all carriers for a specific LTV/FICO/coverage."""
    engine = _get_pmi_engine()
    return await engine.compare_carriers(ltv, fico, coverage, loan_amount)


@router.get(
    "/market-grid",
    response_model=PMIMarketGridResponse,
    summary="Full LTV x FICO market comparison grid",
)
async def market_grid() -> PMIMarketGridResponse:
    """Build a full market comparison grid across all carriers, LTV bands, and FICO tiers."""
    engine = _get_pmi_engine()
    entries = await engine.get_market_grid()

    ltv_buckets = sorted({e["ltv_bucket"] for e in entries})
    fico_buckets = sorted({e["fico_bucket"] for e in entries}, reverse=True)
    carriers = sorted({e["carrier_name"] for e in entries})

    return PMIMarketGridResponse(
        entries=[PMIMarketGridEntry(**e) for e in entries],
        ltv_buckets=ltv_buckets,
        fico_buckets=fico_buckets,
        carriers=carriers,
        generated_at=datetime.now(timezone.utc),
    )


@router.get(
    "/state-rules/{state}",
    response_model=PMIStateRulesResponse,
    summary="State-specific PMI rules and SERFF data",
)
async def state_rules(state: str) -> PMIStateRulesResponse:
    """Return state-specific PMI rules and SERFF filing data."""
    if len(state) != 2:
        raise HTTPException(status_code=400, detail="State must be a 2-letter code")

    db = _get_db_engine()

    query = text("""
        SELECT
            c.legal_name,
            c.naic_code,
            s.approved_rate_range,
            s.state_rules,
            s.effective_date,
            s.expiration_date
        FROM hermes_pmi_serff_data s
        JOIN hermes_carriers c ON c.id = s.carrier_id
        WHERE s.state = :state
        ORDER BY c.legal_name
    """)

    async with db.connect() as conn:
        result = await conn.execute(query, {"state": state.upper()})
        rows = result.mappings().all()

    carriers_data = []
    rules_data = []
    for row in rows:
        carriers_data.append({
            "carrier": row["legal_name"],
            "naic_code": row["naic_code"],
            "approved_rate_range": row["approved_rate_range"],
            "effective_date": row["effective_date"].isoformat() if row["effective_date"] else None,
        })
        if row["state_rules"]:
            rules_data.append({
                "carrier": row["legal_name"],
                "rules": row["state_rules"],
            })

    return PMIStateRulesResponse(
        state=state.upper(),
        carriers=carriers_data,
        rules=rules_data,
    )
