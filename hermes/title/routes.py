"""FastAPI routes for the Hermes Title Insurance pricing engine.

All endpoints are prefixed with ``/v1/title`` and require the
``X-API-Key`` header for authentication.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query, status
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from hermes.title.engine import HermesTitleEngine
from hermes.title.schemas import (
    TitleBenchmarkResponse,
    TitleCarrierInfo,
    TitleCarrierQuote,
    TitleCarrierRatesResponse,
    TitleQuickQuoteRequest,
    TitleQuoteRequest,
    TitleQuoteResponse,
    TitleRateGridEntry,
    TitleSimultaneousGridResponse,
)

logger = logging.getLogger("hermes.title.routes")

router = APIRouter(prefix="/v1/title", tags=["Title Insurance"])

# ---------------------------------------------------------------------------
# Module-level state — set by the main app lifespan handler
# ---------------------------------------------------------------------------

_title_engine: HermesTitleEngine | None = None
_db_engine: AsyncEngine | None = None


def set_title_engine(engine: HermesTitleEngine) -> None:
    """Called by the main app to inject the initialised title engine."""
    global _title_engine
    _title_engine = engine


def set_db_engine(engine: AsyncEngine) -> None:
    """Called by the main app to inject the database engine."""
    global _db_engine
    _db_engine = engine


def _get_title_engine() -> HermesTitleEngine:
    if _title_engine is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Title engine not initialised.",
        )
    return _title_engine


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
    response_model=TitleQuoteResponse,
    summary="Full multi-carrier title insurance pricing",
)
async def title_quote(body: TitleQuoteRequest) -> TitleQuoteResponse:
    """Price a title transaction across all carriers and return ranked quotes."""
    engine = _get_title_engine()
    return await engine.price_policy(body)


@router.post(
    "/quick-quote",
    response_model=TitleQuoteResponse,
    summary="Quick title estimate with minimal inputs",
)
async def title_quick_quote(body: TitleQuickQuoteRequest) -> TitleQuoteResponse:
    """Estimated title premium using only purchase price, loan amount, and state."""
    engine = _get_title_engine()
    return await engine.quick_quote(body)


@router.get(
    "/carriers",
    response_model=list[TitleCarrierInfo],
    summary="List title carriers and rate card status",
)
async def list_title_carriers() -> list[TitleCarrierInfo]:
    """Return all 8 title carriers with their current rate card availability."""
    db = _get_db_engine()

    query = text("""
        SELECT
            c.id AS carrier_id,
            c.legal_name,
            c.naic_code,
            c.am_best_rating,
            COUNT(DISTINCT rc.id) AS rate_cards_loaded,
            ARRAY_AGG(DISTINCT rc.state) FILTER (WHERE rc.state IS NOT NULL)
                AS states
        FROM hermes_carriers c
        LEFT JOIN hermes_title_rate_cards rc
            ON rc.carrier_id = c.id AND rc.is_current = TRUE
        WHERE c.naic_code BETWEEN '60001' AND '60008'
        GROUP BY c.id, c.legal_name, c.naic_code, c.am_best_rating
        ORDER BY c.legal_name
    """)

    async with db.connect() as conn:
        result = await conn.execute(query)
        rows = result.mappings().all()

    return [
        TitleCarrierInfo(
            carrier_id=row["carrier_id"],
            legal_name=row["legal_name"],
            naic_code=row["naic_code"],
            am_best_rating=row["am_best_rating"],
            rate_cards_loaded=row["rate_cards_loaded"],
            states_available=row["states"] or [],
        )
        for row in rows
    ]


@router.get(
    "/carriers/{naic_code}/rates",
    response_model=TitleCarrierRatesResponse,
    summary="Carrier rate grid",
)
async def get_carrier_rates(
    naic_code: str,
    state: str = Query(default="TX", description="State code"),
    policy_type: str = Query(default="owner", description="Policy type: owner, lender"),
) -> TitleCarrierRatesResponse:
    """Return the full rate grid for a specific carrier, state, and policy type."""
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
            r.coverage_min, r.coverage_max,
            r.rate_per_thousand, r.flat_fee, r.minimum_premium,
            rc.effective_date, rc.is_promulgated
        FROM hermes_title_rates r
        JOIN hermes_title_rate_cards rc ON rc.id = r.rate_card_id
        WHERE rc.carrier_id = :cid
          AND rc.state = :state
          AND rc.policy_type = :ptype
          AND rc.is_current = TRUE
        ORDER BY r.coverage_min ASC
    """)

    async with db.connect() as conn:
        result = await conn.execute(rates_q, {
            "cid": carrier_row["id"],
            "state": state.upper(),
            "ptype": policy_type,
        })
        rows = result.mappings().all()

    rates = []
    eff_date = None
    is_promulgated = False
    for row in rows:
        if eff_date is None:
            eff_date = row.get("effective_date")
            is_promulgated = row.get("is_promulgated", False)
        rates.append(TitleRateGridEntry(
            coverage_range=f"${row['coverage_min']:,.0f}-${row['coverage_max']:,.0f}",
            rate_per_thousand=float(row["rate_per_thousand"]),
            flat_fee=float(row["flat_fee"]),
            minimum_premium=float(row["minimum_premium"]),
        ))

    return TitleCarrierRatesResponse(
        carrier_id=carrier_row["id"],
        carrier_name=carrier_row["legal_name"],
        policy_type=policy_type,
        state=state.upper(),
        effective_date=eff_date,
        is_promulgated=is_promulgated,
        rates=rates,
    )


@router.get(
    "/comparison",
    response_model=list[TitleCarrierQuote],
    summary="Cross-carrier comparison for specific transaction",
)
async def compare_carriers(
    purchase_price: float = Query(..., gt=0, description="Purchase price"),
    loan_amount: float = Query(default=0, ge=0, description="Loan amount"),
    state: str = Query(default="TX", description="State code"),
) -> list[TitleCarrierQuote]:
    """Compare title premiums across all carriers for a specific transaction."""
    engine = _get_title_engine()
    policy_type = "simultaneous" if loan_amount > 0 else "owner"
    request = TitleQuoteRequest(
        purchase_price=purchase_price,
        loan_amount=loan_amount,
        state=state,
        policy_type=policy_type,
    )
    response = await engine.price_policy(request)
    return response.quotes


@router.get(
    "/simultaneous-issue-grid",
    response_model=TitleSimultaneousGridResponse,
    summary="Cross-carrier simultaneous issue dispersion grid",
)
async def simultaneous_issue_grid(
    state: str = Query(default="TX", description="State code"),
    purchase_price: float = Query(default=400_000, gt=0, description="Purchase price"),
) -> TitleSimultaneousGridResponse:
    """THE key data product — cross-carrier simultaneous issue dispersion analysis.

    Shows how much the lender policy costs when issued simultaneously with
    the owner policy, across all carriers and loan amounts.
    """
    engine = _get_title_engine()
    return await engine.get_simultaneous_issue_grid(
        state=state.upper(),
        purchase_price=purchase_price,
    )


@router.get(
    "/benchmark",
    response_model=TitleBenchmarkResponse,
    summary="Standard benchmark quote ($400K/$380K)",
)
async def benchmark(
    state: str = Query(default="TX", description="State code"),
    purchase_price: float = Query(default=400_000, gt=0, description="Purchase price"),
    loan_amount: float = Query(default=380_000, ge=0, description="Loan amount"),
) -> TitleBenchmarkResponse:
    """Return a benchmark quote for a standard transaction."""
    engine = _get_title_engine()
    request = TitleQuoteRequest(
        purchase_price=purchase_price,
        loan_amount=loan_amount,
        state=state.upper(),
        policy_type="simultaneous" if loan_amount > 0 else "owner",
    )
    response = await engine.price_policy(request)

    cheapest = response.quotes[0] if response.quotes else None
    best_savings = max(response.quotes, key=lambda q: q.simultaneous_savings) if response.quotes else None

    return TitleBenchmarkResponse(
        state=state.upper(),
        purchase_price=purchase_price,
        loan_amount=loan_amount,
        carriers=response.quotes,
        cheapest_total=cheapest.total_premium if cheapest else None,
        cheapest_carrier=cheapest.carrier_name if cheapest else None,
        max_simultaneous_savings=best_savings.simultaneous_savings if best_savings else None,
        max_savings_carrier=best_savings.carrier_name if best_savings else None,
        generated_at=datetime.now(timezone.utc),
    )
