"""Pydantic request/response schemas for the Title Insurance pricing API."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Optional
from uuid import UUID

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Request schemas
# ---------------------------------------------------------------------------


class TitleQuoteRequest(BaseModel):
    """Full title insurance quote request."""

    purchase_price: float = Field(..., gt=0, description="Purchase price in USD")
    loan_amount: float = Field(
        default=0, ge=0,
        description="Loan amount in USD (0 = cash purchase, no lender policy)",
    )
    state: str = Field(..., min_length=2, max_length=2, description="State code")
    policy_type: str = Field(
        default="simultaneous",
        description="Policy type: owner, lender, simultaneous",
    )
    is_refinance: bool = Field(default=False, description="Refinance transaction")
    years_since_prior_policy: float | None = Field(
        default=None, ge=0,
        description="Years since prior title policy (for reissue credit)",
    )
    endorsements: list[str] = Field(
        default_factory=list,
        description="List of ALTA endorsement codes to include",
    )
    carrier_ids: list[UUID] | None = Field(
        default=None,
        description="Limit quote to specific carrier UUIDs",
    )


class TitleQuickQuoteRequest(BaseModel):
    """Minimal quick-quote — only the 3 essential inputs."""

    purchase_price: float = Field(..., gt=0, description="Purchase price in USD")
    loan_amount: float = Field(default=0, ge=0, description="Loan amount in USD")
    state: str = Field(..., min_length=2, max_length=2, description="State code")


# ---------------------------------------------------------------------------
# Response schemas
# ---------------------------------------------------------------------------


class TitleCarrierInfo(BaseModel):
    """Title carrier summary."""

    carrier_id: UUID
    legal_name: str
    naic_code: str
    am_best_rating: str | None = None
    rate_cards_loaded: int = 0
    states_available: list[str] = Field(default_factory=list)


class TitleCarrierQuote(BaseModel):
    """Single carrier's title insurance quote result."""

    carrier_id: UUID
    carrier_name: str
    naic_code: str
    am_best_rating: str | None = None
    owner_premium: float = 0.0
    lender_premium: float = 0.0
    simultaneous_premium: float = 0.0
    simultaneous_savings: float = 0.0
    simultaneous_discount_pct: float = 0.0
    reissue_credit: float = 0.0
    endorsement_fees: float = 0.0
    total_premium: float = 0.0
    is_promulgated: bool = False
    rate_card_source: str = "manual"
    rate_card_effective: date | None = None


class TitleQuoteResponse(BaseModel):
    """Multi-carrier title quote response envelope."""

    quotes: list[TitleCarrierQuote]
    best_total: TitleCarrierQuote | None = None
    best_simultaneous_savings: TitleCarrierQuote | None = None
    purchase_price: float
    loan_amount: float
    state: str
    policy_type: str
    carriers_quoted: int
    processing_time_ms: float
    request_id: UUID | None = None


class TitleSimultaneousGridEntry(BaseModel):
    """Single cell in the cross-carrier simultaneous issue grid."""

    carrier_name: str
    carrier_id: UUID
    naic_code: str
    loan_amount: float
    owner_premium: float
    lender_standalone: float
    simultaneous_premium: float
    simultaneous_savings: float
    discount_pct: float
    is_promulgated: bool = False


class TitleSimultaneousGridResponse(BaseModel):
    """Cross-carrier simultaneous issue dispersion analysis."""

    entries: list[TitleSimultaneousGridEntry]
    loan_amounts: list[float]
    carriers: list[str]
    max_savings_carrier: str | None = None
    max_savings_amount: float = 0.0
    generated_at: datetime


class TitleRateGridEntry(BaseModel):
    """Rate grid entry for a single carrier."""

    coverage_range: str
    rate_per_thousand: float
    flat_fee: float
    minimum_premium: float


class TitleCarrierRatesResponse(BaseModel):
    """Carrier rate grid response."""

    carrier_id: UUID
    carrier_name: str
    policy_type: str
    state: str
    effective_date: date | None = None
    is_promulgated: bool = False
    rates: list[TitleRateGridEntry]


class TitleBenchmarkResponse(BaseModel):
    """Benchmark response for a standard $400K/$380K transaction."""

    state: str
    purchase_price: float
    loan_amount: float
    carriers: list[TitleCarrierQuote]
    cheapest_total: float | None = None
    cheapest_carrier: str | None = None
    max_simultaneous_savings: float | None = None
    max_savings_carrier: str | None = None
    generated_at: datetime
