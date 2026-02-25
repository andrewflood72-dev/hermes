"""Pydantic request/response schemas for the PMI pricing API."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Optional
from uuid import UUID

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Request schemas
# ---------------------------------------------------------------------------


class PMIQuoteRequest(BaseModel):
    """Full PMI quote request with all loan parameters."""

    loan_amount: float = Field(..., gt=0, description="Loan amount in USD")
    property_value: float = Field(..., gt=0, description="Appraised property value in USD")
    fico_score: int = Field(..., ge=300, le=850, description="Borrower FICO score")
    dti: float | None = Field(default=None, ge=0, le=65, description="Debt-to-income ratio (%)")
    property_type: str = Field(
        default="single_family",
        description="Property type: single_family, condo, townhouse, 2_unit, 3_4_unit, manufactured",
    )
    occupancy: str = Field(
        default="primary",
        description="Occupancy type: primary, secondary, investment",
    )
    loan_purpose: str = Field(
        default="purchase",
        description="Loan purpose: purchase, rate_term_refi, cash_out_refi",
    )
    loan_term: int = Field(default=360, description="Loan term in months (360=30yr, 180=15yr)")
    state: str | None = Field(default=None, min_length=2, max_length=2, description="State code")
    coverage_pct: float | None = Field(
        default=None, ge=1, le=50,
        description="Override coverage %. If omitted, uses GSE minimums.",
    )
    premium_type: str | None = Field(
        default=None,
        description="Filter to specific premium type: monthly, single, split, lender_paid",
    )
    carrier_ids: list[UUID] | None = Field(
        default=None,
        description="Limit quote to specific carrier UUIDs",
    )


class PMIQuickQuoteRequest(BaseModel):
    """Minimal quick-quote — only the 3 essential inputs."""

    loan_amount: float = Field(..., gt=0, description="Loan amount in USD")
    property_value: float = Field(..., gt=0, description="Appraised property value in USD")
    fico_score: int = Field(..., ge=300, le=850, description="Borrower FICO score")


# ---------------------------------------------------------------------------
# Response schemas
# ---------------------------------------------------------------------------


class PMICarrierInfo(BaseModel):
    """PMI carrier summary."""

    carrier_id: UUID
    legal_name: str
    naic_code: str
    am_best_rating: str | None = None
    rate_cards_loaded: int = 0
    premium_types_available: list[str] = Field(default_factory=list)


class PMICarrierQuote(BaseModel):
    """Single carrier's quote result."""

    carrier_id: UUID
    carrier_name: str
    naic_code: str
    am_best_rating: str | None = None
    premium_type: str
    base_rate_pct: float
    adjusted_rate_pct: float
    monthly_premium: float
    annual_premium: float
    single_premium: float | None = None
    coverage_pct: float
    ltv: float
    adjustments_applied: list[dict[str, Any]] = Field(default_factory=list)
    rate_card_source: str = "manual"
    rate_card_effective: date | None = None


class PMIQuoteResponse(BaseModel):
    """Multi-carrier quote response envelope."""

    quotes: list[PMICarrierQuote]
    best_monthly: PMICarrierQuote | None = None
    best_annual: PMICarrierQuote | None = None
    loan_amount: float
    property_value: float
    ltv: float
    fico_score: int
    coverage_pct: float
    carriers_quoted: int
    processing_time_ms: float
    request_id: UUID | None = None


class PMIMarketGridEntry(BaseModel):
    """Single cell in the LTV×FICO market comparison grid."""

    carrier_name: str
    carrier_id: UUID
    ltv_bucket: str
    fico_bucket: str
    coverage_pct: float
    rate_pct: float
    monthly_per_100k: float


class PMIMarketGridResponse(BaseModel):
    """Full market grid comparison response."""

    entries: list[PMIMarketGridEntry]
    ltv_buckets: list[str]
    fico_buckets: list[str]
    carriers: list[str]
    generated_at: datetime


class PMIRateGridEntry(BaseModel):
    """Rate grid entry for a single carrier."""

    ltv_range: str
    fico_range: str
    coverage_pct: float
    rate_pct: float
    monthly_per_100k: float


class PMICarrierRatesResponse(BaseModel):
    """Carrier rate grid response."""

    carrier_id: UUID
    carrier_name: str
    premium_type: str
    effective_date: date | None = None
    rates: list[PMIRateGridEntry]


class PMIStateRulesResponse(BaseModel):
    """State-specific PMI rules and SERFF data."""

    state: str
    carriers: list[dict[str, Any]]
    rules: list[dict[str, Any]]
