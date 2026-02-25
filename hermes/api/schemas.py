"""Pydantic schemas for all Hermes API request/response models.

These schemas are deliberately decoupled from the internal matching models so
that the public API surface can evolve independently of the core engine.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Request schemas
# ---------------------------------------------------------------------------


class RiskProfileInput(BaseModel):
    """Inbound risk profile submitted to the /v1/match endpoint.

    Attributes
    ----------
    entity_name:
        Legal name of the insured entity.
    naics_code:
        NAICS code for the primary business activity.
    state:
        Two-letter state code where the risk is domiciled.
    zip_code:
        Five-digit ZIP code of the primary business location.
    years_in_business:
        Number of full years the entity has been operating.
    annual_revenue:
        Most recent annual gross revenue in USD.
    employee_count:
        Number of full-time equivalent employees.
    construction_type:
        Building construction type (e.g. ``"frame"``, ``"masonry"``,
        ``"fire_resistive"``). Required for property lines.
    loss_ratio_3yr:
        Three-year average loss ratio (0.0 – 1.0 scale, e.g. 0.45 = 45%).
    experience_mod:
        Workers' Compensation experience modification factor.
        Typically between 0.50 and 2.00.
    coverage_lines:
        List of lines of business requested (e.g. ``["Commercial Auto",
        "General Liability"]``).
    requested_limits:
        Dict mapping coverage component to requested limit, e.g.
        ``{"occurrence": 1000000, "aggregate": 2000000, "deductible": 5000}``.
    atlas_risk_scores:
        Optional risk scores from the Atlas platform, forwarded as-is.
    """

    entity_name: str = Field(..., description="Legal name of the insured entity")
    naics_code: str = Field(..., description="NAICS code for primary business activity")
    state: str = Field(..., min_length=2, max_length=2, description="Two-letter state code")
    zip_code: str = Field(..., min_length=5, max_length=10, description="ZIP code")
    years_in_business: int = Field(..., ge=0, description="Years the entity has been operating")
    annual_revenue: float = Field(..., ge=0.0, description="Annual gross revenue in USD")
    employee_count: int = Field(..., ge=0, description="Full-time equivalent employee count")
    construction_type: str | None = Field(
        default=None,
        description="Building construction type (required for property lines)",
    )
    loss_ratio_3yr: float | None = Field(
        default=None,
        ge=0.0,
        le=5.0,
        description="Three-year average loss ratio (0.0-1.0 scale)",
    )
    experience_mod: float | None = Field(
        default=None,
        ge=0.0,
        le=5.0,
        description="Workers' Comp experience modification factor",
    )
    coverage_lines: list[str] = Field(
        ...,
        min_length=1,
        description="Lines of business requested",
    )
    requested_limits: dict[str, Any] = Field(
        default_factory=dict,
        description="Coverage limit requests: {component: amount}",
    )
    atlas_risk_scores: dict[str, Any] | None = Field(
        default=None,
        description="Optional Atlas platform risk scores",
    )


# ---------------------------------------------------------------------------
# Response schemas
# ---------------------------------------------------------------------------


class CarrierMatchResponse(BaseModel):
    """Single carrier result returned within a :class:`MatchResponse`.

    Attributes
    ----------
    carrier_id:
        UUID of the carrier.
    carrier_name:
        Legal name.
    naic_code:
        NAIC code.
    am_best_rating:
        AM Best financial strength rating, if available.
    eligibility_status:
        ``"pass"`` | ``"conditional"`` | ``"fail"``.
    eligibility_notes:
        Failure messages or conditional notes.
    appetite_score:
        Appetite score 0-100.
    estimated_premium:
        Dict with premium breakdown keys (``final_estimated``, ``confidence``,
        etc.).
    competitiveness_rank:
        Rank among all eligible carriers (1 = best).
    coverage_highlights:
        Notable coverage features.
    recent_signals:
        Appetite signals from the past 90 days.
    filing_references:
        Key SERFF filings for context.
    placement_probability:
        Estimated 0.0-1.0 probability of successful placement.
    """

    carrier_id: UUID
    carrier_name: str
    naic_code: str
    am_best_rating: str | None = None
    eligibility_status: str
    eligibility_notes: list[str] = Field(default_factory=list)
    appetite_score: float
    estimated_premium: dict[str, Any] = Field(default_factory=dict)
    competitiveness_rank: int
    coverage_highlights: list[dict] = Field(default_factory=list)
    recent_signals: list[dict] = Field(default_factory=list)
    filing_references: list[dict] = Field(default_factory=list)
    placement_probability: float


class MatchResponse(BaseModel):
    """Response envelope for POST /v1/match.

    Attributes
    ----------
    matches:
        Ranked list of carrier matches.
    state:
        State evaluated.
    lines_matched:
        Lines of business that were evaluated.
    carriers_evaluated:
        Total number of carriers evaluated (including ineligible).
    carriers_eligible:
        Number of carriers that passed eligibility.
    match_time_ms:
        Wall-clock time for the matching operation in milliseconds.
    """

    matches: list[CarrierMatchResponse]
    state: str
    lines_matched: list[str]
    carriers_evaluated: int
    carriers_eligible: int
    match_time_ms: float


class AppetiteResponse(BaseModel):
    """Response for GET /v1/carriers/{naic_code}/appetite.

    Attributes
    ----------
    carrier_id:
        UUID of the carrier.
    carrier_name:
        Legal name.
    state:
        State queried.
    line:
        Line of business queried.
    appetite_score:
        Computed appetite score 0-100.
    eligible_classes:
        NAICS/class codes the carrier actively writes.
    ineligible_classes:
        Explicitly excluded class codes.
    preferred_classes:
        Sweet-spot class codes.
    limit_range:
        Dict with ``min`` and ``max`` limit values.
    rate_competitiveness:
        Rate competitiveness index 0-100.
    last_rate_change:
        Last rate change percentage (positive = increase).
    recent_signals:
        Recent appetite signals.
    """

    carrier_id: UUID
    carrier_name: str
    state: str
    line: str
    appetite_score: float
    eligible_classes: list[str] = Field(default_factory=list)
    ineligible_classes: list[str] = Field(default_factory=list)
    preferred_classes: list[str] = Field(default_factory=list)
    limit_range: dict[str, Any] = Field(default_factory=dict)
    rate_competitiveness: float | None = None
    last_rate_change: float | None = None
    recent_signals: list[dict] = Field(default_factory=list)


class RateComparisonEntry(BaseModel):
    """Single carrier entry within a :class:`RateComparisonResponse`."""

    carrier_name: str
    naic_code: str
    base_rate: float | None = None
    territory_factor: float | None = None
    estimated_premium: float | None = None
    rate_change_pct: float | None = None
    confidence: float | None = None


class RateComparisonResponse(BaseModel):
    """Response for GET /v1/rates/{state}/{line}/{class_code}.

    Attributes
    ----------
    state:
        State queried.
    line:
        Line of business.
    class_code:
        NAICS/class code queried.
    carriers:
        List of carrier rate entries sorted by estimated_premium ascending.
    """

    state: str
    line: str
    class_code: str
    carriers: list[RateComparisonEntry]


class FilingEntry(BaseModel):
    """Single filing entry within a :class:`FilingSearchResponse`."""

    serff_tracking: str
    carrier_name: str
    filing_type: str | None = None
    line: str | None = None
    status: str | None = None
    effective_date: str | None = None
    rate_change_pct: float | None = None


class FilingSearchResponse(BaseModel):
    """Response for GET /v1/filings.

    Attributes
    ----------
    filings:
        List of matching filings.
    total_found:
        Total number of filings matching the query filters.
    """

    filings: list[FilingEntry]
    total_found: int


class MarketIntelResponse(BaseModel):
    """Response for GET /v1/market-intelligence.

    Attributes
    ----------
    state:
        State queried.
    line:
        Line of business.
    period:
        ISO date range string, e.g. ``"2024-10-01/2025-01-01"``.
    avg_rate_change:
        Average rate change across all filings in period (percentage).
    filing_count:
        Number of rate filings in the period.
    market_trend:
        One of ``"hardening"``, ``"softening"``, ``"stable"``, ``"mixed"``.
    new_entrants:
        Carrier names entering the market in the period.
    withdrawals:
        Carrier names leaving the market.
    top_signals:
        Most significant appetite shifts observed.
    """

    state: str
    line: str
    period: str
    avg_rate_change: float | None = None
    filing_count: int = 0
    market_trend: str | None = None
    new_entrants: list[str] = Field(default_factory=list)
    withdrawals: list[str] = Field(default_factory=list)
    top_signals: list[dict] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Title matching schemas
# ---------------------------------------------------------------------------


class TitleRiskInput(BaseModel):
    """Inbound title risk profile submitted to the /v1/match/title endpoint.

    Attributes
    ----------
    purchase_price:
        Property purchase price in USD.
    loan_amount:
        Mortgage loan amount.  0 for a cash purchase (no lender policy).
    state:
        Two-letter state code.
    policy_type:
        ``"owner"`` | ``"lender"`` | ``"simultaneous"`` (default).
    is_refinance:
        Whether this is a refinance transaction.
    years_since_prior_policy:
        Years since the prior policy (for reissue credit calculation).
    endorsements:
        List of ALTA endorsement codes requested.
    property_type:
        ``"residential"`` | ``"commercial"`` | ``"vacant_land"`` (informational).
    transaction_type:
        ``"purchase"`` | ``"refinance"`` | ``"construction"`` (informational).
    """

    purchase_price: float = Field(..., gt=0, description="Purchase price in USD")
    loan_amount: float = Field(default=0, ge=0, description="Loan amount (0 = cash purchase)")
    state: str = Field(..., min_length=2, max_length=2, description="Two-letter state code")
    policy_type: str = Field(
        default="simultaneous",
        description='Policy type: "owner", "lender", or "simultaneous"',
    )
    is_refinance: bool = Field(default=False, description="Refinance transaction flag")
    years_since_prior_policy: float | None = Field(
        default=None, ge=0, description="Years since prior policy (for reissue credit)"
    )
    endorsements: list[str] = Field(
        default_factory=list, description="ALTA endorsement codes requested"
    )
    property_type: str | None = Field(
        default=None,
        description='Property type: "residential", "commercial", "vacant_land"',
    )
    transaction_type: str | None = Field(
        default=None,
        description='Transaction type: "purchase", "refinance", "construction"',
    )


class TitleCarrierMatchResponse(BaseModel):
    """Single title carrier result within a :class:`TitleMatchResponse`.

    Includes all base matching fields plus title-specific premium breakdown.
    """

    carrier_id: UUID
    carrier_name: str
    naic_code: str
    am_best_rating: str | None = None
    eligibility_status: str
    eligibility_notes: list[str] = Field(default_factory=list)
    appetite_score: float
    competitiveness_rank: int
    placement_probability: float
    owner_premium: float = 0.0
    lender_premium: float = 0.0
    simultaneous_premium: float = 0.0
    simultaneous_savings: float = 0.0
    simultaneous_discount_pct: float = 0.0
    reissue_credit: float = 0.0
    endorsement_fees: float = 0.0
    total_premium: float = 0.0
    is_promulgated: bool = False


class TitleMatchResponse(BaseModel):
    """Response envelope for POST /v1/match/title.

    Attributes
    ----------
    matches:
        Ranked list of title carrier matches.
    state:
        State evaluated.
    purchase_price:
        Purchase price from the request.
    loan_amount:
        Loan amount from the request.
    policy_type:
        Policy type from the request.
    carriers_evaluated:
        Total carriers evaluated.
    carriers_eligible:
        Carriers that passed eligibility.
    match_time_ms:
        Wall-clock time in milliseconds.
    best_total:
        Lowest total premium across all carriers.
    best_simultaneous_savings:
        Highest simultaneous savings across all carriers.
    """

    matches: list[TitleCarrierMatchResponse]
    state: str
    purchase_price: float
    loan_amount: float
    policy_type: str
    carriers_evaluated: int
    carriers_eligible: int
    match_time_ms: float
    best_total: float | None = None
    best_simultaneous_savings: float | None = None


class HealthResponse(BaseModel):
    """Response for GET /v1/health.

    Attributes
    ----------
    status:
        ``"ok"`` | ``"degraded"`` | ``"error"``.
    version:
        Hermes version string.
    carriers_loaded:
        Number of active carrier records in the database.
    filings_indexed:
        Total number of filing records indexed.
    states_active:
        Number of states with at least one current appetite profile.
    last_scrape:
        ISO timestamp of the most recent completed scrape.
    last_parse:
        ISO timestamp of the most recent completed parse.
    """

    status: str
    version: str
    carriers_loaded: int = 0
    filings_indexed: int = 0
    states_active: int = 0
    last_scrape: str | None = None
    last_parse: str | None = None
