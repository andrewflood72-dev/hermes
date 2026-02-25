"""Pydantic request/response schemas for the MGA Proposal Agent."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, Field

# All supported product types for MGA proposals
PRODUCT_TYPES = Literal[
    "pmi", "title", "surety", "cyber", "gap", "crime",
    "eo", "trade_credit", "inland_marine", "builders_risk",
    "student_loan", "travel", "home_warranty",
]


# ---------------------------------------------------------------------------
# Request
# ---------------------------------------------------------------------------


class MGAProposalRequest(BaseModel):
    """Input parameters for generating an MGA business proposal."""

    program_type: PRODUCT_TYPES = Field(
        default="pmi",
        description="Product vertical: pmi, title, surety, cyber, gap, etc.",
    )
    target_volume: float = Field(
        default=5_000_000_000,
        gt=0,
        description="Target annual origination volume in USD",
    )
    distribution_partner: str | None = Field(
        default=None,
        description="Primary distribution partner name (product-specific)",
    )
    target_states: list[str] = Field(
        default_factory=lambda: ["CA", "TX", "FL", "NY", "IL"],
        description="Target state codes for initial launch",
    )
    custom_context: str | None = Field(
        default=None,
        description="Additional context or requirements for the proposal",
    )
    embedded_distribution_context: str | None = Field(
        default=None,
        description="Embedded distribution channel context (Variable 5 per master prompt)",
    )
    serff_data: str | None = Field(
        default=None,
        description="Raw SERFF filing data to include in proposal generation",
    )


# ---------------------------------------------------------------------------
# Internal structures
# ---------------------------------------------------------------------------


class ProposalSection(BaseModel):
    """A single section of the generated proposal."""

    title: str
    content: str
    data_tables: list[dict[str, Any]] = Field(default_factory=list)
    key_metrics: dict[str, Any] = Field(default_factory=dict)
    confidence: float = Field(default=0.85, ge=0, le=1)


class FinancialProjection(BaseModel):
    """Single year of the 5-year financial projection."""

    year: int
    premium_volume: float
    loss_ratio: float
    expense_ratio: float
    commission_rate: float
    net_income: float
    cumulative_income: float


# ---------------------------------------------------------------------------
# Response
# ---------------------------------------------------------------------------


class MGAProposalResponse(BaseModel):
    """Full generated MGA proposal."""

    id: UUID
    program_type: str
    title: str
    sections: dict[str, ProposalSection]
    financial_projections: list[FinancialProjection]
    executive_summary: str
    status: str
    token_usage: dict[str, Any] = Field(default_factory=dict)
    generated_at: datetime


class MGAProposalListItem(BaseModel):
    """Summary item for proposal listing."""

    id: UUID
    program_type: str
    title: str
    status: str
    generated_by: str | None = None
    created_at: datetime
