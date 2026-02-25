"""SQLAlchemy ORM models for the Hermes Title Insurance pricing engine.

Tables
------
hermes_title_rate_cards          — versioned rate card per carrier/policy_type/state
hermes_title_rates               — tiered coverage bands (rate per $1,000)
hermes_title_simultaneous_issue  — simultaneous issue discount schedule
hermes_title_reissue_credits     — time-tiered refinance credits
hermes_title_endorsements        — ALTA endorsement pricing
hermes_title_serff_data          — regulatory filing supplements
hermes_title_quote_log           — audit trail for all quote requests
"""

from __future__ import annotations

import uuid
from datetime import date, datetime
from decimal import Decimal
from typing import List, Optional

from sqlalchemy import (
    Boolean,
    Date,
    DateTime,
    ForeignKey,
    Integer,
    Numeric,
    String,
    Text,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from hermes.db import Base


# ── Title Rate Card ─────────────────────────────────────────────


class TitleRateCard(Base):
    """Versioned rate card for a single carrier, policy type, and state."""

    __tablename__ = "hermes_title_rate_cards"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    carrier_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("hermes_carriers.id", ondelete="CASCADE"),
        nullable=False,
    )
    policy_type: Mapped[str] = mapped_column(String(30), nullable=False)
    state: Mapped[str] = mapped_column(String(2), nullable=False)
    is_promulgated: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    effective_date: Mapped[date] = mapped_column(Date, nullable=False)
    expiration_date: Mapped[Optional[date]] = mapped_column(Date)
    is_current: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    source: Mapped[str] = mapped_column(String(50), nullable=False, default="manual")
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    superseded_by: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("hermes_title_rate_cards.id"),
    )
    notes: Mapped[Optional[str]] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    # Relationships
    rates: Mapped[List["TitleRate"]] = relationship(
        "TitleRate", back_populates="rate_card", cascade="all, delete-orphan"
    )
    simultaneous_issue: Mapped[List["TitleSimultaneousIssue"]] = relationship(
        "TitleSimultaneousIssue", back_populates="rate_card", cascade="all, delete-orphan"
    )
    reissue_credits: Mapped[List["TitleReissueCredit"]] = relationship(
        "TitleReissueCredit", back_populates="rate_card", cascade="all, delete-orphan"
    )
    endorsements: Mapped[List["TitleEndorsement"]] = relationship(
        "TitleEndorsement", back_populates="rate_card", cascade="all, delete-orphan"
    )


# ── Title Rate ──────────────────────────────────────────────────


class TitleRate(Base):
    """Single coverage band in a tiered title rate schedule."""

    __tablename__ = "hermes_title_rates"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    rate_card_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("hermes_title_rate_cards.id", ondelete="CASCADE"),
        nullable=False,
    )
    coverage_min: Mapped[Decimal] = mapped_column(Numeric(14, 2), nullable=False, default=0)
    coverage_max: Mapped[Decimal] = mapped_column(Numeric(14, 2), nullable=False)
    rate_per_thousand: Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False)
    flat_fee: Mapped[Decimal] = mapped_column(Numeric(10, 2), nullable=False, default=0)
    minimum_premium: Mapped[Decimal] = mapped_column(Numeric(10, 2), nullable=False, default=0)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    rate_card: Mapped["TitleRateCard"] = relationship(
        "TitleRateCard", back_populates="rates"
    )


# ── Title Simultaneous Issue ────────────────────────────────────


class TitleSimultaneousIssue(Base):
    """Simultaneous issue discount schedule — THE key arbitrage table."""

    __tablename__ = "hermes_title_simultaneous_issue"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    rate_card_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("hermes_title_rate_cards.id", ondelete="CASCADE"),
        nullable=False,
    )
    loan_min: Mapped[Decimal] = mapped_column(Numeric(14, 2), nullable=False, default=0)
    loan_max: Mapped[Decimal] = mapped_column(Numeric(14, 2), nullable=False)
    discount_rate_per_thousand: Mapped[Decimal] = mapped_column(
        Numeric(10, 4), nullable=False, default=0
    )
    discount_pct: Mapped[Decimal] = mapped_column(Numeric(6, 2), nullable=False, default=0)
    flat_fee: Mapped[Decimal] = mapped_column(Numeric(10, 2), nullable=False, default=0)
    conditions: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    rate_card: Mapped["TitleRateCard"] = relationship(
        "TitleRateCard", back_populates="simultaneous_issue"
    )


# ── Title Reissue Credit ────────────────────────────────────────


class TitleReissueCredit(Base):
    """Time-tiered refinance discount based on years since prior policy."""

    __tablename__ = "hermes_title_reissue_credits"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    rate_card_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("hermes_title_rate_cards.id", ondelete="CASCADE"),
        nullable=False,
    )
    years_since_min: Mapped[Decimal] = mapped_column(Numeric(4, 1), nullable=False, default=0)
    years_since_max: Mapped[Decimal] = mapped_column(Numeric(4, 1), nullable=False)
    credit_pct: Mapped[Decimal] = mapped_column(Numeric(6, 2), nullable=False, default=0)
    conditions: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    rate_card: Mapped["TitleRateCard"] = relationship(
        "TitleRateCard", back_populates="reissue_credits"
    )


# ── Title Endorsement ───────────────────────────────────────────


class TitleEndorsement(Base):
    """ALTA endorsement pricing — flat fee, rate per thousand, or pct of base."""

    __tablename__ = "hermes_title_endorsements"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    rate_card_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("hermes_title_rate_cards.id", ondelete="CASCADE"),
        nullable=False,
    )
    endorsement_code: Mapped[str] = mapped_column(String(20), nullable=False)
    endorsement_name: Mapped[str] = mapped_column(String(200), nullable=False)
    flat_fee: Mapped[Decimal] = mapped_column(Numeric(10, 2), nullable=False, default=0)
    rate_per_thousand: Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False, default=0)
    pct_of_base: Mapped[Decimal] = mapped_column(Numeric(6, 4), nullable=False, default=0)
    description: Mapped[Optional[str]] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    rate_card: Mapped["TitleRateCard"] = relationship(
        "TitleRateCard", back_populates="endorsements"
    )


# ── Title SERFF Data ────────────────────────────────────────────


class TitleSERFFData(Base):
    """Regulatory filing supplements from SERFF for title carriers."""

    __tablename__ = "hermes_title_serff_data"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    carrier_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("hermes_carriers.id", ondelete="CASCADE"),
        nullable=False,
    )
    filing_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("hermes_filings.id", ondelete="SET NULL"),
    )
    state: Mapped[str] = mapped_column(String(2), nullable=False)
    approved_rates: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    loss_ratio_data: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    underwriting_rules: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    effective_date: Mapped[Optional[date]] = mapped_column(Date)
    expiration_date: Mapped[Optional[date]] = mapped_column(Date)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )


# ── Title Quote Log ─────────────────────────────────────────────


class TitleQuoteLog(Base):
    """Audit trail entry for a title insurance quote request."""

    __tablename__ = "hermes_title_quote_log"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    request_data: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    response_data: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    carriers_quoted: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    best_premium: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 2))
    best_carrier_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("hermes_carriers.id", ondelete="SET NULL"),
    )
    processing_time_ms: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 1))
    source: Mapped[str] = mapped_column(String(50), nullable=False, default="api")
    ip_address: Mapped[Optional[str]] = mapped_column(String(45))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
