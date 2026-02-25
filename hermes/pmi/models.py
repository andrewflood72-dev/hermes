"""SQLAlchemy ORM models for the Hermes PMI pricing engine.

Tables
------
hermes_pmi_rate_cards    — versioned rate card per carrier/premium_type/state
hermes_pmi_rates         — LTV×FICO×coverage grid cells
hermes_pmi_adjustments   — JSONB condition-based rate modifiers
hermes_pmi_serff_data    — regulatory filing supplements
hermes_pmi_quote_log     — audit trail for all quote requests
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


# ── PMI Rate Card ────────────────────────────────────────────────


class PMIRateCard(Base):
    """Versioned rate card for a single carrier, premium type, and state."""

    __tablename__ = "hermes_pmi_rate_cards"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    carrier_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("hermes_carriers.id", ondelete="CASCADE"),
        nullable=False,
    )
    premium_type: Mapped[str] = mapped_column(String(20), nullable=False)
    state: Mapped[Optional[str]] = mapped_column(String(2))
    effective_date: Mapped[date] = mapped_column(Date, nullable=False)
    expiration_date: Mapped[Optional[date]] = mapped_column(Date)
    is_current: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    source: Mapped[str] = mapped_column(String(50), nullable=False, default="manual")
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    superseded_by: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("hermes_pmi_rate_cards.id"),
    )
    notes: Mapped[Optional[str]] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    # Relationships
    carrier: Mapped["Carrier"] = relationship(  # type: ignore[name-defined]
        "Carrier", back_populates="pmi_rate_cards"
    )
    rates: Mapped[List["PMIRate"]] = relationship(
        "PMIRate", back_populates="rate_card", cascade="all, delete-orphan"
    )
    adjustments: Mapped[List["PMIAdjustment"]] = relationship(
        "PMIAdjustment", back_populates="rate_card", cascade="all, delete-orphan"
    )


# ── PMI Rate ─────────────────────────────────────────────────────


class PMIRate(Base):
    """Single cell in a carrier's LTV × FICO × coverage rate grid."""

    __tablename__ = "hermes_pmi_rates"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    rate_card_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("hermes_pmi_rate_cards.id", ondelete="CASCADE"),
        nullable=False,
    )
    ltv_min: Mapped[Decimal] = mapped_column(Numeric(5, 2), nullable=False)
    ltv_max: Mapped[Decimal] = mapped_column(Numeric(5, 2), nullable=False)
    fico_min: Mapped[int] = mapped_column(Integer, nullable=False)
    fico_max: Mapped[int] = mapped_column(Integer, nullable=False)
    coverage_pct: Mapped[Decimal] = mapped_column(Numeric(5, 2), nullable=False)
    rate_pct: Mapped[Decimal] = mapped_column(Numeric(8, 4), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    # Relationships
    rate_card: Mapped["PMIRateCard"] = relationship(
        "PMIRateCard", back_populates="rates"
    )


# ── PMI Adjustment ───────────────────────────────────────────────


class PMIAdjustment(Base):
    """Rate modifier with JSONB condition matching."""

    __tablename__ = "hermes_pmi_adjustments"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    rate_card_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("hermes_pmi_rate_cards.id", ondelete="CASCADE"),
        nullable=False,
    )
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    condition: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    adjustment_method: Mapped[str] = mapped_column(String(20), nullable=False)
    adjustment_value: Mapped[Decimal] = mapped_column(Numeric(8, 4), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    # Relationships
    rate_card: Mapped["PMIRateCard"] = relationship(
        "PMIRateCard", back_populates="adjustments"
    )


# ── PMI SERFF Data ───────────────────────────────────────────────


class PMISERFFData(Base):
    """Regulatory filing supplements from SERFF for PMI carriers."""

    __tablename__ = "hermes_pmi_serff_data"

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
    approved_rate_range: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    state_rules: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    actuarial_data: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    effective_date: Mapped[Optional[date]] = mapped_column(Date)
    expiration_date: Mapped[Optional[date]] = mapped_column(Date)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    # Relationships
    carrier: Mapped["Carrier"] = relationship(  # type: ignore[name-defined]
        "Carrier", back_populates="pmi_serff_data"
    )


# ── PMI Quote Log ───────────────────────────────────────────────


class PMIQuoteLog(Base):
    """Audit trail entry for a PMI quote request."""

    __tablename__ = "hermes_pmi_quote_log"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    request_data: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    response_data: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    carriers_quoted: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    best_rate: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4))
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
