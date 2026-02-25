"""SQLAlchemy ORM models matching the Hermes PostgreSQL schema."""

import uuid
from contextlib import asynccontextmanager
from datetime import datetime, date
from decimal import Decimal
from typing import AsyncGenerator, Optional, List

from sqlalchemy import (
    String, Text, Integer, BigInteger, Boolean, Date,
    DateTime, Numeric, ForeignKey, func,
)
from sqlalchemy.dialects.postgresql import UUID, JSONB, ARRAY
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

from hermes.config import settings


# ── Engine & Session ──────────────────────────────────────────────

engine = create_async_engine(settings.database_url, echo=False, pool_size=10)
async_session = async_sessionmaker(engine, expire_on_commit=False)


@asynccontextmanager
async def get_connection() -> AsyncGenerator:
    """Yield a raw asyncpg connection from the SQLAlchemy connection pool.

    Used by the parser layer which executes raw SQL with asyncpg-style
    positional parameters (``$1``, ``$2``, …).  The connection is returned
    to the pool when the context manager exits.

    Example::

        async with get_connection() as conn:
            await conn.execute("INSERT INTO ...", val1, val2)
    """
    async with engine.connect() as sa_conn:
        # Unwrap the raw asyncpg connection from the SQLAlchemy wrapper.
        raw_conn = await sa_conn.get_raw_connection()
        try:
            yield raw_conn.driver_connection
            await sa_conn.commit()
        except Exception:
            await sa_conn.rollback()
            raise


class Base(DeclarativeBase):
    pass


# ── Carrier Domain (Migration 001) ───────────────────────────────

class Carrier(Base):
    __tablename__ = "hermes_carriers"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    naic_code: Mapped[str] = mapped_column(String(10), nullable=False, unique=True)
    legal_name: Mapped[str] = mapped_column(String(500), nullable=False)
    group_name: Mapped[Optional[str]] = mapped_column(String(500))
    group_naic_code: Mapped[Optional[str]] = mapped_column(String(10))
    am_best_rating: Mapped[Optional[str]] = mapped_column(String(10))
    am_best_outlook: Mapped[Optional[str]] = mapped_column(String(20))
    sp_rating: Mapped[Optional[str]] = mapped_column(String(10))
    treasury_570_listed: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    domicile_state: Mapped[Optional[str]] = mapped_column(String(2))
    company_type: Mapped[Optional[str]] = mapped_column(String(50))
    direct_written_premium: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 2))
    commercial_lines_pct: Mapped[Optional[Decimal]] = mapped_column(Numeric(5, 2))
    website: Mapped[Optional[str]] = mapped_column(String(500))
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="active")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    # Relationships
    licenses: Mapped[List["CarrierLicense"]] = relationship(
        "CarrierLicense", back_populates="carrier", cascade="all, delete-orphan"
    )
    contacts: Mapped[List["CarrierContact"]] = relationship(
        "CarrierContact", back_populates="carrier", cascade="all, delete-orphan"
    )
    filings: Mapped[List["Filing"]] = relationship(
        "Filing", back_populates="carrier"
    )
    rate_tables: Mapped[List["RateTable"]] = relationship(
        "RateTable", back_populates="carrier"
    )
    underwriting_rules: Mapped[List["UnderwritingRule"]] = relationship(
        "UnderwritingRule", back_populates="carrier"
    )
    coverage_options: Mapped[List["CoverageOption"]] = relationship(
        "CoverageOption", back_populates="carrier"
    )
    credits_surcharges: Mapped[List["CreditSurcharge"]] = relationship(
        "CreditSurcharge", back_populates="carrier"
    )
    exclusions: Mapped[List["Exclusion"]] = relationship(
        "Exclusion", back_populates="carrier"
    )
    policy_forms: Mapped[List["PolicyForm"]] = relationship(
        "PolicyForm", back_populates="carrier"
    )
    appetite_profiles: Mapped[List["AppetiteProfile"]] = relationship(
        "AppetiteProfile", back_populates="carrier"
    )
    appetite_signals: Mapped[List["AppetiteSignal"]] = relationship(
        "AppetiteSignal", back_populates="carrier"
    )
    rankings: Mapped[List["CarrierRanking"]] = relationship(
        "CarrierRanking", back_populates="carrier"
    )
    pmi_rate_cards: Mapped[List["PMIRateCard"]] = relationship(
        "PMIRateCard", back_populates="carrier"
    )
    pmi_serff_data: Mapped[List["PMISERFFData"]] = relationship(
        "PMISERFFData", back_populates="carrier"
    )


class CarrierLicense(Base):
    __tablename__ = "hermes_carrier_licenses"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    carrier_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("hermes_carriers.id", ondelete="CASCADE"),
        nullable=False,
    )
    state: Mapped[str] = mapped_column(String(2), nullable=False)
    license_number: Mapped[Optional[str]] = mapped_column(String(50))
    license_status: Mapped[str] = mapped_column(String(20), nullable=False, default="active")
    lines_authorized: Mapped[Optional[List[str]]] = mapped_column(ARRAY(Text))
    surplus_lines_flag: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    admitted_flag: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    effective_date: Mapped[Optional[date]] = mapped_column(Date)
    expiration_date: Mapped[Optional[date]] = mapped_column(Date)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    # Relationships
    carrier: Mapped["Carrier"] = relationship("Carrier", back_populates="licenses")


class CarrierContact(Base):
    __tablename__ = "hermes_carrier_contacts"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    carrier_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("hermes_carriers.id", ondelete="CASCADE"),
        nullable=False,
    )
    role: Mapped[str] = mapped_column(String(50), nullable=False)
    name: Mapped[Optional[str]] = mapped_column(String(200))
    title: Mapped[Optional[str]] = mapped_column(String(200))
    email: Mapped[Optional[str]] = mapped_column(String(200))
    phone: Mapped[Optional[str]] = mapped_column(String(50))
    territory: Mapped[Optional[List[str]]] = mapped_column(ARRAY(Text))
    department: Mapped[Optional[str]] = mapped_column(String(200))
    notes: Mapped[Optional[str]] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    # Relationships
    carrier: Mapped["Carrier"] = relationship("Carrier", back_populates="contacts")


# ── Filing Domain (Migration 002) ────────────────────────────────

class Filing(Base):
    __tablename__ = "hermes_filings"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    serff_tracking_number: Mapped[str] = mapped_column(String(50), nullable=False)
    carrier_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True), ForeignKey("hermes_carriers.id")
    )
    carrier_naic_code: Mapped[Optional[str]] = mapped_column(String(10))
    carrier_name_filed: Mapped[Optional[str]] = mapped_column(String(500))
    state: Mapped[str] = mapped_column(String(2), nullable=False)
    filing_type: Mapped[str] = mapped_column(String(20), nullable=False)
    line_of_business: Mapped[str] = mapped_column(String(100), nullable=False)
    sub_line: Mapped[Optional[str]] = mapped_column(String(100))
    product_name: Mapped[Optional[str]] = mapped_column(String(500))
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="pending")
    effective_date: Mapped[Optional[date]] = mapped_column(Date)
    filed_date: Mapped[Optional[date]] = mapped_column(Date)
    disposition_date: Mapped[Optional[date]] = mapped_column(Date)
    expiration_date: Mapped[Optional[date]] = mapped_column(Date)
    filing_description: Mapped[Optional[str]] = mapped_column(Text)
    overall_rate_change_pct: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4))
    rate_change_min_pct: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4))
    rate_change_max_pct: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4))
    affected_policyholders: Mapped[Optional[int]] = mapped_column(Integer)
    state_portal_url: Mapped[Optional[str]] = mapped_column(String(1000))
    raw_metadata: Mapped[Optional[dict]] = mapped_column(JSONB, default=dict)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    # Relationships
    carrier: Mapped[Optional["Carrier"]] = relationship("Carrier", back_populates="filings")
    documents: Mapped[List["FilingDocument"]] = relationship(
        "FilingDocument", back_populates="filing", cascade="all, delete-orphan"
    )
    changes: Mapped[List["FilingChange"]] = relationship(
        "FilingChange",
        back_populates="filing",
        foreign_keys="FilingChange.filing_id",
        cascade="all, delete-orphan",
    )
    rate_tables: Mapped[List["RateTable"]] = relationship(
        "RateTable", back_populates="filing", cascade="all, delete-orphan"
    )
    underwriting_rules: Mapped[List["UnderwritingRule"]] = relationship(
        "UnderwritingRule", back_populates="filing", cascade="all, delete-orphan"
    )
    coverage_options: Mapped[List["CoverageOption"]] = relationship(
        "CoverageOption", back_populates="filing", cascade="all, delete-orphan"
    )
    credits_surcharges: Mapped[List["CreditSurcharge"]] = relationship(
        "CreditSurcharge", back_populates="filing", cascade="all, delete-orphan"
    )
    exclusions: Mapped[List["Exclusion"]] = relationship(
        "Exclusion", back_populates="filing", cascade="all, delete-orphan"
    )
    policy_forms: Mapped[List["PolicyForm"]] = relationship(
        "PolicyForm", back_populates="filing", cascade="all, delete-orphan"
    )
    appetite_signals: Mapped[List["AppetiteSignal"]] = relationship(
        "AppetiteSignal", back_populates="source_filing"
    )
    parse_logs: Mapped[List["ParseLog"]] = relationship(
        "ParseLog", back_populates="filing"
    )


class FilingDocument(Base):
    __tablename__ = "hermes_filing_documents"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    filing_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("hermes_filings.id", ondelete="CASCADE"),
        nullable=False,
    )
    document_name: Mapped[str] = mapped_column(String(500), nullable=False)
    document_type: Mapped[Optional[str]] = mapped_column(String(50))
    file_path: Mapped[Optional[str]] = mapped_column(String(1000))
    file_size_bytes: Mapped[Optional[int]] = mapped_column(BigInteger)
    mime_type: Mapped[Optional[str]] = mapped_column(String(100))
    page_count: Mapped[Optional[int]] = mapped_column(Integer)
    parsed_flag: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    parse_confidence: Mapped[Optional[Decimal]] = mapped_column(Numeric(5, 4))
    parse_version: Mapped[Optional[str]] = mapped_column(String(20))
    confidential_flag: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    download_url: Mapped[Optional[str]] = mapped_column(String(2000))
    checksum_sha256: Mapped[Optional[str]] = mapped_column(String(64))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    # Relationships
    filing: Mapped["Filing"] = relationship("Filing", back_populates="documents")
    parse_logs: Mapped[List["ParseLog"]] = relationship(
        "ParseLog", back_populates="document"
    )


class FilingChange(Base):
    __tablename__ = "hermes_filing_changes"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    filing_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("hermes_filings.id", ondelete="CASCADE"),
        nullable=False,
    )
    change_type: Mapped[str] = mapped_column(String(20), nullable=False)
    overall_rate_change_pct: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4))
    effective_date: Mapped[Optional[date]] = mapped_column(Date)
    prior_filing_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True), ForeignKey("hermes_filings.id")
    )
    description: Mapped[Optional[str]] = mapped_column(Text)
    change_details: Mapped[Optional[dict]] = mapped_column(JSONB, default=dict)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    # Relationships
    filing: Mapped["Filing"] = relationship(
        "Filing", back_populates="changes", foreign_keys=[filing_id]
    )
    prior_filing: Mapped[Optional["Filing"]] = relationship(
        "Filing", foreign_keys=[prior_filing_id]
    )


# ── Rate Intelligence (Migration 003) ────────────────────────────

class RateTable(Base):
    __tablename__ = "hermes_rate_tables"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    filing_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("hermes_filings.id", ondelete="CASCADE"),
        nullable=False,
    )
    carrier_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("hermes_carriers.id"), nullable=False
    )
    state: Mapped[str] = mapped_column(String(2), nullable=False)
    line: Mapped[str] = mapped_column(String(100), nullable=False)
    sub_line: Mapped[Optional[str]] = mapped_column(String(100))
    table_name: Mapped[Optional[str]] = mapped_column(String(500))
    table_type: Mapped[Optional[str]] = mapped_column(String(50))
    effective_date: Mapped[date] = mapped_column(Date, nullable=False)
    expiration_date: Mapped[Optional[date]] = mapped_column(Date)
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    is_current: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    superseded_by: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True), ForeignKey("hermes_rate_tables.id")
    )
    source_document_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True), ForeignKey("hermes_filing_documents.id")
    )
    extraction_confidence: Mapped[Optional[Decimal]] = mapped_column(Numeric(5, 4))
    notes: Mapped[Optional[str]] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    # Relationships
    filing: Mapped["Filing"] = relationship("Filing", back_populates="rate_tables")
    carrier: Mapped["Carrier"] = relationship("Carrier", back_populates="rate_tables")
    source_document: Mapped[Optional["FilingDocument"]] = relationship("FilingDocument")
    superseding_table: Mapped[Optional["RateTable"]] = relationship(
        "RateTable", foreign_keys=[superseded_by], remote_side="RateTable.id"
    )
    base_rates: Mapped[List["BaseRate"]] = relationship(
        "BaseRate", back_populates="rate_table", cascade="all, delete-orphan"
    )
    rating_factors: Mapped[List["RatingFactor"]] = relationship(
        "RatingFactor", back_populates="rate_table", cascade="all, delete-orphan"
    )
    territory_definitions: Mapped[List["TerritoryDefinition"]] = relationship(
        "TerritoryDefinition", back_populates="rate_table", cascade="all, delete-orphan"
    )
    class_code_mappings: Mapped[List["ClassCodeMapping"]] = relationship(
        "ClassCodeMapping", back_populates="rate_table", cascade="all, delete-orphan"
    )
    premium_algorithms: Mapped[List["PremiumAlgorithm"]] = relationship(
        "PremiumAlgorithm", back_populates="rate_table", cascade="all, delete-orphan"
    )


class BaseRate(Base):
    __tablename__ = "hermes_base_rates"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    rate_table_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("hermes_rate_tables.id", ondelete="CASCADE"),
        nullable=False,
    )
    class_code: Mapped[str] = mapped_column(String(20), nullable=False)
    class_description: Mapped[Optional[str]] = mapped_column(String(500))
    territory: Mapped[Optional[str]] = mapped_column(String(20))
    base_rate: Mapped[Decimal] = mapped_column(Numeric(12, 6), nullable=False)
    rate_per_unit: Mapped[Optional[str]] = mapped_column(String(100))
    minimum_premium: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 2))
    maximum_premium: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 2))
    exposure_basis: Mapped[Optional[str]] = mapped_column(String(100))
    effective_date: Mapped[Optional[date]] = mapped_column(Date)
    confidence: Mapped[Decimal] = mapped_column(Numeric(5, 4), nullable=False, default=Decimal("1.0"))
    source_page: Mapped[Optional[int]] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    # Relationships
    rate_table: Mapped["RateTable"] = relationship("RateTable", back_populates="base_rates")


class RatingFactor(Base):
    __tablename__ = "hermes_rating_factors"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    rate_table_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("hermes_rate_tables.id", ondelete="CASCADE"),
        nullable=False,
    )
    factor_type: Mapped[str] = mapped_column(String(50), nullable=False)
    factor_key: Mapped[str] = mapped_column(String(200), nullable=False)
    factor_value: Mapped[Decimal] = mapped_column(Numeric(12, 6), nullable=False)
    factor_description: Mapped[Optional[str]] = mapped_column(String(500))
    applies_to_line: Mapped[Optional[str]] = mapped_column(String(100))
    effective_date: Mapped[Optional[date]] = mapped_column(Date)
    confidence: Mapped[Decimal] = mapped_column(Numeric(5, 4), nullable=False, default=Decimal("1.0"))
    source_page: Mapped[Optional[int]] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    # Relationships
    rate_table: Mapped["RateTable"] = relationship("RateTable", back_populates="rating_factors")


class TerritoryDefinition(Base):
    __tablename__ = "hermes_territory_definitions"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    rate_table_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("hermes_rate_tables.id", ondelete="CASCADE"),
        nullable=False,
    )
    territory_code: Mapped[str] = mapped_column(String(20), nullable=False)
    territory_name: Mapped[Optional[str]] = mapped_column(String(200))
    zip_codes: Mapped[Optional[list]] = mapped_column(JSONB, default=list)
    counties: Mapped[Optional[list]] = mapped_column(JSONB, default=list)
    cities: Mapped[Optional[list]] = mapped_column(JSONB, default=list)
    description: Mapped[Optional[str]] = mapped_column(Text)
    risk_tier: Mapped[Optional[str]] = mapped_column(String(20))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    # Relationships
    rate_table: Mapped["RateTable"] = relationship(
        "RateTable", back_populates="territory_definitions"
    )


class ClassCodeMapping(Base):
    __tablename__ = "hermes_class_code_mappings"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    rate_table_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("hermes_rate_tables.id", ondelete="CASCADE"),
        nullable=False,
    )
    carrier_class_code: Mapped[str] = mapped_column(String(20), nullable=False)
    carrier_class_desc: Mapped[Optional[str]] = mapped_column(String(500))
    iso_class_code: Mapped[Optional[str]] = mapped_column(String(20))
    naic_class_code: Mapped[Optional[str]] = mapped_column(String(20))
    naics_codes: Mapped[Optional[list]] = mapped_column(JSONB, default=list)
    sic_codes: Mapped[Optional[list]] = mapped_column(JSONB, default=list)
    description: Mapped[Optional[str]] = mapped_column(Text)
    eligibility_status: Mapped[str] = mapped_column(String(20), nullable=False, default="eligible")
    hazard_group: Mapped[Optional[str]] = mapped_column(String(10))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    # Relationships
    rate_table: Mapped["RateTable"] = relationship(
        "RateTable", back_populates="class_code_mappings"
    )


class PremiumAlgorithm(Base):
    __tablename__ = "hermes_premium_algorithms"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    rate_table_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("hermes_rate_tables.id", ondelete="CASCADE"),
        nullable=False,
    )
    line: Mapped[str] = mapped_column(String(100), nullable=False)
    algorithm_name: Mapped[Optional[str]] = mapped_column(String(200))
    algorithm_description: Mapped[Optional[str]] = mapped_column(Text)
    formula_text: Mapped[Optional[str]] = mapped_column(Text)
    formula_structured: Mapped[Optional[dict]] = mapped_column(JSONB, default=dict)
    variables: Mapped[Optional[list]] = mapped_column(JSONB, default=list)
    examples: Mapped[Optional[list]] = mapped_column(JSONB, default=list)
    notes: Mapped[Optional[str]] = mapped_column(Text)
    confidence: Mapped[Decimal] = mapped_column(Numeric(5, 4), nullable=False, default=Decimal("1.0"))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    # Relationships
    rate_table: Mapped["RateTable"] = relationship(
        "RateTable", back_populates="premium_algorithms"
    )


# ── Underwriting Rules (Migration 004) ───────────────────────────

class UnderwritingRule(Base):
    __tablename__ = "hermes_underwriting_rules"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    filing_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("hermes_filings.id", ondelete="CASCADE"),
        nullable=False,
    )
    carrier_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("hermes_carriers.id"), nullable=False
    )
    state: Mapped[str] = mapped_column(String(2), nullable=False)
    line: Mapped[str] = mapped_column(String(100), nullable=False)
    rule_type: Mapped[str] = mapped_column(String(50), nullable=False)
    rule_category: Mapped[Optional[str]] = mapped_column(String(100))
    rule_text: Mapped[str] = mapped_column(Text, nullable=False)
    rule_structured: Mapped[Optional[dict]] = mapped_column(JSONB, default=dict)
    section_reference: Mapped[Optional[str]] = mapped_column(String(200))
    effective_date: Mapped[Optional[date]] = mapped_column(Date)
    expiration_date: Mapped[Optional[date]] = mapped_column(Date)
    is_current: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    superseded_by: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True), ForeignKey("hermes_underwriting_rules.id")
    )
    source_document_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True), ForeignKey("hermes_filing_documents.id")
    )
    confidence: Mapped[Decimal] = mapped_column(Numeric(5, 4), nullable=False, default=Decimal("1.0"))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    # Relationships
    filing: Mapped["Filing"] = relationship("Filing", back_populates="underwriting_rules")
    carrier: Mapped["Carrier"] = relationship("Carrier", back_populates="underwriting_rules")
    source_document: Mapped[Optional["FilingDocument"]] = relationship("FilingDocument")
    superseding_rule: Mapped[Optional["UnderwritingRule"]] = relationship(
        "UnderwritingRule",
        foreign_keys=[superseded_by],
        remote_side="UnderwritingRule.id",
    )
    eligibility_criteria: Mapped[List["EligibilityCriterion"]] = relationship(
        "EligibilityCriterion", back_populates="rule", cascade="all, delete-orphan"
    )


class EligibilityCriterion(Base):
    __tablename__ = "hermes_eligibility_criteria"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    rule_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("hermes_underwriting_rules.id", ondelete="CASCADE"),
        nullable=False,
    )
    criterion_type: Mapped[str] = mapped_column(String(100), nullable=False)
    criterion_value: Mapped[str] = mapped_column(String(1000), nullable=False)
    criterion_operator: Mapped[str] = mapped_column(String(20), nullable=False, default="equals")
    criterion_unit: Mapped[Optional[str]] = mapped_column(String(50))
    is_hard_rule: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    description: Mapped[Optional[str]] = mapped_column(Text)
    confidence: Mapped[Decimal] = mapped_column(Numeric(5, 4), nullable=False, default=Decimal("1.0"))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    # Relationships
    rule: Mapped["UnderwritingRule"] = relationship(
        "UnderwritingRule", back_populates="eligibility_criteria"
    )


class CoverageOption(Base):
    __tablename__ = "hermes_coverage_options"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    filing_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("hermes_filings.id", ondelete="CASCADE"),
        nullable=False,
    )
    carrier_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("hermes_carriers.id"), nullable=False
    )
    state: Mapped[str] = mapped_column(String(2), nullable=False)
    line: Mapped[str] = mapped_column(String(100), nullable=False)
    coverage_type: Mapped[str] = mapped_column(String(200), nullable=False)
    limit_min: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 2))
    limit_max: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 2))
    default_limit: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 2))
    deductible_options: Mapped[Optional[list]] = mapped_column(JSONB, default=list)
    default_deductible: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 2))
    coinsurance_options: Mapped[Optional[list]] = mapped_column(JSONB, default=list)
    sublimits: Mapped[Optional[dict]] = mapped_column(JSONB, default=dict)
    waiting_periods: Mapped[Optional[list]] = mapped_column(JSONB, default=list)
    retroactive_date_options: Mapped[Optional[list]] = mapped_column(JSONB, default=list)
    source_document_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True), ForeignKey("hermes_filing_documents.id")
    )
    effective_date: Mapped[Optional[date]] = mapped_column(Date)
    is_current: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    confidence: Mapped[Decimal] = mapped_column(Numeric(5, 4), nullable=False, default=Decimal("1.0"))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    # Relationships
    filing: Mapped["Filing"] = relationship("Filing", back_populates="coverage_options")
    carrier: Mapped["Carrier"] = relationship("Carrier", back_populates="coverage_options")
    source_document: Mapped[Optional["FilingDocument"]] = relationship("FilingDocument")


class CreditSurcharge(Base):
    __tablename__ = "hermes_credits_surcharges"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    filing_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("hermes_filings.id", ondelete="CASCADE"),
        nullable=False,
    )
    carrier_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("hermes_carriers.id"), nullable=False
    )
    state: Mapped[str] = mapped_column(String(2), nullable=False)
    line: Mapped[str] = mapped_column(String(100), nullable=False)
    credit_type: Mapped[str] = mapped_column(String(100), nullable=False)
    credit_or_surcharge: Mapped[str] = mapped_column(String(10), nullable=False, default="credit")
    range_min: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4))
    range_max: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4))
    default_value: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4))
    conditions: Mapped[Optional[dict]] = mapped_column(JSONB, default=dict)
    description: Mapped[Optional[str]] = mapped_column(Text)
    effective_date: Mapped[Optional[date]] = mapped_column(Date)
    is_current: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    confidence: Mapped[Decimal] = mapped_column(Numeric(5, 4), nullable=False, default=Decimal("1.0"))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    # Relationships
    filing: Mapped["Filing"] = relationship("Filing", back_populates="credits_surcharges")
    carrier: Mapped["Carrier"] = relationship("Carrier", back_populates="credits_surcharges")


class Exclusion(Base):
    __tablename__ = "hermes_exclusions"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    filing_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("hermes_filings.id", ondelete="CASCADE"),
        nullable=False,
    )
    carrier_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("hermes_carriers.id"), nullable=False
    )
    state: Mapped[str] = mapped_column(String(2), nullable=False)
    line: Mapped[str] = mapped_column(String(100), nullable=False)
    exclusion_type: Mapped[str] = mapped_column(String(50), nullable=False)
    exclusion_text: Mapped[str] = mapped_column(Text, nullable=False)
    exclusion_summary: Mapped[Optional[str]] = mapped_column(String(500))
    exclusion_category: Mapped[Optional[str]] = mapped_column(String(100))
    form_reference: Mapped[Optional[str]] = mapped_column(String(100))
    is_optional: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    buyback_form: Mapped[Optional[str]] = mapped_column(String(100))
    source_document_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True), ForeignKey("hermes_filing_documents.id")
    )
    effective_date: Mapped[Optional[date]] = mapped_column(Date)
    is_current: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    confidence: Mapped[Decimal] = mapped_column(Numeric(5, 4), nullable=False, default=Decimal("1.0"))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    # Relationships
    filing: Mapped["Filing"] = relationship("Filing", back_populates="exclusions")
    carrier: Mapped["Carrier"] = relationship("Carrier", back_populates="exclusions")
    source_document: Mapped[Optional["FilingDocument"]] = relationship("FilingDocument")


# ── Form Intelligence (Migration 005) ────────────────────────────

class PolicyForm(Base):
    __tablename__ = "hermes_policy_forms"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    filing_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("hermes_filings.id", ondelete="CASCADE"),
        nullable=False,
    )
    carrier_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("hermes_carriers.id"), nullable=False
    )
    state: Mapped[str] = mapped_column(String(2), nullable=False)
    line: Mapped[str] = mapped_column(String(100), nullable=False)
    form_number: Mapped[str] = mapped_column(String(100), nullable=False)
    form_name: Mapped[Optional[str]] = mapped_column(String(500))
    form_edition_date: Mapped[Optional[str]] = mapped_column(String(20))
    form_type: Mapped[str] = mapped_column(String(50), nullable=False)
    iso_equivalent: Mapped[Optional[str]] = mapped_column(String(100))
    is_manuscript: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    replaces_form: Mapped[Optional[str]] = mapped_column(String(100))
    mandatory: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    additional_premium: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    source_document_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True), ForeignKey("hermes_filing_documents.id")
    )
    effective_date: Mapped[Optional[date]] = mapped_column(Date)
    is_current: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    confidence: Mapped[Decimal] = mapped_column(Numeric(5, 4), nullable=False, default=Decimal("1.0"))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    # Relationships
    filing: Mapped["Filing"] = relationship("Filing", back_populates="policy_forms")
    carrier: Mapped["Carrier"] = relationship("Carrier", back_populates="policy_forms")
    source_document: Mapped[Optional["FilingDocument"]] = relationship("FilingDocument")
    provisions: Mapped[List["FormProvision"]] = relationship(
        "FormProvision", back_populates="form", cascade="all, delete-orphan"
    )
    comparisons_as_a: Mapped[List["FormComparison"]] = relationship(
        "FormComparison",
        back_populates="form_a",
        foreign_keys="FormComparison.form_id_a",
        cascade="all, delete-orphan",
    )
    comparisons_as_b: Mapped[List["FormComparison"]] = relationship(
        "FormComparison",
        back_populates="form_b",
        foreign_keys="FormComparison.form_id_b",
        cascade="all, delete-orphan",
    )


class FormProvision(Base):
    __tablename__ = "hermes_form_provisions"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    form_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("hermes_policy_forms.id", ondelete="CASCADE"),
        nullable=False,
    )
    provision_type: Mapped[str] = mapped_column(String(50), nullable=False)
    provision_key: Mapped[Optional[str]] = mapped_column(String(200))
    provision_text_summary: Mapped[str] = mapped_column(Text, nullable=False)
    provision_text_full: Mapped[Optional[str]] = mapped_column(Text)
    section_reference: Mapped[Optional[str]] = mapped_column(String(100))
    is_coverage_broadening: Mapped[Optional[bool]] = mapped_column(Boolean)
    is_coverage_restricting: Mapped[Optional[bool]] = mapped_column(Boolean)
    iso_comparison_notes: Mapped[Optional[str]] = mapped_column(Text)
    confidence: Mapped[Decimal] = mapped_column(Numeric(5, 4), nullable=False, default=Decimal("1.0"))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    # Relationships
    form: Mapped["PolicyForm"] = relationship("PolicyForm", back_populates="provisions")


class FormComparison(Base):
    __tablename__ = "hermes_form_comparisons"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    form_id_a: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("hermes_policy_forms.id", ondelete="CASCADE"),
        nullable=False,
    )
    form_id_b: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("hermes_policy_forms.id", ondelete="CASCADE"),
        nullable=False,
    )
    comparison_type: Mapped[Optional[str]] = mapped_column(String(50))
    differences: Mapped[Optional[list]] = mapped_column(JSONB, default=list)
    broader_coverage_form: Mapped[Optional[str]] = mapped_column(String(1))
    significance_score: Mapped[Optional[Decimal]] = mapped_column(Numeric(5, 2))
    summary: Mapped[Optional[str]] = mapped_column(Text)
    computed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    # Relationships
    form_a: Mapped["PolicyForm"] = relationship(
        "PolicyForm",
        back_populates="comparisons_as_a",
        foreign_keys=[form_id_a],
    )
    form_b: Mapped["PolicyForm"] = relationship(
        "PolicyForm",
        back_populates="comparisons_as_b",
        foreign_keys=[form_id_b],
    )


# ── Appetite Profiles (Migration 006) ────────────────────────────

class AppetiteProfile(Base):
    __tablename__ = "hermes_appetite_profiles"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    carrier_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("hermes_carriers.id"), nullable=False
    )
    state: Mapped[str] = mapped_column(String(2), nullable=False)
    line: Mapped[str] = mapped_column(String(100), nullable=False)
    appetite_score: Mapped[Decimal] = mapped_column(
        Numeric(5, 2), nullable=False, default=Decimal("5.0")
    )
    eligible_classes: Mapped[Optional[list]] = mapped_column(JSONB, default=list)
    ineligible_classes: Mapped[Optional[list]] = mapped_column(JSONB, default=list)
    preferred_classes: Mapped[Optional[list]] = mapped_column(JSONB, default=list)
    territory_preferences: Mapped[Optional[dict]] = mapped_column(JSONB, default=dict)
    limit_range_min: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 2))
    limit_range_max: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 2))
    typical_deductible_min: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 2))
    typical_deductible_max: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 2))
    target_premium_min: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 2))
    target_premium_max: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 2))
    rate_competitiveness_index: Mapped[Optional[Decimal]] = mapped_column(Numeric(5, 2))
    last_rate_change_pct: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4))
    last_rate_change_date: Mapped[Optional[date]] = mapped_column(Date)
    filing_frequency_score: Mapped[Optional[Decimal]] = mapped_column(Numeric(5, 2))
    years_active_in_state: Mapped[Optional[int]] = mapped_column(Integer)
    market_share_estimate: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4))
    source_filing_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    computed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    is_current: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    # Relationships
    carrier: Mapped["Carrier"] = relationship("Carrier", back_populates="appetite_profiles")
    signals: Mapped[List["AppetiteSignal"]] = relationship(
        "AppetiteSignal", back_populates="profile", cascade="all, delete-orphan"
    )


class AppetiteSignal(Base):
    __tablename__ = "hermes_appetite_signals"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    profile_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("hermes_appetite_profiles.id", ondelete="CASCADE"),
        nullable=False,
    )
    carrier_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("hermes_carriers.id"), nullable=False
    )
    state: Mapped[str] = mapped_column(String(2), nullable=False)
    line: Mapped[str] = mapped_column(String(100), nullable=False)
    signal_type: Mapped[str] = mapped_column(String(50), nullable=False)
    signal_strength: Mapped[Decimal] = mapped_column(
        Numeric(5, 2), nullable=False, default=Decimal("5.0")
    )
    signal_date: Mapped[date] = mapped_column(Date, nullable=False)
    signal_description: Mapped[Optional[str]] = mapped_column(Text)
    source_filing_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True), ForeignKey("hermes_filings.id")
    )
    confidence: Mapped[Decimal] = mapped_column(Numeric(5, 4), nullable=False, default=Decimal("1.0"))
    acknowledged: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    # Relationships
    profile: Mapped["AppetiteProfile"] = relationship(
        "AppetiteProfile", back_populates="signals"
    )
    carrier: Mapped["Carrier"] = relationship("Carrier", back_populates="appetite_signals")
    source_filing: Mapped[Optional["Filing"]] = relationship(
        "Filing", back_populates="appetite_signals"
    )


class CarrierRanking(Base):
    __tablename__ = "hermes_carrier_rankings"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    state: Mapped[str] = mapped_column(String(2), nullable=False)
    line: Mapped[str] = mapped_column(String(100), nullable=False)
    class_code: Mapped[str] = mapped_column(String(20), nullable=False)
    carrier_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("hermes_carriers.id"), nullable=False
    )
    rank: Mapped[int] = mapped_column(Integer, nullable=False)
    estimated_premium_index: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 2))
    appetite_score: Mapped[Optional[Decimal]] = mapped_column(Numeric(5, 2))
    composite_score: Mapped[Optional[Decimal]] = mapped_column(Numeric(5, 2))
    notes: Mapped[Optional[str]] = mapped_column(Text)
    computed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    # Relationships
    carrier: Mapped["Carrier"] = relationship("Carrier", back_populates="rankings")


class MarketIntelligence(Base):
    __tablename__ = "hermes_market_intelligence"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    state: Mapped[str] = mapped_column(String(2), nullable=False)
    line: Mapped[str] = mapped_column(String(100), nullable=False)
    period_start: Mapped[date] = mapped_column(Date, nullable=False)
    period_end: Mapped[date] = mapped_column(Date, nullable=False)
    avg_rate_change_pct: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4))
    median_rate_change_pct: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4))
    filing_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    rate_increase_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    rate_decrease_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    new_entrant_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    withdrawal_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    new_entrants: Mapped[Optional[list]] = mapped_column(JSONB, default=list)
    withdrawals: Mapped[Optional[list]] = mapped_column(JSONB, default=list)
    top_appetite_shifts: Mapped[Optional[list]] = mapped_column(JSONB, default=list)
    market_trend: Mapped[Optional[str]] = mapped_column(String(20))
    summary: Mapped[Optional[str]] = mapped_column(Text)
    computed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )


# ── Metadata (Migration 007) ──────────────────────────────────────

class ScrapeLog(Base):
    __tablename__ = "hermes_scrape_log"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    state: Mapped[str] = mapped_column(String(2), nullable=False)
    line: Mapped[Optional[str]] = mapped_column(String(100))
    carrier_naic: Mapped[Optional[str]] = mapped_column(String(10))
    search_params: Mapped[Optional[dict]] = mapped_column(JSONB, default=dict)
    filings_found: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    filings_new: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    filings_updated: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    documents_downloaded: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    errors: Mapped[Optional[list]] = mapped_column(JSONB, default=list)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    duration_seconds: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2))
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="running")
    error_message: Mapped[Optional[str]] = mapped_column(Text)
    scrape_type: Mapped[str] = mapped_column(String(20), nullable=False, default="incremental")
    watermark_date: Mapped[Optional[date]] = mapped_column(Date)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )


class ParseLog(Base):
    __tablename__ = "hermes_parse_log"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    filing_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True), ForeignKey("hermes_filings.id")
    )
    document_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True), ForeignKey("hermes_filing_documents.id")
    )
    parser_type: Mapped[Optional[str]] = mapped_column(String(50))
    parser_version: Mapped[Optional[str]] = mapped_column(String(20))
    tables_extracted: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    rules_extracted: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    forms_extracted: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    factors_extracted: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    confidence_avg: Mapped[Optional[Decimal]] = mapped_column(Numeric(5, 4))
    confidence_min: Mapped[Optional[Decimal]] = mapped_column(Numeric(5, 4))
    ai_calls_made: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    ai_tokens_used: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    ai_cost_usd: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4))
    errors: Mapped[Optional[list]] = mapped_column(JSONB, default=list)
    warnings: Mapped[Optional[list]] = mapped_column(JSONB, default=list)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    duration_seconds: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2))
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="running")
    error_message: Mapped[Optional[str]] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    # Relationships
    filing: Mapped[Optional["Filing"]] = relationship("Filing", back_populates="parse_logs")
    document: Mapped[Optional["FilingDocument"]] = relationship(
        "FilingDocument", back_populates="parse_logs"
    )
    review_items: Mapped[List["ParseReviewItem"]] = relationship(
        "ParseReviewItem", back_populates="parse_log", cascade="all, delete-orphan"
    )


class ParseReviewItem(Base):
    __tablename__ = "hermes_parse_review_queue"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    parse_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True), ForeignKey("hermes_parse_log.id")
    )
    filing_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True), ForeignKey("hermes_filings.id")
    )
    document_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True), ForeignKey("hermes_filing_documents.id")
    )
    table_name: Mapped[str] = mapped_column(String(100), nullable=False)
    record_id: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True))
    field_name: Mapped[str] = mapped_column(String(100), nullable=False)
    extracted_value: Mapped[Optional[str]] = mapped_column(Text)
    confidence: Mapped[Decimal] = mapped_column(Numeric(5, 4), nullable=False)
    context_text: Mapped[Optional[str]] = mapped_column(Text)
    source_page: Mapped[Optional[int]] = mapped_column(Integer)
    review_priority: Mapped[str] = mapped_column(String(10), nullable=False, default="medium")
    reviewed_flag: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    corrected_value: Mapped[Optional[str]] = mapped_column(Text)
    reviewer: Mapped[Optional[str]] = mapped_column(String(100))
    reviewed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    review_notes: Mapped[Optional[str]] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    # Relationships
    parse_log: Mapped[Optional["ParseLog"]] = relationship(
        "ParseLog", back_populates="review_items"
    )


class StateConfig(Base):
    __tablename__ = "hermes_state_config"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    state: Mapped[str] = mapped_column(String(2), nullable=False, unique=True)
    state_name: Mapped[str] = mapped_column(String(100), nullable=False)
    sfa_portal_url: Mapped[Optional[str]] = mapped_column(String(500))
    sfa_accessible: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    tier: Mapped[int] = mapped_column(Integer, nullable=False, default=3)
    lines_available: Mapped[Optional[List[str]]] = mapped_column(ARRAY(Text))
    notes: Mapped[Optional[str]] = mapped_column(Text)
    last_scraped_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    scrape_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
