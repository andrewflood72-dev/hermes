-- Hermes Migration 003: Rate Intelligence
-- Rate tables, base rates, rating factors, territory definitions, class codes, premium algorithms

-- ── Rate Tables ──────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS hermes_rate_tables (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    filing_id       UUID NOT NULL REFERENCES hermes_filings(id) ON DELETE CASCADE,
    carrier_id      UUID NOT NULL REFERENCES hermes_carriers(id),
    state           VARCHAR(2) NOT NULL,
    line            VARCHAR(100) NOT NULL,
    sub_line        VARCHAR(100),
    table_name      VARCHAR(500),
    table_type      VARCHAR(50),  -- base_rate, factor, ilf, deductible, territory
    effective_date  DATE NOT NULL,
    expiration_date DATE,
    version         INTEGER DEFAULT 1,
    is_current      BOOLEAN DEFAULT TRUE,
    superseded_by   UUID REFERENCES hermes_rate_tables(id),
    source_document_id UUID REFERENCES hermes_filing_documents(id),
    extraction_confidence NUMERIC(5,4),
    notes           TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_hermes_rates_filing ON hermes_rate_tables(filing_id);
CREATE INDEX idx_hermes_rates_carrier ON hermes_rate_tables(carrier_id);
CREATE INDEX idx_hermes_rates_state_line ON hermes_rate_tables(state, line);
CREATE INDEX idx_hermes_rates_current ON hermes_rate_tables(is_current) WHERE is_current = TRUE;
CREATE INDEX idx_hermes_rates_effective ON hermes_rate_tables(effective_date);

-- ── Base Rates ───────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS hermes_base_rates (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    rate_table_id   UUID NOT NULL REFERENCES hermes_rate_tables(id) ON DELETE CASCADE,
    class_code      VARCHAR(20) NOT NULL,
    class_description VARCHAR(500),
    territory       VARCHAR(20),
    base_rate       NUMERIC(12,6) NOT NULL,
    rate_per_unit   VARCHAR(100),  -- per $100 payroll, per $1000 revenue, per unit, per $100 TIV
    minimum_premium NUMERIC(12,2),
    maximum_premium NUMERIC(12,2),
    exposure_basis  VARCHAR(100),  -- payroll, revenue, area, units, receipts
    effective_date  DATE,
    confidence      NUMERIC(5,4) DEFAULT 1.0,
    source_page     INTEGER,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_hermes_baserate_table ON hermes_base_rates(rate_table_id);
CREATE INDEX idx_hermes_baserate_class ON hermes_base_rates(class_code);
CREATE INDEX idx_hermes_baserate_territory ON hermes_base_rates(territory);
CREATE INDEX idx_hermes_baserate_class_terr ON hermes_base_rates(class_code, territory);

-- ── Rating Factors ───────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS hermes_rating_factors (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    rate_table_id   UUID NOT NULL REFERENCES hermes_rate_tables(id) ON DELETE CASCADE,
    factor_type     VARCHAR(50) NOT NULL,  -- territory, ilf, deductible, schedule_credit, experience_mod, protective_safeguard, loss_free, new_venture, coinsurance, construction
    factor_key      VARCHAR(200) NOT NULL,  -- the lookup key (territory code, limit amount, deductible amount, etc.)
    factor_value    NUMERIC(12,6) NOT NULL,
    factor_description VARCHAR(500),
    applies_to_line VARCHAR(100),
    effective_date  DATE,
    confidence      NUMERIC(5,4) DEFAULT 1.0,
    source_page     INTEGER,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_hermes_factors_table ON hermes_rating_factors(rate_table_id);
CREATE INDEX idx_hermes_factors_type ON hermes_rating_factors(factor_type);
CREATE INDEX idx_hermes_factors_key ON hermes_rating_factors(factor_key);
CREATE INDEX idx_hermes_factors_type_key ON hermes_rating_factors(factor_type, factor_key);

-- ── Territory Definitions ────────────────────────────────────────

CREATE TABLE IF NOT EXISTS hermes_territory_definitions (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    rate_table_id   UUID NOT NULL REFERENCES hermes_rate_tables(id) ON DELETE CASCADE,
    territory_code  VARCHAR(20) NOT NULL,
    territory_name  VARCHAR(200),
    zip_codes       JSONB DEFAULT '[]',  -- array of zip codes or zip ranges
    counties        JSONB DEFAULT '[]',  -- array of county names/FIPS codes
    cities          JSONB DEFAULT '[]',
    description     TEXT,
    risk_tier       VARCHAR(20),  -- preferred, standard, substandard
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_hermes_terr_table ON hermes_territory_definitions(rate_table_id);
CREATE INDEX idx_hermes_terr_code ON hermes_territory_definitions(territory_code);
CREATE INDEX idx_hermes_terr_zips ON hermes_territory_definitions USING gin(zip_codes);

-- ── Class Code Mappings ──────────────────────────────────────────

CREATE TABLE IF NOT EXISTS hermes_class_code_mappings (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    rate_table_id       UUID NOT NULL REFERENCES hermes_rate_tables(id) ON DELETE CASCADE,
    carrier_class_code  VARCHAR(20) NOT NULL,
    carrier_class_desc  VARCHAR(500),
    iso_class_code      VARCHAR(20),
    naic_class_code     VARCHAR(20),
    naics_codes         JSONB DEFAULT '[]',
    sic_codes           JSONB DEFAULT '[]',
    description         TEXT,
    eligibility_status  VARCHAR(20) DEFAULT 'eligible',  -- eligible, ineligible, refer
    hazard_group        VARCHAR(10),
    created_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_hermes_classmaps_table ON hermes_class_code_mappings(rate_table_id);
CREATE INDEX idx_hermes_classmaps_carrier ON hermes_class_code_mappings(carrier_class_code);
CREATE INDEX idx_hermes_classmaps_iso ON hermes_class_code_mappings(iso_class_code);
CREATE INDEX idx_hermes_classmaps_naics ON hermes_class_code_mappings USING gin(naics_codes);

-- ── Premium Algorithms ───────────────────────────────────────────

CREATE TABLE IF NOT EXISTS hermes_premium_algorithms (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    rate_table_id       UUID NOT NULL REFERENCES hermes_rate_tables(id) ON DELETE CASCADE,
    line                VARCHAR(100) NOT NULL,
    algorithm_name      VARCHAR(200),
    algorithm_description TEXT,
    formula_text        TEXT,  -- human-readable formula
    formula_structured  JSONB DEFAULT '{}',  -- machine-executable structured formula
    variables           JSONB DEFAULT '[]',  -- list of required input variables
    examples            JSONB DEFAULT '[]',  -- worked examples from filing
    notes               TEXT,
    confidence          NUMERIC(5,4) DEFAULT 1.0,
    created_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_hermes_algo_table ON hermes_premium_algorithms(rate_table_id);
CREATE INDEX idx_hermes_algo_line ON hermes_premium_algorithms(line);

-- Triggers
CREATE TRIGGER trg_rate_tables_updated BEFORE UPDATE ON hermes_rate_tables
    FOR EACH ROW EXECUTE FUNCTION hermes_update_timestamp();
