-- Hermes Migration 009: PMI Pricing Engine
-- Creates tables for Private Mortgage Insurance rate cards, rates,
-- adjustments, SERFF filing data, and quote audit logging.
-- Seeds 6 US PMI carriers into hermes_carriers.

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ── PMI Rate Cards ──────────────────────────────────────────────
-- Versioned rate card per carrier, premium type, and state.

CREATE TABLE IF NOT EXISTS hermes_pmi_rate_cards (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    carrier_id UUID NOT NULL REFERENCES hermes_carriers(id) ON DELETE CASCADE,
    premium_type VARCHAR(20) NOT NULL CHECK (premium_type IN (
        'monthly', 'single', 'split', 'lender_paid'
    )),
    state VARCHAR(2),  -- NULL = nationwide
    effective_date DATE NOT NULL,
    expiration_date DATE,
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    source VARCHAR(50) NOT NULL DEFAULT 'manual',  -- manual, serff, api
    version INTEGER NOT NULL DEFAULT 1,
    superseded_by UUID REFERENCES hermes_pmi_rate_cards(id),
    notes TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pmi_rate_cards_carrier
    ON hermes_pmi_rate_cards(carrier_id);
CREATE INDEX IF NOT EXISTS idx_pmi_rate_cards_carrier_type_state
    ON hermes_pmi_rate_cards(carrier_id, premium_type, state);
CREATE INDEX IF NOT EXISTS idx_pmi_rate_cards_current
    ON hermes_pmi_rate_cards(is_current) WHERE is_current = TRUE;

CREATE TRIGGER trg_pmi_rate_cards_updated
    BEFORE UPDATE ON hermes_pmi_rate_cards
    FOR EACH ROW EXECUTE FUNCTION hermes_update_timestamp();

-- ── PMI Rates ───────────────────────────────────────────────────
-- LTV × FICO × coverage grid cells. Each row is a single cell
-- in the carrier's published rate card.

CREATE TABLE IF NOT EXISTS hermes_pmi_rates (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    rate_card_id UUID NOT NULL REFERENCES hermes_pmi_rate_cards(id) ON DELETE CASCADE,
    ltv_min NUMERIC(5,2) NOT NULL,   -- e.g. 85.01
    ltv_max NUMERIC(5,2) NOT NULL,   -- e.g. 90.00
    fico_min INTEGER NOT NULL,        -- e.g. 720
    fico_max INTEGER NOT NULL,        -- e.g. 759
    coverage_pct NUMERIC(5,2) NOT NULL, -- e.g. 30.00 (30% coverage)
    rate_pct NUMERIC(8,4) NOT NULL,    -- annual rate as %, e.g. 0.5200
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pmi_rates_card
    ON hermes_pmi_rates(rate_card_id);
CREATE INDEX IF NOT EXISTS idx_pmi_rates_lookup
    ON hermes_pmi_rates(rate_card_id, ltv_min, ltv_max, fico_min, fico_max, coverage_pct);

CREATE TRIGGER trg_pmi_rates_updated
    BEFORE UPDATE ON hermes_pmi_rates
    FOR EACH ROW EXECUTE FUNCTION hermes_update_timestamp();

-- ── PMI Adjustments ─────────────────────────────────────────────
-- Rate modifiers keyed by JSONB conditions (DTI range, property
-- type, occupancy, loan purpose, etc.)

CREATE TABLE IF NOT EXISTS hermes_pmi_adjustments (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    rate_card_id UUID NOT NULL REFERENCES hermes_pmi_rate_cards(id) ON DELETE CASCADE,
    name VARCHAR(100) NOT NULL,        -- e.g. 'high_dti', 'investment_property'
    condition JSONB NOT NULL DEFAULT '{}',  -- e.g. {"dti_min": 43, "dti_max": 50}
    adjustment_method VARCHAR(20) NOT NULL CHECK (adjustment_method IN (
        'additive', 'multiplicative', 'override'
    )),
    adjustment_value NUMERIC(8,4) NOT NULL, -- basis points (additive) or factor (multiplicative)
    description TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pmi_adjustments_card
    ON hermes_pmi_adjustments(rate_card_id);
CREATE INDEX IF NOT EXISTS idx_pmi_adjustments_condition
    ON hermes_pmi_adjustments USING gin(condition);

CREATE TRIGGER trg_pmi_adjustments_updated
    BEFORE UPDATE ON hermes_pmi_adjustments
    FOR EACH ROW EXECUTE FUNCTION hermes_update_timestamp();

-- ── PMI SERFF Data ──────────────────────────────────────────────
-- Regulatory filing supplements — approved rate ranges, state
-- rules, and actuarial data extracted from SERFF filings.

CREATE TABLE IF NOT EXISTS hermes_pmi_serff_data (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    carrier_id UUID NOT NULL REFERENCES hermes_carriers(id) ON DELETE CASCADE,
    filing_id UUID REFERENCES hermes_filings(id) ON DELETE SET NULL,
    state VARCHAR(2) NOT NULL,
    approved_rate_range JSONB NOT NULL DEFAULT '{}',  -- {"min": 0.20, "max": 2.50}
    state_rules JSONB NOT NULL DEFAULT '{}',          -- state-specific constraints
    actuarial_data JSONB NOT NULL DEFAULT '{}',       -- loss ratios, reserves, etc.
    effective_date DATE,
    expiration_date DATE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pmi_serff_carrier
    ON hermes_pmi_serff_data(carrier_id);
CREATE INDEX IF NOT EXISTS idx_pmi_serff_state
    ON hermes_pmi_serff_data(state);
CREATE INDEX IF NOT EXISTS idx_pmi_serff_carrier_state
    ON hermes_pmi_serff_data(carrier_id, state);

CREATE TRIGGER trg_pmi_serff_updated
    BEFORE UPDATE ON hermes_pmi_serff_data
    FOR EACH ROW EXECUTE FUNCTION hermes_update_timestamp();

-- ── PMI Quote Log ───────────────────────────────────────────────
-- Audit trail for all quote requests and results.

CREATE TABLE IF NOT EXISTS hermes_pmi_quote_log (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    request_data JSONB NOT NULL DEFAULT '{}',
    response_data JSONB NOT NULL DEFAULT '{}',
    carriers_quoted INTEGER NOT NULL DEFAULT 0,
    best_rate NUMERIC(8,4),
    best_carrier_id UUID REFERENCES hermes_carriers(id) ON DELETE SET NULL,
    processing_time_ms NUMERIC(10,1),
    source VARCHAR(50) NOT NULL DEFAULT 'api',  -- api, better_mortgage, internal
    ip_address VARCHAR(45),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pmi_quote_log_created
    ON hermes_pmi_quote_log(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_pmi_quote_log_source
    ON hermes_pmi_quote_log(source);

-- ── Seed 6 US PMI Carriers ─────────────────────────────────────
-- The "Big 6" private mortgage insurance companies.
-- Uses ON CONFLICT to avoid duplicates if re-run.

INSERT INTO hermes_carriers (
    naic_code, legal_name, group_name, group_naic_code,
    am_best_rating, domicile_state, company_type,
    treasury_570_listed, status
) VALUES

-- PMI carriers use dedicated NAIC codes (50501-50506) to avoid
-- collisions with P&C carriers seeded in migration 008.

-- 1. MGIC (Mortgage Guaranty Insurance Corporation)
('50501', 'Mortgage Guaranty Insurance Corp',
 'MGIC Investment Corp', '50501',
 'A', 'WI', 'stock', FALSE, 'active'),

-- 2. Radian Guaranty
('50502', 'Radian Guaranty Inc',
 'Radian Group Inc', '50502',
 'A', 'PA', 'stock', FALSE, 'active'),

-- 3. Essent Guaranty
('50503', 'Essent Guaranty Inc',
 'Essent Group Ltd', '50503',
 'A', 'PA', 'stock', FALSE, 'active'),

-- 4. Arch Mortgage Insurance (Arch MI)
('50504', 'Arch Mortgage Insurance Co',
 'Arch Capital Group Ltd', '50504',
 'A+', 'WI', 'stock', FALSE, 'active'),

-- 5. Enact Mortgage Insurance (formerly Genworth)
('50505', 'Enact Mortgage Insurance Corp',
 'Enact Holdings Inc', '50505',
 'A', 'NC', 'stock', FALSE, 'active'),

-- 6. National Mortgage Insurance (National MI)
('50506', 'National Mortgage Insurance Corp',
 'NMI Holdings Inc', '50506',
 'A', 'WI', 'stock', FALSE, 'active')

ON CONFLICT (naic_code) DO NOTHING;
