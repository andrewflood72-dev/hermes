-- Hermes Migration 011: Title Insurance Pricing Engine
-- Creates tables for title insurance rate cards, tiered rates, simultaneous
-- issue discounts, reissue credits, endorsement pricing, SERFF data, and
-- quote audit logging.  Seeds 8 major US title insurance carriers.

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ── Title Rate Cards ──────────────────────────────────────────
-- Versioned rate card per carrier, policy type, and state.
-- is_promulgated = TRUE for states like TX where rates are state-set.

CREATE TABLE IF NOT EXISTS hermes_title_rate_cards (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    carrier_id UUID NOT NULL REFERENCES hermes_carriers(id) ON DELETE CASCADE,
    policy_type VARCHAR(30) NOT NULL CHECK (policy_type IN (
        'owner', 'lender', 'simultaneous', 'reissue', 'endorsement'
    )),
    state VARCHAR(2) NOT NULL,
    is_promulgated BOOLEAN NOT NULL DEFAULT FALSE,
    effective_date DATE NOT NULL,
    expiration_date DATE,
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    source VARCHAR(50) NOT NULL DEFAULT 'manual',
    version INTEGER NOT NULL DEFAULT 1,
    superseded_by UUID REFERENCES hermes_title_rate_cards(id),
    notes TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_title_rate_cards_carrier
    ON hermes_title_rate_cards(carrier_id);
CREATE INDEX IF NOT EXISTS idx_title_rate_cards_lookup
    ON hermes_title_rate_cards(carrier_id, policy_type, state);
CREATE INDEX IF NOT EXISTS idx_title_rate_cards_current
    ON hermes_title_rate_cards(is_current) WHERE is_current = TRUE;
CREATE INDEX IF NOT EXISTS idx_title_rate_cards_state
    ON hermes_title_rate_cards(state);

CREATE TRIGGER trg_title_rate_cards_updated
    BEFORE UPDATE ON hermes_title_rate_cards
    FOR EACH ROW EXECUTE FUNCTION hermes_update_timestamp();

-- ── Title Rates ───────────────────────────────────────────────
-- Tiered coverage bands: rate_per_thousand applied to each band slice.
-- Title premiums are one-time at closing, not recurring like PMI.

CREATE TABLE IF NOT EXISTS hermes_title_rates (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    rate_card_id UUID NOT NULL REFERENCES hermes_title_rate_cards(id) ON DELETE CASCADE,
    coverage_min NUMERIC(14,2) NOT NULL DEFAULT 0,
    coverage_max NUMERIC(14,2) NOT NULL,
    rate_per_thousand NUMERIC(10,4) NOT NULL,
    flat_fee NUMERIC(10,2) NOT NULL DEFAULT 0,
    minimum_premium NUMERIC(10,2) NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_title_rates_card
    ON hermes_title_rates(rate_card_id);
CREATE INDEX IF NOT EXISTS idx_title_rates_lookup
    ON hermes_title_rates(rate_card_id, coverage_min, coverage_max);

CREATE TRIGGER trg_title_rates_updated
    BEFORE UPDATE ON hermes_title_rates
    FOR EACH ROW EXECUTE FUNCTION hermes_update_timestamp();

-- ── Title Simultaneous Issue ──────────────────────────────────
-- THE key arbitrage table.  Discount schedule when owner + lender
-- policies are issued simultaneously.  20-40pp dispersion across
-- carriers — this is where the MGA value proposition lives.

CREATE TABLE IF NOT EXISTS hermes_title_simultaneous_issue (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    rate_card_id UUID NOT NULL REFERENCES hermes_title_rate_cards(id) ON DELETE CASCADE,
    loan_min NUMERIC(14,2) NOT NULL DEFAULT 0,
    loan_max NUMERIC(14,2) NOT NULL,
    discount_rate_per_thousand NUMERIC(10,4) NOT NULL DEFAULT 0,
    discount_pct NUMERIC(6,2) NOT NULL DEFAULT 0,
    flat_fee NUMERIC(10,2) NOT NULL DEFAULT 0,
    conditions JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_title_simul_card
    ON hermes_title_simultaneous_issue(rate_card_id);
CREATE INDEX IF NOT EXISTS idx_title_simul_lookup
    ON hermes_title_simultaneous_issue(rate_card_id, loan_min, loan_max);

CREATE TRIGGER trg_title_simul_updated
    BEFORE UPDATE ON hermes_title_simultaneous_issue
    FOR EACH ROW EXECUTE FUNCTION hermes_update_timestamp();

-- ── Title Reissue Credits ─────────────────────────────────────
-- Time-tiered discounts for refinance transactions where a prior
-- policy exists.  Credit decays over years since original policy.

CREATE TABLE IF NOT EXISTS hermes_title_reissue_credits (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    rate_card_id UUID NOT NULL REFERENCES hermes_title_rate_cards(id) ON DELETE CASCADE,
    years_since_min NUMERIC(4,1) NOT NULL DEFAULT 0,
    years_since_max NUMERIC(4,1) NOT NULL,
    credit_pct NUMERIC(6,2) NOT NULL DEFAULT 0,
    conditions JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_title_reissue_card
    ON hermes_title_reissue_credits(rate_card_id);

CREATE TRIGGER trg_title_reissue_updated
    BEFORE UPDATE ON hermes_title_reissue_credits
    FOR EACH ROW EXECUTE FUNCTION hermes_update_timestamp();

-- ── Title Endorsements ────────────────────────────────────────
-- ALTA endorsement pricing: can be flat fee, rate per thousand,
-- or percentage of base premium.

CREATE TABLE IF NOT EXISTS hermes_title_endorsements (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    rate_card_id UUID NOT NULL REFERENCES hermes_title_rate_cards(id) ON DELETE CASCADE,
    endorsement_code VARCHAR(20) NOT NULL,
    endorsement_name VARCHAR(200) NOT NULL,
    flat_fee NUMERIC(10,2) NOT NULL DEFAULT 0,
    rate_per_thousand NUMERIC(10,4) NOT NULL DEFAULT 0,
    pct_of_base NUMERIC(6,4) NOT NULL DEFAULT 0,
    description TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_title_endorse_card
    ON hermes_title_endorsements(rate_card_id);
CREATE INDEX IF NOT EXISTS idx_title_endorse_code
    ON hermes_title_endorsements(endorsement_code);

CREATE TRIGGER trg_title_endorse_updated
    BEFORE UPDATE ON hermes_title_endorsements
    FOR EACH ROW EXECUTE FUNCTION hermes_update_timestamp();

-- ── Title SERFF Data ──────────────────────────────────────────
-- Regulatory filing supplements — approved rates, loss ratio data,
-- and underwriting rules extracted from SERFF filings.

CREATE TABLE IF NOT EXISTS hermes_title_serff_data (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    carrier_id UUID NOT NULL REFERENCES hermes_carriers(id) ON DELETE CASCADE,
    filing_id UUID REFERENCES hermes_filings(id) ON DELETE SET NULL,
    state VARCHAR(2) NOT NULL,
    approved_rates JSONB NOT NULL DEFAULT '{}',
    loss_ratio_data JSONB NOT NULL DEFAULT '{}',
    underwriting_rules JSONB NOT NULL DEFAULT '{}',
    effective_date DATE,
    expiration_date DATE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_title_serff_carrier
    ON hermes_title_serff_data(carrier_id);
CREATE INDEX IF NOT EXISTS idx_title_serff_state
    ON hermes_title_serff_data(state);
CREATE INDEX IF NOT EXISTS idx_title_serff_carrier_state
    ON hermes_title_serff_data(carrier_id, state);

CREATE TRIGGER trg_title_serff_updated
    BEFORE UPDATE ON hermes_title_serff_data
    FOR EACH ROW EXECUTE FUNCTION hermes_update_timestamp();

-- ── Title Quote Log ───────────────────────────────────────────
-- Audit trail for all title quote requests and results.

CREATE TABLE IF NOT EXISTS hermes_title_quote_log (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    request_data JSONB NOT NULL DEFAULT '{}',
    response_data JSONB NOT NULL DEFAULT '{}',
    carriers_quoted INTEGER NOT NULL DEFAULT 0,
    best_premium NUMERIC(12,2),
    best_carrier_id UUID REFERENCES hermes_carriers(id) ON DELETE SET NULL,
    processing_time_ms NUMERIC(10,1),
    source VARCHAR(50) NOT NULL DEFAULT 'api',
    ip_address VARCHAR(45),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_title_quote_log_created
    ON hermes_title_quote_log(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_title_quote_log_source
    ON hermes_title_quote_log(source);

-- ── Seed 8 US Title Insurance Carriers ───────────────────────
-- The major title insurance underwriters.
-- Uses NAIC codes 60001-60008 to avoid collisions with P&C (008)
-- and PMI (009) carrier seeds.

INSERT INTO hermes_carriers (
    naic_code, legal_name, group_name, group_naic_code,
    am_best_rating, domicile_state, company_type,
    treasury_570_listed, status
) VALUES

-- 1. Fidelity National Title Insurance Company
('60001', 'Fidelity National Title Insurance Co',
 'Fidelity National Financial Inc', '60001',
 'A', 'FL', 'stock', TRUE, 'active'),

-- 2. First American Title Insurance Company
('60002', 'First American Title Insurance Co',
 'First American Financial Corp', '60002',
 'A', 'NE', 'stock', TRUE, 'active'),

-- 3. Old Republic National Title Insurance Company
('60003', 'Old Republic National Title Insurance Co',
 'Old Republic International Corp', '60003',
 'A+', 'MN', 'stock', TRUE, 'active'),

-- 4. Stewart Title Guaranty Company
('60004', 'Stewart Title Guaranty Co',
 'Stewart Information Services Corp', '60004',
 'A-', 'TX', 'stock', TRUE, 'active'),

-- 5. WFG National Title Insurance Company
('60005', 'WFG National Title Insurance Co',
 'Williston Financial Group', '60005',
 'A-', 'OR', 'stock', FALSE, 'active'),

-- 6. Investors Title Insurance Company
('60006', 'Investors Title Insurance Co',
 'Investors Title Company', '60006',
 'A', 'NC', 'stock', FALSE, 'active'),

-- 7. Westcor Land Title Insurance Company
('60007', 'Westcor Land Title Insurance Co',
 'Williston Financial Group', '60007',
 'A-', 'FL', 'stock', FALSE, 'active'),

-- 8. North American Title Insurance Company
('60008', 'North American Title Insurance Co',
 'Doma Holdings Inc', '60008',
 'B++', 'CA', 'stock', FALSE, 'active')

ON CONFLICT (naic_code) DO NOTHING;
