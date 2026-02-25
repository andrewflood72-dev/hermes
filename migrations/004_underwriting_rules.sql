-- Hermes Migration 004: Underwriting Rules
-- Eligibility criteria, coverage options, credits/surcharges, exclusions

-- ── Underwriting Rules ───────────────────────────────────────────

CREATE TABLE IF NOT EXISTS hermes_underwriting_rules (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    filing_id       UUID NOT NULL REFERENCES hermes_filings(id) ON DELETE CASCADE,
    carrier_id      UUID NOT NULL REFERENCES hermes_carriers(id),
    state           VARCHAR(2) NOT NULL,
    line            VARCHAR(100) NOT NULL,
    rule_type       VARCHAR(50) NOT NULL,  -- eligibility, rating, territory, classification, general
    rule_category   VARCHAR(100),  -- eligible_class, ineligible_class, min_years, max_loss_ratio, construction, territory_restriction, etc.
    rule_text       TEXT NOT NULL,  -- original text from filing
    rule_structured JSONB DEFAULT '{}',  -- structured extraction
    section_reference VARCHAR(200),  -- page/section in source document
    effective_date  DATE,
    expiration_date DATE,
    is_current      BOOLEAN DEFAULT TRUE,
    superseded_by   UUID REFERENCES hermes_underwriting_rules(id),
    source_document_id UUID REFERENCES hermes_filing_documents(id),
    confidence      NUMERIC(5,4) DEFAULT 1.0,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_hermes_rules_filing ON hermes_underwriting_rules(filing_id);
CREATE INDEX idx_hermes_rules_carrier ON hermes_underwriting_rules(carrier_id);
CREATE INDEX idx_hermes_rules_state_line ON hermes_underwriting_rules(state, line);
CREATE INDEX idx_hermes_rules_type ON hermes_underwriting_rules(rule_type);
CREATE INDEX idx_hermes_rules_category ON hermes_underwriting_rules(rule_category);
CREATE INDEX idx_hermes_rules_current ON hermes_underwriting_rules(is_current) WHERE is_current = TRUE;

-- ── Eligibility Criteria ─────────────────────────────────────────

CREATE TABLE IF NOT EXISTS hermes_eligibility_criteria (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    rule_id         UUID NOT NULL REFERENCES hermes_underwriting_rules(id) ON DELETE CASCADE,
    criterion_type  VARCHAR(100) NOT NULL,  -- eligible_class, ineligible_class, min_years_business, max_loss_ratio, territory_restriction, construction_type, min_employees, max_employees, revenue_range, operations_restriction
    criterion_value VARCHAR(1000) NOT NULL,  -- the value or values (may be JSON array for IN operator)
    criterion_operator VARCHAR(20) NOT NULL DEFAULT 'equals',  -- equals, gt, lt, gte, lte, in, not_in, between, contains, not_contains
    criterion_unit  VARCHAR(50),  -- years, percent, dollars, etc.
    is_hard_rule    BOOLEAN DEFAULT TRUE,  -- hard = must pass, soft = affects scoring
    description     TEXT,
    confidence      NUMERIC(5,4) DEFAULT 1.0,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_hermes_elig_rule ON hermes_eligibility_criteria(rule_id);
CREATE INDEX idx_hermes_elig_type ON hermes_eligibility_criteria(criterion_type);
CREATE INDEX idx_hermes_elig_hard ON hermes_eligibility_criteria(is_hard_rule);

-- ── Coverage Options ─────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS hermes_coverage_options (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    filing_id       UUID NOT NULL REFERENCES hermes_filings(id) ON DELETE CASCADE,
    carrier_id      UUID NOT NULL REFERENCES hermes_carriers(id),
    state           VARCHAR(2) NOT NULL,
    line            VARCHAR(100) NOT NULL,
    coverage_type   VARCHAR(200) NOT NULL,  -- occurrence, claims-made, aggregate, per-project, blanket
    limit_min       NUMERIC(15,2),
    limit_max       NUMERIC(15,2),
    default_limit   NUMERIC(15,2),
    deductible_options JSONB DEFAULT '[]',  -- array of available deductible amounts
    default_deductible NUMERIC(15,2),
    coinsurance_options JSONB DEFAULT '[]',  -- array of coinsurance percentages
    sublimits       JSONB DEFAULT '{}',  -- named sublimits: {coverage: amount}
    waiting_periods JSONB DEFAULT '[]',
    retroactive_date_options JSONB DEFAULT '[]',
    source_document_id UUID REFERENCES hermes_filing_documents(id),
    effective_date  DATE,
    is_current      BOOLEAN DEFAULT TRUE,
    confidence      NUMERIC(5,4) DEFAULT 1.0,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_hermes_coverage_filing ON hermes_coverage_options(filing_id);
CREATE INDEX idx_hermes_coverage_carrier ON hermes_coverage_options(carrier_id);
CREATE INDEX idx_hermes_coverage_state_line ON hermes_coverage_options(state, line);

-- ── Credits and Surcharges ───────────────────────────────────────

CREATE TABLE IF NOT EXISTS hermes_credits_surcharges (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    filing_id       UUID NOT NULL REFERENCES hermes_filings(id) ON DELETE CASCADE,
    carrier_id      UUID NOT NULL REFERENCES hermes_carriers(id),
    state           VARCHAR(2) NOT NULL,
    line            VARCHAR(100) NOT NULL,
    credit_type     VARCHAR(100) NOT NULL,  -- protective_safeguard, loss_free, sprinkler, alarm, new_venture, claims_free, safety_program, fleet_size, experience_mod, schedule_rating
    credit_or_surcharge VARCHAR(10) NOT NULL DEFAULT 'credit',  -- credit, surcharge
    range_min       NUMERIC(8,4),  -- e.g. -0.25 for 25% credit
    range_max       NUMERIC(8,4),  -- e.g. -0.05 for 5% credit
    default_value   NUMERIC(8,4),
    conditions      JSONB DEFAULT '{}',  -- conditions for applying
    description     TEXT,
    effective_date  DATE,
    is_current      BOOLEAN DEFAULT TRUE,
    confidence      NUMERIC(5,4) DEFAULT 1.0,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_hermes_credits_filing ON hermes_credits_surcharges(filing_id);
CREATE INDEX idx_hermes_credits_carrier ON hermes_credits_surcharges(carrier_id);
CREATE INDEX idx_hermes_credits_state_line ON hermes_credits_surcharges(state, line);
CREATE INDEX idx_hermes_credits_type ON hermes_credits_surcharges(credit_type);

-- ── Exclusions ───────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS hermes_exclusions (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    filing_id       UUID NOT NULL REFERENCES hermes_filings(id) ON DELETE CASCADE,
    carrier_id      UUID NOT NULL REFERENCES hermes_carriers(id),
    state           VARCHAR(2) NOT NULL,
    line            VARCHAR(100) NOT NULL,
    exclusion_type  VARCHAR(50) NOT NULL,  -- standard, non_standard, endorsement, absolute
    exclusion_text  TEXT NOT NULL,
    exclusion_summary VARCHAR(500),
    exclusion_category VARCHAR(100),  -- pollution, cyber, epl, professional, terrorism, mold, lead, asbestos, nuclear, communicable_disease
    form_reference  VARCHAR(100),
    is_optional     BOOLEAN DEFAULT FALSE,  -- can be bought back via endorsement
    buyback_form    VARCHAR(100),
    source_document_id UUID REFERENCES hermes_filing_documents(id),
    effective_date  DATE,
    is_current      BOOLEAN DEFAULT TRUE,
    confidence      NUMERIC(5,4) DEFAULT 1.0,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_hermes_excl_filing ON hermes_exclusions(filing_id);
CREATE INDEX idx_hermes_excl_carrier ON hermes_exclusions(carrier_id);
CREATE INDEX idx_hermes_excl_state_line ON hermes_exclusions(state, line);
CREATE INDEX idx_hermes_excl_category ON hermes_exclusions(exclusion_category);
CREATE INDEX idx_hermes_excl_type ON hermes_exclusions(exclusion_type);

-- Triggers
CREATE TRIGGER trg_rules_updated BEFORE UPDATE ON hermes_underwriting_rules
    FOR EACH ROW EXECUTE FUNCTION hermes_update_timestamp();
CREATE TRIGGER trg_coverage_updated BEFORE UPDATE ON hermes_coverage_options
    FOR EACH ROW EXECUTE FUNCTION hermes_update_timestamp();
