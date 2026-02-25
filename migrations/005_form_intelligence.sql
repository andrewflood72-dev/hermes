-- Hermes Migration 005: Form Intelligence
-- Policy forms, provisions, and form comparisons

-- ── Policy Forms ─────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS hermes_policy_forms (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    filing_id       UUID NOT NULL REFERENCES hermes_filings(id) ON DELETE CASCADE,
    carrier_id      UUID NOT NULL REFERENCES hermes_carriers(id),
    state           VARCHAR(2) NOT NULL,
    line            VARCHAR(100) NOT NULL,
    form_number     VARCHAR(100) NOT NULL,
    form_name       VARCHAR(500),
    form_edition_date VARCHAR(20),  -- e.g. "01 2024", "Ed. 10/23"
    form_type       VARCHAR(50) NOT NULL,  -- policy, endorsement, application, schedule, certificate, notice, declarations
    iso_equivalent  VARCHAR(100),  -- ISO form number if this is a carrier proprietary equivalent
    is_manuscript   BOOLEAN DEFAULT FALSE,  -- true if carrier proprietary form
    replaces_form   VARCHAR(100),  -- prior form number this replaces
    mandatory       BOOLEAN DEFAULT TRUE,  -- mandatory vs optional endorsement
    additional_premium BOOLEAN DEFAULT FALSE,  -- endorsement carries additional premium
    source_document_id UUID REFERENCES hermes_filing_documents(id),
    effective_date  DATE,
    is_current      BOOLEAN DEFAULT TRUE,
    confidence      NUMERIC(5,4) DEFAULT 1.0,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_hermes_forms_filing ON hermes_policy_forms(filing_id);
CREATE INDEX idx_hermes_forms_carrier ON hermes_policy_forms(carrier_id);
CREATE INDEX idx_hermes_forms_state_line ON hermes_policy_forms(state, line);
CREATE INDEX idx_hermes_forms_number ON hermes_policy_forms(form_number);
CREATE INDEX idx_hermes_forms_type ON hermes_policy_forms(form_type);
CREATE INDEX idx_hermes_forms_iso ON hermes_policy_forms(iso_equivalent);

-- ── Form Provisions ──────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS hermes_form_provisions (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    form_id         UUID NOT NULL REFERENCES hermes_policy_forms(id) ON DELETE CASCADE,
    provision_type  VARCHAR(50) NOT NULL,  -- coverage_grant, exclusion, condition, definition, limit, deductible, endorsement_modification
    provision_key   VARCHAR(200),  -- short identifier for this provision
    provision_text_summary TEXT NOT NULL,  -- AI-summarized provision
    provision_text_full TEXT,  -- full original text
    section_reference VARCHAR(100),  -- section/paragraph in the form
    is_coverage_broadening BOOLEAN,
    is_coverage_restricting BOOLEAN,
    iso_comparison_notes TEXT,  -- how this differs from standard ISO
    confidence      NUMERIC(5,4) DEFAULT 1.0,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_hermes_provisions_form ON hermes_form_provisions(form_id);
CREATE INDEX idx_hermes_provisions_type ON hermes_form_provisions(provision_type);
CREATE INDEX idx_hermes_provisions_key ON hermes_form_provisions(provision_key);

-- ── Form Comparisons ─────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS hermes_form_comparisons (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    form_id_a           UUID NOT NULL REFERENCES hermes_policy_forms(id) ON DELETE CASCADE,
    form_id_b           UUID NOT NULL REFERENCES hermes_policy_forms(id) ON DELETE CASCADE,
    comparison_type     VARCHAR(50),  -- carrier_vs_carrier, carrier_vs_iso, version_vs_version
    differences         JSONB DEFAULT '[]',  -- array of {provision, form_a_text, form_b_text, significance}
    broader_coverage_form VARCHAR(1),  -- 'a' or 'b' or null if equivalent
    significance_score  NUMERIC(5,2),  -- 0-100 how significant the differences are
    summary             TEXT,
    computed_at         TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(form_id_a, form_id_b)
);

CREATE INDEX idx_hermes_comparisons_a ON hermes_form_comparisons(form_id_a);
CREATE INDEX idx_hermes_comparisons_b ON hermes_form_comparisons(form_id_b);

-- Triggers
CREATE TRIGGER trg_forms_updated BEFORE UPDATE ON hermes_policy_forms
    FOR EACH ROW EXECUTE FUNCTION hermes_update_timestamp();
