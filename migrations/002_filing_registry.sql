-- Hermes Migration 002: Filing Registry
-- Master index of SERFF filings, documents, and change history

-- ── Filings ──────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS hermes_filings (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    serff_tracking_number VARCHAR(50) NOT NULL,
    carrier_id          UUID REFERENCES hermes_carriers(id),
    carrier_naic_code   VARCHAR(10),
    carrier_name_filed  VARCHAR(500),  -- name as it appears in filing
    state               VARCHAR(2) NOT NULL,
    filing_type         VARCHAR(20) NOT NULL,  -- rate, rule, form, combination
    line_of_business    VARCHAR(100) NOT NULL,
    sub_line            VARCHAR(100),
    product_name        VARCHAR(500),
    status              VARCHAR(20) NOT NULL DEFAULT 'pending',  -- approved, pending, withdrawn, disapproved, closed
    effective_date      DATE,
    filed_date          DATE,
    disposition_date    DATE,
    expiration_date     DATE,
    filing_description  TEXT,
    overall_rate_change_pct NUMERIC(8,4),  -- stated overall rate change
    rate_change_min_pct NUMERIC(8,4),
    rate_change_max_pct NUMERIC(8,4),
    affected_policyholders INTEGER,
    state_portal_url    VARCHAR(1000),
    raw_metadata        JSONB DEFAULT '{}',  -- full metadata from SERFF portal
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(serff_tracking_number, state)
);

CREATE INDEX idx_hermes_filings_serff ON hermes_filings(serff_tracking_number);
CREATE INDEX idx_hermes_filings_carrier ON hermes_filings(carrier_id);
CREATE INDEX idx_hermes_filings_naic ON hermes_filings(carrier_naic_code);
CREATE INDEX idx_hermes_filings_state ON hermes_filings(state);
CREATE INDEX idx_hermes_filings_type ON hermes_filings(filing_type);
CREATE INDEX idx_hermes_filings_line ON hermes_filings(line_of_business);
CREATE INDEX idx_hermes_filings_status ON hermes_filings(status);
CREATE INDEX idx_hermes_filings_effective ON hermes_filings(effective_date);
CREATE INDEX idx_hermes_filings_filed ON hermes_filings(filed_date);
CREATE INDEX idx_hermes_filings_state_line ON hermes_filings(state, line_of_business);
CREATE INDEX idx_hermes_filings_carrier_state ON hermes_filings(carrier_id, state, line_of_business);

-- ── Filing Documents ─────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS hermes_filing_documents (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    filing_id       UUID NOT NULL REFERENCES hermes_filings(id) ON DELETE CASCADE,
    document_name   VARCHAR(500) NOT NULL,
    document_type   VARCHAR(50),  -- rate_exhibit, actuarial_memo, rule_manual, policy_form, endorsement, application, schedule, supporting
    file_path       VARCHAR(1000),
    file_size_bytes BIGINT,
    mime_type       VARCHAR(100),
    page_count      INTEGER,
    parsed_flag     BOOLEAN DEFAULT FALSE,
    parse_confidence NUMERIC(5,4),  -- 0.0000 to 1.0000
    parse_version   VARCHAR(20),
    confidential_flag BOOLEAN DEFAULT FALSE,
    download_url    VARCHAR(2000),
    checksum_sha256 VARCHAR(64),
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_hermes_docs_filing ON hermes_filing_documents(filing_id);
CREATE INDEX idx_hermes_docs_type ON hermes_filing_documents(document_type);
CREATE INDEX idx_hermes_docs_parsed ON hermes_filing_documents(parsed_flag);

-- ── Filing Changes ───────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS hermes_filing_changes (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    filing_id           UUID NOT NULL REFERENCES hermes_filings(id) ON DELETE CASCADE,
    change_type         VARCHAR(20) NOT NULL,  -- new, amendment, withdrawal, rate_revision
    overall_rate_change_pct NUMERIC(8,4),
    effective_date      DATE,
    prior_filing_id     UUID REFERENCES hermes_filings(id),
    description         TEXT,
    change_details      JSONB DEFAULT '{}',
    created_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_hermes_changes_filing ON hermes_filing_changes(filing_id);
CREATE INDEX idx_hermes_changes_type ON hermes_filing_changes(change_type);
CREATE INDEX idx_hermes_changes_prior ON hermes_filing_changes(prior_filing_id);

-- Triggers
CREATE TRIGGER trg_filings_updated BEFORE UPDATE ON hermes_filings
    FOR EACH ROW EXECUTE FUNCTION hermes_update_timestamp();
CREATE TRIGGER trg_docs_updated BEFORE UPDATE ON hermes_filing_documents
    FOR EACH ROW EXECUTE FUNCTION hermes_update_timestamp();
