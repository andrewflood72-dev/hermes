-- Hermes Migration 007: Metadata
-- Scrape logs, parse logs, review queue, and system configuration

-- ── Scrape Log ───────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS hermes_scrape_log (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    state           VARCHAR(2) NOT NULL,
    line            VARCHAR(100),
    carrier_naic    VARCHAR(10),
    search_params   JSONB DEFAULT '{}',
    filings_found   INTEGER DEFAULT 0,
    filings_new     INTEGER DEFAULT 0,
    filings_updated INTEGER DEFAULT 0,
    documents_downloaded INTEGER DEFAULT 0,
    errors          JSONB DEFAULT '[]',
    started_at      TIMESTAMPTZ NOT NULL,
    completed_at    TIMESTAMPTZ,
    duration_seconds NUMERIC(10,2),
    status          VARCHAR(20) DEFAULT 'running',  -- running, completed, failed, partial
    error_message   TEXT,
    scrape_type     VARCHAR(20) DEFAULT 'incremental',  -- seed, incremental, full, targeted
    watermark_date  DATE,  -- last filing date from previous run
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_hermes_scrape_state ON hermes_scrape_log(state);
CREATE INDEX idx_hermes_scrape_status ON hermes_scrape_log(status);
CREATE INDEX idx_hermes_scrape_started ON hermes_scrape_log(started_at DESC);

-- ── Parse Log ────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS hermes_parse_log (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    filing_id       UUID REFERENCES hermes_filings(id),
    document_id     UUID REFERENCES hermes_filing_documents(id),
    parser_type     VARCHAR(50),  -- rate, rule, form, classifier
    parser_version  VARCHAR(20),
    tables_extracted INTEGER DEFAULT 0,
    rules_extracted INTEGER DEFAULT 0,
    forms_extracted INTEGER DEFAULT 0,
    factors_extracted INTEGER DEFAULT 0,
    confidence_avg  NUMERIC(5,4),
    confidence_min  NUMERIC(5,4),
    ai_calls_made   INTEGER DEFAULT 0,
    ai_tokens_used  INTEGER DEFAULT 0,
    ai_cost_usd     NUMERIC(10,4),
    errors          JSONB DEFAULT '[]',
    warnings        JSONB DEFAULT '[]',
    started_at      TIMESTAMPTZ NOT NULL,
    completed_at    TIMESTAMPTZ,
    duration_seconds NUMERIC(10,2),
    status          VARCHAR(20) DEFAULT 'running',  -- running, completed, failed, partial
    error_message   TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_hermes_parse_filing ON hermes_parse_log(filing_id);
CREATE INDEX idx_hermes_parse_document ON hermes_parse_log(document_id);
CREATE INDEX idx_hermes_parse_status ON hermes_parse_log(status);
CREATE INDEX idx_hermes_parse_started ON hermes_parse_log(started_at DESC);

-- ── Parse Review Queue ───────────────────────────────────────────

CREATE TABLE IF NOT EXISTS hermes_parse_review_queue (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    parse_id        UUID REFERENCES hermes_parse_log(id),
    filing_id       UUID REFERENCES hermes_filings(id),
    document_id     UUID REFERENCES hermes_filing_documents(id),
    table_name      VARCHAR(100) NOT NULL,  -- target table for the extracted data
    record_id       UUID,  -- ID of the record in the target table
    field_name      VARCHAR(100) NOT NULL,
    extracted_value TEXT,
    confidence      NUMERIC(5,4) NOT NULL,
    context_text    TEXT,  -- surrounding text for reviewer context
    source_page     INTEGER,
    review_priority VARCHAR(10) DEFAULT 'medium',  -- high, medium, low
    reviewed_flag   BOOLEAN DEFAULT FALSE,
    corrected_value TEXT,
    reviewer        VARCHAR(100),
    reviewed_at     TIMESTAMPTZ,
    review_notes    TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_hermes_review_parse ON hermes_parse_review_queue(parse_id);
CREATE INDEX idx_hermes_review_filing ON hermes_parse_review_queue(filing_id);
CREATE INDEX idx_hermes_review_pending ON hermes_parse_review_queue(reviewed_flag) WHERE reviewed_flag = FALSE;
CREATE INDEX idx_hermes_review_priority ON hermes_parse_review_queue(review_priority, created_at);
CREATE INDEX idx_hermes_review_confidence ON hermes_parse_review_queue(confidence);

-- ── State Configuration ──────────────────────────────────────────

CREATE TABLE IF NOT EXISTS hermes_state_config (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    state           VARCHAR(2) NOT NULL UNIQUE,
    state_name      VARCHAR(100) NOT NULL,
    sfa_portal_url  VARCHAR(500),
    sfa_accessible  BOOLEAN DEFAULT TRUE,
    tier            INTEGER DEFAULT 3,  -- 1, 2, or 3 priority tier
    lines_available TEXT[],
    notes           TEXT,
    last_scraped_at TIMESTAMPTZ,
    scrape_enabled  BOOLEAN DEFAULT FALSE,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_hermes_stateconfig_state ON hermes_state_config(state);
CREATE INDEX idx_hermes_stateconfig_tier ON hermes_state_config(tier);
CREATE INDEX idx_hermes_stateconfig_enabled ON hermes_state_config(scrape_enabled);

-- Trigger
CREATE TRIGGER trg_stateconfig_updated BEFORE UPDATE ON hermes_state_config
    FOR EACH ROW EXECUTE FUNCTION hermes_update_timestamp();
