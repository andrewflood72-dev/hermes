-- Hermes Migration 006: Appetite Profiles (Synthesized)
-- Carrier appetite profiles, signals, rankings, and market intelligence

-- ── Appetite Profiles ────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS hermes_appetite_profiles (
    id                      UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    carrier_id              UUID NOT NULL REFERENCES hermes_carriers(id),
    state                   VARCHAR(2) NOT NULL,
    line                    VARCHAR(100) NOT NULL,
    appetite_score          NUMERIC(5,2) NOT NULL DEFAULT 5.0,  -- 1-10 scale, 10 = most aggressive
    eligible_classes        JSONB DEFAULT '[]',  -- class codes this carrier actively writes
    ineligible_classes      JSONB DEFAULT '[]',  -- explicitly excluded class codes
    preferred_classes       JSONB DEFAULT '[]',  -- sweet-spot class codes
    territory_preferences   JSONB DEFAULT '{}',  -- {territory_code: preference_score}
    limit_range_min         NUMERIC(15,2),
    limit_range_max         NUMERIC(15,2),
    typical_deductible_min  NUMERIC(15,2),
    typical_deductible_max  NUMERIC(15,2),
    target_premium_min      NUMERIC(15,2),
    target_premium_max      NUMERIC(15,2),
    rate_competitiveness_index NUMERIC(5,2),  -- 0-100, higher = more competitive pricing
    last_rate_change_pct    NUMERIC(8,4),
    last_rate_change_date   DATE,
    filing_frequency_score  NUMERIC(5,2),  -- 0-10, higher = more active filing
    years_active_in_state   INTEGER,
    market_share_estimate   NUMERIC(8,4),  -- estimated market share in this state/line
    source_filing_count     INTEGER DEFAULT 0,
    computed_at             TIMESTAMPTZ DEFAULT NOW(),
    is_current              BOOLEAN DEFAULT TRUE,
    created_at              TIMESTAMPTZ DEFAULT NOW(),
    updated_at              TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(carrier_id, state, line)
);

CREATE INDEX idx_hermes_appetite_carrier ON hermes_appetite_profiles(carrier_id);
CREATE INDEX idx_hermes_appetite_state_line ON hermes_appetite_profiles(state, line);
CREATE INDEX idx_hermes_appetite_score ON hermes_appetite_profiles(appetite_score DESC);
CREATE INDEX idx_hermes_appetite_competitive ON hermes_appetite_profiles(rate_competitiveness_index DESC);
CREATE INDEX idx_hermes_appetite_current ON hermes_appetite_profiles(is_current) WHERE is_current = TRUE;

-- ── Appetite Signals ─────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS hermes_appetite_signals (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    profile_id      UUID NOT NULL REFERENCES hermes_appetite_profiles(id) ON DELETE CASCADE,
    carrier_id      UUID NOT NULL REFERENCES hermes_carriers(id),
    state           VARCHAR(2) NOT NULL,
    line            VARCHAR(100) NOT NULL,
    signal_type     VARCHAR(50) NOT NULL,  -- rate_decrease, rate_increase, new_filing, expanded_classes, contracted_classes, territory_expansion, territory_contraction, new_endorsement, filing_withdrawal, new_state_entry, market_exit
    signal_strength NUMERIC(5,2) DEFAULT 5.0,  -- 1-10 how significant
    signal_date     DATE NOT NULL,
    signal_description TEXT,
    source_filing_id UUID REFERENCES hermes_filings(id),
    confidence      NUMERIC(5,4) DEFAULT 1.0,
    acknowledged    BOOLEAN DEFAULT FALSE,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_hermes_signals_profile ON hermes_appetite_signals(profile_id);
CREATE INDEX idx_hermes_signals_carrier ON hermes_appetite_signals(carrier_id);
CREATE INDEX idx_hermes_signals_state_line ON hermes_appetite_signals(state, line);
CREATE INDEX idx_hermes_signals_type ON hermes_appetite_signals(signal_type);
CREATE INDEX idx_hermes_signals_date ON hermes_appetite_signals(signal_date DESC);
CREATE INDEX idx_hermes_signals_unacked ON hermes_appetite_signals(acknowledged) WHERE acknowledged = FALSE;

-- ── Carrier Rankings ─────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS hermes_carrier_rankings (
    id                      UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    state                   VARCHAR(2) NOT NULL,
    line                    VARCHAR(100) NOT NULL,
    class_code              VARCHAR(20) NOT NULL,
    carrier_id              UUID NOT NULL REFERENCES hermes_carriers(id),
    rank                    INTEGER NOT NULL,
    estimated_premium_index NUMERIC(8,2),  -- relative premium (100 = market average)
    appetite_score          NUMERIC(5,2),
    composite_score         NUMERIC(5,2),  -- blended ranking score
    notes                   TEXT,
    computed_at             TIMESTAMPTZ DEFAULT NOW(),
    updated_at              TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(state, line, class_code, carrier_id)
);

CREATE INDEX idx_hermes_rankings_state_line_class ON hermes_carrier_rankings(state, line, class_code);
CREATE INDEX idx_hermes_rankings_carrier ON hermes_carrier_rankings(carrier_id);
CREATE INDEX idx_hermes_rankings_rank ON hermes_carrier_rankings(state, line, class_code, rank);

-- ── Market Intelligence ──────────────────────────────────────────

CREATE TABLE IF NOT EXISTS hermes_market_intelligence (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    state               VARCHAR(2) NOT NULL,
    line                VARCHAR(100) NOT NULL,
    period_start        DATE NOT NULL,
    period_end          DATE NOT NULL,
    avg_rate_change_pct NUMERIC(8,4),
    median_rate_change_pct NUMERIC(8,4),
    filing_count        INTEGER DEFAULT 0,
    rate_increase_count INTEGER DEFAULT 0,
    rate_decrease_count INTEGER DEFAULT 0,
    new_entrant_count   INTEGER DEFAULT 0,
    withdrawal_count    INTEGER DEFAULT 0,
    new_entrants        JSONB DEFAULT '[]',  -- carrier names entering market
    withdrawals         JSONB DEFAULT '[]',  -- carrier names leaving market
    top_appetite_shifts JSONB DEFAULT '[]',  -- notable appetite changes
    market_trend        VARCHAR(20),  -- hardening, softening, stable, mixed
    summary             TEXT,
    computed_at         TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_hermes_market_state_line ON hermes_market_intelligence(state, line);
CREATE INDEX idx_hermes_market_period ON hermes_market_intelligence(period_start, period_end);
CREATE INDEX idx_hermes_market_trend ON hermes_market_intelligence(market_trend);

-- Triggers
CREATE TRIGGER trg_appetite_updated BEFORE UPDATE ON hermes_appetite_profiles
    FOR EACH ROW EXECUTE FUNCTION hermes_update_timestamp();
CREATE TRIGGER trg_rankings_updated BEFORE UPDATE ON hermes_carrier_rankings
    FOR EACH ROW EXECUTE FUNCTION hermes_update_timestamp();
