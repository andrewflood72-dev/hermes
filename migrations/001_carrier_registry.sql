-- Hermes Migration 001: Carrier Registry
-- Master carrier records, licenses, and contacts

-- Ensure extensions exist (Atlas should have created these, but be safe)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- ── Carriers ─────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS hermes_carriers (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    naic_code       VARCHAR(10) NOT NULL UNIQUE,
    legal_name      VARCHAR(500) NOT NULL,
    group_name      VARCHAR(500),
    group_naic_code VARCHAR(10),
    am_best_rating  VARCHAR(10),
    am_best_outlook VARCHAR(20),
    sp_rating       VARCHAR(10),
    treasury_570_listed BOOLEAN DEFAULT FALSE,
    domicile_state  VARCHAR(2),
    company_type    VARCHAR(50),  -- stock, mutual, reciprocal, lloyds, RRG
    direct_written_premium NUMERIC(15,2),  -- most recent annual DWP
    commercial_lines_pct   NUMERIC(5,2),   -- % of book that is commercial
    website         VARCHAR(500),
    status          VARCHAR(20) DEFAULT 'active',  -- active, inactive, runoff
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_hermes_carriers_naic ON hermes_carriers(naic_code);
CREATE INDEX idx_hermes_carriers_name_trgm ON hermes_carriers USING gin(legal_name gin_trgm_ops);
CREATE INDEX idx_hermes_carriers_group ON hermes_carriers(group_naic_code);
CREATE INDEX idx_hermes_carriers_domicile ON hermes_carriers(domicile_state);
CREATE INDEX idx_hermes_carriers_status ON hermes_carriers(status);

-- ── Carrier Licenses ─────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS hermes_carrier_licenses (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    carrier_id      UUID NOT NULL REFERENCES hermes_carriers(id) ON DELETE CASCADE,
    state           VARCHAR(2) NOT NULL,
    license_number  VARCHAR(50),
    license_status  VARCHAR(20) NOT NULL DEFAULT 'active',  -- active, suspended, revoked
    lines_authorized TEXT[],  -- array of authorized lines
    surplus_lines_flag BOOLEAN DEFAULT FALSE,
    admitted_flag    BOOLEAN DEFAULT TRUE,
    effective_date  DATE,
    expiration_date DATE,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(carrier_id, state)
);

CREATE INDEX idx_hermes_licenses_carrier ON hermes_carrier_licenses(carrier_id);
CREATE INDEX idx_hermes_licenses_state ON hermes_carrier_licenses(state);
CREATE INDEX idx_hermes_licenses_status ON hermes_carrier_licenses(license_status);

-- ── Carrier Contacts ─────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS hermes_carrier_contacts (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    carrier_id      UUID NOT NULL REFERENCES hermes_carriers(id) ON DELETE CASCADE,
    role            VARCHAR(50) NOT NULL,  -- underwriter, claims, marketing, actuarial
    name            VARCHAR(200),
    title           VARCHAR(200),
    email           VARCHAR(200),
    phone           VARCHAR(50),
    territory       TEXT[],  -- states or regions covered
    department      VARCHAR(200),
    notes           TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_hermes_contacts_carrier ON hermes_carrier_contacts(carrier_id);
CREATE INDEX idx_hermes_contacts_role ON hermes_carrier_contacts(role);

-- Update trigger
CREATE OR REPLACE FUNCTION hermes_update_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_carriers_updated BEFORE UPDATE ON hermes_carriers
    FOR EACH ROW EXECUTE FUNCTION hermes_update_timestamp();
CREATE TRIGGER trg_licenses_updated BEFORE UPDATE ON hermes_carrier_licenses
    FOR EACH ROW EXECUTE FUNCTION hermes_update_timestamp();
CREATE TRIGGER trg_contacts_updated BEFORE UPDATE ON hermes_carrier_contacts
    FOR EACH ROW EXECUTE FUNCTION hermes_update_timestamp();
