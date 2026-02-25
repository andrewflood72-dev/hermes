-- Migration 010: MGA Proposal Agent
-- Stores AI-generated MGA business proposals

CREATE TABLE IF NOT EXISTS hermes_mga_proposals (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    program_type    VARCHAR(50)  NOT NULL,          -- e.g. 'pmi', 'commercial', 'specialty'
    title           TEXT         NOT NULL,
    request_data    JSONB        NOT NULL DEFAULT '{}',
    proposal_data   JSONB        NOT NULL DEFAULT '{}',
    status          VARCHAR(20)  NOT NULL DEFAULT 'draft',  -- draft, generating, complete, failed
    token_usage     JSONB        NOT NULL DEFAULT '{}',
    generated_by    VARCHAR(100),
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- Index for listing/filtering
CREATE INDEX IF NOT EXISTS idx_mga_proposals_program_type ON hermes_mga_proposals (program_type);
CREATE INDEX IF NOT EXISTS idx_mga_proposals_status       ON hermes_mga_proposals (status);
CREATE INDEX IF NOT EXISTS idx_mga_proposals_created      ON hermes_mga_proposals (created_at DESC);

-- Auto-update trigger
CREATE OR REPLACE TRIGGER trg_mga_proposals_updated
    BEFORE UPDATE ON hermes_mga_proposals
    FOR EACH ROW EXECUTE FUNCTION hermes_update_timestamp();
