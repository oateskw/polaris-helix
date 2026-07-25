PROVISION_SQL = """
CREATE TABLE IF NOT EXISTS agent_system_logs (
    id          BIGSERIAL    PRIMARY KEY,
    agent_name  TEXT         NOT NULL CHECK (agent_name IN (
                    'vanguard', 'beacon', 'echo', 'shadow', 'aura', 'prism', 
                    'vector', 'nexus', 'helix', 'pulsar', 'radar', 'nova', 'chiron'
                 )),
    log_level   TEXT         NOT NULL DEFAULT 'INFO'
                             CHECK (log_level IN ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL')),
    message     TEXT         NOT NULL,
    metadata    JSONB,
    timestamp   TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_asl_agent_name  ON agent_system_logs (agent_name);
CREATE INDEX IF NOT EXISTS idx_asl_timestamp   ON agent_system_logs (timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_asl_log_level   ON agent_system_logs (log_level);
CREATE UNIQUE INDEX IF NOT EXISTS idx_asl_event_id
    ON agent_system_logs ((metadata->>'event_id'))
    WHERE metadata->>'event_id' IS NOT NULL;

CREATE TABLE IF NOT EXISTS telegram_processed_updates (
    update_id    BIGINT      PRIMARY KEY,
    command      TEXT        NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_tpu_processed_at
    ON telegram_processed_updates (processed_at DESC);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'error_logs_require_python_traceback'
          AND conrelid = 'agent_system_logs'::regclass
    ) THEN
        ALTER TABLE agent_system_logs
        ADD CONSTRAINT error_logs_require_python_traceback
        CHECK (
            log_level <> 'ERROR'
            OR COALESCE(
                jsonb_typeof(metadata) = 'object'
                AND LENGTH(BTRIM(metadata->>'traceback')) > 0
                AND (metadata->>'traceback') ~ 'File "[^"]+", line [0-9]+',
                FALSE
            )
        ) NOT VALID;
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS cold_leads (
    email       TEXT         PRIMARY KEY,
    first_name  TEXT,
    last_name   TEXT,
    company     TEXT,
    title       TEXT,
    website     TEXT,
    industry    TEXT,
    status      TEXT         NOT NULL DEFAULT 'scraped',
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_cold_leads_industry ON cold_leads (industry);
CREATE INDEX IF NOT EXISTS idx_cold_leads_status   ON cold_leads (status);

CREATE TABLE IF NOT EXISTS helix_inbound_blueprints (
    id                     BIGSERIAL        PRIMARY KEY,
    headcount              INTEGER          NOT NULL,
    weekly_hours_wasted    INTEGER          NOT NULL,
    annual_wasted_hours    INTEGER          NOT NULL,
    annual_financial_bleed DOUBLE PRECISION NOT NULL,
    friction_points        JSONB            NOT NULL,
    name                   TEXT             NOT NULL,
    company_name           TEXT             NOT NULL,
    email                  TEXT             NOT NULL,
    status                 VARCHAR(50)      NOT NULL DEFAULT 'raw_calculation_captured',
    industry_name          VARCHAR(100)     DEFAULT 'Unassigned',
    created_at             TIMESTAMPTZ      NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS helix_brain_scheduler (
    industry_id            SERIAL           PRIMARY KEY,
    industry_name          VARCHAR(100)     UNIQUE NOT NULL,
    moltset_category       VARCHAR(100)     DEFAULT 'Professional and Business Services',
    bottleneck_profile     TEXT             NOT NULL,
    pain_tokens            TEXT[]           NOT NULL,
    target_region          VARCHAR(100)     DEFAULT 'Texas',
    is_active              BOOLEAN          DEFAULT TRUE,
    last_targeted_at       TIMESTAMP        DEFAULT NULL,
    priority_score         INT              DEFAULT 1,
    minimum_cooldown_hours INT              DEFAULT 24,
    assigned_days          INT[]            DEFAULT ARRAY[]::INT[],
    CONSTRAINT valid_assigned_days CHECK (
        assigned_days <@ ARRAY[0,1,2,3,4,5,6] AND
        array_length(assigned_days, 1) IS NULL OR array_length(assigned_days, 1) <= 7
    )
);

CREATE INDEX IF NOT EXISTS idx_hbs_last_targeted ON helix_brain_scheduler (last_targeted_at);
CREATE INDEX IF NOT EXISTS idx_hbs_assigned_days ON helix_brain_scheduler USING GIN (assigned_days);
CREATE INDEX IF NOT EXISTS idx_hbs_active_industries ON helix_brain_scheduler (is_active, industry_id) WHERE is_active = TRUE;

CREATE TABLE IF NOT EXISTS helix_infrastructure_oracle_ledger (
    vendor_name       VARCHAR(50)    PRIMARY KEY,
    credits_remaining NUMERIC(10, 2) NOT NULL DEFAULT 100.00,
    credit_unit       VARCHAR(20)    NOT NULL DEFAULT 'USD',
    is_circuit_broken BOOLEAN        DEFAULT FALSE,
    last_updated_at   TIMESTAMPTZ    NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS chiron_healing_sessions (
    id                 BIGSERIAL    PRIMARY KEY,
    triggering_log_id  BIGINT       NOT NULL,
    target_agent       TEXT         NOT NULL,
    error_message      TEXT         NOT NULL,
    full_traceback     TEXT         NOT NULL,
    status             TEXT         NOT NULL DEFAULT 'triggered',
    created_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    CONSTRAINT unique_trigger_log UNIQUE (triggering_log_id)
);
"""

SEED_SQL = """
INSERT INTO helix_infrastructure_oracle_ledger
    (vendor_name, credits_remaining, credit_unit, is_circuit_broken)
VALUES
    ('MyEmailVerifier', 100.00, 'CREDITS', FALSE),
    ('PhantomBusters', 1200.00, 'MINUTES', FALSE),
    ('Moltset', 200.00, 'USD', FALSE),
    ('Anthropic', 100.00, 'USD', FALSE)
ON CONFLICT (vendor_name) DO NOTHING;
"""
