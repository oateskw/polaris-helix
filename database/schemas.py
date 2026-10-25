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

CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS helix_semantic_memory (
    id                BIGSERIAL    PRIMARY KEY,
    agent_name        TEXT         NOT NULL,
    campaign_id       TEXT,
    industry          TEXT,
    outcome_type      TEXT         NOT NULL,
    narrative_summary TEXT         NOT NULL,
    embedding         vector(1536),
    metadata          JSONB        NOT NULL DEFAULT '{}'::jsonb,
    created_at        TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS helix_agent_manifests (
    id                  BIGSERIAL    PRIMARY KEY,
    agent_name          TEXT         NOT NULL,
    is_active           BOOLEAN      NOT NULL DEFAULT TRUE,
    optimization_status TEXT         NOT NULL DEFAULT 'IDLE',
    last_optimized_at   TIMESTAMPTZ
);

ALTER TABLE helix_agent_manifests
    ADD COLUMN IF NOT EXISTS purpose           TEXT,
    ADD COLUMN IF NOT EXISTS dependencies      TEXT,
    ADD COLUMN IF NOT EXISTS critical_paths    TEXT,
    ADD COLUMN IF NOT EXISTS target_repository TEXT,
    ADD COLUMN IF NOT EXISTS target_files      TEXT[];

CREATE TABLE IF NOT EXISTS helix_agent_schedule (
    agent_name     TEXT     NOT NULL,
    weekday_int    SMALLINT NOT NULL CHECK (weekday_int BETWEEN 0 AND 6),
    is_enabled     BOOLEAN  NOT NULL DEFAULT TRUE,
    max_daily_runs INTEGER,
    scheduled_time TIME,
    PRIMARY KEY (agent_name, weekday_int)
);

CREATE TABLE IF NOT EXISTS helix_agent_prompts (
    id            SERIAL       PRIMARY KEY,
    agent_name    VARCHAR(50)  NOT NULL,
    prompt_key    VARCHAR(100) NOT NULL,
    system_prompt TEXT         NOT NULL,
    updated_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    CONSTRAINT unique_agent_prompt_key UNIQUE (agent_name, prompt_key)
);

DO $$
BEGIN
    IF to_regclass('helix_semantic_memory') IS NOT NULL THEN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'helix_semantic_memory'
          AND column_name = 'embedding'
          AND udt_name = 'vector'
    ) THEN
        EXECUTE 'CREATE INDEX IF NOT EXISTS idx_hsm_embedding_ivfflat '
            'ON helix_semantic_memory USING ivfflat '
            '(embedding vector_cosine_ops) WITH (lists = 100)';
    END IF;

    EXECUTE 'CREATE INDEX IF NOT EXISTS idx_hsm_agent_created '
        'ON helix_semantic_memory (LOWER(agent_name), created_at DESC)';
    EXECUTE 'CREATE INDEX IF NOT EXISTS idx_hsm_industry_created '
        'ON helix_semantic_memory (LOWER(industry), created_at DESC)';
    EXECUTE 'CREATE INDEX IF NOT EXISTS idx_hsm_outcome_type '
        'ON helix_semantic_memory (LOWER(outcome_type))';
    EXECUTE 'CREATE INDEX IF NOT EXISTS idx_hsm_source_event_id '
        'ON helix_semantic_memory ((metadata->>''source_event_id'')) '
        'WHERE metadata->>''source_event_id'' IS NOT NULL';
    END IF;
END $$;

DO $$
BEGIN
    IF to_regclass('helix_agent_manifests') IS NOT NULL THEN
        ALTER TABLE helix_agent_manifests
            ADD COLUMN IF NOT EXISTS optimization_status TEXT NOT NULL DEFAULT 'IDLE',
            ADD COLUMN IF NOT EXISTS last_optimized_at TIMESTAMPTZ,
            ADD COLUMN IF NOT EXISTS last_optimization_pr_url TEXT;

        UPDATE helix_agent_manifests
        SET optimization_status = 'IDLE'
        WHERE optimization_status IS NULL;

        EXECUTE 'CREATE INDEX IF NOT EXISTS idx_ham_chiron_eligibility '
                'ON helix_agent_manifests '
                '(is_active, optimization_status, last_optimized_at, LOWER(agent_name))';
    END IF;
END $$;

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

ALTER TABLE helix_brain_scheduler ADD COLUMN IF NOT EXISTS campaign_strategy TEXT;

CREATE TABLE IF NOT EXISTS helix_infrastructure_oracle_ledger (
    vendor_name       VARCHAR(50)    PRIMARY KEY,
    credits_remaining NUMERIC(10, 2) NOT NULL DEFAULT 100.00,
    credit_unit       VARCHAR(20)    NOT NULL DEFAULT 'USD',
    is_circuit_broken BOOLEAN        DEFAULT FALSE,
    last_updated_at   TIMESTAMPTZ    NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS helix_outcome_events (
    id                BIGSERIAL      PRIMARY KEY,
    lead_email        TEXT           NOT NULL,
    industry_name     TEXT,
    target_region     TEXT,
    campaign_strategy TEXT,
    stage             TEXT           NOT NULL CHECK (stage IN (
                          'reply', 'positive_reply', 'meeting_booked', 'deal_won',
                          'bounced', 'opt_out', 'blueprint_submitted'
                      )),
    amount            NUMERIC(12, 2),
    source            TEXT           NOT NULL CHECK (source IN (
                          'vanguard_status', 'telegram', 'webhook', 'blueprint'
                      )),
    source_event_id   TEXT           NOT NULL DEFAULT '',
    occurred_at       TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    metadata          JSONB          NOT NULL DEFAULT '{}'::jsonb
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_hoe_dedupe
    ON helix_outcome_events (lead_email, stage, source_event_id);
CREATE INDEX IF NOT EXISTS idx_hoe_industry_time
    ON helix_outcome_events (industry_name, occurred_at DESC);

CREATE TABLE IF NOT EXISTS helix_strategy_proposals (
    id         BIGSERIAL    PRIMARY KEY,
    kind       TEXT         NOT NULL CHECK (kind IN (
                   'adjust_priority', 'pause_industry', 'resume_industry'
               )),
    payload    JSONB        NOT NULL,
    rationale  TEXT         NOT NULL,
    evidence   JSONB        NOT NULL DEFAULT '{}'::jsonb,
    status     TEXT         NOT NULL DEFAULT 'pending' CHECK (status IN (
                   'pending', 'approved', 'rejected', 'applied', 'expired'
               )),
    created_at TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    decided_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_hsp_status_created
    ON helix_strategy_proposals (status, created_at DESC);

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

ALTER TABLE chiron_healing_sessions
    ADD COLUMN IF NOT EXISTS git_branch_name     TEXT,
    ADD COLUMN IF NOT EXISTS target_file_path    TEXT,
    ADD COLUMN IF NOT EXISTS pull_request_number INTEGER,
    ADD COLUMN IF NOT EXISTS pull_request_url    TEXT,
    ADD COLUMN IF NOT EXISTS failure_reason      TEXT,
    ADD COLUMN IF NOT EXISTS updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW();
"""

SEED_SQL = """
-- Vendor names must match the VENDOR_* constants in the code exactly: the circuit
-- matrix is keyed by this column. The NOT EXISTS check is case-insensitive so an
-- older row spelled 'Moltset' is not duplicated.
INSERT INTO helix_infrastructure_oracle_ledger
    (vendor_name, credits_remaining, credit_unit, is_circuit_broken)
SELECT v.vendor_name, v.credits_remaining, v.credit_unit, v.is_circuit_broken
FROM (VALUES
    ('MyEmailVerifier', 100.00, 'CREDITS', FALSE),
    ('PhantomBusters', 1200.00, 'MINUTES', FALSE),
    ('MoltSet', 200.00, 'USD', FALSE),
    ('Anthropic', 100.00, 'USD', FALSE)
) AS v(vendor_name, credits_remaining, credit_unit, is_circuit_broken)
WHERE NOT EXISTS (
    SELECT 1 FROM helix_infrastructure_oracle_ledger l
    WHERE LOWER(l.vendor_name) = LOWER(v.vendor_name)
)
ON CONFLICT (vendor_name) DO NOTHING;
"""
