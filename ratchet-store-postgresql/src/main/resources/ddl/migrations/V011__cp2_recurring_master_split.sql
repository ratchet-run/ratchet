-- V011: CP2 recurring-master split.
-- Moves recurring-master persistence out of scheduler_job into the dedicated
-- scheduler_recurring_job table. Adds scheduler_recurring_job_archive for cancel /
-- exhaust forensics. Drops the rec_status / next_fire / idx_job_recurring_pending shim
-- columns from scheduler_job. Drops the fk_bk_owner_job FK from
-- scheduler_business_key_reservation. Pre-release — no data-bearing environments to
-- migrate.

BEGIN;

-- 1. New definition table.
CREATE TABLE IF NOT EXISTS scheduler_recurring_job
(
    id                    uuid           NOT NULL,
    priority              INT            NOT NULL DEFAULT 2,
    max_retries           INT            NOT NULL DEFAULT 0,
    backoff_policy        TEXT           NOT NULL DEFAULT 'NONE',
    backoff_param_ms      INT            NOT NULL DEFAULT 0,
    timeout_sec           INT            NOT NULL DEFAULT 0,
    cron_expr             VARCHAR(64)    NOT NULL,
    zone_id               VARCHAR(32)    NOT NULL DEFAULT 'UTC',
    next_fire             TIMESTAMPTZ(6) NOT NULL,
    is_paused             BOOLEAN        NOT NULL DEFAULT FALSE,
    paused_at             TIMESTAMPTZ(6),
    payload               JSONB          NOT NULL,
    params                JSONB,
    on_success_payload    JSONB,
    on_failure_payload    JSONB,
    business_key          TEXT,
    resource_name         VARCHAR(100),
    target_class          TEXT GENERATED ALWAYS AS (payload ->> 'target') STORED,
    method_name           TEXT GENERATED ALWAYS AS (payload ->> 'method') STORED,
    created_at            TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    caller_principal      VARCHAR(255),
    CONSTRAINT pk_scheduler_recurring_job PRIMARY KEY (id),
    CONSTRAINT chk_rec_priority CHECK (priority BETWEEN 0 AND 4),
    CONSTRAINT chk_rec_backoff_policy CHECK (backoff_policy IN ('NONE', 'FIXED', 'EXPONENTIAL'))
);

CREATE INDEX IF NOT EXISTS idx_rec_claim
    ON scheduler_recurring_job (next_fire) WHERE is_paused = FALSE;
CREATE INDEX IF NOT EXISTS idx_rec_business_key
    ON scheduler_recurring_job (business_key);
CREATE INDEX IF NOT EXISTS idx_rec_target_class
    ON scheduler_recurring_job (target_class);

-- 2. New archive table.
CREATE TABLE IF NOT EXISTS scheduler_recurring_job_archive
(
    id                    uuid           NOT NULL,
    cron_expr             VARCHAR(64)    NOT NULL,
    zone_id               VARCHAR(32)    NOT NULL,
    payload               JSONB          NOT NULL,
    params                JSONB,
    on_success_payload    JSONB,
    on_failure_payload    JSONB,
    business_key          TEXT,
    created_at            TIMESTAMPTZ(6) NOT NULL,
    caller_principal      VARCHAR(255),
    archived_at           TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    archive_reason        TEXT           NOT NULL,
    CONSTRAINT pk_scheduler_recurring_job_archive PRIMARY KEY (id),
    CONSTRAINT chk_recurring_archive_reason CHECK (archive_reason IN ('CANCELED', 'EXHAUSTED'))
);

CREATE INDEX IF NOT EXISTS idx_archive_rec_business_key
    ON scheduler_recurring_job_archive (business_key);
CREATE INDEX IF NOT EXISTS idx_archive_rec_archived_at
    ON scheduler_recurring_job_archive (archived_at);

-- 3. recurring_master_id + FK on scheduler_job.
ALTER TABLE scheduler_job
    ADD COLUMN IF NOT EXISTS recurring_master_id uuid;
ALTER TABLE scheduler_job
    ADD CONSTRAINT fk_job_recurring_master FOREIGN KEY (recurring_master_id)
        REFERENCES scheduler_recurring_job (id) ON DELETE SET NULL;
CREATE INDEX IF NOT EXISTS idx_job_recurring_master_id
    ON scheduler_job (recurring_master_id);

-- 4. Drop shim columns + recurring claim index.
DROP INDEX IF EXISTS idx_job_recurring_pending;
ALTER TABLE scheduler_job
    DROP CONSTRAINT IF EXISTS chk_rec_status,
    DROP COLUMN IF EXISTS rec_status,
    DROP COLUMN IF EXISTS next_fire;

-- 5. Drop the bkres FK (owner_job_id is polymorphic post-CP2).
ALTER TABLE scheduler_business_key_reservation
    DROP CONSTRAINT IF EXISTS fk_bk_owner_job;

INSERT INTO ratchet_schema_version (version, description)
VALUES ('011', 'CP2 recurring-master split: scheduler_recurring_job + scheduler_recurring_job_archive + recurring_master_id');

COMMIT;
