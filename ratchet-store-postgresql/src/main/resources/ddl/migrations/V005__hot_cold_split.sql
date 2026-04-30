-- Ratchet PostgreSQL V005 — hot/cold store split.
--
-- Mirrors MySQL V002: scheduler_job becomes cold metadata +
-- terminal state; scheduler_job_queue holds live state. Recurring masters stay on the
-- cold table and use a rec_status CHAR(1) shim ('P' = active, 'A' = paused).
--
-- Pre-release migration: no production data. Existing dev/test data does not need to
-- be preserved — operators rebuild from postgresql-schema.sql.
--
-- Final shape (post-V005):
--   scheduler_job (COLD): job-shape immutables + payload + observability + terminal_*
--                         + rec_status. NO status / scheduled_time / attempts /
--                         picked_* / paused_from_status / last_error / updated_at /
--                         version columns — those moved to scheduler_job_queue.
--   scheduler_job_queue (HOT): live state for executable jobs. Row exists iff the
--                              job is PENDING/RUNNING/PAUSED. Deleted on terminal
--                              transition. Duplicates immutable claim fields
--                              (job_type, priority, business_key, timeout_sec,
--                              max_retries) so claimNextBatchOptimized populates the
--                              JobClaimDto from a single RETURNING clause.
--
-- last_error lives on scheduler_job_queue. At every terminal transition, the
-- lifecycle code copies last_error → terminal_error on cold BEFORE deleting the
-- queue row. terminal_error is the cold survivor of last_error.

-- 1. Hot queue table.
CREATE TABLE IF NOT EXISTS scheduler_job_queue
(
    job_id             uuid         NOT NULL,
    status             TEXT         NOT NULL DEFAULT 'PENDING',
    job_type           TEXT         NOT NULL,
    priority           INT          NOT NULL DEFAULT 2,
    scheduled_time     TIMESTAMPTZ(6) NOT NULL,
    business_key       TEXT,
    timeout_sec        INT          NOT NULL DEFAULT 0,
    max_retries        INT          NOT NULL DEFAULT 0,
    attempts           INT          NOT NULL DEFAULT 0,
    picked_by          VARCHAR(64),
    picked_at          TIMESTAMPTZ(6),
    paused_from_status TEXT,
    last_error         TEXT,
    version            INT          NOT NULL DEFAULT 0,
    updated_at         TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_scheduler_job_queue PRIMARY KEY (job_id),
    CONSTRAINT chk_queue_status CHECK (status IN ('PENDING', 'RUNNING', 'PAUSED')),
    CONSTRAINT chk_queue_job_type CHECK (job_type IN
                                         ('SINGLE', 'RECURRING', 'BATCH_PARENT', 'BATCH_CHILD',
                                          'CHAIN_STEP', 'DLQ_ALERT', 'WORKFLOW_BRANCH', 'WORKFLOW_JOIN')),
    CONSTRAINT chk_queue_priority CHECK (priority BETWEEN 0 AND 4),
    CONSTRAINT chk_queue_paused_from_status CHECK (paused_from_status IS NULL OR paused_from_status IN ('PENDING', 'RUNNING', 'PAUSED')),
    CONSTRAINT fk_job_queue_job FOREIGN KEY (job_id) REFERENCES scheduler_job (job_id) ON DELETE CASCADE
);

-- Hot-path claim index. Partial on PENDING — RUNNING and PAUSED rows are not claim
-- candidates and inflate write amplification if covered.
CREATE INDEX IF NOT EXISTS idx_claim_executable
    ON scheduler_job_queue (job_type, scheduled_time ASC, priority DESC, job_id ASC)
    WHERE status = 'PENDING';

-- Orphan-detection scan (resetOrphanJobs / resetOrphanJobsForNode).
CREATE INDEX IF NOT EXISTS idx_queue_orphan
    ON scheduler_job_queue (status, picked_at, picked_by);

-- 2. Cold columns on scheduler_job.
ALTER TABLE scheduler_job
    ADD COLUMN IF NOT EXISTS terminal_status TEXT,
    ADD COLUMN IF NOT EXISTS terminal_error  TEXT,
    ADD COLUMN IF NOT EXISTS total_attempts  INT,
    ADD COLUMN IF NOT EXISTS terminated_at   TIMESTAMPTZ(6),
    ADD COLUMN IF NOT EXISTS rec_status      CHAR(1);

ALTER TABLE scheduler_job
    DROP CONSTRAINT IF EXISTS chk_terminal_status;
ALTER TABLE scheduler_job
    ADD CONSTRAINT chk_terminal_status
    CHECK (terminal_status IS NULL OR terminal_status IN ('SUCCEEDED', 'FAILED', 'CANCELED'));

ALTER TABLE scheduler_job
    DROP CONSTRAINT IF EXISTS chk_rec_status;
ALTER TABLE scheduler_job
    ADD CONSTRAINT chk_rec_status
    CHECK (rec_status IS NULL OR rec_status IN ('P', 'A'));

-- 3. Drop hot-path indexes that are now obsolete.
DROP INDEX IF EXISTS idx_job_poll_composite;
DROP INDEX IF EXISTS idx_job_claim_cover;
DROP INDEX IF EXISTS idx_recurring_due;
DROP INDEX IF EXISTS idx_job_recurring_composite;
DROP INDEX IF EXISTS idx_job_due;
DROP INDEX IF EXISTS idx_job_priority_due;
DROP INDEX IF EXISTS idx_job_picked_by;
DROP INDEX IF EXISTS idx_job_updated_at;

-- 4. Drop hot columns. Constraints first.
ALTER TABLE scheduler_job
    DROP CONSTRAINT IF EXISTS chk_job_status,
    DROP CONSTRAINT IF EXISTS chk_paused_from_status;

ALTER TABLE scheduler_job
    DROP COLUMN IF EXISTS status,
    DROP COLUMN IF EXISTS paused_from_status,
    DROP COLUMN IF EXISTS scheduled_time,
    DROP COLUMN IF EXISTS attempts,
    DROP COLUMN IF EXISTS picked_by,
    DROP COLUMN IF EXISTS picked_at,
    DROP COLUMN IF EXISTS last_error,
    DROP COLUMN IF EXISTS updated_at,
    DROP COLUMN IF EXISTS version;

-- 5. Cold-table indexes for the post-split workload.
-- Archival / deleteDlqOlderThan scan.
CREATE INDEX IF NOT EXISTS idx_job_terminal
    ON scheduler_job (terminal_status, terminated_at);

-- Transitional: recurring-master claim. Dropped with rec_status when
-- recurring masters move to scheduler_recurring_job.
CREATE INDEX IF NOT EXISTS idx_job_recurring_pending
    ON scheduler_job (job_type, rec_status, next_fire);

-- 6. Record this migration.
INSERT INTO ratchet_schema_version (version, description)
VALUES ('005', 'Hot/cold store split: scheduler_job_queue + cold terminal_* + rec_status shim')
ON CONFLICT (version) DO NOTHING;
