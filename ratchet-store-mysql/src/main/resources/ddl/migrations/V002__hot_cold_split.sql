-- Ratchet MySQL V002 — hot/cold split.
-- Splits scheduler_job into a cold metadata+terminal table and a new hot scheduler_job_queue
-- for live state; adds scheduler_business_key_reservation for active-key uniqueness.
--
-- Migration order matters: new tables + data backfill happen BEFORE the old columns and
-- indexes are dropped so no writer loses data. Recurring masters stay on scheduler_job and
-- use a transitional rec_status shim; they move to scheduler_recurring_job in a future migration.

-- 1. New hot queue table. Empty at create; backfilled from scheduler_job immediately below.
CREATE TABLE IF NOT EXISTS scheduler_job_queue
(
    job_id             BINARY(16)      NOT NULL,
    status             ENUM ('PENDING','RUNNING','PAUSED')                                                                                NOT NULL DEFAULT 'PENDING',
    job_type           ENUM ('SINGLE','RECURRING','BATCH_PARENT','BATCH_CHILD','CHAIN_STEP','DLQ_ALERT','WORKFLOW_BRANCH','WORKFLOW_JOIN') NOT NULL,
    priority           TINYINT UNSIGNED                                                                                                    NOT NULL DEFAULT 2,
    scheduled_time     DATETIME(6)                                                                                                         NOT NULL,
    business_key       VARCHAR(255)                                                                                                        NULL,
    timeout_sec        INT                                                                                                                 NOT NULL DEFAULT 0,
    max_retries        INT                                                                                                                 NOT NULL DEFAULT 0,
    attempts           INT                                                                                                                 NOT NULL DEFAULT 0,
    picked_by          VARCHAR(64)                                                                                                         NULL,
    picked_at          DATETIME(6)                                                                                                         NULL,
    paused_from_status VARCHAR(20)                                                                                                         NULL,
    last_error         TEXT                                                                                                                NULL,
    version            INT                                                                                                                 NOT NULL DEFAULT 0,
    updated_at         DATETIME(6)                                                                                                         NOT NULL,
    PRIMARY KEY (job_id),
    CONSTRAINT chk_queue_priority CHECK (priority BETWEEN 0 AND 4),
    CONSTRAINT chk_queue_paused_from_status CHECK (paused_from_status IS NULL OR paused_from_status IN ('PENDING','RUNNING','PAUSED')),
    CONSTRAINT fk_job_queue_job FOREIGN KEY (job_id) REFERENCES scheduler_job (job_id) ON DELETE CASCADE,
    INDEX idx_claim_executable (status, job_type, scheduled_time ASC, priority DESC, job_id ASC),
    INDEX idx_queue_orphan (status, picked_at, picked_by)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci;

-- 2. New active-key reservation table. Backfilled from current live rows below.
CREATE TABLE IF NOT EXISTS scheduler_business_key_reservation
(
    business_key VARCHAR(255)                 NOT NULL,
    owner_job_id BINARY(16)                   NOT NULL,
    owner_table  ENUM ('QUEUE','RECURRING')   NOT NULL,
    reserved_at  DATETIME(6)                  NOT NULL,
    PRIMARY KEY (business_key),
    INDEX idx_bk_owner (owner_job_id),
    CONSTRAINT fk_bk_owner_job FOREIGN KEY (owner_job_id) REFERENCES scheduler_job (job_id) ON DELETE CASCADE
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci;

-- 3. Backfill hot queue from executable live rows (non-recurring, non-terminal).
INSERT INTO scheduler_job_queue
    (job_id, status, job_type, priority, scheduled_time, business_key, timeout_sec, max_retries,
     attempts, picked_by, picked_at, paused_from_status, last_error, version, updated_at)
SELECT job_id,
       CAST(status AS CHAR) AS status,
       job_type,
       priority,
       scheduled_time,
       business_key,
       timeout_sec,
       max_retries,
       attempts,
       picked_by,
       picked_at,
       paused_from_status,
       last_error,
       version,
       updated_at
FROM scheduler_job
WHERE status IN ('PENDING', 'RUNNING', 'PAUSED')
  AND job_type <> 'RECURRING';

-- 4. Backfill bkres rows for executable live rows that currently hold an active business key.
INSERT INTO scheduler_business_key_reservation (business_key, owner_job_id, owner_table, reserved_at)
SELECT business_key, job_id, 'QUEUE', COALESCE(updated_at, created_at)
FROM scheduler_job
WHERE business_key IS NOT NULL
  AND status IN ('PENDING', 'RUNNING', 'PAUSED')
  AND job_type <> 'RECURRING';

-- 5. Backfill bkres rows for recurring masters that currently hold an active business key.
INSERT INTO scheduler_business_key_reservation (business_key, owner_job_id, owner_table, reserved_at)
SELECT business_key, job_id, 'RECURRING', COALESCE(updated_at, created_at)
FROM scheduler_job
WHERE business_key IS NOT NULL
  AND status IN ('PENDING', 'RUNNING', 'PAUSED')
  AND job_type = 'RECURRING';

-- 6. Add cold columns (terminal fields + rec_status shim).
ALTER TABLE scheduler_job
    ADD COLUMN terminal_status ENUM ('SUCCEEDED','FAILED','CANCELED') NULL AFTER created_by,
    ADD COLUMN terminal_error  TEXT                                   NULL AFTER terminal_status,
    ADD COLUMN total_attempts  INT                                    NULL AFTER terminal_error,
    ADD COLUMN terminated_at   DATETIME(6)                            NULL AFTER total_attempts,
    ADD COLUMN rec_status      CHAR(1)                                NULL,
    ADD CONSTRAINT chk_rec_status CHECK (rec_status IS NULL OR rec_status IN ('P', 'A'));

-- 7. Populate terminal columns from existing terminal rows.
UPDATE scheduler_job
SET terminal_status = CAST(status AS CHAR),
    terminal_error  = last_error,
    total_attempts  = attempts,
    terminated_at   = COALESCE(execution_end_time, updated_at)
WHERE status IN ('SUCCEEDED', 'FAILED', 'CANCELED');

-- 8. Populate rec_status shim for recurring masters.
UPDATE scheduler_job
SET rec_status = CASE status
                     WHEN 'PENDING' THEN 'P'
                     WHEN 'PAUSED' THEN 'A'
                 END
WHERE job_type = 'RECURRING'
  AND status IN ('PENDING', 'PAUSED');

-- 9. Drop pre-split hot-path indexes and the active_business_key uniqueness.
ALTER TABLE scheduler_job
    DROP INDEX uk_active_business_key,
    DROP INDEX idx_job_poll_composite,
    DROP INDEX idx_job_claim_cover,
    DROP INDEX idx_recurring_due,
    DROP INDEX idx_job_recurring_composite,
    DROP INDEX idx_job_due,
    DROP INDEX idx_job_priority_due,
    DROP INDEX idx_job_picked_by,
    DROP INDEX idx_job_type,
    DROP INDEX idx_job_updated_at;

-- 10. Drop live-state columns (and their constraints) now that data has been migrated.
ALTER TABLE scheduler_job
    DROP CONSTRAINT chk_paused_from_status,
    DROP COLUMN active_business_key,
    DROP COLUMN status,
    DROP COLUMN paused_from_status,
    DROP COLUMN scheduled_time,
    DROP COLUMN attempts,
    DROP COLUMN picked_by,
    DROP COLUMN picked_at,
    DROP COLUMN last_error,
    DROP COLUMN updated_at,
    DROP COLUMN version;

-- 11. Add indexes on the now-cold scheduler_job table.
ALTER TABLE scheduler_job
    ADD INDEX idx_job_terminal (terminal_status, terminated_at),
    ADD INDEX idx_job_recurring_pending (job_type, rec_status, next_fire);

-- 12. Record the migration.
INSERT INTO ratchet_schema_version (version, description)
VALUES ('002', 'Hot/cold store split: scheduler_job_queue + scheduler_business_key_reservation + cold terminal fields');
