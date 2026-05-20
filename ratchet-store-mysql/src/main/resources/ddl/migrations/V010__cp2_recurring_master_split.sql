-- V010: CP2 recurring-master split.
-- Moves recurring-master persistence out of scheduler_job into the dedicated
-- scheduler_recurring_job table. Adds scheduler_recurring_job_archive for cancel /
-- exhaust forensics. Drops the rec_status / next_fire / idx_job_recurring_pending shim
-- columns from scheduler_job. Drops the fk_bk_owner_job FK from
-- scheduler_business_key_reservation (owner_job_id is now polymorphic). Pre-release —
-- no data-bearing environments to migrate.
--
-- Requires MySQL 8.0.19 or later. The migration uses the unified ALTER TABLE ... DROP
-- CONSTRAINT syntax for FOREIGN KEY and CHECK constraints, which was added in 8.0.19.
-- Earlier 8.0.x releases would need DROP FOREIGN KEY / DROP CHECK in separate ALTER
-- TABLE statements. Ratchet's IT matrix exercises 8.0.32+, MariaDB 10.6+.

START TRANSACTION;

-- 1. New definition table.
CREATE TABLE IF NOT EXISTS scheduler_recurring_job
(
    id                    BINARY(16)                                                        NOT NULL,
    priority              TINYINT UNSIGNED                                                  NOT NULL DEFAULT 2,
    max_retries           INT                                                               NOT NULL DEFAULT 0,
    backoff_policy        ENUM ('NONE','FIXED','EXPONENTIAL')                               NOT NULL DEFAULT 'NONE',
    backoff_param_ms      INT                                                               NOT NULL DEFAULT 0,
    timeout_sec           INT                                                               NOT NULL DEFAULT 0,
    cron_expr             VARCHAR(64)                                                       NOT NULL,
    zone_id               VARCHAR(32)                                                       NOT NULL DEFAULT 'UTC',
    next_fire             DATETIME(6)                                                       NOT NULL,
    is_paused             BOOLEAN                                                           NOT NULL DEFAULT FALSE,
    paused_at             DATETIME(6)                                                       NULL,
    payload               JSON                                                              NOT NULL,
    params                JSON                                                              NULL,
    on_success_payload    JSON                                                              NULL,
    on_failure_payload    JSON                                                              NULL,
    business_key          VARCHAR(255)                                                      NULL,
    resource_name         VARCHAR(100)                                                      NULL,
    target_class          VARCHAR(255) GENERATED ALWAYS AS (JSON_UNQUOTE(JSON_EXTRACT(payload, '$.target'))) STORED,
    method_name           VARCHAR(128) GENERATED ALWAYS AS (JSON_UNQUOTE(JSON_EXTRACT(payload, '$.method'))) STORED,
    created_at            DATETIME(6)                                                       NOT NULL,
    caller_principal      VARCHAR(255)                                                      NULL,
    PRIMARY KEY (id),
    CONSTRAINT chk_rec_priority CHECK (priority BETWEEN 0 AND 4),
    INDEX idx_rec_claim (is_paused, next_fire),
    INDEX idx_rec_business_key (business_key),
    INDEX idx_rec_target_class (target_class)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci;

-- 2. New archive table.
CREATE TABLE IF NOT EXISTS scheduler_recurring_job_archive
(
    id                    BINARY(16)                            NOT NULL,
    cron_expr             VARCHAR(64)                           NOT NULL,
    zone_id               VARCHAR(32)                           NOT NULL,
    payload               JSON                                  NOT NULL,
    params                JSON                                  NULL,
    on_success_payload    JSON                                  NULL,
    on_failure_payload    JSON                                  NULL,
    business_key          VARCHAR(255)                          NULL,
    created_at            DATETIME(6)                           NOT NULL,
    caller_principal      VARCHAR(255)                          NULL,
    archived_at           DATETIME(6)                           NOT NULL,
    archive_reason        ENUM ('CANCELED','EXHAUSTED')         NOT NULL,
    PRIMARY KEY (id),
    INDEX idx_archive_rec_business_key (business_key),
    INDEX idx_archive_rec_archived_at (archived_at)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci;

-- 3. Add recurring_master_id + FK + index on scheduler_job.
ALTER TABLE scheduler_job
    ADD COLUMN recurring_master_id BINARY(16) NULL,
    ADD INDEX idx_job_recurring_master_id (recurring_master_id),
    ADD CONSTRAINT fk_job_recurring_master FOREIGN KEY (recurring_master_id)
        REFERENCES scheduler_recurring_job (id) ON DELETE SET NULL;

-- 4. Drop the rec_status / next_fire shim + recurring claim index from scheduler_job.
ALTER TABLE scheduler_job
    DROP INDEX idx_job_recurring_pending,
    DROP CONSTRAINT chk_rec_status,
    DROP COLUMN rec_status,
    DROP COLUMN next_fire;

-- 5. Drop the bkres FK (owner_job_id is now polymorphic across two tables).
ALTER TABLE scheduler_business_key_reservation
    DROP CONSTRAINT fk_bk_owner_job;

-- 5b. Drop the job_tag FK — job_id is now polymorphic (annotation-registered recurring masters
-- carry tags whose job_id refers to scheduler_recurring_job, not scheduler_job).
ALTER TABLE scheduler_job_tag
    DROP CONSTRAINT fk_job_tag_job;

-- 6. Record version.
INSERT IGNORE INTO ratchet_schema_version (version, description) VALUES
    ('010', 'CP2 recurring-master split: scheduler_recurring_job + scheduler_recurring_job_archive + recurring_master_id');

COMMIT;
