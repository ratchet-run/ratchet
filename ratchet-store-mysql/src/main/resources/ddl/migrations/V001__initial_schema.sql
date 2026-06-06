-- Ratchet MySQL V001 — squashed baseline schema.
--
-- This file is the single baseline for the Ratchet MySQL schema. It mirrors the
-- canonical end-state in ddl/mysql-schema.sql. Prior intermediate migrations
-- (the historical V001 through V010 chain) were squashed before any production
-- installs existed. Future schema changes ship as incremental migrations layered
-- on top of this baseline.
--
-- IMPORTANT: configure READ COMMITTED isolation — see IsolationCheck.java.

-- 0. Schema version ledger (used by external migration tooling or the opt-in Ratchet migrator)
CREATE TABLE IF NOT EXISTS ratchet_schema_version
(
    version     VARCHAR(20)  NOT NULL,
    applied_at  DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    description VARCHAR(200) NOT NULL,
    checksum    VARCHAR(64)  NULL,
    PRIMARY KEY (version)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci;

-- 1. Cluster nodes
CREATE TABLE IF NOT EXISTS scheduler_node
(
    node_id      VARCHAR(64) NOT NULL,
    heartbeat_ts DATETIME(6) NOT NULL,
    started_at   DATETIME(6) NOT NULL,
    node_info    TEXT        NULL,
    PRIMARY KEY (node_id),
    INDEX idx_node_heartbeat (heartbeat_ts)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci;

-- 2. Distributed locks
CREATE TABLE IF NOT EXISTS scheduler_lock
(
    lock_name  VARCHAR(128) NOT NULL,
    owner_node VARCHAR(64)  NOT NULL,
    locked_at  DATETIME(6)  NOT NULL,
    expires_at DATETIME(6)  NOT NULL,
    PRIMARY KEY (lock_name),
    INDEX idx_lock_expires (expires_at)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci;

-- 3. Resource concurrency config
CREATE TABLE IF NOT EXISTS scheduler_resource_limit
(
    resource_name  VARCHAR(100) NOT NULL,
    max_concurrent INT          NOT NULL,
    retry_delay_ms INT          NOT NULL DEFAULT 5000,
    description    VARCHAR(255) NULL,
    created_at     DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at     DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (resource_name)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci;

-- 3a. Recurring job masters.
-- Long-lived definition rows holding the cron schedule, payload template, and runtime anchor
-- (next_fire) for each repeating job. A recurring master is never executed itself — it spawns
-- child rows on scheduler_job at fire time. Pause / resume / cancel use single-table primitives.
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
    on_success_payload    JSON                                                              NULL,
    on_failure_payload    JSON                                                              NULL,
    business_key          VARCHAR(255)                                                      NULL,
    resource_name         VARCHAR(100)                                                      NULL,
    execution_target      VARCHAR(64)                                                       NULL,
    target_class          VARCHAR(255) GENERATED ALWAYS AS (JSON_UNQUOTE(JSON_EXTRACT(payload, '$.target'))) STORED,
    method_name           VARCHAR(128) GENERATED ALWAYS AS (JSON_UNQUOTE(JSON_EXTRACT(payload, '$.method'))) STORED,
    created_at            DATETIME(6)                                                       NOT NULL,
    caller_principal      VARCHAR(255)                                                      NULL,
    encrypted_payload     BOOLEAN                                                           NOT NULL DEFAULT FALSE,
    PRIMARY KEY (id),
    CONSTRAINT chk_rec_priority CHECK (priority BETWEEN 0 AND 4),
    -- Claim index: filter unpaused rows first, then order by next_fire.
    INDEX idx_rec_claim (is_paused, next_fire),
    INDEX idx_rec_business_key (business_key),
    INDEX idx_rec_target_class (target_class)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci;

-- 3b. Archived recurring definitions.
-- Denormalized snapshot written atomically in the same transaction as the DELETE from
-- scheduler_recurring_job. CANCELED rows are explicit cancels (admin command or annotation
-- orphan cleanup); EXHAUSTED rows are cron schedules that yielded no next fire.
-- No FK back to the live table — by construction the live row is gone when this row is written.
CREATE TABLE IF NOT EXISTS scheduler_recurring_job_archive
(
    id                    BINARY(16)                            NOT NULL,
    cron_expr             VARCHAR(64)                           NOT NULL,
    zone_id               VARCHAR(32)                           NOT NULL,
    payload               JSON                                  NOT NULL,
    on_success_payload    JSON                                  NULL,
    on_failure_payload    JSON                                  NULL,
    business_key          VARCHAR(255)                          NULL,
    execution_target      VARCHAR(64)                           NULL,
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

-- 4. Main job table — COLD metadata + terminal fields.
-- Live queue state (status, scheduled_time, picked_*, attempts, version, last_error,
-- paused_from_status, updated_at) lives on scheduler_job_queue instead. See
-- "hot/cold queue-state refactor" plan. Immutable job-shape fields are duplicated on
-- scheduler_job_queue for the claim-path DTO — no mutation path writes them in both places.
CREATE TABLE IF NOT EXISTS scheduler_job
(
    job_id                BINARY(16)      NOT NULL,
    -- Immutable job-shape fields (duplicated on scheduler_job_queue per §duplication rule).
    job_type              ENUM ('SINGLE','RECURRING','BATCH_PARENT','BATCH_CHILD','CHAIN_STEP','DLQ_ALERT','WORKFLOW_BRANCH','WORKFLOW_JOIN') NOT NULL,
    priority              TINYINT UNSIGNED                                                                                                    NOT NULL DEFAULT 2,
    max_retries           INT                                                                                                                 NOT NULL DEFAULT 0,
    backoff_policy        ENUM ('NONE','FIXED','EXPONENTIAL')                                                                                 NOT NULL DEFAULT 'NONE',
    backoff_param_ms      INT                                                                                                                 NOT NULL DEFAULT 0,
    timeout_sec           INT                                                                                                                 NOT NULL DEFAULT 0,
    cron_expr             VARCHAR(64)                                                                                                         NOT NULL DEFAULT '',
    zone_id               VARCHAR(32)                                                                                                         NOT NULL DEFAULT 'UTC',
    -- Payload + params (insert-once; never mutated after enqueue).
    payload               JSON                                                                                                                NOT NULL,
    params                JSON                                                                                                                NULL,
    -- W3C TraceContext carrier captured at enqueue time; passed to TracingCollector at execution
    -- start so distributed spans are parented to the submitting caller's trace.
    trace_context         JSON                                                                                                                NULL,
    -- Generated column extracting the W3C traceparent header for indexed traceCorrelationId lookups.
    -- TracingCollector SPI contract requires the key 'traceparent' in the flat-map.
    trace_id_extracted    VARCHAR(55) GENERATED ALWAYS AS (JSON_UNQUOTE(JSON_EXTRACT(trace_context, '$.traceparent'))) STORED NULL,
    target_class          VARCHAR(255) GENERATED ALWAYS AS (JSON_UNQUOTE(JSON_EXTRACT(payload, '$.target'))) STORED,
    method_name           VARCHAR(128) GENERATED ALWAYS AS (JSON_UNQUOTE(JSON_EXTRACT(payload, '$.method'))) STORED,
    idempotency_key       VARCHAR(36)                                                                                                         NOT NULL,
    -- business_key is immutable after enqueue; active-uniqueness owned by
    -- scheduler_business_key_reservation (not a UNIQUE KEY here anymore).
    business_key          VARCHAR(255)                                                                                                        NULL,
    resource_name         VARCHAR(100)                                                                                                        NULL,
    -- Immutable routing label: which configured executor pool runs the job. NULL = inherit the
    -- deployment default. Denormalized onto scheduler_job_queue for the claim projection. Reserved
    -- values 'platform'/'virtual' today; stored as a string so future named pools need no migration.
    execution_target      VARCHAR(64)                                                                                                         NULL,
    on_success_payload    JSON                                                                                                                NULL,
    on_failure_payload    JSON                                                                                                                NULL,
    depends_on            BINARY(16)                                                                                                          NULL,
    superseded_by         BINARY(16)                                                                                                          NULL,
    created_at            DATETIME(6)                                                                                                         NOT NULL,
    -- Captured at creation from jakarta.security.enterprise.SecurityContext when resolvable; null
    -- otherwise. No enforcement performed — downstream consumers read this field for audit or to
    -- build their own authorization layer. See JobSchedulerService Javadoc.
    caller_principal      VARCHAR(255)                                                                                                        NULL,
    -- Terminal fields — NULL while live; set exactly once at terminal transition; only
    -- cleared by resetFailedToPending. Archival / deleteDlqOlderThan use terminated_at.
    terminal_status       ENUM ('SUCCEEDED','FAILED','CANCELED')                                                                              NULL,
    terminal_error        TEXT                                                                                                                NULL,
    total_attempts        INT                                                                                                                 NULL,
    terminated_at         DATETIME(6)                                                                                                         NULL,
    execution_start_time  DATETIME(6)                                                                                                         NULL,
    execution_end_time    DATETIME(6)                                                                                                         NULL,
    execution_duration_ms BIGINT                                                                                                              NULL,
    queue_wait_ms         BIGINT                                                                                                              NULL,
    job_result            JSON                                                                                                                NULL,
    result_type           VARCHAR(100)                                                                                                        NULL,
    -- Recurring-child lineage pointer. Set for child rows spawned by a recurring master; NULL
    -- for all other rows. depends_on is reserved for chain / batch / workflow same-table parent
    -- pointers. ON DELETE SET NULL so cancel of the master does not cascade-delete in-flight
    -- children.
    recurring_master_id   BINARY(16)                                                                                                          NULL,
    -- Per-row payload-encryption metadata (cleartext by design). encrypted_payload marks whether
    -- this row's protected surfaces are ciphertext; encryption_key_id records the key the
    -- creation-time payload was written under, so a key cannot be retired while a live row still
    -- references it.
    encrypted_payload     BOOLEAN                                                                                                             NOT NULL DEFAULT FALSE,
    encryption_key_id     VARCHAR(256)                                                                                                        NULL,
    PRIMARY KEY (job_id),
    UNIQUE KEY uk_idempotency_key (idempotency_key),
    CONSTRAINT chk_job_priority CHECK (priority BETWEEN 0 AND 4),
    CONSTRAINT fk_job_recurring_master FOREIGN KEY (recurring_master_id) REFERENCES scheduler_recurring_job (id) ON DELETE SET NULL,
    -- Lookup/relationship indexes.
    INDEX idx_job_depends_on (depends_on),
    INDEX idx_job_superseded_by (superseded_by),
    INDEX idx_job_recurring_master_id (recurring_master_id),
    -- Indexed for the key-rotation drain check (SELECT ... WHERE encryption_key_id = ? AND status...).
    INDEX idx_job_encryption_key_id (encryption_key_id),
    -- business_key is observability-only here; uniqueness is in scheduler_business_key_reservation.
    INDEX idx_job_business_key (business_key),
    -- Audit / archival indexes.
    INDEX idx_job_created_at (created_at),
    -- Archival / deleteDlqOlderThan scan (terminal_status, terminated_at).
    INDEX idx_job_terminal (terminal_status, terminated_at)
    -- Optional companion: ddl/mysql-debug-indexes.sql adds debug-only indexes
    -- (idx_target_class, idx_method_name) that are not on the hot insert path.
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci;

-- 4a. Hot authoritative queue state for executable jobs.
-- Row exists iff the job is live (PENDING / RUNNING / PAUSED / WAITING). DELETED at terminal. All
-- claim, pickup, retry, orphan, pause, resume reads and writes target this table. Immutable
-- job-shape fields (job_type, priority, business_key, timeout_sec, max_retries) are
-- denormalized from scheduler_job for single-table claim DTO population — they are set at
-- enqueue and never mutated.
CREATE TABLE IF NOT EXISTS scheduler_job_queue
(
    job_id             BINARY(16)      NOT NULL,
    status             ENUM ('PENDING','RUNNING','PAUSED','WAITING')                                                                      NOT NULL DEFAULT 'PENDING',
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
    signal_key              VARCHAR(255)                                                                                                   NULL,
    signal_timeout          DATETIME(3)                                                                                                    NULL,
    signal_payload          TEXT                                                                                                           NULL,
    signal_payload_type     VARCHAR(16)                                                                                                    NULL,
    signal_outcome          VARCHAR(32)                                                                                                    NULL,
    signal_rejection_reason TEXT                                                                                                           NULL,
    signal_delivered_at     DATETIME(3)                                                                                                    NULL,
    signal_delivered_by     VARCHAR(255)                                                                                                   NULL,
    signal_delivery_id      VARCHAR(36)                                                                                                    NULL,
    -- Denormalized from scheduler_job: claim-time routing label read into JobClaimDto.
    execution_target        VARCHAR(64)                                                                                                    NULL,
    PRIMARY KEY (job_id),
    CONSTRAINT chk_queue_priority CHECK (priority BETWEEN 0 AND 4),
    CONSTRAINT chk_queue_paused_from_status CHECK (paused_from_status IS NULL OR paused_from_status IN ('PENDING','RUNNING','PAUSED')),
    CONSTRAINT fk_job_queue_job FOREIGN KEY (job_id) REFERENCES scheduler_job (job_id) ON DELETE CASCADE,
    -- Executable claim index: filter to due rows first; computed age-boost ordering is sorted
    -- after the index scan.
    INDEX idx_claim_executable (status, job_type, scheduled_time ASC, priority DESC, job_id ASC),
    -- Orphan scan: status='RUNNING' AND picked_at < :cutoff AND picked_by NOT IN (alive).
    INDEX idx_queue_orphan (status, picked_at, picked_by),
    INDEX idx_signal_key_status (signal_key, status),
    INDEX idx_signal_timeout_status (status, signal_timeout),
    INDEX idx_signal_delivery_id (signal_delivery_id)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci;

-- 4b. Business-key active-uniqueness reservation table.
-- Authoritative ownership lookup for active business keys across both the executable queue
-- (owner_table='QUEUE') and recurring masters (owner_table='RECURRING'). INSERTed in the
-- same transaction as the live-row insert; DELETEd in the same transaction as the terminal
-- hot DELETE or recurring cancel. The unique key on business_key serializes concurrent
-- enqueues — a duplicate insert triggers a unique violation translated to the existing
-- DuplicateBusinessKey error path, replacing the race-prone UNION SELECT check.
CREATE TABLE IF NOT EXISTS scheduler_business_key_reservation
(
    business_key VARCHAR(255)                 NOT NULL,
    owner_job_id BINARY(16)                   NOT NULL,
    owner_table  ENUM ('QUEUE','RECURRING')   NOT NULL,
    reserved_at  DATETIME(6)                  NOT NULL,
    PRIMARY KEY (business_key),
    INDEX idx_bk_owner (owner_job_id)
    -- owner_job_id is polymorphic across scheduler_job (QUEUE owner) and
    -- scheduler_recurring_job (RECURRING owner); the FK is dropped at the application layer
    -- because no single parent table exists. Cancel paths DELETE the reservation rows.
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci;

-- 5. Job tags (composite PK)
CREATE TABLE IF NOT EXISTS scheduler_job_tag
(
    job_id BINARY(16)      NOT NULL,
    tag    VARCHAR(64)     NOT NULL,
    PRIMARY KEY (job_id, tag),
    INDEX idx_job_tag_tag_job (tag, job_id)
    -- No fk_job_tag_job. job_id is polymorphic (executable jobs → scheduler_job, recurring
    -- masters → scheduler_recurring_job). Cancel paths DELETE associated tag rows explicitly;
    -- the @Recurring registration path writes tags whose job_id refers to
    -- scheduler_recurring_job, which would violate any single-FK constraint.
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci;

-- 6. Batch progress
CREATE TABLE IF NOT EXISTS scheduler_batch
(
    batch_id             BINARY(16)      NOT NULL,
    total_items          INT             NOT NULL DEFAULT 0,
    completed_items      INT             NOT NULL DEFAULT 0,
    failed_items         INT             NOT NULL DEFAULT 0,
    completion_processed TINYINT(1)      NOT NULL DEFAULT 0,
    version              INT             NOT NULL DEFAULT 0,
    progress_hook        JSON            NULL,
    PRIMARY KEY (batch_id),
    CONSTRAINT fk_batch_job FOREIGN KEY (batch_id) REFERENCES scheduler_job (job_id) ON DELETE CASCADE
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci;

-- 7. Batch performance metrics
CREATE TABLE IF NOT EXISTS scheduler_batch_metrics
(
    batch_id           BINARY(16)      NOT NULL,
    total_duration_ms  BIGINT          NULL,
    child_execution_ms BIGINT          NULL,
    overhead_ms        BIGINT          NULL,
    child_count        INT             NOT NULL DEFAULT 0,
    success_count      INT             NOT NULL DEFAULT 0,
    failure_count      INT             NOT NULL DEFAULT 0,
    started_at         DATETIME(6)     NULL,
    completed_at       DATETIME(6)     NULL,
    version            INT             NOT NULL DEFAULT 0,
    PRIMARY KEY (batch_id),
    CONSTRAINT fk_batch_metrics_job FOREIGN KEY (batch_id) REFERENCES scheduler_job (job_id) ON DELETE CASCADE
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci;

-- 8. Execution history
CREATE TABLE IF NOT EXISTS scheduler_job_execution
(
    id            BINARY(16)      NOT NULL,
    job_id        BINARY(16)                                       NOT NULL,
    attempt       INT                                              NOT NULL,
    node_id       VARCHAR(64)                                      NOT NULL,
    started_at    DATETIME(6)                                      NOT NULL,
    ended_at      DATETIME(6)                                      NULL,
    status        ENUM ('RUNNING','SUCCEEDED','FAILED','CANCELED') NOT NULL DEFAULT 'RUNNING',
    error_message TEXT                                             NULL,
    error_class   VARCHAR(255)                                     NULL,
    duration_ms   BIGINT                                           NULL,
    PRIMARY KEY (id),
    INDEX idx_job_execution_job (job_id),
    INDEX idx_job_execution_node (node_id, started_at),
    INDEX idx_job_execution_status (status, started_at),
    CONSTRAINT fk_execution_job FOREIGN KEY (job_id) REFERENCES scheduler_job (job_id) ON DELETE CASCADE
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci;

-- 9. Per-job logs
CREATE TABLE IF NOT EXISTS scheduler_job_log
(
    log_id  BINARY(16)      NOT NULL,
    job_id  BINARY(16)                                   NOT NULL,
    ts      DATETIME(6)                                  NOT NULL,
    level   ENUM ('TRACE','DEBUG','INFO','WARN','ERROR') NOT NULL,
    message TEXT                                         NOT NULL,
    mdc     JSON                                         NULL,
    PRIMARY KEY (log_id),
    INDEX idx_joblog_job_ts (job_id, ts),
    INDEX idx_joblog_ts (ts),
    CONSTRAINT fk_log_job FOREIGN KEY (job_id) REFERENCES scheduler_job (job_id) ON DELETE CASCADE
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci;

-- 10. Archived jobs
CREATE TABLE IF NOT EXISTS scheduler_job_archive
(
    archive_id              BINARY(16)      NOT NULL,
    original_job_id         BINARY(16)                                                                                                          NOT NULL,
    final_status            ENUM ('SUCCEEDED','FAILED','CANCELED')                                                                                 NOT NULL,
    job_type                ENUM ('SINGLE','RECURRING','BATCH_PARENT','BATCH_CHILD','CHAIN_STEP','DLQ_ALERT','WORKFLOW_BRANCH','WORKFLOW_JOIN') NOT NULL,
    priority                TINYINT UNSIGNED                                                                                                    NOT NULL,
    total_attempts          INT                                                                                                                 NOT NULL DEFAULT 0,
    max_retries             INT                                                                                                                 NOT NULL,
    backoff_policy          ENUM ('NONE','FIXED','EXPONENTIAL')                                                                                 NOT NULL,
    backoff_param_ms        INT                                                                                                                 NOT NULL DEFAULT 0,
    timeout_sec             INT                                                                                                                 NOT NULL DEFAULT 0,
    target_class            VARCHAR(255)                                                                                                        NULL,
    method_name             VARCHAR(128)                                                                                                        NULL,
    business_key            VARCHAR(255)                                                                                                        NULL,
    cron_expr               VARCHAR(64)                                                                                                         NULL,
    zone_id                 VARCHAR(32)                                                                                                         NULL,
    original_scheduled_time DATETIME(6)                                                                                                         NOT NULL,
    original_created_at     DATETIME(6)                                                                                                         NOT NULL,
    first_execution_time    DATETIME(6)                                                                                                         NULL,
    completion_time         DATETIME(6)                                                                                                         NULL,
    total_execution_time_ms BIGINT                                                                                                              NULL,
    queue_wait_ms           BIGINT                                                                                                              NULL,
    archived_at             DATETIME(6)                                                                                                         NOT NULL,
    archived_by             VARCHAR(64)                                                                                                         NULL,
    archive_reason          VARCHAR(128)                                                                                                        NULL,
    job_result              JSON                                                                                                                NULL,
    result_type             VARCHAR(100)                                                                                                        NULL,
    final_error             TEXT                                                                                                                NULL,
    payload_summary         TEXT                                                                                                                NULL,
    depended_on             BINARY(16)                                                                                                          NULL,
    superseded_by           BINARY(16)                                                                                                          NULL,
    tags                    VARCHAR(512)                                                                                                        NULL,
    PRIMARY KEY (archive_id),
    CONSTRAINT chk_archive_priority CHECK (priority BETWEEN 0 AND 4),
    INDEX idx_archive_original_id (original_job_id),
    INDEX idx_archive_status (final_status),
    INDEX idx_archive_created_range (original_created_at),
    INDEX idx_archive_completed_range (completion_time),
    INDEX idx_archive_archived_at (archived_at),
    INDEX idx_archive_target_class (target_class),
    INDEX idx_archive_business_key (business_key),
    INDEX idx_archive_job_type (job_type),
    INDEX idx_archive_priority (priority)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci;

-- 11. Workflow conditions
CREATE TABLE IF NOT EXISTS scheduler_workflow_condition
(
    id                   BINARY(16)      NOT NULL,
    parent_job_id        BINARY(16)      NOT NULL,
    child_job_id         BINARY(16)      NOT NULL,
    condition_type       VARCHAR(32)     NOT NULL,
    condition_expression TEXT            NULL,
    condition_priority   INT             NOT NULL DEFAULT 0,
    created_at           DATETIME(6)     NOT NULL,
    PRIMARY KEY (id),
    INDEX idx_workflow_parent (parent_job_id),
    INDEX idx_workflow_child (child_job_id),
    INDEX idx_workflow_priority (parent_job_id, condition_priority),
    CONSTRAINT fk_workflow_parent FOREIGN KEY (parent_job_id) REFERENCES scheduler_job (job_id) ON DELETE CASCADE,
    CONSTRAINT fk_workflow_child FOREIGN KEY (child_job_id) REFERENCES scheduler_job (job_id) ON DELETE CASCADE,
    CONSTRAINT chk_condition_type CHECK (condition_type IN
                                         ('SUCCESS', 'FAILURE', 'CUSTOM', 'RESULT_VALUE',
                                          'BATCH_SUCCESS', 'BATCH_FAILURE', 'BATCH_SUCCESS_RATE',
                                          'BATCH_FAILURE_COUNT', 'BATCH_CUSTOM'))
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci;

-- 12. DLQ alert records
CREATE TABLE IF NOT EXISTS scheduler_dlq_alerts
(
    id            BINARY(16)      NOT NULL,
    job_id        BINARY(16)      NOT NULL,
    error_hash    VARCHAR(64)     NOT NULL,
    alert_sent_at DATETIME(6)     NULL,
    alert_channel VARCHAR(100)    NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_job_error_hash (job_id, error_hash),
    INDEX idx_dlq_sent_at (alert_sent_at),
    CONSTRAINT fk_dlq_alert_job FOREIGN KEY (job_id) REFERENCES scheduler_job (job_id) ON DELETE CASCADE
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci;

-- 13. Active resource permits
CREATE TABLE IF NOT EXISTS scheduler_resource_permit
(
    id            BINARY(16)      NOT NULL,
    resource_name VARCHAR(100)    NOT NULL,
    job_id        BINARY(16)      NOT NULL,
    node_id       VARCHAR(64)     NOT NULL,
    acquired_at   DATETIME(6)     NOT NULL,
    PRIMARY KEY (id),
    INDEX idx_resource_permit_resource (resource_name),
    INDEX idx_resource_permit_job (job_id),
    CONSTRAINT fk_resource_permit_job FOREIGN KEY (job_id) REFERENCES scheduler_job (job_id) ON DELETE CASCADE
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci;
