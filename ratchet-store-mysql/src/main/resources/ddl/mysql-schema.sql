-- Ratchet scheduler schema for MySQL
-- IMPORTANT: configure READ COMMITTED isolation — see IsolationCheck.java

-- 0. Schema version ledger (used by external migration tooling or an optional Ratchet migrator)
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
    -- next_fire is the recurring-master schedule anchor; transitional home — moves to
    -- scheduler_recurring_job in a future migration. NULL for executable jobs.
    next_fire             DATETIME(6)                                                                                                         NULL,
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
    -- TRANSITIONAL: shim column so recurring masters (which still live in this table
    -- during the hot/cold split) can be filtered by the recurring claim index without the full status
    -- column. 'P' = PENDING, 'A' = PAUSED, NULL for non-recurring rows. Dropped in a future migration
    -- when recurring masters move to scheduler_recurring_job.
    rec_status            CHAR(1)                                                                                                             NULL,
    PRIMARY KEY (job_id),
    UNIQUE KEY uk_idempotency_key (idempotency_key),
    CONSTRAINT chk_job_priority CHECK (priority BETWEEN 0 AND 4),
    CONSTRAINT chk_rec_status CHECK (rec_status IS NULL OR rec_status IN ('P','A')),
    -- Lookup/relationship indexes.
    INDEX idx_job_depends_on (depends_on),
    INDEX idx_job_superseded_by (superseded_by),
    -- business_key is observability-only here; uniqueness is in scheduler_business_key_reservation.
    INDEX idx_job_business_key (business_key),
    -- Audit / archival indexes.
    INDEX idx_job_created_at (created_at),
    -- Archival / deleteDlqOlderThan scan (terminal_status, terminated_at).
    INDEX idx_job_terminal (terminal_status, terminated_at),
    -- TRANSITIONAL: recurring-master claim index. Dropped with rec_status in a future migration.
    INDEX idx_job_recurring_pending (job_type, rec_status, next_fire)
    -- DROPPED: idx_target_class and idx_method_name were debug-only and added measurable
    -- write amplification on the hot insert path. Operators who need them can apply
    -- ddl/mysql-debug-indexes.sql (optional companion file).
    -- DROPPED (moved to scheduler_job_queue): idx_job_claim_cover, idx_recurring_due,
    -- idx_job_recurring_composite, idx_job_picked_by, idx_job_type.
    -- DROPPED (ownership moved to scheduler_business_key_reservation):
    -- uk_active_business_key, active_business_key generated column.
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
    INDEX idx_bk_owner (owner_job_id),
    CONSTRAINT fk_bk_owner_job FOREIGN KEY (owner_job_id) REFERENCES scheduler_job (job_id) ON DELETE CASCADE
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci;

-- 5. Job tags (composite PK)
CREATE TABLE IF NOT EXISTS scheduler_job_tag
(
    job_id BINARY(16)      NOT NULL,
    tag    VARCHAR(64)     NOT NULL,
    PRIMARY KEY (job_id, tag),
    INDEX idx_job_tag_tag_job (tag, job_id),
    CONSTRAINT fk_job_tag_job FOREIGN KEY (job_id) REFERENCES scheduler_job (job_id) ON DELETE CASCADE
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

-- Clean-install version ledger: every migration the canonical schema already incorporates.
-- INSERT IGNORE keeps this idempotent against schemas that were built migration-by-migration.
INSERT IGNORE INTO ratchet_schema_version (version, description) VALUES
    ('001', 'Initial schema'),
    ('002', 'Hot/cold store split: scheduler_job_queue + scheduler_business_key_reservation + cold terminal fields'),
    ('003', 'Align executable claim index for due-time filtering under computed priority boosting'),
    ('004', 'Add caller_principal audit column to scheduler_job'),
    ('005', 'Add trace_context propagation column to scheduler_job'),
    ('006', 'Drop created_by column superseded by caller_principal'),
    ('007', 'Query-layer generated column for traceCorrelationId + dashboard indexes'),
    ('008', 'Signal-waiting job columns, decision metadata, and indexes on scheduler_job_queue'),
    ('009', 'Drop legacy JDK-serialized predicate blobs from scheduler_workflow_condition');
