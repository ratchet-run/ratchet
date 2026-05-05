-- 0. ratchet_schema_version
CREATE TABLE IF NOT EXISTS ratchet_schema_version
(
    version VARCHAR(20) NOT NULL,
    applied_at TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    description VARCHAR(200) NOT NULL,
    checksum VARCHAR(64),
    CONSTRAINT pk_ratchet_schema_version PRIMARY KEY (version)
);

-- 1. scheduler_node
CREATE TABLE IF NOT EXISTS scheduler_node
(
    node_id VARCHAR(64) NOT NULL,
    heartbeat_ts TIMESTAMPTZ(6) NOT NULL,
    started_at TIMESTAMPTZ(6) NOT NULL,
    node_info TEXT,
    CONSTRAINT pk_scheduler_node PRIMARY KEY (node_id)
);

CREATE INDEX IF NOT EXISTS idx_node_heartbeat ON scheduler_node (heartbeat_ts);

-- 2. scheduler_lock
CREATE TABLE IF NOT EXISTS scheduler_lock
(
    lock_name  VARCHAR(128) NOT NULL,
    owner_node VARCHAR(64)  NOT NULL,
    locked_at TIMESTAMPTZ(6) NOT NULL,
    expires_at TIMESTAMPTZ(6) NOT NULL,
    CONSTRAINT pk_scheduler_lock PRIMARY KEY (lock_name)
);

CREATE INDEX IF NOT EXISTS idx_lock_expires ON scheduler_lock (expires_at);

-- 3. scheduler_resource_limit
CREATE TABLE IF NOT EXISTS scheduler_resource_limit
(
    resource_name  VARCHAR(100) NOT NULL,
    max_concurrent INT          NOT NULL,
    retry_delay_ms INT          NOT NULL DEFAULT 5000,
    description    VARCHAR(255),
    created_at TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_scheduler_resource_limit PRIMARY KEY (resource_name)
);

-- 4. scheduler_job — COLD metadata + terminal fields.
-- Live queue state (status, scheduled_time, picked_*, attempts, version, last_error,
-- paused_from_status, updated_at) lives on scheduler_job_queue instead. Immutable
-- job-shape fields are duplicated on scheduler_job_queue for the claim-path DTO —
-- no mutation path writes them in both places.
CREATE TABLE IF NOT EXISTS scheduler_job
(
    job_id                uuid NOT NULL,
    -- Immutable job-shape fields (duplicated on scheduler_job_queue per §duplication rule).
    job_type              TEXT        NOT NULL,
    priority              INT         NOT NULL DEFAULT 2,
    max_retries           INT         NOT NULL DEFAULT 0,
    backoff_policy        TEXT        NOT NULL DEFAULT 'NONE',
    backoff_param_ms      INT         NOT NULL DEFAULT 0,
    timeout_sec           INT         NOT NULL DEFAULT 0,
    cron_expr             VARCHAR(64) NOT NULL DEFAULT '',
    zone_id               VARCHAR(32) NOT NULL DEFAULT 'UTC',
    -- next_fire is the recurring-master schedule anchor; transitional home — moves
    -- to scheduler_recurring_job in a future migration. NULL for executable jobs.
    next_fire TIMESTAMPTZ(6),
    -- Payload + params (insert-once; never mutated after enqueue).
    payload               JSONB NOT NULL,
    params                JSONB,
    -- W3C TraceContext carrier captured at enqueue time; passed to TracingCollector at execution
    -- start so distributed spans are parented to the submitting caller's trace.
    trace_context         JSONB,
    target_class          TEXT GENERATED ALWAYS AS (payload ->> 'target') STORED,
    method_name           TEXT GENERATED ALWAYS AS (payload ->> 'method') STORED,
    idempotency_key       VARCHAR(36) NOT NULL,
    -- business_key is immutable after enqueue; active-uniqueness owned by
    -- scheduler_business_key_reservation (not a UNIQUE KEY here anymore).
    business_key          TEXT,
    resource_name         VARCHAR(100),
    on_success_payload    JSONB,
    on_failure_payload    JSONB,
    depends_on            uuid,
    superseded_by         uuid,
    created_at TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    -- Captured at creation from jakarta.security.enterprise.SecurityContext when resolvable; null
    -- otherwise. No enforcement performed — see JobSchedulerService Javadoc.
    caller_principal      VARCHAR(255),
    -- Terminal fields — NULL while live; set exactly once at terminal transition; only
    -- cleared by resetFailedToPending. Archival / deleteDlqOlderThan use terminated_at.
    -- terminal_error is the cold survivor of scheduler_job_queue.last_error: lifecycle
    -- copies last_error → terminal_error before deleting the queue row.
    terminal_status       TEXT,
    terminal_error        TEXT,
    total_attempts        INT,
    terminated_at TIMESTAMPTZ(6),
    execution_start_time TIMESTAMPTZ(6),
    execution_end_time TIMESTAMPTZ(6),
    execution_duration_ms BIGINT,
    queue_wait_ms         BIGINT,
    job_result JSONB,
    result_type           VARCHAR(100),
    -- TRANSITIONAL: shim column so recurring masters (which still live in this
    -- table during the hot/cold split) can be filtered by the recurring claim index without the full
    -- status column. 'P' = PENDING, 'A' = PAUSED, NULL for non-recurring rows. Dropped
    -- when recurring masters move to scheduler_recurring_job.
    rec_status            CHAR(1),
    CONSTRAINT pk_scheduler_job PRIMARY KEY (job_id),
    CONSTRAINT uk_idempotency_key UNIQUE (idempotency_key),
    CONSTRAINT chk_job_type CHECK (job_type IN
                                   ('SINGLE', 'RECURRING', 'BATCH_PARENT', 'BATCH_CHILD',
                                    'CHAIN_STEP', 'DLQ_ALERT', 'WORKFLOW_BRANCH', 'WORKFLOW_JOIN')),
    CONSTRAINT chk_job_priority CHECK (priority BETWEEN 0 AND 4),
    CONSTRAINT chk_backoff_policy CHECK (backoff_policy IN ('NONE', 'FIXED', 'EXPONENTIAL')),
    CONSTRAINT chk_terminal_status CHECK (terminal_status IS NULL OR terminal_status IN ('SUCCEEDED', 'FAILED', 'CANCELED')),
    CONSTRAINT chk_rec_status CHECK (rec_status IS NULL OR rec_status IN ('P', 'A'))
);

-- 4a. Hot authoritative queue state for executable jobs.
-- Row exists iff the job is live (PENDING / RUNNING / PAUSED). DELETED at terminal.
-- All claim, pickup, retry, orphan, pause, resume reads and writes target this table.
-- Immutable job-shape fields (job_type, priority, business_key, timeout_sec,
-- max_retries) are denormalized from scheduler_job for single-table claim DTO
-- population — they are set at enqueue and never mutated.
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
    signal_key              VARCHAR(255),
    signal_timeout          TIMESTAMPTZ,
    signal_payload          TEXT,
    signal_payload_type     VARCHAR(16),
    signal_outcome          VARCHAR(32),
    signal_rejection_reason TEXT,
    signal_delivered_at     TIMESTAMPTZ,
    signal_delivered_by     VARCHAR(255),
    signal_delivery_id      VARCHAR(36),
    CONSTRAINT pk_scheduler_job_queue PRIMARY KEY (job_id),
    CONSTRAINT chk_queue_status CHECK (status IN ('PENDING', 'RUNNING', 'PAUSED', 'WAITING')),
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

-- Orphan-detection scan: status='RUNNING' AND picked_at < cutoff AND picked_by NOT IN (alive).
CREATE INDEX IF NOT EXISTS idx_queue_orphan
    ON scheduler_job_queue (status, picked_at, picked_by);

CREATE INDEX IF NOT EXISTS idx_signal_key_status
    ON scheduler_job_queue (signal_key, status);

CREATE INDEX IF NOT EXISTS idx_signal_timeout_status
    ON scheduler_job_queue (status, signal_timeout);

CREATE INDEX IF NOT EXISTS idx_signal_delivery_id
    ON scheduler_job_queue (signal_delivery_id);

-- 4b. Business-key active-uniqueness reservation table.
-- Authoritative ownership lookup for active business keys. The main scheduler_job.business_key
-- column remains for observability and archive/search projections; uniqueness is enforced here.
CREATE TABLE IF NOT EXISTS scheduler_business_key_reservation
(
    business_key TEXT NOT NULL,
    owner_job_id uuid NOT NULL,
    owner_table TEXT NOT NULL,
    reserved_at TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_scheduler_business_key_reservation PRIMARY KEY (business_key),
    CONSTRAINT chk_bk_owner_table CHECK (owner_table IN ('QUEUE', 'RECURRING')),
    CONSTRAINT fk_bk_owner_job FOREIGN KEY (owner_job_id) REFERENCES scheduler_job (job_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_bk_owner ON scheduler_business_key_reservation (owner_job_id);

-- Lookup/relationship indexes (cold).
CREATE INDEX IF NOT EXISTS idx_job_depends_on ON scheduler_job (depends_on);
CREATE INDEX IF NOT EXISTS idx_job_superseded_by ON scheduler_job (superseded_by);
-- business_key is observability-only here; uniqueness is in scheduler_business_key_reservation.
CREATE INDEX IF NOT EXISTS idx_job_business_key ON scheduler_job (business_key);
-- Audit / archival indexes.
CREATE INDEX IF NOT EXISTS idx_job_created_at ON scheduler_job (created_at);
-- Archival / deleteDlqOlderThan scan (terminal_status, terminated_at).
CREATE INDEX IF NOT EXISTS idx_job_terminal ON scheduler_job (terminal_status, terminated_at);
-- TRANSITIONAL: recurring-master claim. Dropped with rec_status in a future migration.
CREATE INDEX IF NOT EXISTS idx_job_recurring_pending
    ON scheduler_job (job_type, rec_status, next_fire);
-- DROPPED: idx_target_class and idx_method_name were debug-only and added measurable
-- write amplification on the hot insert path. See ddl/postgresql-debug-indexes.sql for
-- the optional companion file that adds them back when needed.
-- DROPPED (moved to scheduler_job_queue): idx_job_claim_cover, idx_recurring_due,
-- idx_job_recurring_composite, idx_job_picked_by, idx_job_due, idx_job_priority_due,
-- idx_job_updated_at, idx_job_poll_composite.
-- DROPPED (ownership moved to scheduler_business_key_reservation): idx_job_active_business_key.

-- 5. scheduler_job_tag
CREATE TABLE IF NOT EXISTS scheduler_job_tag
(
    job_id uuid        NOT NULL,
    tag    VARCHAR(64) NOT NULL,
    CONSTRAINT pk_scheduler_job_tag PRIMARY KEY (job_id, tag),
    CONSTRAINT fk_job_tag_job FOREIGN KEY (job_id) REFERENCES scheduler_job (job_id) ON DELETE CASCADE
);

-- Support run-status and other tag-first aggregations.
CREATE INDEX IF NOT EXISTS idx_job_tag_tag_job ON scheduler_job_tag (tag, job_id);

-- 6. scheduler_batch
CREATE TABLE IF NOT EXISTS scheduler_batch
(
    batch_id             uuid    NOT NULL,
    total_items          INT     NOT NULL DEFAULT 0,
    completed_items      INT     NOT NULL DEFAULT 0,
    failed_items         INT     NOT NULL DEFAULT 0,
    completion_processed BOOLEAN NOT NULL DEFAULT FALSE,
    version              INT     NOT NULL DEFAULT 0,
    progress_hook TEXT,
    CONSTRAINT pk_scheduler_batch PRIMARY KEY (batch_id),
    CONSTRAINT fk_batch_job FOREIGN KEY (batch_id) REFERENCES scheduler_job (job_id) ON DELETE CASCADE
);

-- 7. scheduler_batch_metrics
CREATE TABLE IF NOT EXISTS scheduler_batch_metrics
(
    batch_id           uuid   NOT NULL,
    total_duration_ms  BIGINT,
    child_execution_ms BIGINT,
    overhead_ms        BIGINT,
    child_count        INT    NOT NULL DEFAULT 0,
    success_count      INT    NOT NULL DEFAULT 0,
    failure_count      INT    NOT NULL DEFAULT 0,
    started_at TIMESTAMPTZ(6),
    completed_at TIMESTAMPTZ(6),
    version            INT    NOT NULL DEFAULT 0,
    CONSTRAINT pk_scheduler_batch_metrics PRIMARY KEY (batch_id),
    CONSTRAINT fk_batch_metrics_job FOREIGN KEY (batch_id) REFERENCES scheduler_job (job_id) ON DELETE CASCADE
);

-- 8. scheduler_job_execution
CREATE TABLE IF NOT EXISTS scheduler_job_execution
(
    id            uuid        NOT NULL,
    job_id        uuid        NOT NULL,
    attempt       INT         NOT NULL,
    node_id       VARCHAR(64) NOT NULL,
    started_at TIMESTAMPTZ(6) NOT NULL,
    ended_at TIMESTAMPTZ(6),
    status        TEXT        NOT NULL DEFAULT 'RUNNING',
    error_message TEXT,
    error_class   VARCHAR(255),
    duration_ms   BIGINT,
    CONSTRAINT pk_scheduler_job_execution PRIMARY KEY (id),
    CONSTRAINT chk_execution_status CHECK (status IN ('RUNNING', 'SUCCEEDED', 'FAILED', 'CANCELED')),
    CONSTRAINT fk_execution_job FOREIGN KEY (job_id) REFERENCES scheduler_job (job_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_job_execution_job ON scheduler_job_execution (job_id);
CREATE INDEX IF NOT EXISTS idx_job_execution_node ON scheduler_job_execution (node_id, started_at);
CREATE INDEX IF NOT EXISTS idx_job_execution_status ON scheduler_job_execution (status, started_at);

-- 9. scheduler_job_log
CREATE TABLE IF NOT EXISTS scheduler_job_log
(
    log_id  uuid       NOT NULL,
    job_id  uuid       NOT NULL,
    ts TIMESTAMPTZ(6) NOT NULL,
    level   VARCHAR(8) NOT NULL,
    message TEXT       NOT NULL,
    mdc TEXT,
    CONSTRAINT pk_scheduler_job_log PRIMARY KEY (log_id),
    CONSTRAINT fk_log_job FOREIGN KEY (job_id) REFERENCES scheduler_job (job_id) ON DELETE CASCADE,
    CONSTRAINT chk_log_level CHECK (level IN ('TRACE', 'DEBUG', 'INFO', 'WARN', 'ERROR'))
);

CREATE INDEX IF NOT EXISTS idx_joblog_job_ts ON scheduler_job_log (job_id, ts);
CREATE INDEX IF NOT EXISTS idx_joblog_ts ON scheduler_job_log (ts);

-- 10. scheduler_job_archive
CREATE TABLE IF NOT EXISTS scheduler_job_archive
(
    archive_id              uuid   NOT NULL,
    original_job_id         uuid   NOT NULL,
    final_status            TEXT   NOT NULL,
    job_type                TEXT   NOT NULL,
    priority                INT    NOT NULL,
    total_attempts          INT    NOT NULL DEFAULT 0,
    max_retries             INT    NOT NULL DEFAULT 0,
    backoff_policy          TEXT   NOT NULL DEFAULT 'NONE',
    backoff_param_ms        INT    NOT NULL DEFAULT 0,
    timeout_sec             INT    NOT NULL DEFAULT 0,
    target_class            TEXT,
    method_name             VARCHAR(128),
    business_key            TEXT,
    cron_expr               VARCHAR(64),
    zone_id                 VARCHAR(32),
    original_scheduled_time TIMESTAMPTZ(6) NOT NULL,
    original_created_at TIMESTAMPTZ(6) NOT NULL,
    first_execution_time TIMESTAMPTZ(6),
    completion_time TIMESTAMPTZ(6),
    total_execution_time_ms BIGINT,
    queue_wait_ms           BIGINT,
    archived_at TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    archived_by             VARCHAR(64),
    archive_reason          VARCHAR(128),
    job_result TEXT,
    result_type             VARCHAR(100),
    final_error             TEXT,
    payload_summary         TEXT,
    depended_on             uuid,
    superseded_by           uuid,
    tags                    VARCHAR(512),
    CONSTRAINT pk_scheduler_job_archive PRIMARY KEY (archive_id),
    CONSTRAINT chk_archive_status CHECK (final_status IN ('SUCCEEDED', 'FAILED', 'CANCELED')),
    CONSTRAINT chk_archive_job_type CHECK (job_type IN
                                           ('SINGLE', 'RECURRING', 'BATCH_PARENT', 'BATCH_CHILD',
                                            'CHAIN_STEP', 'DLQ_ALERT', 'WORKFLOW_BRANCH', 'WORKFLOW_JOIN')),
    CONSTRAINT chk_archive_priority CHECK (priority BETWEEN 0 AND 4),
    CONSTRAINT chk_archive_backoff_policy CHECK (backoff_policy IN ('NONE', 'FIXED', 'EXPONENTIAL'))
);

CREATE INDEX IF NOT EXISTS idx_archive_original_id ON scheduler_job_archive (original_job_id);
CREATE INDEX IF NOT EXISTS idx_archive_status ON scheduler_job_archive (final_status);
CREATE INDEX IF NOT EXISTS idx_archive_created_range ON scheduler_job_archive (original_created_at);
CREATE INDEX IF NOT EXISTS idx_archive_completed_range ON scheduler_job_archive (completion_time);
CREATE INDEX IF NOT EXISTS idx_archive_archived_at ON scheduler_job_archive (archived_at);
CREATE INDEX IF NOT EXISTS idx_archive_target_class ON scheduler_job_archive (target_class);
CREATE INDEX IF NOT EXISTS idx_archive_business_key ON scheduler_job_archive (business_key);
CREATE INDEX IF NOT EXISTS idx_archive_job_type ON scheduler_job_archive (job_type);
CREATE INDEX IF NOT EXISTS idx_archive_priority ON scheduler_job_archive (priority);

-- 11. scheduler_workflow_condition
CREATE TABLE IF NOT EXISTS scheduler_workflow_condition
(
    id                   uuid   NOT NULL,
    parent_job_id        uuid   NOT NULL,
    child_job_id         uuid   NOT NULL,
    condition_type       TEXT   NOT NULL,
    condition_expression TEXT,
    condition_priority   INT    NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_scheduler_workflow_condition PRIMARY KEY (id),
    CONSTRAINT fk_workflow_parent FOREIGN KEY (parent_job_id) REFERENCES scheduler_job (job_id) ON DELETE CASCADE,
    CONSTRAINT fk_workflow_child FOREIGN KEY (child_job_id) REFERENCES scheduler_job (job_id) ON DELETE CASCADE,
    CONSTRAINT chk_condition_type CHECK (condition_type IN
                                         ('SUCCESS', 'FAILURE', 'CUSTOM', 'RESULT_VALUE',
                                          'BATCH_SUCCESS', 'BATCH_FAILURE', 'BATCH_SUCCESS_RATE',
                                          'BATCH_FAILURE_COUNT', 'BATCH_CUSTOM'))
);

CREATE INDEX IF NOT EXISTS idx_workflow_parent ON scheduler_workflow_condition (parent_job_id);
CREATE INDEX IF NOT EXISTS idx_workflow_child ON scheduler_workflow_condition (child_job_id);
CREATE INDEX IF NOT EXISTS idx_workflow_priority ON scheduler_workflow_condition (parent_job_id, condition_priority);

-- 12. scheduler_dlq_alerts
CREATE TABLE IF NOT EXISTS scheduler_dlq_alerts
(
    id            uuid        NOT NULL,
    job_id        uuid        NOT NULL,
    error_hash    VARCHAR(64) NOT NULL,
    alert_sent_at TIMESTAMPTZ(6),
    alert_channel VARCHAR(100),
    CONSTRAINT pk_scheduler_dlq_alerts PRIMARY KEY (id),
    CONSTRAINT uk_job_error_hash UNIQUE (job_id, error_hash),
    CONSTRAINT fk_dlq_alert_job FOREIGN KEY (job_id) REFERENCES scheduler_job (job_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_dlq_sent_at ON scheduler_dlq_alerts (alert_sent_at);

-- 13. scheduler_resource_permit
CREATE TABLE IF NOT EXISTS scheduler_resource_permit
(
    id            uuid         NOT NULL,
    resource_name VARCHAR(100) NOT NULL,
    job_id        uuid         NOT NULL,
    node_id       VARCHAR(64)  NOT NULL,
    acquired_at TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_scheduler_resource_permit PRIMARY KEY (id),
    CONSTRAINT fk_resource_permit_job FOREIGN KEY (job_id) REFERENCES scheduler_job (job_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_resource_permit_resource ON scheduler_resource_permit (resource_name);
CREATE INDEX IF NOT EXISTS idx_resource_permit_job ON scheduler_resource_permit (job_id);
