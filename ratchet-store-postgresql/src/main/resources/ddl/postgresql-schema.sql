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

-- 4. scheduler_job
CREATE TABLE IF NOT EXISTS scheduler_job
(
    job_id                BIGINT NOT NULL,
    status                TEXT        NOT NULL DEFAULT 'PENDING',
    paused_from_status    TEXT,
    scheduled_time TIMESTAMPTZ(6) NOT NULL,
    job_type              TEXT        NOT NULL,
    priority              INT         NOT NULL DEFAULT 2,
    attempts              INT         NOT NULL DEFAULT 0,
    max_retries           INT         NOT NULL DEFAULT 0,
    backoff_policy        TEXT        NOT NULL DEFAULT 'NONE',
    backoff_param_ms      INT         NOT NULL DEFAULT 0,
    timeout_sec           INT         NOT NULL DEFAULT 0,
    cron_expr             VARCHAR(64) NOT NULL DEFAULT '',
    zone_id               VARCHAR(32) NOT NULL DEFAULT 'UTC',
    next_fire TIMESTAMPTZ(6),
    payload               JSONB NOT NULL,
    params                JSONB,
    target_class          TEXT GENERATED ALWAYS AS (payload ->> 'target') STORED,
    method_name           TEXT GENERATED ALWAYS AS (payload ->> 'method') STORED,
    idempotency_key       VARCHAR(36) NOT NULL,
    business_key          TEXT,
    resource_name         VARCHAR(100),
    on_success_payload    JSONB,
    on_failure_payload    JSONB,
    depends_on            BIGINT,
    superseded_by         BIGINT,
    picked_by             VARCHAR(64),
    picked_at TIMESTAMPTZ(6),
    last_error            TEXT,
    created_at TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_by            VARCHAR(255),
    -- Captured at creation from jakarta.security.enterprise.SecurityContext when resolvable; null
    -- otherwise. No enforcement performed — see JobSchedulerService Javadoc.
    caller_principal      VARCHAR(255),
    updated_at TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    execution_start_time TIMESTAMPTZ(6),
    execution_end_time TIMESTAMPTZ(6),
    execution_duration_ms BIGINT,
    queue_wait_ms         BIGINT,
    job_result JSONB,
    result_type           VARCHAR(100),
    version               INT         NOT NULL DEFAULT 0,
    CONSTRAINT pk_scheduler_job PRIMARY KEY (job_id),
    CONSTRAINT uk_idempotency_key UNIQUE (idempotency_key),
    CONSTRAINT chk_job_status CHECK (status IN
                                     ('PENDING', 'RUNNING', 'SUCCEEDED', 'FAILED', 'CANCELED',
                                      'PAUSED')),
    CONSTRAINT chk_job_type CHECK (job_type IN
                                   ('SINGLE', 'RECURRING', 'BATCH_PARENT', 'BATCH_CHILD',
                                    'CHAIN_STEP', 'DLQ_ALERT', 'WORKFLOW_BRANCH', 'WORKFLOW_JOIN')),
    CONSTRAINT chk_job_priority CHECK (priority BETWEEN 0 AND 4),
    CONSTRAINT chk_backoff_policy CHECK (backoff_policy IN ('NONE', 'FIXED', 'EXPONENTIAL')),
    CONSTRAINT chk_paused_from_status CHECK (paused_from_status IS NULL OR paused_from_status IN ('PENDING', 'RUNNING', 'SUCCEEDED', 'FAILED', 'CANCELED', 'PAUSED'))
);

-- 4a. Business-key active-uniqueness reservation table.
-- Authoritative ownership lookup for active business keys. The main scheduler_job.business_key
-- column remains for observability and archive/search projections; uniqueness is enforced here.
CREATE TABLE IF NOT EXISTS scheduler_business_key_reservation
(
    business_key TEXT NOT NULL,
    owner_job_id BIGINT NOT NULL,
    owner_table TEXT NOT NULL,
    reserved_at TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_scheduler_business_key_reservation PRIMARY KEY (business_key),
    CONSTRAINT chk_bk_owner_table CHECK (owner_table IN ('QUEUE', 'RECURRING')),
    CONSTRAINT fk_bk_owner_job FOREIGN KEY (owner_job_id) REFERENCES scheduler_job (job_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_bk_owner ON scheduler_business_key_reservation (owner_job_id);

-- Hot-path poller indexes — required, do NOT remove without re-running the perf suite.
CREATE INDEX IF NOT EXISTS idx_job_poll_composite ON scheduler_job (status, priority, scheduled_time);
-- Match the executable claim filter used by PostgresqlJobStore:
--   WHERE status = 'PENDING' AND job_type = ?
--   AND scheduled_time <= statement_timestamp()
-- Computed age-boost ordering is sorted after the index scan.
-- The partial predicate removes the leading status column from the index key and keeps
-- write amplification lower than a full-table covering index.
CREATE INDEX IF NOT EXISTS idx_job_claim_cover
    ON scheduler_job (job_type, scheduled_time ASC, priority DESC, job_id ASC)
    WHERE status = 'PENDING';
CREATE INDEX IF NOT EXISTS idx_recurring_due ON scheduler_job (status, next_fire);
-- Match recurring claim order:
--   WHERE status = 'PENDING' AND job_type = 'RECURRING'
--   AND next_fire <= statement_timestamp()
CREATE INDEX IF NOT EXISTS idx_job_recurring_composite
    ON scheduler_job (next_fire ASC, priority DESC, job_id ASC)
    WHERE status = 'PENDING' AND job_type = 'RECURRING';
-- TODO(perf-audit): idx_job_due (status, scheduled_time) is a left-prefix of idx_job_poll_composite
-- (status, priority, scheduled_time) and likely redundant for the planner. Confirm with
-- pg_stat_user_indexes on a representative workload before dropping to avoid an accidental
-- planner regression on narrow-index lookups.
CREATE INDEX IF NOT EXISTS idx_job_due ON scheduler_job (status, scheduled_time);
CREATE INDEX IF NOT EXISTS idx_job_priority_due ON scheduler_job (priority, scheduled_time);
-- Lookup/relationship indexes.
CREATE INDEX IF NOT EXISTS idx_job_picked_by ON scheduler_job (picked_by);
CREATE INDEX IF NOT EXISTS idx_job_depends_on ON scheduler_job (depends_on);
CREATE INDEX IF NOT EXISTS idx_job_superseded_by ON scheduler_job (superseded_by);
CREATE INDEX IF NOT EXISTS idx_job_business_key ON scheduler_job (business_key);
-- Audit/archival indexes.
CREATE INDEX IF NOT EXISTS idx_job_created_at ON scheduler_job (created_at);
CREATE INDEX IF NOT EXISTS idx_job_updated_at ON scheduler_job (updated_at);
-- DROPPED: idx_target_class and idx_method_name were debug-only and added measurable
-- write amplification on the hot insert path. See ddl/postgresql-debug-indexes.sql for
-- the optional companion file that adds them back when needed.
-- DROPPED: idx_job_active_business_key (ownership moved to scheduler_business_key_reservation).

-- 5. scheduler_job_tag
CREATE TABLE IF NOT EXISTS scheduler_job_tag
(
    job_id BIGINT      NOT NULL,
    tag    VARCHAR(64) NOT NULL,
    CONSTRAINT pk_scheduler_job_tag PRIMARY KEY (job_id, tag),
    CONSTRAINT fk_job_tag_job FOREIGN KEY (job_id) REFERENCES scheduler_job (job_id) ON DELETE CASCADE
);

-- Support run-status and other tag-first aggregations.
CREATE INDEX IF NOT EXISTS idx_job_tag_tag_job ON scheduler_job_tag (tag, job_id);

-- 6. scheduler_batch
CREATE TABLE IF NOT EXISTS scheduler_batch
(
    batch_id             BIGINT  NOT NULL,
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
    batch_id           BIGINT NOT NULL,
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
    id            BIGINT NOT NULL,
    job_id        BIGINT      NOT NULL,
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
    log_id  BIGINT NOT NULL,
    job_id  BIGINT     NOT NULL,
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
    archive_id              BIGINT NOT NULL,
    original_job_id         BIGINT NOT NULL,
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
    depended_on             BIGINT,
    superseded_by           BIGINT,
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
    id                   BIGINT NOT NULL,
    parent_job_id        BIGINT NOT NULL,
    child_job_id         BIGINT NOT NULL,
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
    id            BIGINT NOT NULL,
    job_id        BIGINT      NOT NULL,
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
    id            BIGINT NOT NULL,
    resource_name VARCHAR(100) NOT NULL,
    job_id        BIGINT       NOT NULL,
    node_id       VARCHAR(64)  NOT NULL,
    acquired_at TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_scheduler_resource_permit PRIMARY KEY (id),
    CONSTRAINT fk_resource_permit_job FOREIGN KEY (job_id) REFERENCES scheduler_job (job_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_resource_permit_resource ON scheduler_resource_permit (resource_name);
CREATE INDEX IF NOT EXISTS idx_resource_permit_job ON scheduler_resource_permit (job_id);
