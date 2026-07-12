-- Ratchet scheduler schema for Oracle Database (23ai+).
-- IMPORTANT: the claim path runs under READ COMMITTED with FOR UPDATE SKIP LOCKED — Oracle's
-- default isolation already satisfies this; do NOT escalate to SERIALIZABLE (ORA-08177 retry tax).
--
-- Dialect notes vs. the MySQL/PostgreSQL stores:
--   * UUIDs are RAW(16) (big-endian, UUIDv7 time-ordered) — see UuidRawConverter.
--   * JSON columns are CLOB; the extracted target_class/method_name/trace_id columns are virtual
--     columns computed with JSON_VALUE (Oracle has no STORED generated column on a LOB source).
--   * Enum-like columns are VARCHAR2 + CHECK (Oracle has no ENUM), mirroring the PostgreSQL store.
--   * Booleans use the native Oracle 23ai BOOLEAN type.
--   * Timestamps are TIMESTAMP(6) holding UTC wall-clock; deploy with a UTC JVM / UTC DB session.

-- 0. Schema version ledger (used by external migration tooling or the optional Ratchet migrator)
CREATE TABLE IF NOT EXISTS ratchet_schema_version
(
    version     VARCHAR2(20)  NOT NULL,
    applied_at  TIMESTAMP(6)  DEFAULT SYSTIMESTAMP NOT NULL,
    description VARCHAR2(200) NOT NULL,
    checksum    VARCHAR2(64),
    CONSTRAINT pk_ratchet_schema_version PRIMARY KEY (version)
);

-- 1. Cluster nodes
CREATE TABLE IF NOT EXISTS scheduler_node
(
    node_id      VARCHAR2(64) NOT NULL,
    heartbeat_ts TIMESTAMP(6) NOT NULL,
    started_at   TIMESTAMP(6) NOT NULL,
    node_info    CLOB,
    CONSTRAINT pk_scheduler_node PRIMARY KEY (node_id)
);

CREATE INDEX IF NOT EXISTS idx_node_heartbeat ON scheduler_node (heartbeat_ts);

-- 2. Distributed locks
CREATE TABLE IF NOT EXISTS scheduler_lock
(
    lock_name  VARCHAR2(128) NOT NULL,
    owner_node VARCHAR2(64)  NOT NULL,
    locked_at  TIMESTAMP(6)  NOT NULL,
    expires_at TIMESTAMP(6)  NOT NULL,
    CONSTRAINT pk_scheduler_lock PRIMARY KEY (lock_name)
);

CREATE INDEX IF NOT EXISTS idx_lock_expires ON scheduler_lock (expires_at);

-- 3. Resource concurrency config
CREATE TABLE IF NOT EXISTS scheduler_resource_limit
(
    resource_name  VARCHAR2(100) NOT NULL,
    max_concurrent NUMBER(10)    NOT NULL,
    retry_delay_ms NUMBER(10)    DEFAULT 5000 NOT NULL,
    description    VARCHAR2(255),
    created_at     TIMESTAMP(6)  DEFAULT SYSTIMESTAMP NOT NULL,
    updated_at     TIMESTAMP(6)  DEFAULT SYSTIMESTAMP NOT NULL,
    CONSTRAINT pk_scheduler_resource_limit PRIMARY KEY (resource_name)
);

-- 3a. Recurring job masters.
-- Long-lived definition rows holding the cron schedule, payload template, and runtime anchor
-- (next_fire). Never executed directly — spawns child rows on scheduler_job at fire time.
CREATE TABLE IF NOT EXISTS scheduler_recurring_job
(
    id                 RAW(16)       NOT NULL,
    priority           NUMBER(3)     DEFAULT 2 NOT NULL,
    max_retries        NUMBER(10)    DEFAULT 0 NOT NULL,
    backoff_policy     VARCHAR2(16)  DEFAULT 'NONE' NOT NULL,
    backoff_param_ms   NUMBER(10)    DEFAULT 0 NOT NULL,
    timeout_sec        NUMBER(10)    DEFAULT 0 NOT NULL,
    cron_expr          VARCHAR2(64)  NOT NULL,
    zone_id            VARCHAR2(32)  DEFAULT 'UTC' NOT NULL,
    next_fire          TIMESTAMP(6)  NOT NULL,
    is_paused          BOOLEAN       DEFAULT FALSE NOT NULL,
    paused_at          TIMESTAMP(6),
    payload            CLOB          NOT NULL,
    on_success_payload CLOB,
    on_failure_payload CLOB,
    business_key       VARCHAR2(255),
    resource_name      VARCHAR2(100),
    execution_target   VARCHAR2(64),
    target_class       VARCHAR2(255) GENERATED ALWAYS AS (JSON_VALUE(payload, '$.target' RETURNING VARCHAR2(255))),
    method_name        VARCHAR2(128) GENERATED ALWAYS AS (JSON_VALUE(payload, '$.method' RETURNING VARCHAR2(128))),
    created_at         TIMESTAMP(6)  NOT NULL,
    caller_principal   VARCHAR2(255),
    encrypted_payload  BOOLEAN       DEFAULT FALSE NOT NULL,
    CONSTRAINT pk_scheduler_recurring_job PRIMARY KEY (id),
    CONSTRAINT chk_rec_priority CHECK (priority BETWEEN 0 AND 4),
    CONSTRAINT chk_rec_backoff_policy CHECK (backoff_policy IN ('NONE', 'FIXED', 'EXPONENTIAL'))
);

-- Claim scan: filter unpaused rows, ordered by next_fire.
CREATE INDEX IF NOT EXISTS idx_rec_claim ON scheduler_recurring_job (is_paused, next_fire);
CREATE INDEX IF NOT EXISTS idx_rec_business_key ON scheduler_recurring_job (business_key);
CREATE INDEX IF NOT EXISTS idx_rec_target_class ON scheduler_recurring_job (target_class);

-- 3b. Archived recurring definitions.
-- Denormalized snapshot written atomically with the DELETE from scheduler_recurring_job.
CREATE TABLE IF NOT EXISTS scheduler_recurring_job_archive
(
    id                 RAW(16)      NOT NULL,
    cron_expr          VARCHAR2(64) NOT NULL,
    zone_id            VARCHAR2(32) NOT NULL,
    payload            CLOB         NOT NULL,
    on_success_payload CLOB,
    on_failure_payload CLOB,
    business_key       VARCHAR2(255),
    execution_target   VARCHAR2(64),
    created_at         TIMESTAMP(6) NOT NULL,
    caller_principal   VARCHAR2(255),
    archived_at        TIMESTAMP(6) NOT NULL,
    archive_reason     VARCHAR2(16) NOT NULL,
    CONSTRAINT pk_scheduler_recurring_job_archive PRIMARY KEY (id),
    CONSTRAINT chk_recurring_archive_reason CHECK (archive_reason IN ('CANCELED', 'EXHAUSTED'))
);

CREATE INDEX IF NOT EXISTS idx_archive_rec_business_key ON scheduler_recurring_job_archive (business_key);
CREATE INDEX IF NOT EXISTS idx_archive_rec_archived_at ON scheduler_recurring_job_archive (archived_at);

-- 4. Main job table — COLD metadata + terminal fields.
-- Live queue state lives on scheduler_job_queue. Immutable job-shape fields are duplicated there
-- for the claim-path DTO; no mutation path writes them in both places.
CREATE TABLE IF NOT EXISTS scheduler_job
(
    job_id                RAW(16)      NOT NULL,
    job_type              VARCHAR2(16) NOT NULL,
    priority              NUMBER(3)    DEFAULT 2 NOT NULL,
    max_retries           NUMBER(10)   DEFAULT 0 NOT NULL,
    backoff_policy        VARCHAR2(16) DEFAULT 'NONE' NOT NULL,
    backoff_param_ms      NUMBER(10)   DEFAULT 0 NOT NULL,
    timeout_sec           NUMBER(10)   DEFAULT 0 NOT NULL,
    -- Nullable in Oracle only: Oracle collapses the empty string to NULL, so the engine's
    -- coerced "" sentinel for non-recurring jobs cannot satisfy a NOT NULL column. The shared
    -- row mapper coerces a NULL cron_expr back to "" on read, preserving the cross-store contract.
    cron_expr             VARCHAR2(64),
    zone_id               VARCHAR2(32) DEFAULT 'UTC' NOT NULL,
    payload               CLOB         NOT NULL,
    params                CLOB,
    trace_context         CLOB,
    trace_id_extracted    VARCHAR2(55)  GENERATED ALWAYS AS (JSON_VALUE(trace_context, '$.traceparent' RETURNING VARCHAR2(55))),
    target_class          VARCHAR2(255) GENERATED ALWAYS AS (JSON_VALUE(payload, '$.target' RETURNING VARCHAR2(255))),
    method_name           VARCHAR2(128) GENERATED ALWAYS AS (JSON_VALUE(payload, '$.method' RETURNING VARCHAR2(128))),
    idempotency_key       VARCHAR2(36) NOT NULL,
    business_key          VARCHAR2(255),
    resource_name         VARCHAR2(100),
    execution_target      VARCHAR2(64),
    on_success_payload    CLOB,
    on_failure_payload    CLOB,
    depends_on            RAW(16),
    superseded_by         RAW(16),
    created_at            TIMESTAMP(6) NOT NULL,
    caller_principal      VARCHAR2(255),
    terminal_status       VARCHAR2(16),
    terminal_error        CLOB,
    total_attempts        NUMBER(10),
    terminated_at         TIMESTAMP(6),
    execution_start_time  TIMESTAMP(6),
    execution_end_time    TIMESTAMP(6),
    execution_duration_ms NUMBER(19),
    queue_wait_ms         NUMBER(19),
    job_result            CLOB,
    result_type           VARCHAR2(100),
    recurring_master_id   RAW(16),
    encrypted_payload     BOOLEAN      DEFAULT FALSE NOT NULL,
    encryption_key_id     VARCHAR2(256),
    CONSTRAINT pk_scheduler_job PRIMARY KEY (job_id),
    CONSTRAINT uk_idempotency_key UNIQUE (idempotency_key),
    CONSTRAINT chk_job_type CHECK (job_type IN
                                   ('SINGLE', 'RECURRING', 'BATCH_PARENT', 'BATCH_CHILD',
                                    'CHAIN_STEP', 'DLQ_ALERT', 'WORKFLOW_BRANCH', 'WORKFLOW_JOIN')),
    CONSTRAINT chk_job_priority CHECK (priority BETWEEN 0 AND 4),
    CONSTRAINT chk_backoff_policy CHECK (backoff_policy IN ('NONE', 'FIXED', 'EXPONENTIAL')),
    CONSTRAINT chk_terminal_status CHECK (terminal_status IS NULL OR terminal_status IN ('SUCCEEDED', 'FAILED', 'CANCELED')),
    CONSTRAINT fk_job_recurring_master FOREIGN KEY (recurring_master_id)
        REFERENCES scheduler_recurring_job (id) ON DELETE SET NULL
);

-- 4a. Hot authoritative queue state for executable jobs.
-- Row exists iff the job is live (PENDING / RUNNING / PAUSED / WAITING). DELETED at terminal.
CREATE TABLE IF NOT EXISTS scheduler_job_queue
(
    job_id                  RAW(16)      NOT NULL,
    status                  VARCHAR2(16) DEFAULT 'PENDING' NOT NULL,
    job_type                VARCHAR2(16) NOT NULL,
    priority                NUMBER(3)    DEFAULT 2 NOT NULL,
    scheduled_time          TIMESTAMP(6) NOT NULL,
    business_key            VARCHAR2(255),
    timeout_sec             NUMBER(10)   DEFAULT 0 NOT NULL,
    max_retries             NUMBER(10)   DEFAULT 0 NOT NULL,
    attempts                NUMBER(10)   DEFAULT 0 NOT NULL,
    picked_by               VARCHAR2(64),
    picked_at               TIMESTAMP(6),
    paused_from_status      VARCHAR2(20),
    last_error              CLOB,
    version                 NUMBER(10)   DEFAULT 0 NOT NULL,
    updated_at              TIMESTAMP(6) NOT NULL,
    signal_key              VARCHAR2(255),
    signal_timeout          TIMESTAMP(6),
    signal_payload          CLOB,
    signal_payload_type     VARCHAR2(16),
    signal_outcome          VARCHAR2(32),
    signal_rejection_reason CLOB,
    signal_delivered_at     TIMESTAMP(6),
    signal_delivered_by     VARCHAR2(255),
    signal_delivery_id      VARCHAR2(36),
    execution_target        VARCHAR2(64),
    CONSTRAINT pk_scheduler_job_queue PRIMARY KEY (job_id),
    CONSTRAINT chk_queue_status CHECK (status IN ('PENDING', 'RUNNING', 'PAUSED', 'WAITING')),
    CONSTRAINT chk_queue_job_type CHECK (job_type IN
                                         ('SINGLE', 'RECURRING', 'BATCH_PARENT', 'BATCH_CHILD',
                                          'CHAIN_STEP', 'DLQ_ALERT', 'WORKFLOW_BRANCH', 'WORKFLOW_JOIN')),
    CONSTRAINT chk_queue_priority CHECK (priority BETWEEN 0 AND 4),
    CONSTRAINT chk_queue_paused_from_status CHECK (paused_from_status IS NULL OR paused_from_status IN ('PENDING', 'RUNNING', 'PAUSED')),
    CONSTRAINT fk_job_queue_job FOREIGN KEY (job_id) REFERENCES scheduler_job (job_id) ON DELETE CASCADE
);

-- Executable claim index: status equality leads, then due-time range, then priority/job_id.
-- Oracle has no partial index; status is the leading column instead of a WHERE predicate.
CREATE INDEX IF NOT EXISTS idx_claim_executable
    ON scheduler_job_queue (status, job_type, scheduled_time, priority, job_id);
CREATE INDEX IF NOT EXISTS idx_queue_orphan ON scheduler_job_queue (status, picked_at, picked_by);
CREATE INDEX IF NOT EXISTS idx_signal_key_status ON scheduler_job_queue (signal_key, status);
CREATE INDEX IF NOT EXISTS idx_signal_timeout_status ON scheduler_job_queue (status, signal_timeout);
CREATE INDEX IF NOT EXISTS idx_signal_delivery_id ON scheduler_job_queue (signal_delivery_id);

-- 4b. Business-key active-uniqueness reservation table.
CREATE TABLE IF NOT EXISTS scheduler_business_key_reservation
(
    business_key VARCHAR2(255) NOT NULL,
    owner_job_id RAW(16)       NOT NULL,
    owner_table  VARCHAR2(16)  NOT NULL,
    reserved_at  TIMESTAMP(6)  NOT NULL,
    CONSTRAINT pk_scheduler_business_key_reservation PRIMARY KEY (business_key),
    CONSTRAINT chk_bk_owner_table CHECK (owner_table IN ('QUEUE', 'RECURRING'))
);

CREATE INDEX IF NOT EXISTS idx_bk_owner ON scheduler_business_key_reservation (owner_job_id);

-- Lookup/relationship indexes (cold).
CREATE INDEX IF NOT EXISTS idx_job_depends_on ON scheduler_job (depends_on);
CREATE INDEX IF NOT EXISTS idx_job_superseded_by ON scheduler_job (superseded_by);
CREATE INDEX IF NOT EXISTS idx_job_recurring_master_id ON scheduler_job (recurring_master_id);
CREATE INDEX IF NOT EXISTS idx_job_encryption_key_id ON scheduler_job (encryption_key_id);
CREATE INDEX IF NOT EXISTS idx_job_business_key ON scheduler_job (business_key);
CREATE INDEX IF NOT EXISTS idx_job_created_at ON scheduler_job (created_at);
CREATE INDEX IF NOT EXISTS idx_job_terminal ON scheduler_job (terminal_status, terminated_at);

-- 5. Job tags (composite PK)
CREATE TABLE IF NOT EXISTS scheduler_job_tag
(
    job_id RAW(16)      NOT NULL,
    tag    VARCHAR2(64) NOT NULL,
    CONSTRAINT pk_scheduler_job_tag PRIMARY KEY (job_id, tag)
);

CREATE INDEX IF NOT EXISTS idx_job_tag_tag_job ON scheduler_job_tag (tag, job_id);

-- 6. Batch progress
CREATE TABLE IF NOT EXISTS scheduler_batch
(
    batch_id             RAW(16)    NOT NULL,
    total_items          NUMBER(10) DEFAULT 0 NOT NULL,
    completed_items      NUMBER(10) DEFAULT 0 NOT NULL,
    failed_items         NUMBER(10) DEFAULT 0 NOT NULL,
    completion_processed BOOLEAN    DEFAULT FALSE NOT NULL,
    version              NUMBER(10) DEFAULT 0 NOT NULL,
    progress_hook        CLOB,
    CONSTRAINT pk_scheduler_batch PRIMARY KEY (batch_id),
    CONSTRAINT fk_batch_job FOREIGN KEY (batch_id) REFERENCES scheduler_job (job_id) ON DELETE CASCADE
);

-- 7. Batch performance metrics
CREATE TABLE IF NOT EXISTS scheduler_batch_metrics
(
    batch_id           RAW(16)    NOT NULL,
    total_duration_ms  NUMBER(19),
    child_execution_ms NUMBER(19),
    overhead_ms        NUMBER(19),
    child_count        NUMBER(10) DEFAULT 0 NOT NULL,
    success_count      NUMBER(10) DEFAULT 0 NOT NULL,
    failure_count      NUMBER(10) DEFAULT 0 NOT NULL,
    started_at         TIMESTAMP(6),
    completed_at       TIMESTAMP(6),
    version            NUMBER(10) DEFAULT 0 NOT NULL,
    CONSTRAINT pk_scheduler_batch_metrics PRIMARY KEY (batch_id),
    CONSTRAINT fk_batch_metrics_job FOREIGN KEY (batch_id) REFERENCES scheduler_job (job_id) ON DELETE CASCADE
);

-- 8. Execution history
CREATE TABLE IF NOT EXISTS scheduler_job_execution
(
    id            RAW(16)      NOT NULL,
    job_id        RAW(16)      NOT NULL,
    attempt       NUMBER(10)   NOT NULL,
    node_id       VARCHAR2(64) NOT NULL,
    started_at    TIMESTAMP(6) NOT NULL,
    ended_at      TIMESTAMP(6),
    status        VARCHAR2(16) DEFAULT 'RUNNING' NOT NULL,
    error_message CLOB,
    error_class   VARCHAR2(255),
    duration_ms   NUMBER(19),
    CONSTRAINT pk_scheduler_job_execution PRIMARY KEY (id),
    CONSTRAINT chk_execution_status CHECK (status IN ('RUNNING', 'SUCCEEDED', 'FAILED', 'CANCELED')),
    CONSTRAINT fk_execution_job FOREIGN KEY (job_id) REFERENCES scheduler_job (job_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_job_execution_job ON scheduler_job_execution (job_id);
CREATE INDEX IF NOT EXISTS idx_job_execution_node ON scheduler_job_execution (node_id, started_at);
CREATE INDEX IF NOT EXISTS idx_job_execution_status ON scheduler_job_execution (status, started_at);

-- 9. Per-job logs
CREATE TABLE IF NOT EXISTS scheduler_job_log
(
    log_id  RAW(16)      NOT NULL,
    job_id  RAW(16)      NOT NULL,
    ts      TIMESTAMP(6) NOT NULL,
    -- LEVEL is an Oracle reserved word (CONNECT BY pseudocolumn); the column is a delimited
    -- identifier here and in orm-oracle.xml's JobLogEntity override so the JPA persist path agrees.
    "level" VARCHAR2(8)  NOT NULL,
    message CLOB         NOT NULL,
    mdc     CLOB,
    CONSTRAINT pk_scheduler_job_log PRIMARY KEY (log_id),
    CONSTRAINT fk_log_job FOREIGN KEY (job_id) REFERENCES scheduler_job (job_id) ON DELETE CASCADE,
    CONSTRAINT chk_log_level CHECK ("level" IN ('TRACE', 'DEBUG', 'INFO', 'WARN', 'ERROR'))
);

CREATE INDEX IF NOT EXISTS idx_joblog_job_ts ON scheduler_job_log (job_id, ts);
CREATE INDEX IF NOT EXISTS idx_joblog_ts ON scheduler_job_log (ts);

-- 10. Archived jobs
CREATE TABLE IF NOT EXISTS scheduler_job_archive
(
    archive_id              RAW(16)      NOT NULL,
    original_job_id         RAW(16)      NOT NULL,
    final_status            VARCHAR2(16) NOT NULL,
    job_type                VARCHAR2(16) NOT NULL,
    priority                NUMBER(3)    NOT NULL,
    total_attempts          NUMBER(10)   DEFAULT 0 NOT NULL,
    max_retries             NUMBER(10)   DEFAULT 0 NOT NULL,
    backoff_policy          VARCHAR2(16) DEFAULT 'NONE' NOT NULL,
    backoff_param_ms        NUMBER(10)   DEFAULT 0 NOT NULL,
    timeout_sec             NUMBER(10)   DEFAULT 0 NOT NULL,
    target_class            VARCHAR2(255),
    method_name             VARCHAR2(128),
    business_key            VARCHAR2(255),
    cron_expr               VARCHAR2(64),
    zone_id                 VARCHAR2(32),
    original_scheduled_time TIMESTAMP(6) NOT NULL,
    original_created_at     TIMESTAMP(6) NOT NULL,
    first_execution_time    TIMESTAMP(6),
    completion_time         TIMESTAMP(6),
    total_execution_time_ms NUMBER(19),
    queue_wait_ms           NUMBER(19),
    archived_at             TIMESTAMP(6) NOT NULL,
    archived_by             VARCHAR2(64),
    archive_reason          VARCHAR2(128),
    job_result              CLOB,
    result_type             VARCHAR2(100),
    final_error             CLOB,
    payload_summary         CLOB,
    depended_on             RAW(16),
    superseded_by           RAW(16),
    tags                    VARCHAR2(512),
    properties              CLOB,
    extension_state         CLOB,
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

-- 11. Workflow conditions
CREATE TABLE IF NOT EXISTS scheduler_workflow_condition
(
    id                   RAW(16)      NOT NULL,
    parent_job_id        RAW(16)      NOT NULL,
    child_job_id         RAW(16)      NOT NULL,
    condition_type       VARCHAR2(32) NOT NULL,
    condition_expression CLOB,
    condition_priority   NUMBER(10)   DEFAULT 0 NOT NULL,
    definition_order     NUMBER(10)   DEFAULT 0 NOT NULL,
    created_at           TIMESTAMP(6) NOT NULL,
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
CREATE INDEX IF NOT EXISTS idx_workflow_evaluation_order ON scheduler_workflow_condition (parent_job_id, condition_priority, definition_order);

-- 12. DLQ alert records
CREATE TABLE IF NOT EXISTS scheduler_dlq_alerts
(
    id            RAW(16)      NOT NULL,
    job_id        RAW(16)      NOT NULL,
    error_hash    VARCHAR2(64) NOT NULL,
    alert_sent_at TIMESTAMP(6),
    alert_channel VARCHAR2(100),
    CONSTRAINT pk_scheduler_dlq_alerts PRIMARY KEY (id),
    CONSTRAINT uk_job_error_hash UNIQUE (job_id, error_hash),
    CONSTRAINT fk_dlq_alert_job FOREIGN KEY (job_id) REFERENCES scheduler_job (job_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_dlq_sent_at ON scheduler_dlq_alerts (alert_sent_at);

-- 13. Active resource permits
CREATE TABLE IF NOT EXISTS scheduler_resource_permit
(
    id            RAW(16)       NOT NULL,
    resource_name VARCHAR2(100) NOT NULL,
    job_id        RAW(16)       NOT NULL,
    node_id       VARCHAR2(64)  NOT NULL,
    acquired_at   TIMESTAMP(6)  NOT NULL,
    CONSTRAINT pk_scheduler_resource_permit PRIMARY KEY (id),
    CONSTRAINT fk_resource_permit_job FOREIGN KEY (job_id) REFERENCES scheduler_job (job_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_resource_permit_resource ON scheduler_resource_permit (resource_name);
CREATE INDEX IF NOT EXISTS idx_resource_permit_job ON scheduler_resource_permit (job_id);

-- 14. Per-job extension properties (write-once indexed scalars; plaintext by design — no secrets)
CREATE TABLE IF NOT EXISTS scheduler_job_properties
(
    job_id       RAW(16)        NOT NULL,
    property_key VARCHAR2(255)  NOT NULL,
    value        VARCHAR2(1024),
    CONSTRAINT pk_scheduler_job_properties PRIMARY KEY (job_id, property_key),
    CONSTRAINT fk_job_properties_job FOREIGN KEY (job_id) REFERENCES scheduler_job (job_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_property_kv ON scheduler_job_properties (property_key, value);

-- 15. Per-job extension state (mutable per-namespace blobs with per-row CAS; encrypted at rest when
-- payload encryption is configured — state holds ciphertext, encrypted_state/encryption_key_id
-- mirror the scheduler_job payload-encryption metadata columns)
CREATE TABLE IF NOT EXISTS scheduler_job_extension_state
(
    job_id            RAW(16)      NOT NULL,
    namespace         VARCHAR2(64) NOT NULL,
    state             CLOB         NOT NULL,
    encrypted_state   BOOLEAN      DEFAULT FALSE NOT NULL,
    encryption_key_id VARCHAR2(256),
    version           NUMBER(10)   DEFAULT 0 NOT NULL,
    updated_at        TIMESTAMP(6) NOT NULL,
    CONSTRAINT pk_scheduler_job_extension_state PRIMARY KEY (job_id, namespace),
    CONSTRAINT fk_job_extension_state_job FOREIGN KEY (job_id) REFERENCES scheduler_job (job_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_extension_state_key_id ON scheduler_job_extension_state (encryption_key_id);
