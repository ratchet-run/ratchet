-- Ratchet canonical schema — SQL Server (Microsoft SQL Server 2022+ / Azure SQL).
--
-- Storage durables (mirror of the PostgreSQL/MySQL stores, dialect-translated):
--   * uuid            -> BINARY(16) holding the canonical big-endian 16 bytes (NOT UNIQUEIDENTIFIER,
--                        whose .NET-Guid mixed-endian storage EclipseLink 5.0 / GlassFish reads
--                        byte-swapped from native queries). Raw bytes read identically across every
--                        JPA provider; RowValues.uuidOrNull decodes the byte[]. Native writes
--                        pre-convert via UuidByteArrayConverter.toBytes; JPA-managed writes route
--                        through the converter (orm-sqlserver.xml on EclipseLink; Hibernate maps
--                        UUID -> BINARY(16) natively). Time-sortable, so a clustered PK inserts
--                        sequentially.
--   * JSONB / JSON    -> NVARCHAR(MAX) (SQL Server JSON is text + JSON_VALUE/OPENJSON functions).
--   * TIMESTAMPTZ(6)  -> DATETIME2(6). Ratchet stores UTC instants; DATETIME2 is zoneless so the
--                        UTC convention is preserved and DEFAULTs use SYSUTCDATETIME().
--   * BOOLEAN         -> BIT (0/1).
--   * generated cols  -> PERSISTED computed columns over JSON_VALUE(...), CAST to a bounded VARCHAR
--                        so they fit the nonclustered index key limit (1700 bytes).
--
-- SQL Server cannot index NVARCHAR(MAX) and has no MySQL-style prefix-length syntax, so every
-- indexed / filter-predicate / enum-ish column is a bounded VARCHAR(n); only genuinely freeform
-- columns (payloads, messages, errors) stay NVARCHAR(MAX).
--
-- This file is also the Testcontainers init script, executed by ScriptUtils which splits on ';'.
-- Keep every statement a single flat statement: no GO batch separators, no IF...BEGIN...END guards.
--
-- PERSISTED computed columns and filtered indexes require these session SET options ON. mssql-jdbc
-- already defaults them ON, but set them explicitly so the script is correct under any client.
SET QUOTED_IDENTIFIER ON;
SET ANSI_NULLS ON;

-- 0. ratchet_schema_version
-- Guarded create: the SchemaMigrator creates this table itself before applying V001, so a plain
-- CREATE would collide on the migrator path. (Fresh init-script runs simply create it.)
IF OBJECT_ID(N'ratchet_schema_version', N'U') IS NULL
CREATE TABLE ratchet_schema_version
(
    version     VARCHAR(20)  NOT NULL,
    applied_at  DATETIME2(6) NOT NULL DEFAULT SYSUTCDATETIME(),
    description VARCHAR(200) NOT NULL,
    checksum    VARCHAR(64),
    CONSTRAINT pk_ratchet_schema_version PRIMARY KEY (version)
);

-- 1. scheduler_node
CREATE TABLE scheduler_node
(
    node_id      VARCHAR(64)  NOT NULL,
    heartbeat_ts DATETIME2(6) NOT NULL,
    started_at   DATETIME2(6) NOT NULL,
    node_info    NVARCHAR(MAX),
    CONSTRAINT pk_scheduler_node PRIMARY KEY (node_id)
);

CREATE INDEX idx_node_heartbeat ON scheduler_node (heartbeat_ts);

-- 2. scheduler_lock
CREATE TABLE scheduler_lock
(
    lock_name  VARCHAR(128) NOT NULL,
    owner_node VARCHAR(64)  NOT NULL,
    locked_at  DATETIME2(6) NOT NULL,
    expires_at DATETIME2(6) NOT NULL,
    CONSTRAINT pk_scheduler_lock PRIMARY KEY (lock_name)
);

CREATE INDEX idx_lock_expires ON scheduler_lock (expires_at);

-- 3. scheduler_resource_limit
CREATE TABLE scheduler_resource_limit
(
    resource_name  VARCHAR(100) NOT NULL,
    max_concurrent INT          NOT NULL,
    retry_delay_ms INT          NOT NULL DEFAULT 5000,
    description    VARCHAR(255),
    created_at     DATETIME2(6) NOT NULL DEFAULT SYSUTCDATETIME(),
    updated_at     DATETIME2(6) NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT pk_scheduler_resource_limit PRIMARY KEY (resource_name)
);

-- 3a. scheduler_recurring_job — recurring-master definitions.
CREATE TABLE scheduler_recurring_job
(
    id                 BINARY(16) NOT NULL,
    priority           INT              NOT NULL DEFAULT 2,
    max_retries        INT              NOT NULL DEFAULT 0,
    backoff_policy     VARCHAR(16)      NOT NULL DEFAULT 'NONE',
    backoff_param_ms   INT              NOT NULL DEFAULT 0,
    timeout_sec        INT              NOT NULL DEFAULT 0,
    cron_expr          VARCHAR(64)      NOT NULL,
    zone_id            VARCHAR(32)      NOT NULL DEFAULT 'UTC',
    next_fire          DATETIME2(6)     NOT NULL,
    is_paused          BIT              NOT NULL DEFAULT 0,
    paused_at          DATETIME2(6),
    payload            NVARCHAR(MAX)    NOT NULL,
    on_success_payload NVARCHAR(MAX),
    on_failure_payload NVARCHAR(MAX),
    business_key       VARCHAR(512),
    resource_name      VARCHAR(100),
    execution_target   VARCHAR(64),
    target_class       AS (CAST(JSON_VALUE(payload, '$.target') AS VARCHAR(255))) PERSISTED,
    method_name        AS (CAST(JSON_VALUE(payload, '$.method') AS VARCHAR(128))) PERSISTED,
    created_at         DATETIME2(6)     NOT NULL DEFAULT SYSUTCDATETIME(),
    caller_principal   VARCHAR(255),
    encrypted_payload  BIT              NOT NULL DEFAULT 0,
    misfire_policy     VARCHAR(16)      NOT NULL DEFAULT 'CATCH_UP',
    max_catch_up_executions INT          NOT NULL DEFAULT 11,
    CONSTRAINT pk_scheduler_recurring_job PRIMARY KEY (id),
    CONSTRAINT chk_rec_priority CHECK (priority BETWEEN 0 AND 4),
    CONSTRAINT chk_rec_backoff_policy CHECK (backoff_policy IN ('NONE', 'FIXED', 'EXPONENTIAL')),
    CONSTRAINT chk_rec_misfire_policy CHECK (
        (misfire_policy = 'CATCH_UP' AND max_catch_up_executions >= 1)
        OR (misfire_policy IN ('SKIP', 'FIRE_ONCE') AND max_catch_up_executions = 0)
    )
);

-- Claim index: filtered on unpaused rows for claim-scan efficiency.
CREATE INDEX idx_rec_claim ON scheduler_recurring_job (next_fire) WHERE is_paused = 0;
CREATE INDEX idx_rec_business_key ON scheduler_recurring_job (business_key);
CREATE INDEX idx_rec_target_class ON scheduler_recurring_job (target_class);

-- 3b. scheduler_recurring_job_archive.
CREATE TABLE scheduler_recurring_job_archive
(
    id                 BINARY(16) NOT NULL,
    cron_expr          VARCHAR(64)      NOT NULL,
    zone_id            VARCHAR(32)      NOT NULL,
    payload            NVARCHAR(MAX)    NOT NULL,
    on_success_payload NVARCHAR(MAX),
    on_failure_payload NVARCHAR(MAX),
    business_key       VARCHAR(512),
    execution_target   VARCHAR(64),
    created_at         DATETIME2(6)     NOT NULL,
    caller_principal   VARCHAR(255),
    archived_at        DATETIME2(6)     NOT NULL DEFAULT SYSUTCDATETIME(),
    archive_reason     VARCHAR(16)      NOT NULL,
    CONSTRAINT pk_scheduler_recurring_job_archive PRIMARY KEY (id),
    CONSTRAINT chk_recurring_archive_reason CHECK (archive_reason IN ('CANCELED', 'EXHAUSTED'))
);

CREATE INDEX idx_archive_rec_business_key ON scheduler_recurring_job_archive (business_key);
CREATE INDEX idx_archive_rec_archived_at ON scheduler_recurring_job_archive (archived_at);

-- 4. scheduler_job — COLD metadata + terminal fields.
CREATE TABLE scheduler_job
(
    job_id                BINARY(16) NOT NULL,
    job_type              VARCHAR(20)      NOT NULL,
    priority              INT              NOT NULL DEFAULT 2,
    max_retries           INT              NOT NULL DEFAULT 0,
    backoff_policy        VARCHAR(16)      NOT NULL DEFAULT 'NONE',
    backoff_param_ms      INT              NOT NULL DEFAULT 0,
    timeout_sec           INT              NOT NULL DEFAULT 0,
    cron_expr             VARCHAR(64)      NOT NULL DEFAULT '',
    zone_id               VARCHAR(32)      NOT NULL DEFAULT 'UTC',
    payload               NVARCHAR(MAX)    NOT NULL,
    params                NVARCHAR(MAX),
    trace_context         NVARCHAR(MAX),
    target_class          AS (CAST(JSON_VALUE(payload, '$.target') AS VARCHAR(255))) PERSISTED,
    method_name           AS (CAST(JSON_VALUE(payload, '$.method') AS VARCHAR(128))) PERSISTED,
    idempotency_key       VARCHAR(36)      NOT NULL,
    business_key          VARCHAR(512),
    resource_name         VARCHAR(100),
    execution_target      VARCHAR(64),
    on_success_payload    NVARCHAR(MAX),
    on_failure_payload    NVARCHAR(MAX),
    depends_on            BINARY(16),
    superseded_by         BINARY(16),
    created_at            DATETIME2(6)     NOT NULL DEFAULT SYSUTCDATETIME(),
    caller_principal      VARCHAR(255),
    terminal_status       VARCHAR(16),
    terminal_error        NVARCHAR(MAX),
    total_attempts        INT,
    terminated_at         DATETIME2(6),
    execution_start_time  DATETIME2(6),
    execution_end_time    DATETIME2(6),
    execution_duration_ms BIGINT,
    queue_wait_ms         BIGINT,
    job_result            NVARCHAR(MAX),
    result_type           VARCHAR(100),
    recurring_master_id   BINARY(16),
    encrypted_payload     BIT              NOT NULL DEFAULT 0,
    encryption_key_id     VARCHAR(256),
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
CREATE TABLE scheduler_job_queue
(
    job_id                  BINARY(16) NOT NULL,
    status                  VARCHAR(16)      NOT NULL DEFAULT 'PENDING',
    job_type                VARCHAR(20)      NOT NULL,
    priority                INT              NOT NULL DEFAULT 2,
    scheduled_time          DATETIME2(6)     NOT NULL,
    business_key            VARCHAR(512),
    timeout_sec             INT              NOT NULL DEFAULT 0,
    max_retries             INT              NOT NULL DEFAULT 0,
    attempts                INT              NOT NULL DEFAULT 0,
    picked_by               VARCHAR(64),
    picked_at               DATETIME2(6),
    paused_from_status      VARCHAR(16),
    last_error              NVARCHAR(MAX),
    version                 INT              NOT NULL DEFAULT 0,
    updated_at              DATETIME2(6)     NOT NULL DEFAULT SYSUTCDATETIME(),
    signal_key              VARCHAR(255),
    signal_timeout          DATETIME2(6),
    signal_payload          NVARCHAR(MAX),
    signal_payload_type     VARCHAR(16),
    signal_outcome          VARCHAR(32),
    signal_rejection_reason NVARCHAR(MAX),
    signal_delivered_at     DATETIME2(6),
    signal_delivered_by     VARCHAR(255),
    signal_delivery_id      VARCHAR(36),
    execution_target        VARCHAR(64),
    CONSTRAINT pk_scheduler_job_queue PRIMARY KEY (job_id),
    CONSTRAINT chk_queue_status CHECK (status IN ('PENDING', 'RUNNING', 'PAUSED', 'WAITING')),
    CONSTRAINT chk_queue_job_type CHECK (job_type IN
                                         ('SINGLE', 'RECURRING', 'BATCH_PARENT', 'BATCH_CHILD',
                                          'CHAIN_STEP', 'DLQ_ALERT', 'WORKFLOW_BRANCH', 'WORKFLOW_JOIN')),
    CONSTRAINT chk_queue_priority CHECK (priority BETWEEN 0 AND 4),
    CONSTRAINT chk_queue_paused_from_status CHECK (paused_from_status IS NULL OR paused_from_status IN ('PENDING', 'RUNNING', 'PAUSED')),
    CONSTRAINT fk_job_queue_job FOREIGN KEY (job_id) REFERENCES scheduler_job (job_id) ON DELETE CASCADE
);

-- Hot-path claim index. Filtered on PENDING.
CREATE INDEX idx_claim_executable
    ON scheduler_job_queue (job_type, scheduled_time ASC, priority DESC, job_id ASC)
    WHERE status = 'PENDING';

CREATE INDEX idx_queue_orphan ON scheduler_job_queue (status, picked_at, picked_by);
CREATE INDEX idx_signal_key_status ON scheduler_job_queue (signal_key, status);
CREATE INDEX idx_signal_timeout_status ON scheduler_job_queue (status, signal_timeout);
CREATE INDEX idx_signal_delivery_id ON scheduler_job_queue (signal_delivery_id);

-- 4b. Business-key active-uniqueness reservation table.
CREATE TABLE scheduler_business_key_reservation
(
    business_key VARCHAR(512)     NOT NULL,
    owner_job_id BINARY(16) NOT NULL,
    owner_table  VARCHAR(16)      NOT NULL,
    reserved_at  DATETIME2(6)     NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT pk_scheduler_business_key_reservation PRIMARY KEY (business_key),
    CONSTRAINT chk_bk_owner_table CHECK (owner_table IN ('QUEUE', 'RECURRING'))
);

CREATE INDEX idx_bk_owner ON scheduler_business_key_reservation (owner_job_id);

-- Lookup/relationship indexes (cold).
CREATE INDEX idx_job_depends_on ON scheduler_job (depends_on);
CREATE INDEX idx_job_superseded_by ON scheduler_job (superseded_by);
CREATE INDEX idx_job_business_key ON scheduler_job (business_key);
CREATE INDEX idx_job_created_at ON scheduler_job (created_at);
CREATE INDEX idx_job_terminal ON scheduler_job (terminal_status, terminated_at);
CREATE INDEX idx_job_recurring_master_id ON scheduler_job (recurring_master_id);
CREATE INDEX idx_job_encryption_key_id ON scheduler_job (encryption_key_id);

-- 5. scheduler_job_tag
CREATE TABLE scheduler_job_tag
(
    job_id BINARY(16) NOT NULL,
    tag    VARCHAR(64)      NOT NULL,
    CONSTRAINT pk_scheduler_job_tag PRIMARY KEY (job_id, tag)
);

CREATE INDEX idx_job_tag_tag_job ON scheduler_job_tag (tag, job_id);

-- 6. scheduler_batch
CREATE TABLE scheduler_batch
(
    batch_id             BINARY(16) NOT NULL,
    total_items          INT              NOT NULL DEFAULT 0,
    completed_items      INT              NOT NULL DEFAULT 0,
    failed_items         INT              NOT NULL DEFAULT 0,
    completion_processed BIT              NOT NULL DEFAULT 0,
    version              INT              NOT NULL DEFAULT 0,
    progress_hook        NVARCHAR(MAX),
    CONSTRAINT pk_scheduler_batch PRIMARY KEY (batch_id),
    CONSTRAINT fk_batch_job FOREIGN KEY (batch_id) REFERENCES scheduler_job (job_id) ON DELETE CASCADE
);

-- 7. scheduler_batch_metrics
CREATE TABLE scheduler_batch_metrics
(
    batch_id           BINARY(16) NOT NULL,
    total_duration_ms  BIGINT,
    child_execution_ms BIGINT,
    overhead_ms        BIGINT,
    child_count        INT              NOT NULL DEFAULT 0,
    success_count      INT              NOT NULL DEFAULT 0,
    failure_count      INT              NOT NULL DEFAULT 0,
    started_at         DATETIME2(6),
    completed_at       DATETIME2(6),
    version            INT              NOT NULL DEFAULT 0,
    CONSTRAINT pk_scheduler_batch_metrics PRIMARY KEY (batch_id),
    CONSTRAINT fk_batch_metrics_job FOREIGN KEY (batch_id) REFERENCES scheduler_job (job_id) ON DELETE CASCADE
);

-- 8. scheduler_job_execution
CREATE TABLE scheduler_job_execution
(
    id            BINARY(16) NOT NULL,
    job_id        BINARY(16) NOT NULL,
    attempt       INT              NOT NULL,
    node_id       VARCHAR(64)      NOT NULL,
    started_at    DATETIME2(6)     NOT NULL,
    ended_at      DATETIME2(6),
    status        VARCHAR(16)      NOT NULL DEFAULT 'RUNNING',
    error_message NVARCHAR(MAX),
    error_class   VARCHAR(255),
    duration_ms   BIGINT,
    CONSTRAINT pk_scheduler_job_execution PRIMARY KEY (id),
    CONSTRAINT chk_execution_status CHECK (status IN ('RUNNING', 'SUCCEEDED', 'FAILED', 'CANCELED')),
    CONSTRAINT fk_execution_job FOREIGN KEY (job_id) REFERENCES scheduler_job (job_id) ON DELETE CASCADE
);

CREATE INDEX idx_job_execution_job ON scheduler_job_execution (job_id);
CREATE INDEX idx_job_execution_node ON scheduler_job_execution (node_id, started_at);
CREATE INDEX idx_job_execution_status ON scheduler_job_execution (status, started_at);

-- 9. scheduler_job_log
CREATE TABLE scheduler_job_log
(
    log_id  BINARY(16) NOT NULL,
    job_id  BINARY(16) NOT NULL,
    ts      DATETIME2(6)     NOT NULL,
    level   VARCHAR(8)       NOT NULL,
    message NVARCHAR(MAX)    NOT NULL,
    mdc     NVARCHAR(MAX),
    CONSTRAINT pk_scheduler_job_log PRIMARY KEY (log_id),
    CONSTRAINT fk_log_job FOREIGN KEY (job_id) REFERENCES scheduler_job (job_id) ON DELETE CASCADE,
    CONSTRAINT chk_log_level CHECK (level IN ('TRACE', 'DEBUG', 'INFO', 'WARN', 'ERROR'))
);

CREATE INDEX idx_joblog_job_ts ON scheduler_job_log (job_id, ts);
CREATE INDEX idx_joblog_ts ON scheduler_job_log (ts);

-- 10. scheduler_job_archive
CREATE TABLE scheduler_job_archive
(
    archive_id              BINARY(16) NOT NULL,
    original_job_id         BINARY(16) NOT NULL,
    final_status            VARCHAR(16)      NOT NULL,
    job_type                VARCHAR(20)      NOT NULL,
    priority                INT              NOT NULL,
    total_attempts          INT              NOT NULL DEFAULT 0,
    max_retries             INT              NOT NULL DEFAULT 0,
    backoff_policy          VARCHAR(16)      NOT NULL DEFAULT 'NONE',
    backoff_param_ms        INT              NOT NULL DEFAULT 0,
    timeout_sec             INT              NOT NULL DEFAULT 0,
    target_class            VARCHAR(255),
    method_name             VARCHAR(128),
    business_key            VARCHAR(512),
    cron_expr               VARCHAR(64),
    zone_id                 VARCHAR(32),
    original_scheduled_time DATETIME2(6)     NOT NULL,
    original_created_at     DATETIME2(6)     NOT NULL,
    first_execution_time    DATETIME2(6),
    completion_time         DATETIME2(6),
    total_execution_time_ms BIGINT,
    queue_wait_ms           BIGINT,
    archived_at             DATETIME2(6)     NOT NULL DEFAULT SYSUTCDATETIME(),
    archived_by             VARCHAR(64),
    archive_reason          VARCHAR(128),
    job_result              NVARCHAR(MAX),
    result_type             VARCHAR(100),
    final_error             NVARCHAR(MAX),
    payload_summary         NVARCHAR(MAX),
    depended_on             BINARY(16),
    superseded_by           BINARY(16),
    tags                    VARCHAR(512),
    properties              NVARCHAR(MAX),
    extension_state         NVARCHAR(MAX),
    CONSTRAINT pk_scheduler_job_archive PRIMARY KEY (archive_id),
    CONSTRAINT chk_archive_status CHECK (final_status IN ('SUCCEEDED', 'FAILED', 'CANCELED')),
    CONSTRAINT chk_archive_job_type CHECK (job_type IN
                                           ('SINGLE', 'RECURRING', 'BATCH_PARENT', 'BATCH_CHILD',
                                            'CHAIN_STEP', 'DLQ_ALERT', 'WORKFLOW_BRANCH', 'WORKFLOW_JOIN')),
    CONSTRAINT chk_archive_priority CHECK (priority BETWEEN 0 AND 4),
    CONSTRAINT chk_archive_backoff_policy CHECK (backoff_policy IN ('NONE', 'FIXED', 'EXPONENTIAL'))
);

CREATE INDEX idx_archive_original_id ON scheduler_job_archive (original_job_id);
CREATE INDEX idx_archive_status ON scheduler_job_archive (final_status);
CREATE INDEX idx_archive_created_range ON scheduler_job_archive (original_created_at);
CREATE INDEX idx_archive_completed_range ON scheduler_job_archive (completion_time);
CREATE INDEX idx_archive_archived_at ON scheduler_job_archive (archived_at);
CREATE INDEX idx_archive_target_class ON scheduler_job_archive (target_class);
CREATE INDEX idx_archive_business_key ON scheduler_job_archive (business_key);
CREATE INDEX idx_archive_job_type ON scheduler_job_archive (job_type);
CREATE INDEX idx_archive_priority ON scheduler_job_archive (priority);

-- 11. scheduler_workflow_condition
-- SQL Server forbids two ON DELETE CASCADE foreign keys from one table to the same parent
-- ("multiple cascade paths"). Both parent_job_id and child_job_id reference scheduler_job, so
-- neither cascades; the store deletes condition rows explicitly on job removal.
CREATE TABLE scheduler_workflow_condition
(
    id                   BINARY(16) NOT NULL,
    parent_job_id        BINARY(16) NOT NULL,
    child_job_id         BINARY(16) NOT NULL,
    condition_type       VARCHAR(32)      NOT NULL,
    condition_expression NVARCHAR(MAX),
    condition_priority   INT              NOT NULL DEFAULT 0,
    definition_order     INT              NOT NULL DEFAULT 0,
    created_at           DATETIME2(6)     NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT pk_scheduler_workflow_condition PRIMARY KEY (id),
    CONSTRAINT fk_workflow_parent FOREIGN KEY (parent_job_id) REFERENCES scheduler_job (job_id),
    CONSTRAINT fk_workflow_child FOREIGN KEY (child_job_id) REFERENCES scheduler_job (job_id),
    CONSTRAINT chk_condition_type CHECK (condition_type IN
                                         ('SUCCESS', 'FAILURE', 'CUSTOM', 'RESULT_VALUE',
                                          'BATCH_SUCCESS', 'BATCH_FAILURE', 'BATCH_SUCCESS_RATE',
                                          'BATCH_FAILURE_COUNT', 'BATCH_CUSTOM'))
);

CREATE INDEX idx_workflow_parent ON scheduler_workflow_condition (parent_job_id);
CREATE INDEX idx_workflow_child ON scheduler_workflow_condition (child_job_id);
CREATE INDEX idx_workflow_priority ON scheduler_workflow_condition (parent_job_id, condition_priority);
CREATE INDEX idx_workflow_evaluation_order ON scheduler_workflow_condition (parent_job_id, condition_priority, definition_order);

-- 12. scheduler_dlq_alerts
CREATE TABLE scheduler_dlq_alerts
(
    id            BINARY(16) NOT NULL,
    job_id        BINARY(16) NOT NULL,
    error_hash    VARCHAR(64)      NOT NULL,
    alert_sent_at DATETIME2(6),
    alert_channel VARCHAR(100),
    CONSTRAINT pk_scheduler_dlq_alerts PRIMARY KEY (id),
    CONSTRAINT uk_job_error_hash UNIQUE (job_id, error_hash),
    CONSTRAINT fk_dlq_alert_job FOREIGN KEY (job_id) REFERENCES scheduler_job (job_id) ON DELETE CASCADE
);

CREATE INDEX idx_dlq_sent_at ON scheduler_dlq_alerts (alert_sent_at);

-- 13. scheduler_resource_permit
CREATE TABLE scheduler_resource_permit
(
    id            BINARY(16) NOT NULL,
    resource_name VARCHAR(100)     NOT NULL,
    job_id        BINARY(16) NOT NULL,
    node_id       VARCHAR(64)      NOT NULL,
    acquired_at   DATETIME2(6)     NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT pk_scheduler_resource_permit PRIMARY KEY (id),
    CONSTRAINT fk_resource_permit_job FOREIGN KEY (job_id) REFERENCES scheduler_job (job_id) ON DELETE CASCADE
);

CREATE INDEX idx_resource_permit_resource ON scheduler_resource_permit (resource_name);
CREATE INDEX idx_resource_permit_job ON scheduler_resource_permit (job_id);

-- 14. scheduler_job_properties (write-once indexed scalars; plaintext by design — no secrets)
-- property_key/value are non-unicode VARCHAR so the (property_key, value) index key fits SQL
-- Server's 1700-byte nonclustered-index limit (255 + 1024 = 1279 < 1700); block metadata is ASCII.
CREATE TABLE scheduler_job_properties
(
    job_id       BINARY(16)    NOT NULL,
    property_key VARCHAR(255)  NOT NULL,
    value        VARCHAR(1024),
    CONSTRAINT pk_scheduler_job_properties PRIMARY KEY (job_id, property_key),
    CONSTRAINT fk_job_properties_job FOREIGN KEY (job_id) REFERENCES scheduler_job (job_id) ON DELETE CASCADE
);

CREATE INDEX idx_property_kv ON scheduler_job_properties (property_key, value);

-- 15. scheduler_job_extension_state (mutable per-namespace blobs with per-row CAS; encrypted at rest
-- when payload encryption is configured — state holds ciphertext, encrypted_state/encryption_key_id
-- mirror the scheduler_job payload-encryption metadata columns)
CREATE TABLE scheduler_job_extension_state
(
    job_id            BINARY(16)    NOT NULL,
    namespace         VARCHAR(64)   NOT NULL,
    state             NVARCHAR(MAX) NOT NULL,
    encrypted_state   BIT           NOT NULL DEFAULT 0,
    encryption_key_id VARCHAR(256),
    version           INT           NOT NULL DEFAULT 0,
    updated_at        DATETIME2(6)  NOT NULL,
    CONSTRAINT pk_scheduler_job_extension_state PRIMARY KEY (job_id, namespace),
    CONSTRAINT fk_job_extension_state_job FOREIGN KEY (job_id) REFERENCES scheduler_job (job_id) ON DELETE CASCADE
);

CREATE INDEX idx_extension_state_key_id ON scheduler_job_extension_state (encryption_key_id);
