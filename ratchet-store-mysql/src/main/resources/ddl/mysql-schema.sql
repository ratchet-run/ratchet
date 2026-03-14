-- Ratchet scheduler schema for MySQL
-- V1: Initial consolidated DDL

-- 1. Cluster nodes
CREATE TABLE scheduler_node
(
    node_id      VARCHAR(64) NOT NULL,
    heartbeat_ts DATETIME(6) NOT NULL,
    started_at   DATETIME(6) NOT NULL,
    node_info    JSON        NULL,
    PRIMARY KEY (node_id),
    INDEX idx_node_heartbeat (heartbeat_ts)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci;

-- 2. Distributed locks
CREATE TABLE scheduler_lock
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
CREATE TABLE scheduler_resource_limit
(
    resource_name  VARCHAR(100) NOT NULL,
    max_concurrent INT          NOT NULL,
    retry_delay_ms INT          NOT NULL DEFAULT 5000,
    description    VARCHAR(255) NULL,
    created_at     DATETIME(6)  NOT NULL,
    updated_at     DATETIME(6)  NOT NULL,
    PRIMARY KEY (resource_name)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci;

-- 4. Main job table
CREATE TABLE scheduler_job
(
    job_id                BIGINT UNSIGNED NOT NULL,
    status                ENUM ('PENDING','RUNNING','SUCCEEDED','FAILED','CANCELED','PAUSED')                                                 NOT NULL DEFAULT 'PENDING',
    paused_from_status    VARCHAR(20)                                                                                                         NULL,
    scheduled_time        DATETIME(6)                                                                                                         NOT NULL,
    job_type              ENUM ('SINGLE','RECURRING','BATCH_PARENT','BATCH_CHILD','CHAIN_STEP','DLQ_ALERT','WORKFLOW_BRANCH','WORKFLOW_JOIN') NOT NULL,
    priority              TINYINT UNSIGNED                                                                                                    NOT NULL DEFAULT 2,
    attempts              INT                                                                                                                 NOT NULL DEFAULT 0,
    max_retries           INT                                                                                                                 NOT NULL DEFAULT 0,
    backoff_policy        ENUM ('NONE','FIXED','EXPONENTIAL')                                                                                 NOT NULL DEFAULT 'NONE',
    backoff_param_ms      INT                                                                                                                 NOT NULL DEFAULT 0,
    timeout_sec           INT                                                                                                                 NOT NULL DEFAULT 0,
    cron_expr             VARCHAR(64)                                                                                                         NOT NULL DEFAULT '',
    zone_id               VARCHAR(32)                                                                                                         NOT NULL DEFAULT 'UTC',
    next_fire             DATETIME(6)                                                                                                         NULL,
    payload               JSON                                                                                                                NOT NULL,
    params                JSON                                                                                                                NULL,
    target_class          VARCHAR(255) GENERATED ALWAYS AS (JSON_UNQUOTE(JSON_EXTRACT(payload, '$.target'))) STORED,
    method_name           VARCHAR(128) GENERATED ALWAYS AS (JSON_UNQUOTE(JSON_EXTRACT(payload, '$.method'))) STORED,
    idempotency_key       VARCHAR(36)                                                                                                         NOT NULL,
    business_key          VARCHAR(255)                                                                                                        NULL,
    resource_name         VARCHAR(100)                                                                                                        NULL,
    depends_on            BIGINT UNSIGNED                                                                                                     NULL,
    superseded_by         BIGINT UNSIGNED                                                                                                     NULL,
    picked_by             VARCHAR(64)                                                                                                         NULL,
    picked_at             DATETIME(6)                                                                                                         NULL,
    last_error            TEXT                                                                                                                NULL,
    created_at            DATETIME(6)                                                                                                         NOT NULL,
    created_by            VARCHAR(255)                                                                                                        NULL,
    updated_at            DATETIME(6)                                                                                                         NOT NULL,
    execution_start_time  DATETIME(6)                                                                                                         NULL,
    execution_end_time    DATETIME(6)                                                                                                         NULL,
    execution_duration_ms BIGINT                                                                                                              NULL,
    queue_wait_ms         BIGINT                                                                                                              NULL,
    job_result            JSON                                                                                                                NULL,
    result_type           VARCHAR(100)                                                                                                        NULL,
    version               INT                                                                                                                 NOT NULL DEFAULT 0,
    active_business_key   VARCHAR(255) GENERATED ALWAYS AS (
        CASE WHEN status IN ('PENDING', 'RUNNING', 'PAUSED') THEN business_key ELSE NULL END
        ) STORED,
    PRIMARY KEY (job_id),
    UNIQUE KEY uk_idempotency_key (idempotency_key),
    UNIQUE KEY uk_active_business_key (active_business_key),
    CONSTRAINT chk_job_priority CHECK (priority BETWEEN 0 AND 4),
    INDEX idx_job_due (status, scheduled_time),
    INDEX idx_job_priority_due (priority, scheduled_time),
    INDEX idx_job_picked_by (picked_by),
    INDEX idx_target_class (target_class),
    INDEX idx_method_name (method_name),
    INDEX idx_recurring_due (status, next_fire),
    INDEX idx_job_poll_composite (status, priority, scheduled_time),
    INDEX idx_job_type (job_type),
    INDEX idx_job_recurring_composite (job_type, status, next_fire),
    INDEX idx_job_depends_on (depends_on),
    INDEX idx_job_superseded_by (superseded_by),
    INDEX idx_job_business_key (business_key),
    INDEX idx_job_created_at (created_at),
    INDEX idx_job_updated_at (updated_at)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci;

-- 5. Job tags (composite PK)
CREATE TABLE scheduler_job_tag
(
    job_id BIGINT UNSIGNED NOT NULL,
    tag    VARCHAR(64)     NOT NULL,
    PRIMARY KEY (job_id, tag),
    CONSTRAINT fk_job_tag_job FOREIGN KEY (job_id) REFERENCES scheduler_job (job_id) ON DELETE CASCADE
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci;

-- 6. Batch progress
CREATE TABLE scheduler_batch
(
    batch_id             BIGINT UNSIGNED NOT NULL,
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
CREATE TABLE scheduler_batch_metrics
(
    batch_id           BIGINT UNSIGNED NOT NULL,
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
CREATE TABLE scheduler_job_execution
(
    id            BIGINT UNSIGNED NOT NULL,
    job_id        BIGINT UNSIGNED                                  NOT NULL,
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
CREATE TABLE scheduler_job_log
(
    log_id  BIGINT UNSIGNED NOT NULL,
    job_id  BIGINT UNSIGNED                              NOT NULL,
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
CREATE TABLE scheduler_job_archive
(
    archive_id              BIGINT UNSIGNED NOT NULL,
    original_job_id         BIGINT UNSIGNED                                                                                                     NOT NULL,
    final_status            ENUM ('SUCCEEDED','FAILED','CANCELED')                                                                                 NOT NULL,
    job_type                ENUM ('SINGLE','RECURRING','BATCH_PARENT','BATCH_CHILD','CHAIN_STEP','DLQ_ALERT','WORKFLOW_BRANCH','WORKFLOW_JOIN') NOT NULL,
    priority                TINYINT UNSIGNED                                                                                                    NOT NULL,
    total_attempts          INT                                                                                                                 NOT NULL DEFAULT 0,
    max_retries             INT                                                                                                                 NOT NULL,
    backoff_policy          ENUM ('NONE','FIXED','EXPONENTIAL')                                                                                 NOT NULL,
    backoff_param_ms        INT                                                                                                                 NOT NULL,
    timeout_sec             INT                                                                                                                 NOT NULL,
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
    depended_on             BIGINT UNSIGNED                                                                                                     NULL,
    superseded_by           BIGINT UNSIGNED                                                                                                     NULL,
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
CREATE TABLE scheduler_workflow_condition
(
    id                   BIGINT UNSIGNED NOT NULL,
    parent_job_id        BIGINT UNSIGNED NOT NULL,
    child_job_id         BIGINT UNSIGNED NOT NULL,
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
CREATE TABLE scheduler_dlq_alerts
(
    id            BIGINT UNSIGNED NOT NULL,
    job_id        BIGINT UNSIGNED NOT NULL,
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
CREATE TABLE scheduler_resource_permit
(
    id            BIGINT UNSIGNED NOT NULL,
    resource_name VARCHAR(100)    NOT NULL,
    job_id        BIGINT UNSIGNED NOT NULL,
    node_id       VARCHAR(64)     NOT NULL,
    acquired_at   DATETIME(6)     NOT NULL,
    PRIMARY KEY (id),
    INDEX idx_resource_permit_resource (resource_name),
    INDEX idx_resource_permit_job (job_id),
    CONSTRAINT fk_resource_permit_job FOREIGN KEY (job_id) REFERENCES scheduler_job (job_id) ON DELETE CASCADE
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci;
