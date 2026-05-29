-- Read-only operator views: hyphenated UUID strings instead of raw BINARY(16).
--
-- BIN_TO_UUID(col) (no swap flag) reads bytes in standard order. The MySQL store
-- writes UUIDs in standard byte order via Hibernate's built-in UUID handler /
-- UuidByteArrayConverter under EclipseLink, matching java.util.UUID.toString().
-- Passing the v1-time-reorder flag (BIN_TO_UUID(col, 1)) would scramble the
-- first 8 bytes on read because nothing on the write side performs the inverse
-- swap; results would not match any stored row.
--
-- These views are operator-only — Ratchet itself never reads from them. Apply
-- once, after the base schema, with:
--   mysql -u <user> -p ratchet < ratchet-store-mysql/src/main/resources/ddl/views/vw_jobs.sql

CREATE OR REPLACE VIEW vw_jobs AS
SELECT
    BIN_TO_UUID(j.job_id)              AS job_id,
    BIN_TO_UUID(j.depends_on)          AS depends_on,
    BIN_TO_UUID(j.superseded_by)       AS superseded_by,
    BIN_TO_UUID(j.recurring_master_id) AS recurring_master_id,
    j.job_type,
    j.priority,
    j.max_retries,
    j.payload,
    j.idempotency_key,
    j.business_key,
    j.created_at,
    j.caller_principal AS created_by,
    j.terminal_status,
    j.terminated_at
FROM scheduler_job j;

-- Operator view for recurring-master definitions.
CREATE OR REPLACE VIEW vw_recurring_jobs AS
SELECT
    BIN_TO_UUID(r.id) AS id,
    r.cron_expr,
    r.zone_id,
    r.next_fire,
    r.is_paused,
    r.paused_at,
    r.priority,
    r.max_retries,
    r.business_key,
    r.resource_name,
    r.execution_target,
    r.target_class,
    r.method_name,
    r.created_at,
    r.caller_principal
FROM scheduler_recurring_job r;

-- Operator view for archived recurring definitions.
CREATE OR REPLACE VIEW vw_recurring_jobs_archive AS
SELECT
    BIN_TO_UUID(a.id) AS id,
    a.cron_expr,
    a.zone_id,
    a.business_key,
    a.execution_target,
    a.created_at,
    a.caller_principal,
    a.archived_at,
    a.archive_reason
FROM scheduler_recurring_job_archive a;

-- Equivalent operator view for hot queue state.
CREATE OR REPLACE VIEW vw_job_queue AS
SELECT
    BIN_TO_UUID(q.job_id) AS job_id,
    q.status,
    q.job_type,
    q.scheduled_time,
    q.attempts,
    q.picked_by,
    q.picked_at,
    q.last_error,
    q.version,
    q.updated_at
FROM scheduler_job_queue q;

-- Equivalent operator view for execution audit history.
CREATE OR REPLACE VIEW vw_job_execution AS
SELECT
    BIN_TO_UUID(e.id)     AS id,
    BIN_TO_UUID(e.job_id) AS job_id,
    e.attempt,
    e.node_id,
    e.started_at,
    e.ended_at,
    e.status,
    e.error_message,
    e.error_class,
    e.duration_ms
FROM scheduler_job_execution e;
