-- Read-only operator views: hyphenated UUID strings instead of raw RAW(16).
--
-- Oracle has no BIN_TO_UUID(); RAWTOHEX(col) renders the 16 stored bytes as 32 uppercase
-- hex characters, and REGEXP_REPLACE re-inserts the canonical 8-4-4-4-12 dashes. The store
-- writes UUIDs in standard big-endian byte order via UuidRawConverter, so LOWER() of the
-- formatted hex round-trips to java.util.UUID.toString() with no byte-order swap. NULL UUID
-- columns (depends_on, superseded_by) stay NULL because RAWTOHEX(NULL) is NULL.
--
-- These views are operator-only — Ratchet itself never reads from them. Apply once, after the
-- base schema, with:
--   sqlplus <user>/<pass>@<service> @stores/ratchet-store-oracle/src/main/resources/ddl/views/vw_jobs.sql

CREATE OR REPLACE VIEW vw_jobs AS
SELECT
    LOWER(REGEXP_REPLACE(RAWTOHEX(j.job_id),              '(.{8})(.{4})(.{4})(.{4})(.{12})', '\1-\2-\3-\4-\5')) AS job_id,
    LOWER(REGEXP_REPLACE(RAWTOHEX(j.depends_on),          '(.{8})(.{4})(.{4})(.{4})(.{12})', '\1-\2-\3-\4-\5')) AS depends_on,
    LOWER(REGEXP_REPLACE(RAWTOHEX(j.superseded_by),       '(.{8})(.{4})(.{4})(.{4})(.{12})', '\1-\2-\3-\4-\5')) AS superseded_by,
    LOWER(REGEXP_REPLACE(RAWTOHEX(j.recurring_master_id), '(.{8})(.{4})(.{4})(.{4})(.{12})', '\1-\2-\3-\4-\5')) AS recurring_master_id,
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
    LOWER(REGEXP_REPLACE(RAWTOHEX(r.id), '(.{8})(.{4})(.{4})(.{4})(.{12})', '\1-\2-\3-\4-\5')) AS id,
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
    LOWER(REGEXP_REPLACE(RAWTOHEX(a.id), '(.{8})(.{4})(.{4})(.{4})(.{12})', '\1-\2-\3-\4-\5')) AS id,
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
    LOWER(REGEXP_REPLACE(RAWTOHEX(q.job_id), '(.{8})(.{4})(.{4})(.{4})(.{12})', '\1-\2-\3-\4-\5')) AS job_id,
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
    LOWER(REGEXP_REPLACE(RAWTOHEX(e.id),     '(.{8})(.{4})(.{4})(.{4})(.{12})', '\1-\2-\3-\4-\5')) AS id,
    LOWER(REGEXP_REPLACE(RAWTOHEX(e.job_id), '(.{8})(.{4})(.{4})(.{4})(.{12})', '\1-\2-\3-\4-\5')) AS job_id,
    e.attempt,
    e.node_id,
    e.started_at,
    e.ended_at,
    e.status,
    e.error_message,
    e.error_class,
    e.duration_ms
FROM scheduler_job_execution e;
