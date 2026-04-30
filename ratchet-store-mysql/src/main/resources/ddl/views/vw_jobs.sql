-- Read-only operator views: hyphenated UUID strings instead of raw BINARY(16).
--
-- BIN_TO_UUID(uuid_bin, 1) flips byte-order to match the Java/PG canonical
-- representation. Without the second arg, MySQL returns the timestamp-shifted
-- variant which doesn't match what Java's UUID.toString() produces.
--
-- These views are operator-only — Ratchet itself never reads from them. Apply
-- once, after the base schema, with:
--   mysql -u <user> -p ratchet < ratchet-store-mysql/src/main/resources/ddl/views/vw_jobs.sql

CREATE OR REPLACE VIEW vw_jobs AS
SELECT
    BIN_TO_UUID(j.job_id, 1)        AS job_id,
    BIN_TO_UUID(j.depends_on, 1)    AS depends_on,
    BIN_TO_UUID(j.superseded_by, 1) AS superseded_by,
    j.job_type,
    j.priority,
    j.max_retries,
    j.next_fire,
    j.payload,
    j.idempotency_key,
    j.business_key,
    j.created_at,
    j.created_by,
    j.terminal_status,
    j.terminated_at
FROM scheduler_job j;

-- Equivalent operator view for hot queue state.
CREATE OR REPLACE VIEW vw_job_queue AS
SELECT
    BIN_TO_UUID(q.job_id, 1) AS job_id,
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
    BIN_TO_UUID(e.id, 1)     AS id,
    BIN_TO_UUID(e.job_id, 1) AS job_id,
    e.attempt,
    e.node_id,
    e.started_at,
    e.ended_at,
    e.status,
    e.error_message,
    e.error_class,
    e.duration_ms
FROM scheduler_job_execution e;
