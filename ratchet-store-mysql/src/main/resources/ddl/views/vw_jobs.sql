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
    BIN_TO_UUID(j.job_id)        AS job_id,
    BIN_TO_UUID(j.depends_on)    AS depends_on,
    BIN_TO_UUID(j.superseded_by) AS superseded_by,
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
