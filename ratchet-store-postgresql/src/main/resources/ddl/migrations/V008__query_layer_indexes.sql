-- Ratchet PostgreSQL V008 — Query-layer: traceCorrelationId expression index + dashboard indexes.
--
-- The TracingCollector SPI contract requires that the key `traceparent` be present in the flat-map
-- returned by captureCurrentContext(). The WHERE clause `trace_context->>'traceparent' = ?`
-- matches this expression index exactly. Implementations that use a different propagation key
-- must add their own expression index with the correct key name.

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_sj_traceparent
    ON scheduler_job ((trace_context->>'traceparent'));

-- Dashboard priority query shapes:

-- (terminal_status, created_at) — findJobs(status=FAILED, sortField=CREATED_AT)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_job_terminal_status_created
    ON scheduler_job (terminal_status, created_at);

-- (caller_principal, created_at) — per-user job history dashboard
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_job_caller_principal_created
    ON scheduler_job (caller_principal, created_at);

-- (business_key) — idempotency and business-key lookups
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_job_business_key_query
    ON scheduler_job (business_key);

-- (depends_on) — batch/chain child listing
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_job_depends_on_query
    ON scheduler_job (depends_on);

-- Queue table: (status, scheduled_time) covers dashboard live-status queries
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_jq_status_scheduled
    ON scheduler_job_queue (status, scheduled_time);

-- Archive table: trace context absent from archive; no trace index needed.
-- For archive principal filtering, add caller_principal when required:
-- ALTER TABLE scheduler_job_archive ADD COLUMN IF NOT EXISTS caller_principal VARCHAR(255) NULL;
-- CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_archive_caller_principal
--     ON scheduler_job_archive (caller_principal);

INSERT INTO ratchet_schema_version (version, description)
VALUES ('008', 'Query-layer expression index for traceCorrelationId + dashboard indexes');
