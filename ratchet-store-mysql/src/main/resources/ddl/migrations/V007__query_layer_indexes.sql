-- Ratchet MySQL V007 — Query-layer: traceCorrelationId generated column + dashboard indexes.
--
-- trace_id_extracted: generated stored column extracting the W3C `traceparent` header from the
-- trace_context JSON blob. The TracingCollector SPI contract requires that the key `traceparent`
-- be present in the flat-map returned by captureCurrentContext(). Implementations that use a
-- different propagation key must override the DDL or omit traceCorrelationId filtering.
-- The W3C traceparent format is `XX-TTTTT...T-SSSS...S-FF` (max 55 chars).
--
-- `IF NOT EXISTS` on `ADD COLUMN` and `CREATE INDEX` is MariaDB-only syntax, not portable to
-- MySQL 8.x. SchemaMigrator's version table provides per-migration idempotency, so these
-- statements run unguarded.

ALTER TABLE scheduler_job
    ADD COLUMN trace_id_extracted VARCHAR(55)
        GENERATED ALWAYS AS (JSON_UNQUOTE(JSON_EXTRACT(trace_context, '$.traceparent'))) STORED NULL;

-- Index for traceCorrelationId lookups
CREATE INDEX idx_job_trace_id_extracted
    ON scheduler_job (trace_id_extracted);

-- Dashboard priority query shapes (not covered by execution-path indexes):

-- (status, created_at) — findJobs(status=FAILED, sortField=CREATED_AT)
CREATE INDEX idx_job_terminal_status_created
    ON scheduler_job (terminal_status, created_at);

-- (caller_principal, created_at) — per-user job history dashboard
CREATE INDEX idx_job_caller_principal_created
    ON scheduler_job (caller_principal, created_at);

-- (business_key) — idempotency and business-key lookups
CREATE INDEX idx_job_business_key_query
    ON scheduler_job (business_key);

-- (depends_on) — batch/chain child listing
CREATE INDEX idx_job_depends_on_query
    ON scheduler_job (depends_on);

-- Archive table: trace context is absent from archive; no trace index needed.
-- For archive principal filtering, add caller_principal to scheduler_job_archive when required:
-- ALTER TABLE scheduler_job_archive ADD COLUMN caller_principal VARCHAR(255) NULL;
-- CREATE INDEX idx_archive_caller_principal ON scheduler_job_archive (caller_principal);

INSERT INTO ratchet_schema_version (version, description)
VALUES ('007', 'Query-layer generated column for traceCorrelationId + dashboard indexes');
