-- Ratchet PostgreSQL V006 — enqueue-time trace context propagation.
--
-- Stores the W3C TraceContext carrier map (traceparent / tracestate keys) captured at job
-- submission time. At execution start, TracingCollector receives this map and can create a child
-- span parented to the original caller's trace — enabling distributed tracing across the async
-- job boundary. Null when no TracingCollector is active or captureCurrentContext() returns empty.

ALTER TABLE scheduler_job
    ADD COLUMN IF NOT EXISTS trace_context JSONB;

INSERT INTO ratchet_schema_version (version, description)
VALUES ('006', 'Add trace_context propagation column to scheduler_job');
