-- Ratchet PostgreSQL V009 — Signal-waiting job support.
--
-- Signal state lives on scheduler_job_queue (the live row) because WAITING is a
-- non-terminal status. Signal delivery is a single UPDATE … WHERE status='WAITING'
-- on scheduler_job_queue, which avoids a read-modify-write race.
--
-- The status CHECK constraint is extended to include 'WAITING'.
-- Two indexes are added to scheduler_job_queue:
--   idx_signal_key_status  — broadcast delivery (WHERE signal_key = ? AND status = 'WAITING')
--   idx_signal_timeout_status — timeout scanner (WHERE status = 'WAITING' AND signal_timeout <= now)

ALTER TABLE scheduler_job_queue
    ADD COLUMN IF NOT EXISTS signal_key VARCHAR(255),
    ADD COLUMN IF NOT EXISTS signal_timeout TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS signal_payload TEXT,
    ADD COLUMN IF NOT EXISTS signal_payload_type VARCHAR(16),
    ADD COLUMN IF NOT EXISTS signal_outcome VARCHAR(32),
    ADD COLUMN IF NOT EXISTS signal_rejection_reason TEXT,
    ADD COLUMN IF NOT EXISTS signal_delivered_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS signal_delivered_by VARCHAR(255),
    ADD COLUMN IF NOT EXISTS signal_delivery_id VARCHAR(36);

-- Extend the status constraint to include WAITING.
ALTER TABLE scheduler_job_queue
    DROP CONSTRAINT IF EXISTS chk_queue_status;
ALTER TABLE scheduler_job_queue
    ADD CONSTRAINT chk_queue_status CHECK (status IN ('PENDING', 'RUNNING', 'PAUSED', 'WAITING'));

-- Extend paused_from_status constraint (pausing a WAITING job is rejected in code; no DDL change needed).

CREATE INDEX IF NOT EXISTS idx_signal_key_status
    ON scheduler_job_queue (signal_key, status);

CREATE INDEX IF NOT EXISTS idx_signal_timeout_status
    ON scheduler_job_queue (status, signal_timeout);

CREATE INDEX IF NOT EXISTS idx_signal_delivery_id
    ON scheduler_job_queue (signal_delivery_id);

INSERT INTO ratchet_schema_version (version, description)
VALUES ('009', 'Signal-waiting job columns, decision metadata, and indexes on scheduler_job_queue');
