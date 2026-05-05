-- Ratchet MySQL V008 — Signal-waiting job support.
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
    ADD COLUMN IF NOT EXISTS signal_key VARCHAR(255) NULL,
    ADD COLUMN IF NOT EXISTS signal_timeout DATETIME(3) NULL,
    ADD COLUMN IF NOT EXISTS signal_payload TEXT NULL,
    ADD COLUMN IF NOT EXISTS signal_payload_type VARCHAR(16) NULL,
    ADD COLUMN IF NOT EXISTS signal_outcome VARCHAR(32) NULL,
    ADD COLUMN IF NOT EXISTS signal_rejection_reason TEXT NULL,
    ADD COLUMN IF NOT EXISTS signal_delivered_at DATETIME(3) NULL,
    ADD COLUMN IF NOT EXISTS signal_delivered_by VARCHAR(255) NULL,
    ADD COLUMN IF NOT EXISTS signal_delivery_id VARCHAR(36) NULL;

-- Extend the status constraint to include WAITING.
ALTER TABLE scheduler_job_queue
    MODIFY COLUMN status ENUM('PENDING','RUNNING','PAUSED','WAITING') NOT NULL DEFAULT 'PENDING';

CREATE INDEX IF NOT EXISTS idx_signal_key_status
    ON scheduler_job_queue (signal_key, status);

CREATE INDEX IF NOT EXISTS idx_signal_timeout_status
    ON scheduler_job_queue (status, signal_timeout);

CREATE INDEX IF NOT EXISTS idx_signal_delivery_id
    ON scheduler_job_queue (signal_delivery_id);

INSERT INTO ratchet_schema_version (version, description)
VALUES ('008', 'Signal-waiting job columns, decision metadata, and indexes on scheduler_job_queue');
