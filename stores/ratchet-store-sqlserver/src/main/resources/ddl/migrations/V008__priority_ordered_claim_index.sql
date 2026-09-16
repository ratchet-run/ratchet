-- Priority-ordered claims for one job type with priority boosting disabled.
-- Retain idx_claim_executable for selective due-time scans and boosted claims.
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
     WHERE name = 'idx_claim_pending_priority'
       AND object_id = OBJECT_ID('scheduler_job_queue'))
    CREATE INDEX idx_claim_pending_priority
        ON scheduler_job_queue (job_type, priority DESC, scheduled_time ASC, job_id ASC)
        WHERE status = 'PENDING';
