-- Removes the unused DLQ alert subsystem.
DROP TABLE IF EXISTS scheduler_dlq_alerts;

-- DLQ_ALERT never had a persistence write path. Remove the half-shipped discriminator from the
-- durable schema while preserving V001 as the immutable installation baseline.
ALTER TABLE scheduler_job DROP CONSTRAINT IF EXISTS chk_job_type;
ALTER TABLE scheduler_job
    ADD CONSTRAINT chk_job_type CHECK (job_type IN
        ('SINGLE', 'RECURRING', 'BATCH_PARENT', 'BATCH_CHILD', 'CHAIN_STEP', 'WORKFLOW_BRANCH', 'WORKFLOW_JOIN'));
ALTER TABLE scheduler_job_queue DROP CONSTRAINT IF EXISTS chk_queue_job_type;
ALTER TABLE scheduler_job_queue
    ADD CONSTRAINT chk_queue_job_type CHECK (job_type IN
        ('SINGLE', 'RECURRING', 'BATCH_PARENT', 'BATCH_CHILD', 'CHAIN_STEP', 'WORKFLOW_BRANCH', 'WORKFLOW_JOIN'));
ALTER TABLE scheduler_job_archive DROP CONSTRAINT IF EXISTS chk_archive_job_type;
ALTER TABLE scheduler_job_archive
    ADD CONSTRAINT chk_archive_job_type CHECK (job_type IN
        ('SINGLE', 'RECURRING', 'BATCH_PARENT', 'BATCH_CHILD', 'CHAIN_STEP', 'WORKFLOW_BRANCH', 'WORKFLOW_JOIN'));
