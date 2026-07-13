-- Removes the unused DLQ alert subsystem.
DROP TABLE IF EXISTS scheduler_dlq_alerts;

-- DLQ_ALERT never had a persistence write path. Remove the half-shipped discriminator from the
-- durable schema while preserving V001 as the immutable installation baseline.
ALTER TABLE scheduler_job
    MODIFY COLUMN job_type ENUM ('SINGLE','RECURRING','BATCH_PARENT','BATCH_CHILD','CHAIN_STEP','WORKFLOW_BRANCH','WORKFLOW_JOIN') NOT NULL;
ALTER TABLE scheduler_job_queue
    MODIFY COLUMN job_type ENUM ('SINGLE','RECURRING','BATCH_PARENT','BATCH_CHILD','CHAIN_STEP','WORKFLOW_BRANCH','WORKFLOW_JOIN') NOT NULL;
ALTER TABLE scheduler_job_archive
    MODIFY COLUMN job_type ENUM ('SINGLE','RECURRING','BATCH_PARENT','BATCH_CHILD','CHAIN_STEP','WORKFLOW_BRANCH','WORKFLOW_JOIN') NOT NULL;
