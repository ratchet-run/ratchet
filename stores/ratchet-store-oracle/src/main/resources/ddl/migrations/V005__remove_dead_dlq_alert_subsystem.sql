-- ratchet:single-statement
DECLARE
    -- Removes the unused DLQ alert subsystem.
    table_count NUMBER;
BEGIN
    SELECT COUNT(*)
      INTO table_count
      FROM user_tables
     WHERE table_name = 'SCHEDULER_DLQ_ALERTS';

    IF table_count > 0 THEN
        EXECUTE IMMEDIATE 'DROP TABLE scheduler_dlq_alerts CASCADE CONSTRAINTS PURGE';
    END IF;

    -- DLQ_ALERT never had a persistence write path. Remove the half-shipped discriminator from
    -- the durable schema while preserving V001 as the immutable installation baseline.
    EXECUTE IMMEDIATE 'ALTER TABLE scheduler_job DROP CONSTRAINT chk_job_type';
    EXECUTE IMMEDIATE 'ALTER TABLE scheduler_job ADD CONSTRAINT chk_job_type CHECK (job_type IN (''SINGLE'', ''RECURRING'', ''BATCH_PARENT'', ''BATCH_CHILD'', ''CHAIN_STEP'', ''WORKFLOW_BRANCH'', ''WORKFLOW_JOIN''))';
    EXECUTE IMMEDIATE 'ALTER TABLE scheduler_job_queue DROP CONSTRAINT chk_queue_job_type';
    EXECUTE IMMEDIATE 'ALTER TABLE scheduler_job_queue ADD CONSTRAINT chk_queue_job_type CHECK (job_type IN (''SINGLE'', ''RECURRING'', ''BATCH_PARENT'', ''BATCH_CHILD'', ''CHAIN_STEP'', ''WORKFLOW_BRANCH'', ''WORKFLOW_JOIN''))';
    EXECUTE IMMEDIATE 'ALTER TABLE scheduler_job_archive DROP CONSTRAINT chk_archive_job_type';
    EXECUTE IMMEDIATE 'ALTER TABLE scheduler_job_archive ADD CONSTRAINT chk_archive_job_type CHECK (job_type IN (''SINGLE'', ''RECURRING'', ''BATCH_PARENT'', ''BATCH_CHILD'', ''CHAIN_STEP'', ''WORKFLOW_BRANCH'', ''WORKFLOW_JOIN''))';

END;
