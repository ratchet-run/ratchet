-- Priority-ordered claims for one job type with priority boosting disabled.
-- Retain idx_claim_executable for selective due-time scans and boosted claims.
SET @ratchet_priority_index_ddl =
    (SELECT IF(COUNT(*) = 0,
               'CREATE INDEX idx_claim_pending_priority ON scheduler_job_queue (status, job_type, priority DESC, scheduled_time ASC, job_id ASC)',
               'SELECT 1')
       FROM information_schema.statistics
      WHERE table_schema = DATABASE()
        AND table_name = 'scheduler_job_queue'
        AND index_name = 'idx_claim_pending_priority');
PREPARE ratchet_priority_index_statement FROM @ratchet_priority_index_ddl;
EXECUTE ratchet_priority_index_statement;
DEALLOCATE PREPARE ratchet_priority_index_statement;
