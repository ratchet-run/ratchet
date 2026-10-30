-- Add the crash redelivery counter, including adoption of a current consolidated schema.
SET @ratchet_crash_count_ddl =
    (SELECT IF(COUNT(*) = 0,
               'ALTER TABLE scheduler_job_queue ADD crash_count INT NOT NULL DEFAULT 0',
               'SELECT 1')
       FROM information_schema.columns
      WHERE table_schema = DATABASE()
        AND table_name = 'scheduler_job_queue'
        AND column_name = 'crash_count');
PREPARE ratchet_crash_count_statement FROM @ratchet_crash_count_ddl;
EXECUTE ratchet_crash_count_statement;
DEALLOCATE PREPARE ratchet_crash_count_statement;
