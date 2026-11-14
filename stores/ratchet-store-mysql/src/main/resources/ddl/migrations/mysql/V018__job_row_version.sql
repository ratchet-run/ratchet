-- Add the job-row version counter, including adoption of a current consolidated schema.
SET @ratchet_row_version_ddl =
    (SELECT IF(COUNT(*) = 0,
               'ALTER TABLE scheduler_job ADD row_version INT NOT NULL DEFAULT 0',
               'SELECT 1')
       FROM information_schema.columns
      WHERE table_schema = DATABASE()
        AND table_name = 'scheduler_job'
        AND column_name = 'row_version');
PREPARE ratchet_row_version_statement FROM @ratchet_row_version_ddl;
EXECUTE ratchet_row_version_statement;
DEALLOCATE PREPARE ratchet_row_version_statement;
