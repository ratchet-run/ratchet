-- The JPA converter maps JobPriority to Integer, so Hibernate schema validation expects INTEGER.
-- PostgreSQL and SQL Server already use INT. Changing the type rebuilds each table with a copy
-- and blocks writes until done; large installs should run this migration in a quiet window.

SET @ratchet_priority_recurring_job_ddl =
    (SELECT IF(COUNT(*) > 0,
               'ALTER TABLE scheduler_recurring_job MODIFY priority INT NOT NULL DEFAULT 2',
               'SELECT 1')
       FROM information_schema.columns
      WHERE table_schema = DATABASE()
        AND table_name = 'scheduler_recurring_job'
        AND column_name = 'priority'
        AND DATA_TYPE = 'tinyint');
PREPARE ratchet_priority_recurring_job_statement FROM @ratchet_priority_recurring_job_ddl;
EXECUTE ratchet_priority_recurring_job_statement;
DEALLOCATE PREPARE ratchet_priority_recurring_job_statement;

SET @ratchet_priority_job_ddl =
    (SELECT IF(COUNT(*) > 0,
               'ALTER TABLE scheduler_job MODIFY priority INT NOT NULL DEFAULT 2',
               'SELECT 1')
       FROM information_schema.columns
      WHERE table_schema = DATABASE()
        AND table_name = 'scheduler_job'
        AND column_name = 'priority'
        AND DATA_TYPE = 'tinyint');
PREPARE ratchet_priority_job_statement FROM @ratchet_priority_job_ddl;
EXECUTE ratchet_priority_job_statement;
DEALLOCATE PREPARE ratchet_priority_job_statement;

SET @ratchet_priority_job_queue_ddl =
    (SELECT IF(COUNT(*) > 0,
               'ALTER TABLE scheduler_job_queue MODIFY priority INT NOT NULL DEFAULT 2',
               'SELECT 1')
       FROM information_schema.columns
      WHERE table_schema = DATABASE()
        AND table_name = 'scheduler_job_queue'
        AND column_name = 'priority'
        AND DATA_TYPE = 'tinyint');
PREPARE ratchet_priority_job_queue_statement FROM @ratchet_priority_job_queue_ddl;
EXECUTE ratchet_priority_job_queue_statement;
DEALLOCATE PREPARE ratchet_priority_job_queue_statement;

SET @ratchet_priority_job_archive_ddl =
    (SELECT IF(COUNT(*) > 0,
               'ALTER TABLE scheduler_job_archive MODIFY priority INT NOT NULL',
               'SELECT 1')
       FROM information_schema.columns
      WHERE table_schema = DATABASE()
        AND table_name = 'scheduler_job_archive'
        AND column_name = 'priority'
        AND DATA_TYPE = 'tinyint');
PREPARE ratchet_priority_job_archive_statement FROM @ratchet_priority_job_archive_ddl;
EXECUTE ratchet_priority_job_archive_statement;
DEALLOCATE PREPARE ratchet_priority_job_archive_statement;
