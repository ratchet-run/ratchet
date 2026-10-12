-- MySQL TEXT holds only 65,535 bytes, unlike the other stores: values at the payload limit,
-- encrypted values and long multi-byte errors overflowed it. MEDIUMTEXT holds 16 MB, the same
-- ceiling as a MongoDB document. Changing the type rebuilds each table with a copy and blocks
-- writes until done; large installs should run this migration in a quiet window.

SET @ratchet_widen_job_ddl =
    (SELECT IF(COUNT(*) > 0,
               'ALTER TABLE scheduler_job MODIFY terminal_error MEDIUMTEXT NULL',
               'SELECT 1')
       FROM information_schema.columns
      WHERE table_schema = DATABASE()
        AND table_name = 'scheduler_job'
        AND column_name IN ('terminal_error')
        AND DATA_TYPE = 'text');
PREPARE ratchet_widen_job_statement FROM @ratchet_widen_job_ddl;
EXECUTE ratchet_widen_job_statement;
DEALLOCATE PREPARE ratchet_widen_job_statement;

SET @ratchet_widen_job_queue_ddl =
    (SELECT IF(COUNT(*) > 0,
               'ALTER TABLE scheduler_job_queue MODIFY last_error MEDIUMTEXT NULL, MODIFY signal_payload MEDIUMTEXT NULL, MODIFY signal_rejection_reason MEDIUMTEXT NULL',
               'SELECT 1')
       FROM information_schema.columns
      WHERE table_schema = DATABASE()
        AND table_name = 'scheduler_job_queue'
        AND column_name IN ('last_error', 'signal_payload', 'signal_rejection_reason')
        AND DATA_TYPE = 'text');
PREPARE ratchet_widen_job_queue_statement FROM @ratchet_widen_job_queue_ddl;
EXECUTE ratchet_widen_job_queue_statement;
DEALLOCATE PREPARE ratchet_widen_job_queue_statement;

SET @ratchet_widen_job_execution_ddl =
    (SELECT IF(COUNT(*) > 0,
               'ALTER TABLE scheduler_job_execution MODIFY error_message MEDIUMTEXT NULL',
               'SELECT 1')
       FROM information_schema.columns
      WHERE table_schema = DATABASE()
        AND table_name = 'scheduler_job_execution'
        AND column_name IN ('error_message')
        AND DATA_TYPE = 'text');
PREPARE ratchet_widen_job_execution_statement FROM @ratchet_widen_job_execution_ddl;
EXECUTE ratchet_widen_job_execution_statement;
DEALLOCATE PREPARE ratchet_widen_job_execution_statement;

SET @ratchet_widen_job_log_ddl =
    (SELECT IF(COUNT(*) > 0,
               'ALTER TABLE scheduler_job_log MODIFY message MEDIUMTEXT NOT NULL',
               'SELECT 1')
       FROM information_schema.columns
      WHERE table_schema = DATABASE()
        AND table_name = 'scheduler_job_log'
        AND column_name IN ('message')
        AND DATA_TYPE = 'text');
PREPARE ratchet_widen_job_log_statement FROM @ratchet_widen_job_log_ddl;
EXECUTE ratchet_widen_job_log_statement;
DEALLOCATE PREPARE ratchet_widen_job_log_statement;

SET @ratchet_widen_job_archive_ddl =
    (SELECT IF(COUNT(*) > 0,
               'ALTER TABLE scheduler_job_archive MODIFY final_error MEDIUMTEXT NULL, MODIFY payload_summary MEDIUMTEXT NULL',
               'SELECT 1')
       FROM information_schema.columns
      WHERE table_schema = DATABASE()
        AND table_name = 'scheduler_job_archive'
        AND column_name IN ('final_error', 'payload_summary')
        AND DATA_TYPE = 'text');
PREPARE ratchet_widen_job_archive_statement FROM @ratchet_widen_job_archive_ddl;
EXECUTE ratchet_widen_job_archive_statement;
DEALLOCATE PREPARE ratchet_widen_job_archive_statement;

SET @ratchet_widen_workflow_condition_ddl =
    (SELECT IF(COUNT(*) > 0,
               'ALTER TABLE scheduler_workflow_condition MODIFY condition_expression MEDIUMTEXT NULL',
               'SELECT 1')
       FROM information_schema.columns
      WHERE table_schema = DATABASE()
        AND table_name = 'scheduler_workflow_condition'
        AND column_name IN ('condition_expression')
        AND DATA_TYPE = 'text');
PREPARE ratchet_widen_workflow_condition_statement FROM @ratchet_widen_workflow_condition_ddl;
EXECUTE ratchet_widen_workflow_condition_statement;
DEALLOCATE PREPARE ratchet_widen_workflow_condition_statement;

SET @ratchet_widen_job_extension_state_ddl =
    (SELECT IF(COUNT(*) > 0,
               'ALTER TABLE scheduler_job_extension_state MODIFY state MEDIUMTEXT NOT NULL',
               'SELECT 1')
       FROM information_schema.columns
      WHERE table_schema = DATABASE()
        AND table_name = 'scheduler_job_extension_state'
        AND column_name IN ('state')
        AND DATA_TYPE = 'text');
PREPARE ratchet_widen_job_extension_state_statement FROM @ratchet_widen_job_extension_state_ddl;
EXECUTE ratchet_widen_job_extension_state_statement;
DEALLOCATE PREPARE ratchet_widen_job_extension_state_statement;
