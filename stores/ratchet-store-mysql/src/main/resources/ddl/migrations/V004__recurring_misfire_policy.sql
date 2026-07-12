-- Persists the recurring-master policy used when cron occurrences become overdue.
-- MySQL 8.0 lacks ADD COLUMN / ADD CONSTRAINT IF NOT EXISTS, so guard through metadata.

SET @ratchet_misfire_policy_column_ddl =
    (SELECT IF(COUNT(*) = 0,
               'ALTER TABLE scheduler_recurring_job ADD COLUMN misfire_policy ENUM (''SKIP'',''FIRE_ONCE'',''CATCH_UP'') NOT NULL DEFAULT ''CATCH_UP''',
               'SELECT 1')
       FROM information_schema.columns
      WHERE table_schema = DATABASE()
        AND table_name = 'scheduler_recurring_job'
        AND column_name = 'misfire_policy');
PREPARE ratchet_misfire_policy_column_statement FROM @ratchet_misfire_policy_column_ddl;
EXECUTE ratchet_misfire_policy_column_statement;
DEALLOCATE PREPARE ratchet_misfire_policy_column_statement;

SET @ratchet_catch_up_column_ddl =
    (SELECT IF(COUNT(*) = 0,
               'ALTER TABLE scheduler_recurring_job ADD COLUMN max_catch_up_executions INT NOT NULL DEFAULT 11',
               'SELECT 1')
       FROM information_schema.columns
      WHERE table_schema = DATABASE()
        AND table_name = 'scheduler_recurring_job'
        AND column_name = 'max_catch_up_executions');
PREPARE ratchet_catch_up_column_statement FROM @ratchet_catch_up_column_ddl;
EXECUTE ratchet_catch_up_column_statement;
DEALLOCATE PREPARE ratchet_catch_up_column_statement;

SET @ratchet_misfire_constraint_ddl =
    (SELECT IF(COUNT(*) = 0,
               'ALTER TABLE scheduler_recurring_job ADD CONSTRAINT chk_rec_misfire_policy CHECK ((misfire_policy = ''CATCH_UP'' AND max_catch_up_executions >= 1) OR (misfire_policy IN (''SKIP'', ''FIRE_ONCE'') AND max_catch_up_executions = 0))',
               'SELECT 1')
       FROM information_schema.table_constraints
      WHERE table_schema = DATABASE()
        AND table_name = 'scheduler_recurring_job'
        AND constraint_name = 'chk_rec_misfire_policy');
PREPARE ratchet_misfire_constraint_statement FROM @ratchet_misfire_constraint_ddl;
EXECUTE ratchet_misfire_constraint_statement;
DEALLOCATE PREPARE ratchet_misfire_constraint_statement;
