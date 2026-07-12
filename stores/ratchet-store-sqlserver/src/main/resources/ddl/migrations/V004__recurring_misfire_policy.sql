-- Persists the recurring-master policy used when cron occurrences become overdue.
IF COL_LENGTH('scheduler_recurring_job', 'misfire_policy') IS NULL
    ALTER TABLE scheduler_recurring_job
        ADD misfire_policy VARCHAR(16) NOT NULL
            CONSTRAINT df_rec_misfire_policy DEFAULT 'CATCH_UP';

IF COL_LENGTH('scheduler_recurring_job', 'max_catch_up_executions') IS NULL
    ALTER TABLE scheduler_recurring_job
        ADD max_catch_up_executions INT NOT NULL
            CONSTRAINT df_rec_max_catch_up DEFAULT 11;

IF NOT EXISTS (
    SELECT 1 FROM sys.check_constraints
     WHERE name = 'chk_rec_misfire_policy'
       AND parent_object_id = OBJECT_ID('scheduler_recurring_job'))
    EXEC('ALTER TABLE scheduler_recurring_job
        ADD CONSTRAINT chk_rec_misfire_policy CHECK (
            (misfire_policy = ''CATCH_UP'' AND max_catch_up_executions >= 1)
            OR (misfire_policy IN (''SKIP'', ''FIRE_ONCE'') AND max_catch_up_executions = 0))');
