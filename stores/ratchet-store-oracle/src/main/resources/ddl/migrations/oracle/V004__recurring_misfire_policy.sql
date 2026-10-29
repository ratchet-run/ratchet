-- ratchet:single-statement
DECLARE
  artifact_count PLS_INTEGER;
BEGIN
  SELECT COUNT(*) INTO artifact_count
    FROM user_tab_columns
   WHERE table_name = 'SCHEDULER_RECURRING_JOB'
     AND column_name = 'MISFIRE_POLICY';
  IF artifact_count = 0 THEN
    EXECUTE IMMEDIATE 'ALTER TABLE scheduler_recurring_job ADD (misfire_policy VARCHAR2(16) DEFAULT ''CATCH_UP'' NOT NULL)';
  END IF;

  SELECT COUNT(*) INTO artifact_count
    FROM user_tab_columns
   WHERE table_name = 'SCHEDULER_RECURRING_JOB'
     AND column_name = 'MAX_CATCH_UP_EXECUTIONS';
  IF artifact_count = 0 THEN
    EXECUTE IMMEDIATE 'ALTER TABLE scheduler_recurring_job ADD (max_catch_up_executions NUMBER(10) DEFAULT 11 NOT NULL)';
  END IF;

  SELECT COUNT(*) INTO artifact_count
    FROM user_constraints
   WHERE table_name = 'SCHEDULER_RECURRING_JOB'
     AND constraint_name = 'CHK_REC_MISFIRE_POLICY';
  IF artifact_count = 0 THEN
    EXECUTE IMMEDIATE 'ALTER TABLE scheduler_recurring_job ADD CONSTRAINT chk_rec_misfire_policy CHECK ((misfire_policy = ''CATCH_UP'' AND max_catch_up_executions >= 1) OR (misfire_policy IN (''SKIP'', ''FIRE_ONCE'') AND max_catch_up_executions = 0))';
  END IF;
END;
