-- ratchet:single-statement
DECLARE
  column_count PLS_INTEGER;
BEGIN
  SELECT COUNT(*) INTO column_count FROM user_tab_columns
   WHERE table_name = 'SCHEDULER_JOB_QUEUE'
     AND column_name = 'CLAIM_SEQ';
  IF column_count = 0 THEN
    EXECUTE IMMEDIATE 'ALTER TABLE scheduler_job_queue ADD claim_seq NUMBER(19) DEFAULT 0 NOT NULL';
  END IF;
  SELECT COUNT(*) INTO column_count FROM user_tab_columns
   WHERE table_name = 'SCHEDULER_JOB'
     AND column_name = 'CLAIM_SEQ';
  IF column_count = 0 THEN
    EXECUTE IMMEDIATE 'ALTER TABLE scheduler_job ADD claim_seq NUMBER(19) DEFAULT 0 NOT NULL';
  END IF;
END;
