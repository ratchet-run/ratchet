-- ratchet:single-statement
DECLARE
  column_count PLS_INTEGER;
BEGIN
  SELECT COUNT(*) INTO column_count FROM user_tab_columns
   WHERE table_name = 'SCHEDULER_JOB'
     AND column_name = 'ROW_VERSION';
  IF column_count = 0 THEN
    EXECUTE IMMEDIATE 'ALTER TABLE scheduler_job ADD row_version NUMBER(10) DEFAULT 0 NOT NULL';
  END IF;
END;
