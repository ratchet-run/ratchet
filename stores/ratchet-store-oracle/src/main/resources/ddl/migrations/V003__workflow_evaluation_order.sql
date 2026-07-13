-- ratchet:single-statement
DECLARE
  column_count PLS_INTEGER;
  index_count PLS_INTEGER;
BEGIN
  SELECT COUNT(*) INTO column_count
    FROM user_tab_columns
   WHERE table_name = 'SCHEDULER_WORKFLOW_CONDITION'
     AND column_name = 'DEFINITION_ORDER';
  IF column_count = 0 THEN
    EXECUTE IMMEDIATE 'ALTER TABLE scheduler_workflow_condition ADD (definition_order NUMBER(10) DEFAULT 0 NOT NULL)';
  END IF;

  SELECT COUNT(*) INTO index_count
    FROM user_indexes
   WHERE index_name = 'IDX_WORKFLOW_EVALUATION_ORDER';
  IF index_count = 0 THEN
    EXECUTE IMMEDIATE 'CREATE INDEX idx_workflow_evaluation_order ON scheduler_workflow_condition (parent_job_id, condition_priority, definition_order)';
  END IF;
END;
