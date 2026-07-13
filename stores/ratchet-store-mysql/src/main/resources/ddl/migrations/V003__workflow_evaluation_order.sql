-- Persists workflow definition order so equal-priority branches remain first-match-wins.
-- MySQL 8.0 lacks ADD COLUMN / CREATE INDEX IF NOT EXISTS, so guard both through metadata.

SET @ratchet_workflow_order_column_ddl =
    (SELECT IF(COUNT(*) = 0,
               'ALTER TABLE scheduler_workflow_condition ADD COLUMN definition_order INT NOT NULL DEFAULT 0',
               'SELECT 1')
       FROM information_schema.columns
      WHERE table_schema = DATABASE()
        AND table_name = 'scheduler_workflow_condition'
        AND column_name = 'definition_order');
PREPARE ratchet_workflow_order_column_statement FROM @ratchet_workflow_order_column_ddl;
EXECUTE ratchet_workflow_order_column_statement;
DEALLOCATE PREPARE ratchet_workflow_order_column_statement;

SET @ratchet_workflow_order_index_ddl =
    (SELECT IF(COUNT(*) = 0,
               'CREATE INDEX idx_workflow_evaluation_order ON scheduler_workflow_condition (parent_job_id, condition_priority, definition_order)',
               'SELECT 1')
       FROM information_schema.statistics
      WHERE table_schema = DATABASE()
        AND table_name = 'scheduler_workflow_condition'
        AND index_name = 'idx_workflow_evaluation_order');
PREPARE ratchet_workflow_order_index_statement FROM @ratchet_workflow_order_index_ddl;
EXECUTE ratchet_workflow_order_index_statement;
DEALLOCATE PREPARE ratchet_workflow_order_index_statement;
