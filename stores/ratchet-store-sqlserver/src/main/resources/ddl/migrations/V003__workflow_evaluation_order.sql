-- Persists workflow definition order so equal-priority branches remain first-match-wins.
IF COL_LENGTH('scheduler_workflow_condition', 'definition_order') IS NULL
    ALTER TABLE scheduler_workflow_condition
        ADD definition_order INT NOT NULL
            CONSTRAINT df_workflow_definition_order DEFAULT 0;

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
     WHERE name = 'idx_workflow_evaluation_order'
       AND object_id = OBJECT_ID('scheduler_workflow_condition'))
    CREATE INDEX idx_workflow_evaluation_order
        ON scheduler_workflow_condition (parent_job_id, condition_priority, definition_order);
