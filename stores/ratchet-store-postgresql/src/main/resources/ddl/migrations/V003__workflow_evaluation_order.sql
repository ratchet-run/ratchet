-- Persists workflow definition order so equal-priority branches remain first-match-wins.
ALTER TABLE scheduler_workflow_condition
    ADD COLUMN IF NOT EXISTS definition_order INT NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_workflow_evaluation_order
    ON scheduler_workflow_condition (parent_job_id, condition_priority, definition_order);
