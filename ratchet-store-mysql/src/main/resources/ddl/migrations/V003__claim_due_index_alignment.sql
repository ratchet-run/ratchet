-- Ratchet MySQL V003 — align executable claim index with computed priority boosting.
--
-- Priority boosting computes ORDER BY priority + age_boost at claim time. That expression cannot
-- be represented by a static B-tree index, so the useful indexable work is filtering to pending,
-- executable, due rows before the final sort.

ALTER TABLE scheduler_job_queue
    DROP INDEX idx_claim_executable,
    ADD INDEX idx_claim_executable (status, job_type, scheduled_time ASC, priority DESC, job_id ASC);

INSERT INTO ratchet_schema_version (version, description)
VALUES ('003', 'Align executable claim index for due-time filtering under computed priority boosting');
