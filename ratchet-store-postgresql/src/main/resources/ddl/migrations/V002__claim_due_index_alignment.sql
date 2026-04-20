-- Ratchet PostgreSQL V002 — align executable claim index with computed priority boosting.
--
-- Priority boosting computes ORDER BY priority + age_boost at claim time. That expression is not
-- immutable and cannot be represented by a static B-tree index, so the useful indexable work is
-- filtering to pending, executable, due rows before the final sort.

DROP INDEX IF EXISTS idx_job_claim_cover;
DROP INDEX IF EXISTS idx_job_type;
DROP INDEX IF EXISTS idx_job_recurring_composite;

CREATE INDEX IF NOT EXISTS idx_job_claim_cover
    ON scheduler_job (job_type, scheduled_time ASC, priority DESC, job_id ASC)
    WHERE status = 'PENDING';

CREATE INDEX IF NOT EXISTS idx_job_recurring_composite
    ON scheduler_job (next_fire ASC, priority DESC, job_id ASC)
    WHERE status = 'PENDING' AND job_type = 'RECURRING';

INSERT INTO ratchet_schema_version (version, description)
VALUES ('002', 'Align executable claim index for due-time filtering under computed priority boosting');
