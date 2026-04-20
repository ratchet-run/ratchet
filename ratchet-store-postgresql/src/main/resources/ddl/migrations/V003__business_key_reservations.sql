-- Ratchet PostgreSQL V003 — move active business-key uniqueness to a reservation table.
--
-- scheduler_job.business_key remains for observability and archive/search projections. Active
-- uniqueness is now owned by scheduler_business_key_reservation, matching the MySQL model without
-- splitting PostgreSQL live queue state yet.

CREATE TABLE IF NOT EXISTS scheduler_business_key_reservation
(
    business_key TEXT NOT NULL,
    owner_job_id BIGINT NOT NULL,
    owner_table TEXT NOT NULL,
    reserved_at TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_scheduler_business_key_reservation PRIMARY KEY (business_key),
    CONSTRAINT chk_bk_owner_table CHECK (owner_table IN ('QUEUE', 'RECURRING')),
    CONSTRAINT fk_bk_owner_job FOREIGN KEY (owner_job_id) REFERENCES scheduler_job (job_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_bk_owner ON scheduler_business_key_reservation (owner_job_id);

INSERT INTO scheduler_business_key_reservation (business_key, owner_job_id, owner_table, reserved_at)
SELECT business_key,
       job_id,
       CASE WHEN job_type = 'RECURRING' THEN 'RECURRING' ELSE 'QUEUE' END,
       COALESCE(updated_at, created_at, CURRENT_TIMESTAMP)
FROM scheduler_job
WHERE business_key IS NOT NULL
  AND status IN ('PENDING', 'RUNNING', 'PAUSED')
ON CONFLICT (business_key) DO UPDATE SET
    owner_job_id = EXCLUDED.owner_job_id,
    owner_table = EXCLUDED.owner_table,
    reserved_at = EXCLUDED.reserved_at;

DROP INDEX IF EXISTS idx_job_active_business_key;

INSERT INTO ratchet_schema_version (version, description)
VALUES ('003', 'Move active business-key uniqueness to scheduler_business_key_reservation');
