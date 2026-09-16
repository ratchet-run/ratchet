-- ratchet:single-statement
-- Priority-ordered claims for one job type with priority boosting disabled.
-- Retain idx_claim_executable for selective due-time scans and boosted claims.
DECLARE
  index_count PLS_INTEGER;
BEGIN
  SELECT COUNT(*) INTO index_count FROM user_indexes
   WHERE index_name = 'IDX_CLAIM_PENDING_PRIORITY';
  IF index_count = 0 THEN
    EXECUTE IMMEDIATE 'CREATE INDEX idx_claim_pending_priority ON scheduler_job_queue (status, job_type, priority DESC, scheduled_time ASC, job_id ASC)';
  END IF;
END;
