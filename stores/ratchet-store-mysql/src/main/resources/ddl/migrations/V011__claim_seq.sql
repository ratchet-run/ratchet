-- Add the owner fence and preserve it across terminal failure and reset.
-- Includes adoption of a current consolidated schema.
SET @ratchet_claim_seq_ddl =
    (SELECT IF(COUNT(*) = 0,
               'ALTER TABLE scheduler_job_queue ADD claim_seq BIGINT NOT NULL DEFAULT 0',
               'SELECT 1')
       FROM information_schema.columns
      WHERE table_schema = DATABASE()
        AND table_name = 'scheduler_job_queue'
        AND column_name = 'claim_seq');
PREPARE ratchet_claim_seq_statement FROM @ratchet_claim_seq_ddl;
EXECUTE ratchet_claim_seq_statement;
DEALLOCATE PREPARE ratchet_claim_seq_statement;

SET @ratchet_cold_claim_seq_ddl =
    (SELECT IF(COUNT(*) = 0,
               'ALTER TABLE scheduler_job ADD claim_seq BIGINT NOT NULL DEFAULT 0',
               'SELECT 1')
       FROM information_schema.columns
      WHERE table_schema = DATABASE()
        AND table_name = 'scheduler_job'
        AND column_name = 'claim_seq');
PREPARE ratchet_cold_claim_seq_statement FROM @ratchet_cold_claim_seq_ddl;
EXECUTE ratchet_cold_claim_seq_statement;
DEALLOCATE PREPARE ratchet_cold_claim_seq_statement;
