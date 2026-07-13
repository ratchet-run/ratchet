-- Drops the recurring-master foreign key from scheduler_job. Its ON DELETE SET NULL erased
-- recurring_master_id from every historical child row when a master ended, and its immediate
-- check failed the final child insert of a naturally exhausted master (the executor archives
-- the master in the same transaction, before the post-loop bulk insert). The lineage id now
-- survives master archival and resolves through scheduler_recurring_job_archive. The explicit
-- idx_job_recurring_master_id index backing the constraint stays. MySQL 8.0 has no DROP
-- FOREIGN KEY IF EXISTS, so guard through information_schema; this also lets the migrator
-- adopt a current consolidated schema whose version ledger is empty.

SET @ratchet_drop_recurring_fk_ddl =
    (SELECT IF(COUNT(*) > 0,
               'ALTER TABLE scheduler_job DROP FOREIGN KEY fk_job_recurring_master',
               'SELECT 1')
       FROM information_schema.table_constraints
      WHERE constraint_schema = DATABASE()
        AND table_name = 'scheduler_job'
        AND constraint_name = 'fk_job_recurring_master'
        AND constraint_type = 'FOREIGN KEY');
PREPARE ratchet_drop_recurring_fk_statement FROM @ratchet_drop_recurring_fk_ddl;
EXECUTE ratchet_drop_recurring_fk_statement;
DEALLOCATE PREPARE ratchet_drop_recurring_fk_statement;
