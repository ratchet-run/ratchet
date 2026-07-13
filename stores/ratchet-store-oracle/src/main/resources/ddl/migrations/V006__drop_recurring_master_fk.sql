-- ratchet:single-statement
BEGIN
    -- Drops the recurring-master foreign key from scheduler_job. Its ON DELETE SET NULL erased
    -- recurring_master_id from every historical child row when a master ended, and its immediate
    -- check failed the final child insert of a naturally exhausted master (the executor archives
    -- the master in the same transaction, before the post-loop bulk insert). The lineage id now
    -- survives master archival and resolves through scheduler_recurring_job_archive.
    EXECUTE IMMEDIATE 'ALTER TABLE scheduler_job DROP CONSTRAINT fk_job_recurring_master';
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE != -2443 THEN
            RAISE;
        END IF;
END;
