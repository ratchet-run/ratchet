-- ratchet:single-statement
DO $$
BEGIN
  ALTER TABLE scheduler_recurring_job
      ADD COLUMN IF NOT EXISTS misfire_policy TEXT NOT NULL DEFAULT 'CATCH_UP';
  ALTER TABLE scheduler_recurring_job
      ADD COLUMN IF NOT EXISTS max_catch_up_executions INT NOT NULL DEFAULT 11;

  IF NOT EXISTS (
      SELECT 1 FROM pg_constraint
       WHERE conname = 'chk_rec_misfire_policy'
         AND conrelid = 'scheduler_recurring_job'::regclass) THEN
    ALTER TABLE scheduler_recurring_job
        ADD CONSTRAINT chk_rec_misfire_policy CHECK (
            (misfire_policy = 'CATCH_UP' AND max_catch_up_executions >= 1)
            OR (misfire_policy IN ('SKIP', 'FIRE_ONCE') AND max_catch_up_executions = 0));
  END IF;
END
$$;
