IF COL_LENGTH('scheduler_job_queue', 'crash_count') IS NULL
    ALTER TABLE scheduler_job_queue ADD crash_count INT NOT NULL
        CONSTRAINT df_scheduler_job_queue_crash_count DEFAULT 0;
