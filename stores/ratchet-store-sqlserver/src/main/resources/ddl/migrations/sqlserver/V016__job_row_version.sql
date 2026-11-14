IF COL_LENGTH('scheduler_job', 'row_version') IS NULL
    ALTER TABLE scheduler_job ADD row_version INT NOT NULL
        CONSTRAINT df_scheduler_job_row_version DEFAULT 0;
