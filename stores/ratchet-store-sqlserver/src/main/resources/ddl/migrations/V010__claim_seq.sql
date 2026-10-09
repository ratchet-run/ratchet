IF COL_LENGTH('scheduler_job_queue', 'claim_seq') IS NULL
    ALTER TABLE scheduler_job_queue ADD claim_seq BIGINT NOT NULL
        CONSTRAINT df_scheduler_job_queue_claim_seq DEFAULT 0;
IF COL_LENGTH('scheduler_job', 'claim_seq') IS NULL
    ALTER TABLE scheduler_job ADD claim_seq BIGINT NOT NULL
        CONSTRAINT df_scheduler_job_claim_seq DEFAULT 0;
