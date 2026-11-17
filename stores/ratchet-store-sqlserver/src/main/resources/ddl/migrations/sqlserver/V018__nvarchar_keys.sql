-- ratchet:single-statement
-- Stop writers before migration. SchemaMigrator executes this batch in one transaction.
-- Preserve the database collation; Unicode storage does not change comparison semantics.
-- Values already replaced by a code-page conversion cannot be recovered here.
IF EXISTS (SELECT 1 FROM scheduler_recurring_job WHERE LEN(business_key + '#') - 1 > 255)
    THROW 50018, 'V018: scheduler_recurring_job.business_key exceeds 255 characters', 1;
IF EXISTS (SELECT 1 FROM scheduler_recurring_job_archive WHERE LEN(business_key + '#') - 1 > 255)
    THROW 50018, 'V018: scheduler_recurring_job_archive.business_key exceeds 255 characters', 1;
IF EXISTS (SELECT 1 FROM scheduler_job WHERE LEN(business_key + '#') - 1 > 255)
    THROW 50018, 'V018: scheduler_job.business_key exceeds 255 characters', 1;
IF EXISTS (SELECT 1 FROM scheduler_job_queue WHERE LEN(business_key + '#') - 1 > 255)
    THROW 50018, 'V018: scheduler_job_queue.business_key exceeds 255 characters', 1;
IF EXISTS (SELECT 1 FROM scheduler_business_key_reservation WHERE LEN(business_key + '#') - 1 > 255)
    THROW 50018, 'V018: scheduler_business_key_reservation.business_key exceeds 255 characters', 1;
IF EXISTS (SELECT 1 FROM scheduler_job_archive WHERE LEN(business_key + '#') - 1 > 255)
    THROW 50018, 'V018: scheduler_job_archive.business_key exceeds 255 characters', 1;
IF EXISTS (SELECT 1 FROM scheduler_event_outbox WHERE LEN(business_key + '#') - 1 > 255)
    THROW 50018, 'V018: scheduler_event_outbox.business_key exceeds 255 characters', 1;

-- A completed batch is a no-op on repeat, including its dependency rebuilds.
IF EXISTS (SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'scheduler_node') AND name = N'node_id'
      AND system_type_id = TYPE_ID(N'varchar'))
BEGIN
    -- Preserve user statistics on converted tables, including filters and recomputation options.
    -- A filter can depend on a converted column even when the statistics keys do not.
    DECLARE @statistics TABLE (sequence INT IDENTITY, drop_sql NVARCHAR(MAX), create_sql NVARCHAR(MAX));
    INSERT INTO @statistics (drop_sql, create_sql)
    SELECT N'DROP STATISTICS ' + QUOTENAME(OBJECT_SCHEMA_NAME(s.object_id)) + N'.'
               + QUOTENAME(OBJECT_NAME(s.object_id)) + N'.' + QUOTENAME(s.name),
           N'CREATE STATISTICS ' + QUOTENAME(s.name) + N' ON '
               + QUOTENAME(OBJECT_SCHEMA_NAME(s.object_id)) + N'.'
               + QUOTENAME(OBJECT_NAME(s.object_id)) + N' ('
               + (SELECT STRING_AGG(CONVERT(NVARCHAR(MAX), QUOTENAME(c.name)), N', ')
                         WITHIN GROUP (ORDER BY sc.stats_column_id)
                  FROM sys.stats_columns sc JOIN sys.columns c
                    ON c.object_id = sc.object_id AND c.column_id = sc.column_id
                  WHERE sc.object_id = s.object_id AND sc.stats_id = s.stats_id)
               + N')' + CASE WHEN s.has_filter = 1 THEN N' WHERE ' + s.filter_definition ELSE N'' END
               + N' WITH FULLSCAN' + CASE WHEN s.no_recompute = 1 THEN N', NORECOMPUTE' ELSE N'' END
               + CASE WHEN s.is_incremental = 1 THEN N', INCREMENTAL = ON' ELSE N'' END
    FROM sys.stats s
    WHERE s.user_created = 1 AND OBJECT_SCHEMA_NAME(s.object_id) = N'dbo'
      AND OBJECT_NAME(s.object_id) IN (
          N'scheduler_node',
          N'scheduler_lock',
          N'scheduler_resource_limit',
          N'scheduler_recurring_job',
          N'scheduler_recurring_job_archive',
          N'scheduler_job',
          N'scheduler_job_queue',
          N'scheduler_business_key_reservation',
          N'scheduler_job_tag',
          N'scheduler_job_execution',
          N'scheduler_job_archive',
          N'scheduler_resource_permit',
          N'scheduler_idempotency_key',
          N'scheduler_event_outbox',
          N'scheduler_partition_set',
          N'scheduler_resource_waiter');
    DECLARE @sql NVARCHAR(MAX), @sequence INT = 1;
    WHILE @sequence <= (SELECT COUNT(*) FROM @statistics)
    BEGIN
        SELECT @sql = drop_sql FROM @statistics WHERE sequence = @sequence;
        EXEC sys.sp_executesql @sql;
        SET @sequence += 1;
    END;
    DROP INDEX idx_rec_business_key ON scheduler_recurring_job;
    DROP INDEX idx_archive_rec_business_key ON scheduler_recurring_job_archive;
    DROP INDEX idx_queue_orphan ON scheduler_job_queue;
    DROP INDEX idx_signal_key_status ON scheduler_job_queue;
    DROP INDEX idx_job_business_key ON scheduler_job;
    DROP INDEX idx_job_tag_tag_job ON scheduler_job_tag;
    DROP INDEX idx_job_execution_node ON scheduler_job_execution;
    DROP INDEX idx_archive_business_key ON scheduler_job_archive;
    DROP INDEX idx_resource_permit_resource ON scheduler_resource_permit;
    DROP INDEX idx_resource_waiter_order ON scheduler_resource_waiter;
    ALTER TABLE scheduler_node DROP CONSTRAINT pk_scheduler_node;
    ALTER TABLE scheduler_lock DROP CONSTRAINT pk_scheduler_lock;
    ALTER TABLE scheduler_resource_limit DROP CONSTRAINT pk_scheduler_resource_limit;
    ALTER TABLE scheduler_job DROP CONSTRAINT uk_idempotency_key;
    ALTER TABLE scheduler_business_key_reservation DROP CONSTRAINT pk_scheduler_business_key_reservation;
    ALTER TABLE scheduler_job_tag DROP CONSTRAINT pk_scheduler_job_tag;
    ALTER TABLE scheduler_idempotency_key DROP CONSTRAINT pk_scheduler_idempotency;
    ALTER TABLE scheduler_partition_set DROP CONSTRAINT uq_partition_set_key;
    ALTER TABLE scheduler_node ALTER COLUMN node_id NVARCHAR(64) NOT NULL;
    ALTER TABLE scheduler_lock ALTER COLUMN lock_name NVARCHAR(128) NOT NULL;
    ALTER TABLE scheduler_lock ALTER COLUMN owner_node NVARCHAR(64) NOT NULL;
    ALTER TABLE scheduler_resource_limit ALTER COLUMN resource_name NVARCHAR(100) NOT NULL;
    ALTER TABLE scheduler_resource_limit ALTER COLUMN description NVARCHAR(255) NULL;
    ALTER TABLE scheduler_recurring_job ALTER COLUMN business_key NVARCHAR(255) NULL;
    ALTER TABLE scheduler_recurring_job ALTER COLUMN resource_name NVARCHAR(100) NULL;
    ALTER TABLE scheduler_recurring_job ALTER COLUMN execution_target NVARCHAR(64) NULL;
    ALTER TABLE scheduler_recurring_job ALTER COLUMN caller_principal NVARCHAR(255) NULL;
    ALTER TABLE scheduler_recurring_job_archive ALTER COLUMN business_key NVARCHAR(255) NULL;
    ALTER TABLE scheduler_recurring_job_archive ALTER COLUMN execution_target NVARCHAR(64) NULL;
    ALTER TABLE scheduler_recurring_job_archive ALTER COLUMN caller_principal NVARCHAR(255) NULL;
    ALTER TABLE scheduler_job ALTER COLUMN idempotency_key NVARCHAR(36) NOT NULL;
    ALTER TABLE scheduler_job ALTER COLUMN business_key NVARCHAR(255) NULL;
    ALTER TABLE scheduler_job ALTER COLUMN resource_name NVARCHAR(100) NULL;
    ALTER TABLE scheduler_job ALTER COLUMN execution_target NVARCHAR(64) NULL;
    ALTER TABLE scheduler_job ALTER COLUMN caller_principal NVARCHAR(255) NULL;
    ALTER TABLE scheduler_job_queue ALTER COLUMN business_key NVARCHAR(255) NULL;
    ALTER TABLE scheduler_job_queue ALTER COLUMN picked_by NVARCHAR(64) NULL;
    ALTER TABLE scheduler_job_queue ALTER COLUMN signal_key NVARCHAR(255) NULL;
    ALTER TABLE scheduler_job_queue ALTER COLUMN signal_delivered_by NVARCHAR(255) NULL;
    ALTER TABLE scheduler_job_queue ALTER COLUMN execution_target NVARCHAR(64) NULL;
    ALTER TABLE scheduler_business_key_reservation ALTER COLUMN business_key NVARCHAR(255) NOT NULL;
    ALTER TABLE scheduler_business_key_reservation ALTER COLUMN held_by_node NVARCHAR(64) NULL;
    ALTER TABLE scheduler_job_tag ALTER COLUMN tag NVARCHAR(64) NOT NULL;
    ALTER TABLE scheduler_job_execution ALTER COLUMN node_id NVARCHAR(64) NOT NULL;
    ALTER TABLE scheduler_job_archive ALTER COLUMN business_key NVARCHAR(255) NULL;
    ALTER TABLE scheduler_job_archive ALTER COLUMN archived_by NVARCHAR(64) NULL;
    ALTER TABLE scheduler_job_archive ALTER COLUMN tags NVARCHAR(512) NULL;
    ALTER TABLE scheduler_resource_permit ALTER COLUMN resource_name NVARCHAR(100) NOT NULL;
    ALTER TABLE scheduler_resource_permit ALTER COLUMN node_id NVARCHAR(64) NOT NULL;
    ALTER TABLE scheduler_idempotency_key ALTER COLUMN idempotency_key NVARCHAR(36) NOT NULL;
    ALTER TABLE scheduler_event_outbox ALTER COLUMN business_key NVARCHAR(255) NULL;
    ALTER TABLE scheduler_event_outbox ALTER COLUMN node_id NVARCHAR(64) NULL;
    ALTER TABLE scheduler_partition_set ALTER COLUMN set_key NVARCHAR(36) NOT NULL;
    ALTER TABLE scheduler_partition_set ALTER COLUMN name NVARCHAR(255) NOT NULL;
    ALTER TABLE scheduler_partition_set ALTER COLUMN owner_principal NVARCHAR(255) NULL;
    ALTER TABLE scheduler_resource_waiter ALTER COLUMN resource_name NVARCHAR(100) NOT NULL;
    EXEC(N'CREATE INDEX idx_rec_business_key ON scheduler_recurring_job (business_key);');
    EXEC(N'CREATE INDEX idx_archive_rec_business_key ON scheduler_recurring_job_archive (business_key);');
    EXEC(N'CREATE INDEX idx_queue_orphan ON scheduler_job_queue (status, picked_at, picked_by);');
    EXEC(N'CREATE INDEX idx_signal_key_status ON scheduler_job_queue (signal_key, status);');
    EXEC(N'CREATE INDEX idx_job_business_key ON scheduler_job (business_key);');
    EXEC(N'CREATE INDEX idx_job_tag_tag_job ON scheduler_job_tag (tag, job_id);');
    EXEC(N'CREATE INDEX idx_job_execution_node ON scheduler_job_execution (node_id, started_at);');
    EXEC(N'CREATE INDEX idx_archive_business_key ON scheduler_job_archive (business_key);');
    EXEC(N'CREATE INDEX idx_resource_permit_resource ON scheduler_resource_permit (resource_name);');
    EXEC(N'CREATE INDEX idx_resource_waiter_order ON scheduler_resource_waiter
    (resource_name, priority DESC, wait_since ASC, job_id ASC);');
    EXEC(N'ALTER TABLE scheduler_node ADD CONSTRAINT pk_scheduler_node PRIMARY KEY (node_id);');
    EXEC(N'ALTER TABLE scheduler_lock ADD CONSTRAINT pk_scheduler_lock PRIMARY KEY (lock_name);');
    EXEC(N'ALTER TABLE scheduler_resource_limit ADD CONSTRAINT pk_scheduler_resource_limit PRIMARY KEY (resource_name);');
    EXEC(N'ALTER TABLE scheduler_job ADD CONSTRAINT uk_idempotency_key UNIQUE (idempotency_key);');
    EXEC(N'ALTER TABLE scheduler_business_key_reservation ADD CONSTRAINT pk_scheduler_business_key_reservation PRIMARY KEY (business_key);');
    EXEC(N'ALTER TABLE scheduler_job_tag ADD CONSTRAINT pk_scheduler_job_tag PRIMARY KEY (job_id, tag);');
    EXEC(N'ALTER TABLE scheduler_idempotency_key ADD CONSTRAINT pk_scheduler_idempotency PRIMARY KEY (idempotency_key);');
    EXEC(N'ALTER TABLE scheduler_partition_set ADD CONSTRAINT uq_partition_set_key UNIQUE (set_key);');
    SET @sequence = 1;
    WHILE @sequence <= (SELECT COUNT(*) FROM @statistics)
    BEGIN
        SELECT @sql = create_sql FROM @statistics WHERE sequence = @sequence;
        EXEC sys.sp_executesql @sql;
        SET @sequence += 1;
    END;
END;
