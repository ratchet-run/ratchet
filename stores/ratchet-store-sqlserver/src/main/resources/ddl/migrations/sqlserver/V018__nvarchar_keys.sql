-- ratchet:single-statement
-- Stop writers before migration. SchemaMigrator executes this batch in one transaction.
-- Key columns become binary (exact); free-text columns keep their current collation.
-- BIN2 only makes keys more distinct, so unique keys cannot fail when rebuilt.
-- Canonicalise resource references to limit names and delete leftover mis-cased permits.
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
               + CASE WHEN s.has_persisted_sample = 1
                      THEN N' WITH SAMPLE ' + CONVERT(NVARCHAR(32), p.persisted_sample_percent)
                          + N' PERCENT, PERSIST_SAMPLE_PERCENT = ON'
                      ELSE N' WITH FULLSCAN' END
               + CASE WHEN s.no_recompute = 1 THEN N', NORECOMPUTE' ELSE N'' END
               + CASE WHEN s.is_incremental = 1 THEN N', INCREMENTAL = ON' ELSE N'' END
    FROM sys.stats s
    OUTER APPLY sys.dm_db_stats_properties(s.object_id, s.stats_id) p
    WHERE s.user_created = 1 AND s.object_id IN (
          OBJECT_ID(N'scheduler_node'),
          OBJECT_ID(N'scheduler_lock'),
          OBJECT_ID(N'scheduler_resource_limit'),
          OBJECT_ID(N'scheduler_recurring_job'),
          OBJECT_ID(N'scheduler_recurring_job_archive'),
          OBJECT_ID(N'scheduler_job'),
          OBJECT_ID(N'scheduler_job_queue'),
          OBJECT_ID(N'scheduler_business_key_reservation'),
          OBJECT_ID(N'scheduler_job_tag'),
          OBJECT_ID(N'scheduler_job_execution'),
          OBJECT_ID(N'scheduler_job_archive'),
          OBJECT_ID(N'scheduler_resource_permit'),
          OBJECT_ID(N'scheduler_idempotency_key'),
          OBJECT_ID(N'scheduler_event_outbox'),
          OBJECT_ID(N'scheduler_partition_set'),
          OBJECT_ID(N'scheduler_resource_waiter'));
    DECLARE @sql NVARCHAR(MAX), @sequence INT = 1;
    DECLARE @columns TABLE (sequence INT PRIMARY KEY, alter_sql NVARCHAR(MAX));
    INSERT INTO @columns (sequence, alter_sql)
    SELECT v.sequence,
           N'ALTER TABLE ' + QUOTENAME(OBJECT_SCHEMA_NAME(c.object_id)) + N'.'
               + QUOTENAME(OBJECT_NAME(c.object_id)) + N' ALTER COLUMN ' + QUOTENAME(c.name)
               + N' NVARCHAR(' + CONVERT(NVARCHAR(10), v.length) + N') COLLATE '
               + CASE WHEN v.is_key = 1 THEN N'Latin1_General_100_BIN2' ELSE c.collation_name END
               + CASE WHEN v.nullable = 1 THEN N' NULL' ELSE N' NOT NULL' END
    FROM (VALUES
        (1, N'scheduler_node', N'node_id', 64, 0, 1),
        (2, N'scheduler_lock', N'lock_name', 128, 0, 1),
        (3, N'scheduler_lock', N'owner_node', 64, 0, 1),
        (4, N'scheduler_resource_limit', N'resource_name', 100, 0, 1),
        (5, N'scheduler_resource_limit', N'description', 255, 1, 0),
        (6, N'scheduler_recurring_job', N'business_key', 255, 1, 1),
        (7, N'scheduler_recurring_job', N'resource_name', 100, 1, 1),
        (8, N'scheduler_recurring_job', N'execution_target', 64, 1, 1),
        (9, N'scheduler_recurring_job', N'caller_principal', 255, 1, 0),
        (10, N'scheduler_recurring_job_archive', N'business_key', 255, 1, 1),
        (11, N'scheduler_recurring_job_archive', N'execution_target', 64, 1, 1),
        (12, N'scheduler_recurring_job_archive', N'caller_principal', 255, 1, 0),
        (13, N'scheduler_job', N'idempotency_key', 36, 0, 1),
        (14, N'scheduler_job', N'business_key', 255, 1, 1),
        (15, N'scheduler_job', N'resource_name', 100, 1, 1),
        (16, N'scheduler_job', N'execution_target', 64, 1, 1),
        (17, N'scheduler_job', N'caller_principal', 255, 1, 0),
        (18, N'scheduler_job_queue', N'business_key', 255, 1, 1),
        (19, N'scheduler_job_queue', N'picked_by', 64, 1, 1),
        (20, N'scheduler_job_queue', N'signal_key', 255, 1, 1),
        (21, N'scheduler_job_queue', N'signal_delivered_by', 255, 1, 0),
        (22, N'scheduler_job_queue', N'execution_target', 64, 1, 1),
        (23, N'scheduler_business_key_reservation', N'business_key', 255, 0, 1),
        (24, N'scheduler_business_key_reservation', N'held_by_node', 64, 1, 1),
        (25, N'scheduler_job_tag', N'tag', 64, 0, 1),
        (26, N'scheduler_job_execution', N'node_id', 64, 0, 1),
        (27, N'scheduler_job_archive', N'business_key', 255, 1, 1),
        (28, N'scheduler_job_archive', N'archived_by', 64, 1, 1),
        (29, N'scheduler_job_archive', N'tags', 512, 1, 1),
        (30, N'scheduler_resource_permit', N'resource_name', 100, 0, 1),
        (31, N'scheduler_resource_permit', N'node_id', 64, 0, 1),
        (32, N'scheduler_idempotency_key', N'idempotency_key', 36, 0, 1),
        (33, N'scheduler_event_outbox', N'business_key', 255, 1, 1),
        (34, N'scheduler_event_outbox', N'node_id', 64, 1, 1),
        (35, N'scheduler_partition_set', N'set_key', 36, 0, 1),
        (36, N'scheduler_partition_set', N'name', 255, 0, 0),
        (37, N'scheduler_partition_set', N'owner_principal', 255, 1, 0),
        (38, N'scheduler_resource_waiter', N'resource_name', 100, 0, 1)
    ) v(sequence, table_name, column_name, length, nullable, is_key)
    JOIN sys.columns c ON c.object_id = OBJECT_ID(v.table_name) AND c.name = v.column_name;
    IF (SELECT COUNT(*) FROM @columns) < 38
        THROW 50018, 'V018: expected all 38 columns before converting to NVARCHAR', 1;
    UPDATE j SET resource_name = l.resource_name
    FROM scheduler_job j
    JOIN scheduler_resource_limit l ON j.resource_name = l.resource_name
    WHERE CAST(j.resource_name AS VARBINARY(100)) <> CAST(l.resource_name AS VARBINARY(100));
    UPDATE r SET resource_name = l.resource_name
    FROM scheduler_recurring_job r
    JOIN scheduler_resource_limit l ON r.resource_name = l.resource_name
    WHERE CAST(r.resource_name AS VARBINARY(100)) <> CAST(l.resource_name AS VARBINARY(100));
    UPDATE w SET resource_name = l.resource_name
    FROM scheduler_resource_waiter w
    JOIN scheduler_resource_limit l ON w.resource_name = l.resource_name
    WHERE CAST(w.resource_name AS VARBINARY(100)) <> CAST(l.resource_name AS VARBINARY(100));
    DELETE p FROM scheduler_resource_permit p
    JOIN scheduler_resource_limit l ON p.resource_name = l.resource_name
    WHERE CAST(p.resource_name AS VARBINARY(100)) <> CAST(l.resource_name AS VARBINARY(100));
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
    SET @sequence = 1;
    WHILE @sequence <= (SELECT COUNT(*) FROM @columns)
    BEGIN
        SELECT @sql = alter_sql FROM @columns WHERE sequence = @sequence;
        EXEC sys.sp_executesql @sql;
        SET @sequence += 1;
    END;
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
