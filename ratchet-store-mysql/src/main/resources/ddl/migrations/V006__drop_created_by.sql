-- Ratchet MySQL V006 — remove created_by column.
-- callerPrincipal (caller_principal column) covers the same purpose.
-- MDC_JOB_CREATOR now reads from caller_principal directly.
--
-- Conditional drop: `ALTER TABLE ... DROP COLUMN IF EXISTS` is MariaDB-only and not portable
-- to MySQL 8.x. Use a server-side prepared statement gated on information_schema so the
-- migration is idempotent against schemas where the column was already removed by hand or by
-- a clean-install of `mysql-schema.sql` that does not include `created_by`.

SET @col_exists := (
    SELECT COUNT(*)
    FROM information_schema.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = 'scheduler_job'
      AND COLUMN_NAME = 'created_by'
);

SET @drop_sql := IF(@col_exists = 1,
    'ALTER TABLE scheduler_job DROP COLUMN created_by',
    'DO 0');

PREPARE drop_stmt FROM @drop_sql;
EXECUTE drop_stmt;
DEALLOCATE PREPARE drop_stmt;

INSERT INTO ratchet_schema_version (version, description)
VALUES ('006', 'Drop created_by column superseded by caller_principal');
