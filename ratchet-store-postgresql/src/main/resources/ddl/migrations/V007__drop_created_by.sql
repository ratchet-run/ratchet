-- Ratchet PostgreSQL V007 — remove created_by column.
-- callerPrincipal (caller_principal column) covers the same purpose.
-- MDC_JOB_CREATOR now reads from caller_principal directly.

ALTER TABLE scheduler_job
    DROP COLUMN IF EXISTS created_by;

INSERT INTO ratchet_schema_version (version, description)
VALUES ('007', 'Drop created_by column superseded by caller_principal');
