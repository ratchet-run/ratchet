-- ratchet:single-statement
-- Stop old-version writers and retention before applying this migration.
IF OBJECT_ID('scheduler_idempotency_key', 'U') IS NULL
BEGIN
CREATE TABLE scheduler_idempotency_key (
    idempotency_key VARCHAR(36) NOT NULL,
    original_job_id BINARY(16) NOT NULL,
    reserved_at DATETIME2(6) NOT NULL,
    CONSTRAINT pk_scheduler_idempotency PRIMARY KEY (idempotency_key)
);
END;

INSERT INTO scheduler_idempotency_key (idempotency_key, original_job_id, reserved_at)
SELECT idempotency_key, job_id, created_at FROM scheduler_job j WHERE NOT EXISTS
(SELECT 1 FROM scheduler_idempotency_key k WHERE k.idempotency_key = j.idempotency_key);
