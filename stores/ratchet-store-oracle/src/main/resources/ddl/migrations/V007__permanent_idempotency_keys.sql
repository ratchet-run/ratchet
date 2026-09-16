-- ratchet:single-statement
-- Stop old-version writers and retention before applying this migration.
BEGIN
    BEGIN
        EXECUTE IMMEDIATE 'CREATE TABLE scheduler_idempotency_key (
    idempotency_key VARCHAR2(36) NOT NULL,
    original_job_id RAW(16) NOT NULL,
    reserved_at TIMESTAMP(6) NOT NULL,
    CONSTRAINT pk_scheduler_idempotency PRIMARY KEY (idempotency_key)
)';
    EXCEPTION WHEN OTHERS THEN
        IF SQLCODE != -955 THEN RAISE; END IF;
    END;

    EXECUTE IMMEDIATE 'INSERT INTO scheduler_idempotency_key (idempotency_key, original_job_id, reserved_at)
SELECT idempotency_key, job_id, created_at FROM scheduler_job j WHERE NOT EXISTS
(SELECT 1 FROM scheduler_idempotency_key k WHERE k.idempotency_key = j.idempotency_key)';
END;
