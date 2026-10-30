-- Stop old-version writers and retention before applying this migration.
CREATE TABLE IF NOT EXISTS scheduler_idempotency_key (
    idempotency_key VARCHAR(36) NOT NULL,
    original_job_id BINARY(16) NOT NULL,
    reserved_at DATETIME(6) NOT NULL,
    CONSTRAINT pk_scheduler_idempotency PRIMARY KEY (idempotency_key)
);

INSERT INTO scheduler_idempotency_key (idempotency_key, original_job_id, reserved_at)
SELECT idempotency_key, job_id, created_at FROM scheduler_job ON DUPLICATE KEY UPDATE idempotency_key = scheduler_idempotency_key.idempotency_key;
