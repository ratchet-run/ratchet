-- Adds JobExtensionStore persistence to schemas first created by the released V001 migration.
-- MySQL 8.0 has no ADD COLUMN IF NOT EXISTS, so guard archive columns through
-- information_schema. This also lets the migrator adopt a current consolidated schema whose
-- version ledger is empty.

SET @ratchet_properties_ddl =
    (SELECT IF(COUNT(*) = 0,
               'ALTER TABLE scheduler_job_archive ADD COLUMN properties JSON NULL',
               'SELECT 1')
       FROM information_schema.columns
      WHERE table_schema = DATABASE()
        AND table_name = 'scheduler_job_archive'
        AND column_name = 'properties');
PREPARE ratchet_properties_statement FROM @ratchet_properties_ddl;
EXECUTE ratchet_properties_statement;
DEALLOCATE PREPARE ratchet_properties_statement;

SET @ratchet_extension_state_ddl =
    (SELECT IF(COUNT(*) = 0,
               'ALTER TABLE scheduler_job_archive ADD COLUMN extension_state JSON NULL',
               'SELECT 1')
       FROM information_schema.columns
      WHERE table_schema = DATABASE()
        AND table_name = 'scheduler_job_archive'
        AND column_name = 'extension_state');
PREPARE ratchet_extension_state_statement FROM @ratchet_extension_state_ddl;
EXECUTE ratchet_extension_state_statement;
DEALLOCATE PREPARE ratchet_extension_state_statement;

-- Per-job extension properties (write-once indexed scalars; plaintext by design — no secrets)
CREATE TABLE IF NOT EXISTS scheduler_job_properties
(
    job_id       BINARY(16)    NOT NULL,
    property_key VARCHAR(255)  NOT NULL,
    value        VARCHAR(1024) NULL,
    PRIMARY KEY (job_id, property_key),
    INDEX idx_property_kv (property_key, value(255)),
    CONSTRAINT fk_job_properties_job FOREIGN KEY (job_id) REFERENCES scheduler_job (job_id) ON DELETE CASCADE
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci;

-- Per-job extension state (mutable per-namespace blobs with per-row CAS; encrypted at rest when
-- payload encryption is configured — state holds ciphertext, encrypted_state/encryption_key_id
-- mirror the scheduler_job payload-encryption metadata columns)
CREATE TABLE IF NOT EXISTS scheduler_job_extension_state
(
    job_id            BINARY(16)   NOT NULL,
    namespace         VARCHAR(64)  NOT NULL,
    state             TEXT         NOT NULL,
    encrypted_state   BOOLEAN      NOT NULL DEFAULT FALSE,
    encryption_key_id VARCHAR(256) NULL,
    version           INT          NOT NULL DEFAULT 0,
    updated_at        DATETIME(6)  NOT NULL,
    PRIMARY KEY (job_id, namespace),
    INDEX idx_extension_state_key_id (encryption_key_id),
    CONSTRAINT fk_job_extension_state_job FOREIGN KEY (job_id) REFERENCES scheduler_job (job_id) ON DELETE CASCADE
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci;
