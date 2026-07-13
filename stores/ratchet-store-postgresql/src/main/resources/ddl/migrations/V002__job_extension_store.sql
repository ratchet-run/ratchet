-- Adds JobExtensionStore persistence to schemas first created by the released V001 migration.
-- IF NOT EXISTS also lets the migrator adopt a current consolidated schema whose version ledger
-- is empty.

ALTER TABLE scheduler_job_archive
    ADD COLUMN IF NOT EXISTS properties TEXT,
    ADD COLUMN IF NOT EXISTS extension_state TEXT;

-- scheduler_job_properties — write-once indexed scalars (plaintext by design; no secrets)
CREATE TABLE IF NOT EXISTS scheduler_job_properties
(
    job_id       uuid          NOT NULL,
    property_key VARCHAR(255)  NOT NULL,
    value        VARCHAR(1024),
    CONSTRAINT pk_scheduler_job_properties PRIMARY KEY (job_id, property_key),
    CONSTRAINT fk_job_properties_job FOREIGN KEY (job_id) REFERENCES scheduler_job (job_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_property_kv ON scheduler_job_properties (property_key, value);

-- scheduler_job_extension_state — mutable per-namespace blobs with per-row CAS; encrypted at rest
-- when payload encryption is configured (state holds ciphertext, encrypted_state /
-- encryption_key_id mirror the scheduler_job payload-encryption metadata columns)
CREATE TABLE IF NOT EXISTS scheduler_job_extension_state
(
    job_id            uuid         NOT NULL,
    namespace         VARCHAR(64)  NOT NULL,
    state             TEXT         NOT NULL,
    encrypted_state   BOOLEAN      NOT NULL DEFAULT FALSE,
    encryption_key_id VARCHAR(256),
    version           INT          NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_scheduler_job_extension_state PRIMARY KEY (job_id, namespace),
    CONSTRAINT fk_job_extension_state_job FOREIGN KEY (job_id) REFERENCES scheduler_job (job_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_extension_state_key_id ON scheduler_job_extension_state (encryption_key_id);
