/*
 * Copyright 2026 Ratchet Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
ALTER TABLE scheduler_batch ADD COLUMN IF NOT EXISTS partition_set_id uuid NULL;
ALTER TABLE scheduler_batch ADD COLUMN IF NOT EXISTS generation INT NULL;
CREATE TABLE IF NOT EXISTS scheduler_partition_set (
    set_id uuid NOT NULL,
    set_key VARCHAR(36) NOT NULL,
    name VARCHAR(255) NOT NULL,
    partition_count INT NOT NULL,
    latest_generation INT NOT NULL,
    latest_batch_id uuid NOT NULL,
    sealed_generation INT DEFAULT 0 NOT NULL,
    generation_started_at TIMESTAMPTZ(3) NOT NULL,
    owner_principal VARCHAR(255) NULL,
    version INT DEFAULT 0 NOT NULL,
    created_at TIMESTAMPTZ(3) NOT NULL,
    updated_at TIMESTAMPTZ(3) NOT NULL,
    CONSTRAINT pk_scheduler_partition_set PRIMARY KEY (set_id),
    CONSTRAINT uq_partition_set_key UNIQUE (set_key)
);

CREATE TABLE IF NOT EXISTS scheduler_partition (
    set_id uuid NOT NULL,
    partition_index INT NOT NULL,
    range_from BIGINT NOT NULL,
    range_to BIGINT NOT NULL,
    generation INT NOT NULL,
    child_job_id uuid NOT NULL,
    outcome VARCHAR(20) NULL,
    last_error TEXT NULL,
    checkpoint_json TEXT NULL,
    CONSTRAINT pk_scheduler_partition PRIMARY KEY (set_id, partition_index),
    CONSTRAINT fk_partition_set FOREIGN KEY (set_id) REFERENCES scheduler_partition_set (set_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_partition_child ON scheduler_partition (child_job_id);

CREATE INDEX IF NOT EXISTS idx_batch_partition_set ON scheduler_batch (partition_set_id);
