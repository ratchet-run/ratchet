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
IF COL_LENGTH('scheduler_batch', 'partition_set_id') IS NULL
    ALTER TABLE scheduler_batch ADD partition_set_id BINARY(16) NULL;
IF COL_LENGTH('scheduler_batch', 'generation') IS NULL
    ALTER TABLE scheduler_batch ADD generation INT NULL;
IF OBJECT_ID('scheduler_partition_set', 'U') IS NULL
CREATE TABLE scheduler_partition_set (
    set_id BINARY(16) NOT NULL,
    set_key VARCHAR(36) NOT NULL,
    name VARCHAR(255) NOT NULL,
    partition_count INT NOT NULL,
    latest_generation INT NOT NULL,
    latest_batch_id BINARY(16) NOT NULL,
    sealed_generation INT DEFAULT 0 NOT NULL,
    generation_started_at DATETIME2(3) NOT NULL,
    owner_principal VARCHAR(255) NULL,
    version INT DEFAULT 0 NOT NULL,
    created_at DATETIME2(3) NOT NULL,
    updated_at DATETIME2(3) NOT NULL,
    CONSTRAINT pk_scheduler_partition_set PRIMARY KEY (set_id),
    CONSTRAINT uq_partition_set_key UNIQUE (set_key)
);
IF OBJECT_ID('scheduler_partition', 'U') IS NULL
CREATE TABLE scheduler_partition (
    set_id BINARY(16) NOT NULL,
    partition_index INT NOT NULL,
    range_from BIGINT NOT NULL,
    range_to BIGINT NOT NULL,
    generation INT NOT NULL,
    child_job_id BINARY(16) NOT NULL,
    outcome VARCHAR(20) NULL,
    last_error NVARCHAR(MAX) NULL,
    checkpoint_json NVARCHAR(MAX) NULL,
    CONSTRAINT pk_scheduler_partition PRIMARY KEY (set_id, partition_index),
    CONSTRAINT fk_partition_set FOREIGN KEY (set_id) REFERENCES scheduler_partition_set (set_id) ON DELETE CASCADE
);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_partition_child' AND object_id = OBJECT_ID('scheduler_partition'))
CREATE INDEX idx_partition_child ON scheduler_partition (child_job_id);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_batch_partition_set' AND object_id = OBJECT_ID('scheduler_batch'))
CREATE INDEX idx_batch_partition_set ON scheduler_batch (partition_set_id);
