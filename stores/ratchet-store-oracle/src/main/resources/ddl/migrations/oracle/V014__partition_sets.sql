-- ratchet:single-statement
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
DECLARE
  artifact_count PLS_INTEGER;
BEGIN
  SELECT COUNT(*) INTO artifact_count FROM user_tab_columns WHERE table_name = 'SCHEDULER_BATCH' AND column_name = 'PARTITION_SET_ID';
  IF artifact_count = 0 THEN
    EXECUTE IMMEDIATE 'ALTER TABLE scheduler_batch ADD partition_set_id RAW(16)';
  END IF;
  SELECT COUNT(*) INTO artifact_count FROM user_tab_columns WHERE table_name = 'SCHEDULER_BATCH' AND column_name = 'GENERATION';
  IF artifact_count = 0 THEN
    EXECUTE IMMEDIATE 'ALTER TABLE scheduler_batch ADD generation NUMBER(10)';
  END IF;
  SELECT COUNT(*) INTO artifact_count FROM user_tables WHERE table_name = 'SCHEDULER_PARTITION_SET';
  IF artifact_count = 0 THEN
    EXECUTE IMMEDIATE 'CREATE TABLE IF NOT EXISTS scheduler_partition_set (
    set_id RAW(16) NOT NULL,
    set_key VARCHAR2(36) NOT NULL,
    name VARCHAR2(255) NOT NULL,
    partition_count NUMBER(10) NOT NULL,
    latest_generation NUMBER(10) NOT NULL,
    latest_batch_id RAW(16) NOT NULL,
    sealed_generation NUMBER(10) DEFAULT 0 NOT NULL,
    generation_started_at TIMESTAMP(3) NOT NULL,
    owner_principal VARCHAR2(255) NULL,
    version NUMBER(10) DEFAULT 0 NOT NULL,
    created_at TIMESTAMP(3) NOT NULL,
    updated_at TIMESTAMP(3) NOT NULL,
    CONSTRAINT pk_scheduler_partition_set PRIMARY KEY (set_id),
    CONSTRAINT uq_partition_set_key UNIQUE (set_key)
)';
  END IF;
  SELECT COUNT(*) INTO artifact_count FROM user_tables WHERE table_name = 'SCHEDULER_PARTITION';
  IF artifact_count = 0 THEN
    EXECUTE IMMEDIATE 'CREATE TABLE IF NOT EXISTS scheduler_partition (
    set_id RAW(16) NOT NULL,
    partition_index NUMBER(10) NOT NULL,
    range_from NUMBER(19) NOT NULL,
    range_to NUMBER(19) NOT NULL,
    generation NUMBER(10) NOT NULL,
    child_job_id RAW(16) NOT NULL,
    outcome VARCHAR2(20) NULL,
    last_error CLOB NULL,
    checkpoint_json CLOB NULL,
    CONSTRAINT pk_scheduler_partition PRIMARY KEY (set_id, partition_index),
    CONSTRAINT fk_partition_set FOREIGN KEY (set_id) REFERENCES scheduler_partition_set (set_id) ON DELETE CASCADE
)';
  END IF;
  SELECT COUNT(*) INTO artifact_count FROM user_indexes WHERE index_name = 'IDX_PARTITION_CHILD';
  IF artifact_count = 0 THEN
    EXECUTE IMMEDIATE 'CREATE INDEX idx_partition_child ON scheduler_partition (child_job_id)';
  END IF;
  SELECT COUNT(*) INTO artifact_count FROM user_indexes WHERE index_name = 'IDX_BATCH_PARTITION_SET';
  IF artifact_count = 0 THEN
    EXECUTE IMMEDIATE 'CREATE INDEX idx_batch_partition_set ON scheduler_batch (partition_set_id)';
  END IF;
END;
