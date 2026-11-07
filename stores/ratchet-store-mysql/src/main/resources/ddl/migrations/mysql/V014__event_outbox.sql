-- Copyright 2026 Ratchet Contributors
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- Durable terminal events committed with the terminal transition.
CREATE TABLE IF NOT EXISTS scheduler_event_outbox
(
    event_id BINARY(16) NOT NULL,
    event_type VARCHAR(32) NOT NULL,
    job_id BINARY(16) NOT NULL,
    business_key VARCHAR(255) NULL,
    recurring_master_id BINARY(16) NULL,
    job_type VARCHAR(32) NULL,
    priority VARCHAR(16) NULL,
    node_id VARCHAR(64) NULL,
    event_ts DATETIME(6) NOT NULL,
    execution_time_ms BIGINT NULL,
    error_message MEDIUMTEXT NULL,
    retry_attempt INT NOT NULL,
    created_at DATETIME(6) NOT NULL,
    PRIMARY KEY (event_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- MySQL 8.0 lacks CREATE INDEX IF NOT EXISTS, so guard through metadata.
SET @ratchet_event_outbox_index_ddl =
    (SELECT IF(COUNT(*) = 0,
               'CREATE INDEX idx_event_outbox_created ON scheduler_event_outbox (created_at, event_id)',
               'SELECT 1')
       FROM information_schema.statistics
      WHERE table_schema = DATABASE()
        AND table_name = 'scheduler_event_outbox'
        AND index_name = 'idx_event_outbox_created');
PREPARE ratchet_event_outbox_index_statement FROM @ratchet_event_outbox_index_ddl;
EXECUTE ratchet_event_outbox_index_statement;
DEALLOCATE PREPARE ratchet_event_outbox_index_statement;
