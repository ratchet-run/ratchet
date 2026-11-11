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
IF OBJECT_ID('scheduler_event_outbox', 'U') IS NULL
BEGIN
CREATE TABLE scheduler_event_outbox
(
    event_id BINARY(16) NOT NULL,
    event_type VARCHAR(32) NOT NULL,
    job_id BINARY(16) NOT NULL,
    business_key VARCHAR(512) NULL,
    recurring_master_id BINARY(16) NULL,
    job_type VARCHAR(32) NULL,
    priority VARCHAR(16) NULL,
    node_id VARCHAR(64) NULL,
    event_ts DATETIME2(6) NOT NULL,
    execution_time_ms BIGINT NULL,
    error_message NVARCHAR(MAX) NULL,
    retry_attempt INT NOT NULL,
    created_at DATETIME2(6) NOT NULL,
    CONSTRAINT pk_scheduler_event_outbox PRIMARY KEY (event_id)
);
END;

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
     WHERE name = 'idx_event_outbox_created'
       AND object_id = OBJECT_ID('scheduler_event_outbox'))
    CREATE INDEX idx_event_outbox_created
        ON scheduler_event_outbox (created_at, event_id);
