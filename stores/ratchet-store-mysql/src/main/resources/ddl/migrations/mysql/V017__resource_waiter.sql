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
-- No foreign key: waiter inserts must not lock scheduler_job during a park.
SET @ratchet_waiter_table_ddl =
    (SELECT IF(COUNT(*) = 0,
               'CREATE TABLE scheduler_resource_waiter
(
    job_id        BINARY(16) NOT NULL,
    resource_name VARCHAR(100) NOT NULL,
    priority      INT NOT NULL,
    wait_since    DATETIME(6) NOT NULL,
    lease_until   DATETIME(6) NOT NULL,
    PRIMARY KEY (job_id)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_unicode_ci',
               'SELECT 1')
       FROM information_schema.tables
      WHERE table_schema = DATABASE()
        AND table_name = 'scheduler_resource_waiter');
PREPARE ratchet_waiter_statement FROM @ratchet_waiter_table_ddl;
EXECUTE ratchet_waiter_statement;
DEALLOCATE PREPARE ratchet_waiter_statement;
SET @ratchet_waiter_index_ddl =
    (SELECT IF(COUNT(*) = 0,
               'CREATE INDEX idx_resource_waiter_order ON scheduler_resource_waiter
    (resource_name, priority DESC, wait_since ASC, job_id ASC)',
               'SELECT 1')
       FROM information_schema.statistics
      WHERE table_schema = DATABASE()
        AND table_name = 'scheduler_resource_waiter' AND index_name = 'idx_resource_waiter_order');
PREPARE ratchet_waiter_statement FROM @ratchet_waiter_index_ddl;
EXECUTE ratchet_waiter_statement;
DEALLOCATE PREPARE ratchet_waiter_statement;
