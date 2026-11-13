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
CREATE TABLE IF NOT EXISTS scheduler_resource_waiter
(
    job_id        UUID NOT NULL,
    resource_name VARCHAR(100) NOT NULL,
    priority      INTEGER NOT NULL,
    wait_since    TIMESTAMPTZ(6) NOT NULL,
    lease_until   TIMESTAMPTZ(6) NOT NULL,
    CONSTRAINT pk_scheduler_resource_waiter PRIMARY KEY (job_id)
);

CREATE INDEX IF NOT EXISTS idx_resource_waiter_order ON scheduler_resource_waiter
    (resource_name, priority DESC, wait_since ASC, job_id ASC);
