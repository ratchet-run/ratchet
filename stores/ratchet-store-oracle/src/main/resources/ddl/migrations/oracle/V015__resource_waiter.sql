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
BEGIN
  BEGIN
    EXECUTE IMMEDIATE 'CREATE TABLE scheduler_resource_waiter
(
    job_id        RAW(16) NOT NULL,
    resource_name VARCHAR2(100) NOT NULL,
    priority      NUMBER(3) NOT NULL,
    wait_since    TIMESTAMP(6) NOT NULL,
    lease_until   TIMESTAMP(6) NOT NULL,
    CONSTRAINT pk_scheduler_resource_waiter PRIMARY KEY (job_id)
)';
  EXCEPTION
    WHEN OTHERS THEN
      IF SQLCODE != -955 THEN
        RAISE;
      END IF;
  END;
  BEGIN
    EXECUTE IMMEDIATE 'CREATE INDEX idx_resource_waiter_order ON scheduler_resource_waiter
    (resource_name, priority DESC, wait_since ASC, job_id ASC)';
  EXCEPTION
    WHEN OTHERS THEN
      IF SQLCODE != -955 THEN
        RAISE;
      END IF;
  END;
END;
