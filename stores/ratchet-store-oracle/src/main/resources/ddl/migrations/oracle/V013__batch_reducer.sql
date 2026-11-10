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
  column_count PLS_INTEGER;
  index_count PLS_INTEGER;
BEGIN
  SELECT COUNT(*) INTO column_count FROM user_tab_columns
   WHERE table_name = 'SCHEDULER_BATCH' AND column_name = 'REDUCER';
  IF column_count = 0 THEN
    EXECUTE IMMEDIATE 'ALTER TABLE scheduler_batch ADD reducer CLOB';
  END IF;
  SELECT COUNT(*) INTO column_count FROM user_tab_columns
   WHERE table_name = 'SCHEDULER_BATCH' AND column_name = 'FOLD_CLAIMED_AT';
  IF column_count = 0 THEN
    EXECUTE IMMEDIATE 'ALTER TABLE scheduler_batch ADD fold_claimed_at TIMESTAMP(6)';
  END IF;
  SELECT COUNT(*) INTO index_count FROM user_indexes WHERE index_name = 'IDX_JOB_DEPENDS_ON';
  IF index_count > 0 THEN
    EXECUTE IMMEDIATE 'DROP INDEX idx_job_depends_on';
  END IF;
  EXECUTE IMMEDIATE 'CREATE INDEX idx_job_depends_on ON scheduler_job (depends_on, job_id)';
END;
