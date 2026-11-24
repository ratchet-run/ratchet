-- ratchet:single-statement
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

DECLARE
  column_count PLS_INTEGER;
BEGIN
  SELECT COUNT(*) INTO column_count FROM user_tab_columns
   WHERE table_name = 'SCHEDULER_JOB'
     AND column_name = 'COMPLETION_TOKEN';
  IF column_count = 0 THEN
    EXECUTE IMMEDIATE 'ALTER TABLE scheduler_job ADD completion_token RAW(16) NULL';
  END IF;
END;
