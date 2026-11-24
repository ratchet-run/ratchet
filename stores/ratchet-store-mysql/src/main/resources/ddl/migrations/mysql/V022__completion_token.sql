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

SET @ratchet_completion_token_ddl =
    (SELECT IF(COUNT(*) = 0,
               'ALTER TABLE scheduler_job ADD completion_token BINARY(16) NULL',
               'SELECT 1')
       FROM information_schema.columns
      WHERE table_schema = DATABASE()
        AND table_name = 'scheduler_job'
        AND column_name = 'completion_token');
PREPARE ratchet_completion_token_statement FROM @ratchet_completion_token_ddl;
EXECUTE ratchet_completion_token_statement;
DEALLOCATE PREPARE ratchet_completion_token_statement;
