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
-- A held row keeps a business-key reservation after a running job is cancelled, until its body ends.
SET @ratchet_bk_hold_ddl =
    (SELECT IF(COUNT(*) = 0,
               'ALTER TABLE scheduler_business_key_reservation ADD held_until DATETIME(6) NULL',
               'SELECT 1')
       FROM information_schema.columns
      WHERE table_schema = DATABASE()
        AND table_name = 'scheduler_business_key_reservation'
        AND column_name = 'held_until');
PREPARE ratchet_bk_hold_statement FROM @ratchet_bk_hold_ddl;
EXECUTE ratchet_bk_hold_statement;
DEALLOCATE PREPARE ratchet_bk_hold_statement;

SET @ratchet_bk_hold_ddl =
    (SELECT IF(COUNT(*) = 0,
               'ALTER TABLE scheduler_business_key_reservation ADD held_by_node VARCHAR(64) NULL',
               'SELECT 1')
       FROM information_schema.columns
      WHERE table_schema = DATABASE()
        AND table_name = 'scheduler_business_key_reservation'
        AND column_name = 'held_by_node');
PREPARE ratchet_bk_hold_statement FROM @ratchet_bk_hold_ddl;
EXECUTE ratchet_bk_hold_statement;
DEALLOCATE PREPARE ratchet_bk_hold_statement;

SET @ratchet_bk_hold_ddl =
    (SELECT IF(COUNT(*) = 0,
               'ALTER TABLE scheduler_business_key_reservation ADD held_claim_seq BIGINT NULL',
               'SELECT 1')
       FROM information_schema.columns
      WHERE table_schema = DATABASE()
        AND table_name = 'scheduler_business_key_reservation'
        AND column_name = 'held_claim_seq');
PREPARE ratchet_bk_hold_statement FROM @ratchet_bk_hold_ddl;
EXECUTE ratchet_bk_hold_statement;
DEALLOCATE PREPARE ratchet_bk_hold_statement;

SET @ratchet_bk_hold_ddl =
    (SELECT IF(COUNT(*) = 0,
               'CREATE INDEX idx_bk_held_until ON scheduler_business_key_reservation (held_until)',
               'SELECT 1')
       FROM information_schema.statistics
      WHERE table_schema = DATABASE()
        AND table_name = 'scheduler_business_key_reservation'
        AND index_name = 'idx_bk_held_until');
PREPARE ratchet_bk_hold_statement FROM @ratchet_bk_hold_ddl;
EXECUTE ratchet_bk_hold_statement;
DEALLOCATE PREPARE ratchet_bk_hold_statement;
