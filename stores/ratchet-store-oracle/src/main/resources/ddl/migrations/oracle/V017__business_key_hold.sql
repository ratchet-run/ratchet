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
-- A held row keeps a business-key reservation after a running job is cancelled, until its body ends.
DECLARE
  column_count PLS_INTEGER;
BEGIN
  SELECT COUNT(*) INTO column_count FROM user_tab_columns
   WHERE table_name = 'SCHEDULER_BUSINESS_KEY_RESERVATION'
     AND column_name = 'HELD_UNTIL';
  IF column_count = 0 THEN
    EXECUTE IMMEDIATE 'ALTER TABLE scheduler_business_key_reservation ADD held_until TIMESTAMP(6)';
  END IF;
  SELECT COUNT(*) INTO column_count FROM user_tab_columns
   WHERE table_name = 'SCHEDULER_BUSINESS_KEY_RESERVATION'
     AND column_name = 'HELD_BY_NODE';
  IF column_count = 0 THEN
    EXECUTE IMMEDIATE 'ALTER TABLE scheduler_business_key_reservation ADD held_by_node VARCHAR2(64)';
  END IF;
  SELECT COUNT(*) INTO column_count FROM user_tab_columns
   WHERE table_name = 'SCHEDULER_BUSINESS_KEY_RESERVATION'
     AND column_name = 'HELD_CLAIM_SEQ';
  IF column_count = 0 THEN
    EXECUTE IMMEDIATE 'ALTER TABLE scheduler_business_key_reservation ADD held_claim_seq NUMBER(19)';
  END IF;
  BEGIN
    EXECUTE IMMEDIATE 'CREATE INDEX idx_bk_held_until ON scheduler_business_key_reservation (held_until)';
  EXCEPTION
    WHEN OTHERS THEN
      IF SQLCODE != -955 THEN
        RAISE;
      END IF;
  END;
END;
