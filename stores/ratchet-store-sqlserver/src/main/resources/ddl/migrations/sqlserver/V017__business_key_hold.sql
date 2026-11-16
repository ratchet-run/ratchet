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
IF COL_LENGTH('scheduler_business_key_reservation', 'held_until') IS NULL
    ALTER TABLE scheduler_business_key_reservation ADD held_until DATETIME2(6) NULL;
IF COL_LENGTH('scheduler_business_key_reservation', 'held_by_node') IS NULL
    ALTER TABLE scheduler_business_key_reservation ADD held_by_node VARCHAR(64) NULL;
IF COL_LENGTH('scheduler_business_key_reservation', 'held_claim_seq') IS NULL
    ALTER TABLE scheduler_business_key_reservation ADD held_claim_seq BIGINT NULL;
IF NOT EXISTS (SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'scheduler_business_key_reservation')
      AND name = N'idx_bk_held_until')
BEGIN
    EXEC(N'CREATE INDEX idx_bk_held_until ON scheduler_business_key_reservation (held_until)
        WHERE held_until IS NOT NULL');
END;
