-- Remove workflow condition rows whose expression is a JDK-serialized Base64 blob
-- (the old format before predicates were migrated to JobPayload JSON in 0.1.x).
--
-- Affected condition types: CUSTOM, BATCH_CUSTOM, RESULT_VALUE.
-- Threshold types (BATCH_SUCCESS_RATE, BATCH_FAILURE_COUNT) store plain numeric strings
-- and are not touched by this migration.
--
-- Detection: JSON payloads start with '{'; Base64 blobs do not.

DELETE FROM scheduler_workflow_condition
WHERE condition_type IN ('CUSTOM', 'BATCH_CUSTOM', 'RESULT_VALUE')
  AND condition_expression IS NOT NULL
  AND condition_expression NOT LIKE '{%';
