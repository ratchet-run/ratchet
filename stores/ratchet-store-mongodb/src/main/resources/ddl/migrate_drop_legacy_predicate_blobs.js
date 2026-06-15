/**
 * Remove workflow condition documents whose expression is a JDK-serialized Base64 blob
 * (the old format before predicates were migrated to JobPayload JSON in 0.1.x).
 *
 * Run once against each database after upgrading to 0.1.x:
 *   mongosh <connection-string> migrate_drop_legacy_predicate_blobs.js
 */
db.scheduler_workflow_condition.deleteMany({
  condition_type: { $in: ["CUSTOM", "BATCH_CUSTOM", "RESULT_VALUE"] },
  condition_expression: { $exists: true, $ne: null, $not: /^\{/ }
});
