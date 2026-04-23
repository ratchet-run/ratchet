package run.ratchet.store.mongodb;

/**
 * BSON field names used in Mongo filter, update, and projection expressions.
 *
 * <p>{@link DocumentMapper} keeps its own hardcoded literals because it is the single entity ↔ BSON
 * mapping authority. Everything else (operation classes, index initializer) references these
 * constants so a rename touches one file instead of eight.
 */
final class MongoFieldNames {

  private MongoFieldNames() {}

  // Document identifier (MongoDB convention).
  static final String ID = "_id";

  // Job core lifecycle
  static final String STATUS = "status";
  static final String PAUSED_FROM_STATUS = "paused_from_status";
  static final String JOB_TYPE = "job_type";
  static final String PRIORITY = "priority";
  static final String ATTEMPTS = "attempts";
  static final String MAX_RETRIES = "max_retries";
  static final String LAST_ERROR = "last_error";
  static final String VERSION = "version";
  static final String CREATED_AT = "created_at";
  static final String CALLER_PRINCIPAL = "caller_principal";
  static final String UPDATED_AT = "updated_at";

  // Scheduling
  static final String SCHEDULED_TIME = "scheduled_time";
  static final String NEXT_FIRE = "next_fire";
  static final String PICKED_BY = "picked_by";
  static final String PICKED_AT = "picked_at";

  // Keys
  static final String BUSINESS_KEY = "business_key";
  static final String IDEMPOTENCY_KEY = "idempotency_key";

  // Workflow linkage
  static final String DEPENDS_ON = "depends_on";
  static final String TAGS = "tags";
  static final String TARGET_CLASS = "target_class";
  static final String FINAL_STATUS = "final_status";

  // Execution timing + results
  static final String EXECUTION_START_TIME = "execution_start_time";
  static final String EXECUTION_END_TIME = "execution_end_time";
  static final String EXECUTION_DURATION_MS = "execution_duration_ms";
  static final String QUEUE_WAIT_MS = "queue_wait_ms";
  static final String JOB_RESULT = "job_result";
  static final String RESULT_TYPE = "result_type";

  // Batch
  static final String TOTAL_ITEMS = "total_items";
  static final String COMPLETED_ITEMS = "completed_items";
  static final String FAILED_ITEMS = "failed_items";
  static final String COMPLETION_PROCESSED = "completion_processed";

  // Batch metrics
  static final String TOTAL_DURATION_MS = "total_duration_ms";
  static final String CHILD_EXECUTION_MS = "child_execution_ms";
  static final String OVERHEAD_MS = "overhead_ms";
  static final String CHILD_COUNT = "child_count";
  static final String SUCCESS_COUNT = "success_count";
  static final String STARTED_AT = "started_at";
  static final String COMPLETED_AT = "completed_at";

  // Job execution attempt
  static final String JOB_ID = "job_id";
  static final String ATTEMPT = "attempt";

  // Job log
  static final String TS = "ts";

  // Archive
  static final String ARCHIVED_AT = "archived_at";
  static final String ORIGINAL_JOB_ID = "original_job_id";
  static final String ORIGINAL_CREATED_AT = "original_created_at";
  static final String COMPLETION_TIME = "completion_time";
  static final String METHOD_NAME = "method_name";
  static final String SUPERSEDED_BY = "superseded_by";

  // Node
  static final String HEARTBEAT_TS = "heartbeat_ts";
  static final String NODE_ID = "node_id";

  // Distributed lock
  static final String OWNER_NODE = "owner_node";
  static final String LOCKED_AT = "locked_at";
  static final String EXPIRES_AT = "expires_at";

  // Workflow condition
  static final String PARENT_JOB_ID = "parent_job_id";
  static final String CHILD_JOB_ID = "child_job_id";
  static final String CONDITION_TYPE = "condition_type";
  static final String CONDITION_PRIORITY = "condition_priority";

  // DLQ alert
  static final String ERROR_HASH = "error_hash";
  static final String ALERT_SENT_AT = "alert_sent_at";

  // Resource permit / limit
  static final String RESOURCE_NAME = "resource_name";
  static final String ACTIVE_COUNT = "active_count";
  static final String MAX_CONCURRENT = "max_concurrent";
  static final String RETRY_DELAY_MS = "retry_delay_ms";
  static final String DESCRIPTION = "description";
}
