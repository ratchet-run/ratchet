package run.ratchet.tck.store.schema;

import static run.ratchet.tck.store.schema.Column.nullable;
import static run.ratchet.tck.store.schema.Column.required;
import static run.ratchet.tck.store.schema.LogicalType.BOOLEAN;
import static run.ratchet.tck.store.schema.LogicalType.CHAR_1;
import static run.ratchet.tck.store.schema.LogicalType.INT32;
import static run.ratchet.tck.store.schema.LogicalType.INT64;
import static run.ratchet.tck.store.schema.LogicalType.JSON;
import static run.ratchet.tck.store.schema.LogicalType.TEXT;
import static run.ratchet.tck.store.schema.LogicalType.TIMESTAMP_TZ;
import static run.ratchet.tck.store.schema.LogicalType.UUID;

import run.ratchet.tck.store.schema.DeprecatedArtifact.DroppedColumn;
import run.ratchet.tck.store.schema.DeprecatedArtifact.DroppedIndex;
import java.util.List;

/**
 * Canonical schema definition covering every persistent table in the Ratchet scheduler schema. The
 * catalog is the source of truth for the cross-store schema-conformance contract — any conforming
 * store must satisfy every PK, FK, column type, and index declared here for every table.
 *
 * <p>Generated columns ({@code target_class}, {@code method_name} on {@code scheduler_job}) are
 * intentionally omitted because their MySQL {@code STORED} vs PostgreSQL {@code STORED} expressions
 * are introspection-asymmetric and not part of the conformance contract.
 *
 * <p>Three columns are intentionally omitted because their physical types diverge across stores
 * with no single satisfying {@link LogicalType}: {@code scheduler_batch.progress_hook} (MySQL JSON
 * vs PG TEXT), {@code scheduler_job_log.mdc} (MySQL JSON vs PG TEXT), and {@code
 * scheduler_job_archive.job_result} (MySQL JSON vs PG TEXT). The conformance contract verifies
 * presence and conformance, not exclusivity — omitting these does not weaken the contract.
 */
public final class RatchetSchemaCatalog {

  public static final int CURRENT_VERSION = 5;

  public static final SchemaSpec CURRENT =
      new SchemaSpec(
          CURRENT_VERSION,
          List.of(
              schedulerJob(),
              schedulerJobQueue(),
              schedulerNode(),
              schedulerLock(),
              schedulerResourceLimit(),
              schedulerBusinessKeyReservation(),
              schedulerJobTag(),
              schedulerBatch(),
              schedulerBatchMetrics(),
              schedulerJobExecution(),
              schedulerJobLog(),
              schedulerJobArchive(),
              schedulerWorkflowCondition(),
              schedulerDlqAlerts(),
              schedulerResourcePermit()),
          v005Drops());

  private RatchetSchemaCatalog() {}

  private static Table schedulerJob() {
    return Table.builder("scheduler_job")
        .column(required("job_id", UUID))
        .column(required("job_type", TEXT))
        .column(required("priority", INT32))
        .column(required("max_retries", INT32))
        .column(required("backoff_policy", TEXT))
        .column(required("backoff_param_ms", INT32))
        .column(required("timeout_sec", INT32))
        .column(required("cron_expr", TEXT))
        .column(required("zone_id", TEXT))
        .column(nullable("next_fire", TIMESTAMP_TZ))
        .column(required("payload", JSON))
        .column(nullable("params", JSON))
        .column(required("idempotency_key", TEXT))
        .column(nullable("business_key", TEXT))
        .column(nullable("resource_name", TEXT))
        .column(nullable("on_success_payload", JSON))
        .column(nullable("on_failure_payload", JSON))
        .column(nullable("depends_on", UUID))
        .column(nullable("superseded_by", UUID))
        .column(required("created_at", TIMESTAMP_TZ))
        .column(nullable("created_by", TEXT))
        .column(nullable("caller_principal", TEXT))
        // V005 cold survivors / additions.
        .column(nullable("terminal_status", TEXT))
        .column(nullable("terminal_error", TEXT))
        .column(nullable("total_attempts", INT32))
        .column(nullable("terminated_at", TIMESTAMP_TZ))
        .column(nullable("execution_start_time", TIMESTAMP_TZ))
        .column(nullable("execution_end_time", TIMESTAMP_TZ))
        .column(nullable("execution_duration_ms", INT64))
        .column(nullable("queue_wait_ms", INT64))
        .column(nullable("job_result", JSON))
        .column(nullable("result_type", TEXT))
        .column(nullable("rec_status", CHAR_1))
        .primaryKey("job_id")
        .index(Index.unique("uk_idempotency_key", "idempotency_key"))
        .index(Index.of("idx_job_depends_on", "depends_on"))
        .index(Index.of("idx_job_superseded_by", "superseded_by"))
        .index(Index.of("idx_job_business_key", "business_key"))
        .index(Index.of("idx_job_created_at", "created_at"))
        .index(Index.of("idx_job_terminal", "terminal_status", "terminated_at"))
        .index(Index.of("idx_job_recurring_pending", "job_type", "rec_status", "next_fire"))
        .build();
  }

  private static Table schedulerJobQueue() {
    return Table.builder("scheduler_job_queue")
        .column(required("job_id", UUID))
        .column(required("status", TEXT))
        .column(required("job_type", TEXT))
        .column(required("priority", INT32))
        .column(required("scheduled_time", TIMESTAMP_TZ))
        .column(nullable("business_key", TEXT))
        .column(required("timeout_sec", INT32))
        .column(required("max_retries", INT32))
        .column(required("attempts", INT32))
        .column(nullable("picked_by", TEXT))
        .column(nullable("picked_at", TIMESTAMP_TZ))
        .column(nullable("paused_from_status", TEXT))
        .column(nullable("last_error", TEXT))
        .column(required("version", INT32))
        .column(required("updated_at", TIMESTAMP_TZ))
        .primaryKey("job_id")
        .foreignKey(
            new ForeignKey(
                "fk_job_queue_job", "job_id", "scheduler_job", "job_id", OnDeleteAction.CASCADE))
        .index(
            Index.of("idx_claim_executable", "job_type", "scheduled_time", "priority", "job_id")
                .withPartialPredicate(LogicalPredicate.eq("status", "PENDING")))
        .index(Index.of("idx_queue_orphan", "status", "picked_at", "picked_by"))
        .build();
  }

  private static Table schedulerNode() {
    return Table.builder("scheduler_node")
        .column(required("node_id", TEXT))
        .column(required("heartbeat_ts", TIMESTAMP_TZ))
        .column(required("started_at", TIMESTAMP_TZ))
        .column(nullable("node_info", TEXT))
        .primaryKey("node_id")
        .index(Index.of("idx_node_heartbeat", "heartbeat_ts"))
        .build();
  }

  private static Table schedulerLock() {
    return Table.builder("scheduler_lock")
        .column(required("lock_name", TEXT))
        .column(required("owner_node", TEXT))
        .column(required("locked_at", TIMESTAMP_TZ))
        .column(required("expires_at", TIMESTAMP_TZ))
        .primaryKey("lock_name")
        .index(Index.of("idx_lock_expires", "expires_at"))
        .build();
  }

  private static Table schedulerResourceLimit() {
    return Table.builder("scheduler_resource_limit")
        .column(required("resource_name", TEXT))
        .column(required("max_concurrent", INT32))
        .column(required("retry_delay_ms", INT32))
        .column(nullable("description", TEXT))
        .column(required("created_at", TIMESTAMP_TZ))
        .column(required("updated_at", TIMESTAMP_TZ))
        .primaryKey("resource_name")
        .build();
  }

  private static Table schedulerBusinessKeyReservation() {
    // Plan 06 added fk_bk_owner_job to MySQL DDL so it now matches PG. The catalog asserts the
    // FK on both stores.
    return Table.builder("scheduler_business_key_reservation")
        .column(required("business_key", TEXT))
        .column(required("owner_job_id", UUID))
        .column(required("owner_table", TEXT))
        .column(required("reserved_at", TIMESTAMP_TZ))
        .primaryKey("business_key")
        .foreignKey(
            new ForeignKey(
                "fk_bk_owner_job",
                "owner_job_id",
                "scheduler_job",
                "job_id",
                OnDeleteAction.CASCADE))
        .index(Index.of("idx_bk_owner", "owner_job_id"))
        .build();
  }

  private static Table schedulerJobTag() {
    return Table.builder("scheduler_job_tag")
        .column(required("job_id", UUID))
        .column(required("tag", TEXT))
        .primaryKey("job_id", "tag")
        .foreignKey(
            new ForeignKey(
                "fk_job_tag_job", "job_id", "scheduler_job", "job_id", OnDeleteAction.CASCADE))
        .index(Index.of("idx_job_tag_tag_job", "tag", "job_id"))
        .build();
  }

  private static Table schedulerBatch() {
    return Table.builder("scheduler_batch")
        .column(required("batch_id", UUID))
        .column(required("total_items", INT32))
        .column(required("completed_items", INT32))
        .column(required("failed_items", INT32))
        .column(required("completion_processed", BOOLEAN))
        .column(required("version", INT32))
        // progress_hook intentionally omitted: MySQL JSON vs PG TEXT type asymmetry.
        .primaryKey("batch_id")
        .foreignKey(
            new ForeignKey(
                "fk_batch_job", "batch_id", "scheduler_job", "job_id", OnDeleteAction.CASCADE))
        .build();
  }

  private static Table schedulerBatchMetrics() {
    return Table.builder("scheduler_batch_metrics")
        .column(required("batch_id", UUID))
        .column(nullable("total_duration_ms", INT64))
        .column(nullable("child_execution_ms", INT64))
        .column(nullable("overhead_ms", INT64))
        .column(required("child_count", INT32))
        .column(required("success_count", INT32))
        .column(required("failure_count", INT32))
        .column(nullable("started_at", TIMESTAMP_TZ))
        .column(nullable("completed_at", TIMESTAMP_TZ))
        .column(required("version", INT32))
        .primaryKey("batch_id")
        .foreignKey(
            new ForeignKey(
                "fk_batch_metrics_job",
                "batch_id",
                "scheduler_job",
                "job_id",
                OnDeleteAction.CASCADE))
        .build();
  }

  private static Table schedulerJobExecution() {
    return Table.builder("scheduler_job_execution")
        .column(required("id", UUID))
        .column(required("job_id", UUID))
        .column(required("attempt", INT32))
        .column(required("node_id", TEXT))
        .column(required("started_at", TIMESTAMP_TZ))
        .column(nullable("ended_at", TIMESTAMP_TZ))
        .column(required("status", TEXT))
        .column(nullable("error_message", TEXT))
        .column(nullable("error_class", TEXT))
        .column(nullable("duration_ms", INT64))
        .primaryKey("id")
        .foreignKey(
            new ForeignKey(
                "fk_execution_job", "job_id", "scheduler_job", "job_id", OnDeleteAction.CASCADE))
        .index(Index.of("idx_job_execution_job", "job_id"))
        .index(Index.of("idx_job_execution_node", "node_id", "started_at"))
        .index(Index.of("idx_job_execution_status", "status", "started_at"))
        .build();
  }

  private static Table schedulerJobLog() {
    return Table.builder("scheduler_job_log")
        .column(required("log_id", UUID))
        .column(required("job_id", UUID))
        .column(required("ts", TIMESTAMP_TZ))
        .column(required("level", TEXT))
        .column(required("message", TEXT))
        // mdc intentionally omitted: MySQL JSON vs PG TEXT type asymmetry.
        .primaryKey("log_id")
        .foreignKey(
            new ForeignKey(
                "fk_log_job", "job_id", "scheduler_job", "job_id", OnDeleteAction.CASCADE))
        .index(Index.of("idx_joblog_job_ts", "job_id", "ts"))
        .index(Index.of("idx_joblog_ts", "ts"))
        .build();
  }

  private static Table schedulerJobArchive() {
    // No FK on original_job_id by design — archived rows survive job-row deletion.
    return Table.builder("scheduler_job_archive")
        .column(required("archive_id", UUID))
        .column(required("original_job_id", UUID))
        .column(required("final_status", TEXT))
        .column(required("job_type", TEXT))
        .column(required("priority", INT32))
        .column(required("total_attempts", INT32))
        .column(required("max_retries", INT32))
        .column(required("backoff_policy", TEXT))
        .column(required("backoff_param_ms", INT32))
        .column(required("timeout_sec", INT32))
        .column(nullable("target_class", TEXT))
        .column(nullable("method_name", TEXT))
        .column(nullable("business_key", TEXT))
        .column(nullable("cron_expr", TEXT))
        .column(nullable("zone_id", TEXT))
        .column(required("original_scheduled_time", TIMESTAMP_TZ))
        .column(required("original_created_at", TIMESTAMP_TZ))
        .column(nullable("first_execution_time", TIMESTAMP_TZ))
        .column(nullable("completion_time", TIMESTAMP_TZ))
        .column(nullable("total_execution_time_ms", INT64))
        .column(nullable("queue_wait_ms", INT64))
        .column(required("archived_at", TIMESTAMP_TZ))
        .column(nullable("archived_by", TEXT))
        .column(nullable("archive_reason", TEXT))
        // job_result intentionally omitted: MySQL JSON vs PG TEXT type asymmetry.
        .column(nullable("result_type", TEXT))
        .column(nullable("final_error", TEXT))
        .column(nullable("payload_summary", TEXT))
        .column(nullable("depended_on", UUID))
        .column(nullable("superseded_by", UUID))
        .column(nullable("tags", TEXT))
        .primaryKey("archive_id")
        .index(Index.of("idx_archive_original_id", "original_job_id"))
        .index(Index.of("idx_archive_status", "final_status"))
        .index(Index.of("idx_archive_created_range", "original_created_at"))
        .index(Index.of("idx_archive_completed_range", "completion_time"))
        .index(Index.of("idx_archive_archived_at", "archived_at"))
        .index(Index.of("idx_archive_target_class", "target_class"))
        .index(Index.of("idx_archive_business_key", "business_key"))
        .index(Index.of("idx_archive_job_type", "job_type"))
        .index(Index.of("idx_archive_priority", "priority"))
        .build();
  }

  private static Table schedulerWorkflowCondition() {
    return Table.builder("scheduler_workflow_condition")
        .column(required("id", UUID))
        .column(required("parent_job_id", UUID))
        .column(required("child_job_id", UUID))
        .column(required("condition_type", TEXT))
        .column(nullable("condition_expression", TEXT))
        .column(required("condition_priority", INT32))
        .column(required("created_at", TIMESTAMP_TZ))
        .primaryKey("id")
        .foreignKey(
            new ForeignKey(
                "fk_workflow_parent",
                "parent_job_id",
                "scheduler_job",
                "job_id",
                OnDeleteAction.CASCADE))
        .foreignKey(
            new ForeignKey(
                "fk_workflow_child",
                "child_job_id",
                "scheduler_job",
                "job_id",
                OnDeleteAction.CASCADE))
        .index(Index.of("idx_workflow_parent", "parent_job_id"))
        .index(Index.of("idx_workflow_child", "child_job_id"))
        .index(Index.of("idx_workflow_priority", "parent_job_id", "condition_priority"))
        .build();
  }

  private static Table schedulerDlqAlerts() {
    return Table.builder("scheduler_dlq_alerts")
        .column(required("id", UUID))
        .column(required("job_id", UUID))
        .column(required("error_hash", TEXT))
        .column(nullable("alert_sent_at", TIMESTAMP_TZ))
        .column(nullable("alert_channel", TEXT))
        .primaryKey("id")
        .foreignKey(
            new ForeignKey(
                "fk_dlq_alert_job", "job_id", "scheduler_job", "job_id", OnDeleteAction.CASCADE))
        .index(Index.unique("uk_job_error_hash", "job_id", "error_hash"))
        .index(Index.of("idx_dlq_sent_at", "alert_sent_at"))
        .build();
  }

  private static Table schedulerResourcePermit() {
    return Table.builder("scheduler_resource_permit")
        .column(required("id", UUID))
        .column(required("resource_name", TEXT))
        .column(required("job_id", UUID))
        .column(required("node_id", TEXT))
        .column(required("acquired_at", TIMESTAMP_TZ))
        .primaryKey("id")
        .foreignKey(
            new ForeignKey(
                "fk_resource_permit_job",
                "job_id",
                "scheduler_job",
                "job_id",
                OnDeleteAction.CASCADE))
        .index(Index.of("idx_resource_permit_resource", "resource_name"))
        .index(Index.of("idx_resource_permit_job", "job_id"))
        .build();
  }

  /**
   * Artifacts V005 removed. Any conforming store at schema version ≥ 5 must NOT carry these — a
   * presence-only check would silently pass an upgrade that left obsolete columns/indexes behind.
   */
  private static List<DeprecatedArtifact> v005Drops() {
    return List.of(
        // scheduler_job hot columns moved to scheduler_job_queue
        new DroppedColumn("scheduler_job", "status", 5),
        new DroppedColumn("scheduler_job", "paused_from_status", 5),
        new DroppedColumn("scheduler_job", "scheduled_time", 5),
        new DroppedColumn("scheduler_job", "attempts", 5),
        new DroppedColumn("scheduler_job", "picked_by", 5),
        new DroppedColumn("scheduler_job", "picked_at", 5),
        new DroppedColumn("scheduler_job", "last_error", 5),
        new DroppedColumn("scheduler_job", "updated_at", 5),
        new DroppedColumn("scheduler_job", "version", 5),
        // active_business_key column dropped (MySQL had it as a generated column; PG never did,
        // but listing here is harmless — the deprecated check is "must not exist anywhere").
        new DroppedColumn("scheduler_job", "active_business_key", 5),
        // hot-path indexes obsoleted by the split
        new DroppedIndex("scheduler_job", "idx_job_poll_composite", 5),
        new DroppedIndex("scheduler_job", "idx_job_claim_cover", 5),
        new DroppedIndex("scheduler_job", "idx_recurring_due", 5),
        new DroppedIndex("scheduler_job", "idx_job_recurring_composite", 5),
        new DroppedIndex("scheduler_job", "idx_job_due", 5),
        new DroppedIndex("scheduler_job", "idx_job_priority_due", 5),
        new DroppedIndex("scheduler_job", "idx_job_picked_by", 5),
        new DroppedIndex("scheduler_job", "idx_job_updated_at", 5),
        new DroppedIndex("scheduler_job", "idx_job_type", 5),
        new DroppedIndex("scheduler_job", "uk_active_business_key", 5));
  }
}
