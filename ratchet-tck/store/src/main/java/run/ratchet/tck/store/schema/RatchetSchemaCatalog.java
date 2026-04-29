package run.ratchet.tck.store.schema;

import static run.ratchet.tck.store.schema.Column.nullable;
import static run.ratchet.tck.store.schema.Column.required;
import static run.ratchet.tck.store.schema.LogicalType.CHAR_1;
import static run.ratchet.tck.store.schema.LogicalType.INT32;
import static run.ratchet.tck.store.schema.LogicalType.INT64;
import static run.ratchet.tck.store.schema.LogicalType.JSON;
import static run.ratchet.tck.store.schema.LogicalType.TEXT;
import static run.ratchet.tck.store.schema.LogicalType.TIMESTAMP_TZ;

import run.ratchet.tck.store.schema.DeprecatedArtifact.DroppedColumn;
import run.ratchet.tck.store.schema.DeprecatedArtifact.DroppedIndex;
import java.util.List;

/**
 * Canonical schema definition for the post-V005 hot/cold split. Scoped to the two tables the split
 * directly governs ({@code scheduler_job} cold + {@code scheduler_job_queue} hot) plus the
 * artifacts V005 explicitly removes. Other Ratchet tables (archive, workflow, batch, etc.) can be
 * added to this catalog incrementally without changing the contract shape.
 *
 * <p>Generated columns ({@code target_class}, {@code method_name}) are intentionally omitted — they
 * are derived from {@code payload} and their introspection asymmetries (MySQL {@code STORED} vs
 * PostgreSQL {@code STORED}) are not what Phase 7 conformance is about.
 */
public final class RatchetSchemaCatalog {

  public static final int CURRENT_VERSION = 5;

  public static final SchemaSpec CURRENT =
      new SchemaSpec(CURRENT_VERSION, List.of(schedulerJob(), schedulerJobQueue()), v005Drops());

  private RatchetSchemaCatalog() {}

  private static Table schedulerJob() {
    return Table.builder("scheduler_job")
        .column(required("job_id", INT64))
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
        .column(nullable("depends_on", INT64))
        .column(nullable("superseded_by", INT64))
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
        .index(Index.of("idx_job_terminal", "terminal_status", "terminated_at"))
        .index(Index.of("idx_job_recurring_pending", "job_type", "rec_status", "next_fire"))
        .build();
  }

  private static Table schedulerJobQueue() {
    return Table.builder("scheduler_job_queue")
        .column(required("job_id", INT64))
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
