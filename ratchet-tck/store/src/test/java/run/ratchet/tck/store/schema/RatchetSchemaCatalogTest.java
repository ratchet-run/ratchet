package run.ratchet.tck.store.schema;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

class RatchetSchemaCatalogTest {

  @Test
  void schedulerJobQueueIncludesSignalWaitingState() {
    Table queue = table("scheduler_job_queue");

    assertTrue(
        queue.columns().stream().map(Column::name).toList().containsAll(signalColumns()),
        "scheduler_job_queue should declare every signal waiting column");
    assertEquals(
        List.of("signal_key", "status"),
        index(queue, "idx_signal_key_status").columns(),
        "signal key lookup index should match DDL");
    assertEquals(
        List.of("status", "signal_timeout"),
        index(queue, "idx_signal_timeout_status").columns(),
        "signal timeout scan index should match DDL");
    assertEquals(
        List.of("signal_delivery_id"),
        index(queue, "idx_signal_delivery_id").columns(),
        "signal delivery lookup index should match DDL");
    assertTrue(
        RatchetSchemaCatalog.CURRENT_VERSION > 5,
        "catalog version should advance when signal queue metadata is added");
  }

  @Test
  void schedulerRecurringJobIncludesExecutionTarget() {
    Table recurring = table("scheduler_recurring_job");
    Table archive = table("scheduler_recurring_job_archive");

    assertTrue(
        recurring.columns().stream().map(Column::name).toList().contains("execution_target"),
        "scheduler_recurring_job should persist execution_target for occurrence inheritance");
    assertTrue(
        archive.columns().stream().map(Column::name).toList().contains("execution_target"),
        "scheduler_recurring_job_archive should snapshot execution_target");
    assertTrue(
        RatchetSchemaCatalog.CURRENT_VERSION > 7,
        "catalog version should advance when recurring execution_target is added");
  }

  private static List<String> signalColumns() {
    return List.of(
        "signal_key",
        "signal_timeout",
        "signal_payload",
        "signal_payload_type",
        "signal_outcome",
        "signal_rejection_reason",
        "signal_delivered_at",
        "signal_delivered_by",
        "signal_delivery_id");
  }

  private static Table table(String name) {
    return RatchetSchemaCatalog.CURRENT.tables().stream()
        .filter(t -> t.name().equals(name))
        .findFirst()
        .orElseThrow();
  }

  private static Index index(Table table, String name) {
    return table.indexes().stream().filter(i -> i.name().equals(name)).findFirst().orElseThrow();
  }
}
