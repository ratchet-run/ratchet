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

  @Test
  void schedulerJobDeclaresRecurringMasterAndTraceContext() {
    Table job = table("scheduler_job");
    List<String> columns = job.columns().stream().map(Column::name).toList();

    assertTrue(
        columns.contains("recurring_master_id"),
        "scheduler_job should declare recurring_master_id (DDL v010)");
    assertTrue(
        columns.contains("trace_context"),
        "scheduler_job should declare trace_context (W3C TraceContext captured at enqueue)");

    ForeignKey fk =
        job.foreignKeys().stream()
            .filter(f -> f.name().equals("fk_job_recurring_master"))
            .findFirst()
            .orElseThrow();
    assertEquals("recurring_master_id", fk.column());
    assertEquals("scheduler_recurring_job", fk.refTable());
    assertEquals("id", fk.refColumn());
    assertEquals(
        OnDeleteAction.SET_NULL,
        fk.onDelete(),
        "recurring-master FK must be ON DELETE SET NULL to match the DDL");

    assertEquals(
        List.of("recurring_master_id"),
        index(job, "idx_job_recurring_master_id").columns(),
        "recurring master index should match DDL");
  }

  @Test
  void currentVersionTracksWorkflowDefinitionOrderSchemaRevision() {
    assertEquals(
        12,
        RatchetSchemaCatalog.CURRENT_VERSION,
        "CURRENT_VERSION must advance when workflow definition order is added to the schema"
            + " catalog");
  }

  @Test
  void workflowConditionsPersistDefinitionOrder() {
    Table workflowConditions = table("scheduler_workflow_condition");

    assertTrue(
        workflowConditions.columns().stream()
            .map(Column::name)
            .toList()
            .contains("definition_order"),
        "workflow conditions should persist builder registration order");
    assertEquals(
        List.of("parent_job_id", "condition_priority", "definition_order"),
        index(workflowConditions, "idx_workflow_evaluation_order").columns(),
        "workflow evaluation index should follow the routing order");
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
