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
package run.ratchet.store.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;

import jakarta.persistence.Query;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import run.ratchet.api.NodeTagFilter;

class JobClaimSqlSupportTest {

  @Test
  void buildTagFilterSqlOmitsEmptyTagClausesIndependently() {
    assertEquals("", JobClaimSqlSupport.buildTagFilterSql(NodeTagFilter.NONE, "q"));

    assertEquals(
        "\n  AND EXISTS (SELECT 1 FROM scheduler_job_tag t WHERE t.job_id = q.job_id"
            + " AND t.tag IN (?,?))",
        JobClaimSqlSupport.buildTagFilterSql(
            new NodeTagFilter(List.of("fast", "io"), List.of()), "q"));

    assertEquals(
        "\n  AND NOT EXISTS (SELECT 1 FROM scheduler_job_tag t WHERE t.job_id = q.job_id"
            + " AND t.tag IN (?))",
        JobClaimSqlSupport.buildTagFilterSql(new NodeTagFilter(List.of(), List.of("gpu")), "q"));
  }

  @Test
  void bindTagFilterBindsRequiredTagsThenExcludedTags() {
    Query query = mock(Query.class);

    int next =
        JobClaimSqlSupport.bindTagFilter(
            query, new NodeTagFilter(List.of("fast", "io"), List.of("gpu")), 3);

    assertEquals(6, next);
    InOrder order = inOrder(query);
    order.verify(query).setParameter(3, "fast");
    order.verify(query).setParameter(4, "io");
    order.verify(query).setParameter(5, "gpu");
  }

  @Test
  void buildBoostedOrderByUsesDialectProvidedOverdueMinutesExpression() {
    assertEquals(
        "priority DESC, scheduled_time ASC, job_id ASC",
        JobClaimSqlSupport.buildBoostedOrderBy("scheduled_time", "age_minutes", 0));

    assertEquals(
        "(priority + FLOOR(GREATEST(0, age_minutes) / ?)) DESC, scheduled_time ASC, job_id ASC",
        JobClaimSqlSupport.buildBoostedOrderBy("scheduled_time", "age_minutes", 15));

    assertEquals(
        "(priority + FLOOR(GREATEST(0, TIMESTAMPDIFF(MINUTE, scheduled_time, NOW(3))) / ?))"
            + " DESC, scheduled_time ASC, job_id ASC",
        JobClaimSqlSupport.buildBoostedOrderBy(
            "scheduled_time", "TIMESTAMPDIFF(MINUTE, scheduled_time, NOW(3))", 15));
  }

  @Test
  void buildBoostedOrderByQualifiesPriorityAndJobIdWithColumnPrefix() {
    assertEquals(
        "q.priority DESC, q.scheduled_time ASC, q.job_id ASC",
        JobClaimSqlSupport.buildBoostedOrderBy("q.scheduled_time", "age_minutes", 0, "q."));

    assertEquals(
        "(q.priority + FLOOR(GREATEST(0, age_minutes) / ?)) DESC, q.scheduled_time ASC,"
            + " q.job_id ASC",
        JobClaimSqlSupport.buildBoostedOrderBy("q.scheduled_time", "age_minutes", 15, "q."));
  }

  @Test
  void buildBoostedOrderByRejectsUnsafeFragments() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            JobClaimSqlSupport.buildBoostedOrderBy(
                "scheduled_time", "age_minutes); DELETE FROM scheduler_job_queue; --", 15));
  }

  @Test
  void buildBoostedOrderByRejectsUnsafeColumnPrefix() {
    assertThrows(
        IllegalArgumentException.class,
        () -> JobClaimSqlSupport.buildBoostedOrderBy("scheduled_time", "age_minutes", 15, "q; --"));
  }

  @Test
  void reorderByIdPreservesRequestedOrderAndSkipsMissingRows() {
    UUID first = UUID.fromString("00000000-0000-0000-0000-000000000001");
    UUID second = UUID.fromString("00000000-0000-0000-0000-000000000002");
    UUID missing = UUID.fromString("00000000-0000-0000-0000-000000000003");
    Row row1 = new Row(first, "first");
    Row row2 = new Row(second, "second");

    assertEquals(
        List.of(row2, row1),
        JobClaimSqlSupport.reorderById(
            List.of(row1, row2), List.of(second, missing, first), Row::id));
  }

  private record Row(UUID id, String label) {}
}
