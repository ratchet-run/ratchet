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
package run.ratchet.store.oracle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobPriority;
import run.ratchet.api.NodeTagFilter;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.oracle.converter.UuidRawConverter;
import run.ratchet.store.spi.ExecutionTargetFilter;

class OracleJobClaimOperationsTest {

  @Test
  void claimSelectColumnOrderMatchesNamedRowMapping() {
    List<String> columns = OracleJobClaimOperations.claimSelectColumnNames();

    assertEquals(
        List.of(
            "job_id",
            "status",
            "job_type",
            "priority",
            "scheduled_time",
            "version",
            "timeout_sec",
            "picked_by",
            "picked_at",
            "business_key",
            "attempts",
            "max_retries",
            "execution_target",
            "depends_on"),
        columns);
    assertEquals(
        Map.ofEntries(
            Map.entry("job_id", 0),
            Map.entry("status", 1),
            Map.entry("job_type", 2),
            Map.entry("priority", 3),
            Map.entry("scheduled_time", 4),
            Map.entry("version", 5),
            Map.entry("timeout_sec", 6),
            Map.entry("picked_by", 7),
            Map.entry("picked_at", 8),
            Map.entry("business_key", 9),
            Map.entry("attempts", 10),
            Map.entry("max_retries", 11),
            Map.entry("execution_target", 12),
            Map.entry("depends_on", 13)),
        OracleJobClaimOperations.claimSelectColumnIndexes());
  }

  @Test
  void claimSelectProjectsDependsOnFromColdMetadata() {
    assertEquals(expectedClaimSelectClause(), OracleJobClaimOperations.claimSelectClause());
  }

  @Test
  void lockedPhaseRechecksEligibilityAndUsesCurrentData() {
    EntityManager em = mock(EntityManager.class);
    Query candidates = mock(Query.class);
    Query locked = mock(Query.class);
    Query update = mock(Query.class);
    for (Query query : List.of(candidates, locked, update)) {
      when(query.setParameter(anyInt(), any())).thenReturn(query);
    }
    var sqls = new java.util.ArrayList<String>();
    when(em.createNativeQuery(anyString()))
        .thenAnswer(
            invocation -> {
              String sql = invocation.getArgument(0);
              sqls.add(sql);
              return sql.startsWith("UPDATE")
                  ? update
                  : sql.contains("FOR UPDATE") ? locked : candidates;
            });
    UUID id = UUID.randomUUID();
    Object[] before = {
      UuidRawConverter.toBytes(id),
      "PENDING",
      "SINGLE",
      JobPriority.NORMAL.persistedCode(),
      Timestamp.from(Instant.EPOCH),
      0,
      30,
      null,
      null,
      null,
      0,
      3,
      "worker",
      null
    };
    Object[] current = before.clone();
    current[5] = 4;
    current[10] = 2;
    when(candidates.getResultList()).thenReturn(Collections.singletonList(before));
    when(locked.getResultList()).thenReturn(Collections.singletonList(current));
    when(update.executeUpdate()).thenReturn(1);
    var operations = new OracleJobClaimOperations(new OracleStoreContext(em, null, 0), null);
    var claims =
        operations.claimNextBatchOptimized(
            JobExecutionType.SINGLE,
            1,
            "node",
            new NodeTagFilter(List.of("include"), List.of("exclude")),
            ExecutionTargetFilter.matching(java.util.Set.of("worker"), false));
    assertEquals(1, claims.size());
    assertEquals(4, claims.get(0).version());
    assertEquals(2, claims.get(0).attempts());
    String guard = sqls.get(1);
    assertTrue(guard.contains("scheduled_time <="));
    assertTrue(guard.contains("job_type = ?"));
    assertTrue(guard.contains("execution_target"));
    assertTrue(guard.contains("NOT EXISTS"));
    assertTrue(guard.contains("scheduler_job_tag"));

    // A rescheduled or retagged candidate no longer matches the locked-phase query.
    when(locked.getResultList()).thenReturn(List.of());
    assertTrue(
        operations
            .claimNextBatchOptimized(
                JobExecutionType.SINGLE, 1, "node", NodeTagFilter.NONE, ExecutionTargetFilter.any())
            .isEmpty());
    verify(update, times(1)).executeUpdate();
  }

  private static String expectedClaimSelectClause() {
    return "job_id, status, job_type, priority, scheduled_time, version, timeout_sec, picked_by,"
        + " picked_at, business_key, attempts, max_retries, execution_target,"
        + " (SELECT cold_job.depends_on FROM scheduler_job cold_job"
        + " WHERE cold_job.job_id = scheduler_job_queue.job_id) AS depends_on";
  }
}
