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
package run.ratchet.store.sqlserver;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import run.ratchet.api.NodeTagFilter;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.spi.ExecutionTargetFilter;
import run.ratchet.store.sqlserver.converter.UuidByteArrayConverter;

class SqlserverBoostedClaimCandidatesTest {
  private static final Instant NOW = Instant.parse("2026-11-17T12:00:00Z");

  @Test
  void filterBudgetAccepts2091AndRejects2092CombinedParameters() {
    for (boolean exclusion : List.of(false, true)) {
      RecordingQueries queries = new RecordingQueries(0);
      NodeTagFilter tags = new NodeTagFilter(names(1045), names(1045));
      ExecutionTargetFilter targets =
          exclusion
              ? ExecutionTargetFilter.excluding(names(1), true)
              : ExecutionTargetFilter.matching(names(1), true);
      assertDoesNotThrow(() -> candidates(queries, JobExecutionType.SINGLE, 1, tags, targets));
      IllegalArgumentException failure =
          assertThrows(
              IllegalArgumentException.class,
              () ->
                  candidates(
                      queries,
                      JobExecutionType.SINGLE,
                      1,
                      new NodeTagFilter(names(1046), names(1045)),
                      targets));
      assertEquals("Boosted claim exceeds SQL Server's parameter budget", failure.getMessage());
      assertTrue(queries.statements.isEmpty());
    }
  }

  @Test
  void maximumFiltersKeepCursorPagesAndLockBatchesWithin2100Parameters() {
    RecordingQueries queries = new RecordingQueries(8);
    // READPAST skips a row another claim holds, so the stream reads a second page by cursor.
    queries.skipped.add(new UUID(0, 3));
    var claims =
        queries
            .operations()
            .claimNextBatchOptimized(
                JobExecutionType.SINGLE,
                8,
                "node",
                new NodeTagFilter(names(1045), names(1045)),
                ExecutionTargetFilter.matching(names(1), false));

    assertEquals(7, claims.size());
    assertTrue(queries.statements.stream().noneMatch(s -> s.sql.contains("UNION ALL")));
    assertEquals(
        2100,
        queries.statements.stream()
            .filter(s -> s.sql.contains("job_id > ?"))
            .findFirst()
            .orElseThrow()
            .highestParameter());
    List<Statement> locks = queries.locks();
    assertEquals(List.of(7, 1), locks.stream().map(s -> s.lockedIds().size()).toList());
    assertEquals(2100, locks.get(0).highestParameter());
    queries.assertParameterBudget();
  }

  @Test
  void oversizedClaimCapsRunningUpdateAt2097JobsAnd2100Parameters() {
    RecordingQueries queries = new RecordingQueries(2097);
    var claims =
        queries
            .operations()
            .claimNextBatchOptimized(
                JobExecutionType.SINGLE,
                5000,
                "node",
                NodeTagFilter.NONE,
                ExecutionTargetFilter.any());

    assertEquals(2097, claims.size());
    assertEquals(2097, queries.locks().get(0).lockedIds().size());
    assertEquals(2100, queries.runningUpdate().highestParameter());
    assertEquals(2097, queries.statements.get(1).parameters.get(1));
    queries.assertParameterBudget();
  }

  @Test
  void smallFiltersInitializeAllStreamsWithUnion() {
    assertInitialPageStrategy(JobExecutionType.SINGLE, 1, true, 5);
    assertInitialPageStrategy(null, 1, true, 20);
  }

  @Test
  void fiveStreamsUseUnionThrough414FiltersAndFallBackAt415() {
    assertInitialPageStrategy(JobExecutionType.SINGLE, 414, true, 5);
    assertInitialPageStrategy(JobExecutionType.SINGLE, 415, false, 5);
  }

  @Test
  void twentyStreamsUseUnionThrough99FiltersAndFallBackAt100() {
    assertInitialPageStrategy(null, 99, true, 20);
    assertInitialPageStrategy(null, 100, false, 20);
  }

  @Test
  void missingHydrationRowsFailBeforeAnyRunningUpdate() {
    RecordingQueries queries = new RecordingQueries(1);
    SqlserverJobReadOperations reads = mock(SqlserverJobReadOperations.class);
    when(reads.hydrateRowsWithTags(List.of())).thenReturn(List.of());
    var operations = new SqlserverJobClaimOperations(queries.context(), reads);

    IllegalStateException failure =
        assertThrows(
            IllegalStateException.class,
            () -> operations.claimNextBatch(1, "node", NodeTagFilter.NONE));

    assertEquals("Locked claim rows disappeared during hydration", failure.getMessage());
    assertEquals(1, queries.locks().get(0).lockedIds().size());
    assertTrue(queries.locks().get(0).sql.contains("job_type IN ("));
    assertEquals(2, queries.locks().get(0).highestParameter());
    assertTrue(
        queries.statements.stream().anyMatch(s -> s.sql.contains("JOIN scheduler_job_queue")));
    assertFalse(queries.statements.stream().anyMatch(Statement::isRunningUpdate));
    queries.assertParameterBudget();
  }

  private static void assertInitialPageStrategy(
      JobExecutionType type, int filters, boolean union, int streams) {
    RecordingQueries queries = new RecordingQueries(0);
    assertTrue(
        candidates(
                queries,
                type,
                1,
                new NodeTagFilter(names(filters), List.of()),
                ExecutionTargetFilter.any())
            .lock("job_id, scheduled_time")
            .isEmpty());
    assertEquals(union, queries.statements.get(0).sql.contains("UNION ALL"));
    if (union) {
      assertEquals(1, queries.statements.size());
      assertEquals(streams * (6 + filters), queries.statements.get(0).highestParameter());
    } else {
      assertEquals(streams, queries.statements.size());
      assertTrue(queries.statements.stream().noneMatch(s -> s.sql.contains("UNION ALL")));
      assertTrue(queries.statements.stream().allMatch(s -> s.sql.contains("SELECT TOP (?)")));
    }
    queries.assertParameterBudget();
  }

  private static SqlserverBoostedClaimCandidates candidates(
      RecordingQueries queries,
      JobExecutionType type,
      int limit,
      NodeTagFilter tags,
      ExecutionTargetFilter targets) {
    return new SqlserverBoostedClaimCandidates(queries.context(), type, limit, tags, targets, NOW);
  }

  private static List<String> names(int count) {
    return IntStream.range(0, count).mapToObj(i -> "filter-" + i).toList();
  }

  /** Each native query gets its own SQL and binding record, including repeated page reads. */
  private static final class RecordingQueries {
    private final EntityManager em = mock(EntityManager.class);
    private final List<Statement> statements = new ArrayList<>();
    private final Map<UUID, Object[]> claimRows = new HashMap<>();
    private final Set<UUID> skipped = new HashSet<>();
    private final int candidateCount;
    private boolean deliveredCandidates;

    RecordingQueries(int candidateCount) {
      this.candidateCount = candidateCount;
      when(em.createNativeQuery(anyString()))
          .thenAnswer(
              invocation -> {
                Statement statement = new Statement(invocation.getArgument(0));
                statements.add(statement);
                Query query = mock(Query.class);
                when(query.setParameter(anyInt(), any()))
                    .thenAnswer(
                        binding -> {
                          statement.parameters.put(binding.getArgument(0), binding.getArgument(1));
                          return query;
                        });
                when(query.getSingleResult()).thenReturn(Timestamp.from(NOW));
                when(query.getResultList()).thenAnswer(ignored -> results(statement));
                when(query.executeUpdate()).thenAnswer(ignored -> statement.highestParameter() - 3);
                return query;
              });
    }

    SqlserverStoreContext context() {
      return new SqlserverStoreContext(em, 15);
    }

    SqlserverJobClaimOperations operations() {
      return new SqlserverJobClaimOperations(context(), null);
    }

    private List<Object[]> results(Statement statement) {
      if (statement.isLock()) {
        return statement.lockedIds().stream()
            .filter(id -> !skipped.contains(id))
            .map(claimRows::get)
            .toList();
      }
      if (!statement.sql.contains("SELECT TOP (?)") || deliveredCandidates) return List.of();
      deliveredCandidates = true;
      List<Object[]> rows = new ArrayList<>();
      for (int i = 0; i < candidateCount; i++) {
        UUID id = new UUID(0, i + 1);
        byte[] bytes = UuidByteArrayConverter.toBytes(id);
        Timestamp scheduled = Timestamp.from(NOW.minusSeconds(candidateCount - i));
        claimRows.put(
            id,
            new Object[] {
              bytes, "PENDING", "SINGLE", 0, scheduled, 0, 60, null, null, null, 0, 3, null, null,
              0L
            });
        rows.add(
            statement.sql.contains("UNION ALL")
                ? new Object[] {0, bytes, scheduled, 0}
                : new Object[] {bytes, scheduled, 0});
      }
      return rows;
    }

    List<Statement> locks() {
      return statements.stream().filter(Statement::isLock).toList();
    }

    Statement runningUpdate() {
      List<Statement> updates = statements.stream().filter(Statement::isRunningUpdate).toList();
      assertEquals(1, updates.size());
      return updates.get(0);
    }

    void assertParameterBudget() {
      assertFalse(statements.isEmpty());
      for (Statement statement : statements) {
        assertTrue(statement.highestParameter() <= 2100, statement.sql);
      }
    }
  }

  private static final class Statement {
    private final String sql;
    private final Map<Integer, Object> parameters = new HashMap<>();

    Statement(String sql) {
      this.sql = sql;
    }

    int highestParameter() {
      return parameters.keySet().stream().mapToInt(Integer::intValue).max().orElse(0);
    }

    boolean isLock() {
      return sql.contains("UPDLOCK, READPAST, ROWLOCK, FORCESEEK");
    }

    boolean isRunningUpdate() {
      return sql.contains("SET status = 'RUNNING'");
    }

    List<UUID> lockedIds() {
      int end = sql.indexOf(") AND status = 'PENDING'");
      int start = sql.indexOf("WHERE job_id IN (");
      int count = (int) sql.substring(start, end).chars().filter(c -> c == '?').count();
      return IntStream.rangeClosed(1, count)
          .mapToObj(i -> UuidByteArrayConverter.fromBytes((byte[]) parameters.get(i)))
          .toList();
    }
  }
}
