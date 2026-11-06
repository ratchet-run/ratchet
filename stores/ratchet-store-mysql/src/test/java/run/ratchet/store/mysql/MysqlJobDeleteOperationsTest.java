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
package run.ratchet.store.mysql;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;
import static run.ratchet.store.entity.JobExecutionEntity.ORPHAN_RECOVERY_ERROR_MESSAGE;

import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;
import run.ratchet.store.mysql.converter.UuidByteArrayConverter;

class MysqlJobDeleteOperationsTest {
  private final EntityManager em = mock(EntityManager.class);
  private final List<Statement> statements = new ArrayList<>();
  private final List<Object[]> rows = new ArrayList<>();
  private final Set<UUID> rejectedIds = new HashSet<>();
  private final List<String> liveOwners = new ArrayList<>();

  private record Statement(String sql, Query query) {}

  private MysqlJobDeleteOperations operations(int count) {
    for (int i = 0; i < count; i++) {
      seed(i + 1, i % 2, "dead");
    }
    return operations();
  }

  private void seed(int id, int crashCount, String owner) {
    rows.add(new Object[] {UuidByteArrayConverter.toBytes(new UUID(0, id)), 7L, crashCount, owner});
  }

  private MysqlJobDeleteOperations operations() {
    when(em.createNativeQuery(anyString()))
        .thenAnswer(
            invocation -> {
              String sql = invocation.<String>getArgument(0).replaceAll("\\s+", " ").trim();
              Query query = mock(Query.class, Answers.RETURNS_SELF);
              statements.add(new Statement(sql, query));
              if (sql.startsWith("SELECT job_id FROM")) {
                when(query.getResultList()).thenReturn(rows.stream().map(row -> row[0]).toList());
              } else if (sql.startsWith("SELECT node_id FROM scheduler_node")) {
                when(query.getResultList()).thenReturn(liveOwners);
              } else if (sql.startsWith("SELECT")) {
                int chunk =
                    (int) statements.stream().filter(s -> s.sql().contains("FOR UPDATE")).count()
                        - 1;
                int start = chunk * MysqlJobDeleteOperations.ORPHAN_RECOVERY_CHUNK_SIZE;
                int end =
                    Math.min(
                        start + MysqlJobDeleteOperations.ORPHAN_RECOVERY_CHUNK_SIZE, rows.size());
                when(query.getResultList())
                    .thenReturn(
                        rows.subList(start, end).stream()
                            .filter(
                                row -> !rejectedIds.contains(MysqlJobRowMapper.uuidOrNull(row[0])))
                            .toList());
              } else {
                int ids = (int) sql.chars().filter(c -> c == '?').count();
                when(query.executeUpdate())
                    .thenReturn(sql.contains("scheduler_job_execution") ? 99 : ids);
              }
              return query;
            });
    MysqlStoreContext ctx = new MysqlStoreContext(em, null);
    return new MysqlJobDeleteOperations(ctx, new MysqlBusinessKeyReservations(ctx));
  }

  @Test
  void noCandidatesTakesNoLocks() {
    var result = operations(0).resetOrphanJobsBefore(Instant.now(), 1, 100);
    assertEquals(0, result.reset());
    assertTrue(result.exhausted().isEmpty());
    assertEquals(1, statements.size());
    assertTrue(statements.get(0).sql().startsWith("SELECT job_id FROM"));
    assertFalse(statements.get(0).sql().contains("FOR UPDATE"));
    assertFalse(statements.get(0).sql().contains("FOR SHARE"));
    assertTrue(statements.get(0).sql().endsWith("ORDER BY job_id"));
    verify(em, never()).clear();
  }

  @Test
  void candidatesAreLockedAndSplitUsingCurrentCrashCounts() {
    var result = operations(4).resetOrphanJobsBefore(Instant.now(), 1, 100);
    assertEquals(2, result.reset());
    assertEquals(
        List.of(new UUID(0, 2), new UUID(0, 4)),
        result.exhausted().stream().map(row -> row.jobId()).toList());
    assertEquals(7L, result.exhausted().get(0).claimSeq());
    assertEquals(1, result.exhausted().get(0).crashCount());
    assertEquals("dead", result.exhausted().get(0).pickedBy());
    String lock = statements.get(1).sql();
    assertTrue(lock.contains("FORCE INDEX (PRIMARY) WHERE job_id IN"));
    assertTrue(lock.contains("picked_by IS NULL OR picked_by NOT IN"));
    assertTrue(lock.endsWith("ORDER BY job_id FOR UPDATE"));
    assertEquals(5, statements.size());
    verify(statements.get(3).query()).setParameter(1, ORPHAN_RECOVERY_ERROR_MESSAGE);
    verify(statements.get(4).query())
        .setParameter(1, UuidByteArrayConverter.toBytes(new UUID(0, 1)));
    verify(statements.get(4).query())
        .setParameter(2, UuidByteArrayConverter.toBytes(new UUID(0, 3)));
    assertUpdatesUseIds();
    verify(em).clear();
  }

  @Test
  void chunksSumResetsAndCapExhaustedAcrossChunks() {
    int size = MysqlJobDeleteOperations.ORPHAN_RECOVERY_CHUNK_SIZE;
    var result = operations(size + 4).resetOrphanJobsBefore(Instant.now(), 1, size / 2 + 1);
    assertEquals(size / 2 + 2, result.reset());
    assertEquals(size / 2 + 1, result.exhausted().size());
    assertEquals(new UUID(0, size + 2), result.exhausted().get(size / 2).jobId());
    assertEquals(2, statements.stream().filter(s -> s.sql().contains("FOR UPDATE")).count());
    assertEquals(0, statements.stream().filter(s -> isRelease(s.sql())).count());
    assertUpdatesUseIds();
  }

  @Test
  void startupReleasesEveryExhaustedRowBeyondResultLimit() {
    var result = operations(6).resetOrphanJobsForNode("dead", 1, 1);
    assertEquals(3, result.reset());
    assertEquals(1, result.exhausted().size());
    Statement release =
        statements.stream().filter(s -> isRelease(s.sql())).findFirst().orElseThrow();
    for (int i = 1; i <= 3; i++) {
      verify(release.query()).setParameter(i, UuidByteArrayConverter.toBytes(new UUID(0, i * 2)));
    }
    assertEquals(3, release.sql().chars().filter(c -> c == '?').count());
    assertUpdatesUseIds();
    assertNoNodeRead();
  }

  @Test
  void nonPositiveLimitStillReleasesExhaustedRows() {
    var result = operations(2).resetOrphanJobsForNode("dead", 1, 0);
    assertTrue(result.exhausted().isEmpty());
    assertTrue(statements.stream().anyMatch(s -> isRelease(s.sql())));
    assertNoNodeRead();
  }

  @Test
  void lockReadRejectionsNeverReachUpdates() {
    rejectedIds.addAll(List.of(new UUID(0, 1), new UUID(0, 2)));
    var result = operations(4).resetOrphanJobsForNode("dead", 1, 100);
    assertEquals(1, result.reset());
    assertEquals(List.of(new UUID(0, 4)), result.exhausted().stream().map(r -> r.jobId()).toList());
    assertUpdateIds(List.of(3, 4), List.of(3), List.of(4));
    assertNoNodeRead();
  }

  @Test
  void emptyLockReadIssuesNoUpdatesOrNodeRead() {
    rejectedIds.addAll(List.of(new UUID(0, 1), new UUID(0, 2)));
    var result = operations(2).resetOrphanJobsBefore(Instant.now(), 1, 100);
    assertEquals(0, result.reset());
    assertTrue(result.exhausted().isEmpty());
    assertEquals(2, statements.size());
    assertTrue(statements.stream().noneMatch(s -> s.sql().startsWith("UPDATE")));
    assertNoNodeRead();
  }

  @Test
  void periodicScanSkipsRowsWhoseOwnerIsLiveUnderLock() {
    seed(1, 0, "dead");
    seed(2, 0, "back");
    seed(3, 1, "back");
    seed(4, 1, null);
    seed(5, 0, null);
    seed(6, 1, "dead");
    liveOwners.add("back");
    Instant cutoff = Instant.now();
    var result = operations().resetOrphanJobsBefore(cutoff, 1, 100);
    assertEquals(2, result.reset());
    assertEquals(
        List.of(new UUID(0, 4), new UUID(0, 6)),
        result.exhausted().stream().map(r -> r.jobId()).toList());
    Statement nodes = statements.get(2);
    assertTrue(
        nodes
            .sql()
            .startsWith(
                "SELECT node_id FROM scheduler_node FORCE INDEX (PRIMARY) WHERE node_id IN"));
    assertTrue(nodes.sql().endsWith("FOR SHARE"));
    assertEquals(3, nodes.sql().chars().filter(c -> c == '?').count());
    verify(nodes.query()).setParameter(1, "back");
    verify(nodes.query()).setParameter(2, "dead");
    verify(nodes.query()).setParameter(3, Timestamp.from(cutoff));
    verify(nodes.query(), times(3)).setParameter(anyInt(), any());
    assertUpdateIds(List.of(1, 4, 5, 6), List.of(1, 5), List.of());
  }

  @Test
  void periodicScanSkipsNodeReadWhenAllOwnersAreNull() {
    seed(1, 0, null);
    seed(2, 1, null);
    var result = operations().resetOrphanJobsBefore(Instant.now(), 1, 100);
    assertEquals(1, result.reset());
    assertEquals(new UUID(0, 2), result.exhausted().get(0).jobId());
    assertNoNodeRead();
    assertUpdateIds(List.of(1, 2), List.of(1), List.of());
  }

  private void assertNoNodeRead() {
    assertTrue(
        statements.stream()
            .noneMatch(s -> s.sql().startsWith("SELECT node_id FROM scheduler_node")));
  }

  private void assertUpdateIds(List<Integer> closed, List<Integer> reset, List<Integer> released) {
    int expectedUpdates =
        (closed.isEmpty() ? 0 : 1) + (reset.isEmpty() ? 0 : 1) + (released.isEmpty() ? 0 : 1);
    assertEquals(
        expectedUpdates, statements.stream().filter(s -> s.sql().startsWith("UPDATE")).count());
    for (Statement statement : statements) {
      if (!statement.sql().startsWith("UPDATE")) continue;
      boolean close = statement.sql().contains("scheduler_job_execution");
      List<Integer> ids = close ? closed : isRelease(statement.sql()) ? released : reset;
      int first = close ? 2 : 1;
      assertEquals(ids.size() + first - 1, statement.sql().chars().filter(c -> c == '?').count());
      for (int i = 0; i < ids.size(); i++) {
        verify(statement.query())
            .setParameter(first + i, UuidByteArrayConverter.toBytes(new UUID(0, ids.get(i))));
      }
      verify(statement.query(), times(ids.size() + first - 1)).setParameter(anyInt(), any());
    }
  }

  private static boolean isRelease(String sql) {
    return sql.startsWith("UPDATE scheduler_job_queue") && !sql.contains("PENDING");
  }

  private void assertUpdatesUseIds() {
    for (Statement statement : statements) {
      String sql = statement.sql();
      if (!sql.startsWith("UPDATE")) continue;
      assertFalse(sql.contains("scheduler_node"));
      assertFalse(sql.contains("SELECT"));
      if (sql.startsWith("UPDATE scheduler_job_queue")) {
        assertTrue(sql.contains("FORCE INDEX (PRIMARY)"));
        assertTrue(sql.endsWith("ORDER BY job_id"));
      }
    }
  }
}
