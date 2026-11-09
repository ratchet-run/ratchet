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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static run.ratchet.store.entity.JobExecutionEntity.ORPHAN_RECOVERY_ERROR_MESSAGE;

import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;
import run.ratchet.store.sqlserver.converter.UuidByteArrayConverter;

class SqlserverJobDeleteOperationsTest {
  private final EntityManager em = mock(EntityManager.class);
  private final List<Statement> statements = new ArrayList<>();
  private final List<Object[]> rows = new ArrayList<>();
  private final Set<UUID> rejectedIds = new HashSet<>();

  private record Statement(String sql, Query query) {}

  private SqlserverJobDeleteOperations operations(int count) {
    for (int i = 0; i < count; i++) {
      rows.add(
          new Object[] {UuidByteArrayConverter.toBytes(new UUID(0, i + 1)), 7L, i % 2, "dead"});
    }
    when(em.createNativeQuery(anyString()))
        .thenAnswer(
            invocation -> {
              String sql = invocation.<String>getArgument(0).replaceAll("\\s+", " ").trim();
              Query query = mock(Query.class, Answers.RETURNS_SELF);
              statements.add(new Statement(sql, query));
              if (sql.startsWith("SELECT job_id FROM")) {
                when(query.getResultList()).thenReturn(rows.stream().map(row -> row[0]).toList());
              } else if (sql.startsWith("SELECT")) {
                int chunk =
                    (int) statements.stream().filter(s -> s.sql().contains("UPDLOCK")).count() - 1;
                int start = chunk * SqlserverJobDeleteOperations.ORPHAN_RECOVERY_CHUNK_SIZE;
                int end =
                    Math.min(
                        start + SqlserverJobDeleteOperations.ORPHAN_RECOVERY_CHUNK_SIZE,
                        rows.size());
                when(query.getResultList())
                    .thenReturn(
                        rows.subList(start, end).stream()
                            .filter(
                                row ->
                                    !rejectedIds.contains(SqlserverJobRowMapper.uuidOrNull(row[0])))
                            .toList());
              } else {
                int ids = (int) sql.chars().filter(c -> c == '?').count();
                when(query.executeUpdate())
                    .thenReturn(sql.contains("scheduler_job_execution") ? 99 : ids);
              }
              return query;
            });
    SqlserverStoreContext ctx = new SqlserverStoreContext(em);
    return new SqlserverJobDeleteOperations(ctx, new SqlserverBusinessKeyReservations(ctx));
  }

  @Test
  void noCandidatesTakesNoLocks() {
    var result = operations(0).resetOrphanJobsBefore(Instant.now(), 1, 100);
    assertEquals(0, result.reset());
    assertTrue(result.exhausted().isEmpty());
    assertEquals(1, statements.size());
    assertTrue(statements.get(0).sql().startsWith("SELECT job_id FROM"));
    assertFalse(statements.get(0).sql().contains("UPDLOCK"));
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
    assertTrue(
        lock.contains(
            "WITH (UPDLOCK, ROWLOCK, FORCESEEK(pk_scheduler_job_queue(job_id))) WHERE job_id IN"));
    assertTrue(
        lock.contains(
            "AND status = 'RUNNING' AND NOT EXISTS ( SELECT 1 FROM"
                + " scheduler_node n WHERE n.node_id = scheduler_job_queue.picked_by"
                + " AND n.heartbeat_ts >= ? ) AND picked_at < ?"));
    assertTrue(lock.endsWith("ORDER BY job_id"));
    assertEquals(4, statements.size());
    verify(statements.get(2).query()).setParameter(1, ORPHAN_RECOVERY_ERROR_MESSAGE);
    verify(statements.get(3).query())
        .setParameter(1, UuidByteArrayConverter.toBytes(new UUID(0, 1)));
    verify(statements.get(3).query())
        .setParameter(2, UuidByteArrayConverter.toBytes(new UUID(0, 3)));
    assertUpdatesUseIds();
    verify(em).clear();
  }

  @Test
  void chunksSumResetsAndCapExhaustedAcrossChunks() {
    int size = SqlserverJobDeleteOperations.ORPHAN_RECOVERY_CHUNK_SIZE;
    var result = operations(size + 4).resetOrphanJobsBefore(Instant.now(), 1, size / 2 + 1);
    assertEquals(size / 2 + 2, result.reset());
    assertEquals(size / 2 + 1, result.exhausted().size());
    assertEquals(new UUID(0, size + 2), result.exhausted().get(size / 2).jobId());
    assertEquals(2, statements.stream().filter(s -> s.sql().contains("UPDLOCK")).count());
    assertEquals(0, statements.stream().filter(s -> isRelease(s.sql())).count());
    assertUpdatesUseIds();
  }

  @Test
  void startupReleasesEveryExhaustedRowBeyondResultLimit() {
    var result = operations(6).resetOrphanJobsForNode("dead", 1, 1);
    assertTrue(
        statements.get(1).sql().contains("AND status = 'RUNNING' AND picked_by = ? ORDER BY"));
    assertEquals(3, result.reset());
    assertEquals(1, result.exhausted().size());
    Statement release =
        statements.stream().filter(s -> isRelease(s.sql())).findFirst().orElseThrow();
    for (int i = 1; i <= 3; i++) {
      verify(release.query()).setParameter(i, UuidByteArrayConverter.toBytes(new UUID(0, i * 2)));
    }
    assertEquals(3, release.sql().chars().filter(c -> c == '?').count());
    assertUpdatesUseIds();
  }

  @Test
  void startupReleasesExhaustedRowsInEveryChunk() {
    int size = SqlserverJobDeleteOperations.ORPHAN_RECOVERY_CHUNK_SIZE;
    var result = operations(size + 4).resetOrphanJobsForNode("dead", 1, 1);
    assertEquals(size / 2 + 2, result.reset());
    assertEquals(1, result.exhausted().size());
    var releases = statements.stream().filter(s -> isRelease(s.sql())).toList();
    assertEquals(2, releases.size());
    for (int chunk = 0; chunk < releases.size(); chunk++) {
      Statement release = releases.get(chunk);
      int count = chunk == 0 ? size / 2 : 2;
      assertEquals(count, release.sql().chars().filter(c -> c == '?').count());
      for (int i = 1; i <= count; i++) {
        verify(release.query())
            .setParameter(i, UuidByteArrayConverter.toBytes(new UUID(0, chunk * size + i * 2)));
      }
    }
    assertUpdatesUseIds();
  }

  @Test
  void nonPositiveLimitStillReleasesExhaustedRows() {
    var result = operations(2).resetOrphanJobsForNode("dead", 1, 0);
    assertTrue(result.exhausted().isEmpty());
    assertTrue(statements.stream().anyMatch(s -> isRelease(s.sql())));
  }

  @Test
  void lockReadRejectionsNeverReachUpdates() {
    rejectedIds.addAll(List.of(new UUID(0, 1), new UUID(0, 2)));
    var result = operations(4).resetOrphanJobsForNode("dead", 1, 100);
    assertEquals(1, result.reset());
    assertEquals(List.of(new UUID(0, 4)), result.exhausted().stream().map(r -> r.jobId()).toList());
    assertEquals(5, statements.size());
    for (int i = 2; i < statements.size(); i++) {
      Statement statement = statements.get(i);
      for (UUID rejectedId : rejectedIds) {
        for (int parameter = 1; parameter <= 4; parameter++) {
          verify(statement.query(), never())
              .setParameter(parameter, UuidByteArrayConverter.toBytes(rejectedId));
        }
      }
    }
    assertEquals(3, statements.get(2).sql().chars().filter(c -> c == '?').count());
    verify(statements.get(2).query()).setParameter(1, ORPHAN_RECOVERY_ERROR_MESSAGE);
    verify(statements.get(2).query())
        .setParameter(2, UuidByteArrayConverter.toBytes(new UUID(0, 3)));
    verify(statements.get(2).query())
        .setParameter(3, UuidByteArrayConverter.toBytes(new UUID(0, 4)));
    assertEquals(1, statements.get(3).sql().chars().filter(c -> c == '?').count());
    verify(statements.get(3).query())
        .setParameter(1, UuidByteArrayConverter.toBytes(new UUID(0, 3)));
    assertEquals(1, statements.get(4).sql().chars().filter(c -> c == '?').count());
    verify(statements.get(4).query())
        .setParameter(1, UuidByteArrayConverter.toBytes(new UUID(0, 4)));
    assertUpdatesUseIds();
  }

  @Test
  void emptyLockReadIssuesNoUpdates() {
    rejectedIds.addAll(List.of(new UUID(0, 1), new UUID(0, 2)));
    var result = operations(2).resetOrphanJobsForNode("dead", 1, 100);
    assertEquals(0, result.reset());
    assertTrue(result.exhausted().isEmpty());
    assertEquals(2, statements.size());
    assertTrue(statements.stream().noneMatch(s -> s.sql().startsWith("UPDATE")));
  }

  private static boolean isRelease(String sql) {
    return sql.startsWith("UPDATE q") && !sql.contains("PENDING");
  }

  private void assertUpdatesUseIds() {
    for (Statement statement : statements) {
      String sql = statement.sql();
      if (!sql.startsWith("UPDATE")) continue;
      assertFalse(sql.contains("scheduler_node"));
      assertFalse(sql.contains("SELECT"));
      assertTrue(
          sql.contains("WHERE status = 'RUNNING' AND job_id IN (")
              || sql.contains("WHERE q.job_id IN ("));
      if (sql.contains("scheduler_job_queue")) {
        assertTrue(sql.contains("WITH (FORCESEEK, INDEX(pk_scheduler_job_queue))"));
        assertTrue(sql.contains("WHERE q.job_id IN ("));
        assertFalse(sql.contains("ORDER BY"));
      }
    }
  }
}
