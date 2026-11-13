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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static run.ratchet.store.entity.JobExecutionEntity.ORPHAN_RECOVERY_ERROR_MESSAGE;

import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;
import run.ratchet.store.sqlserver.converter.UuidByteArrayConverter;

class SqlserverJobDeleteOperationsTest {
  private final EntityManager em = mock(EntityManager.class);
  private final List<Statement> statements = new ArrayList<>();
  private final List<Object[]> rows = new ArrayList<>();
  private final Set<UUID> rejectedIds = new HashSet<>();
  private final List<String> liveOwners = new ArrayList<>();

  private record Statement(String sql, Query query, Map<Integer, Object> parameters) {}

  private void seed(int id, int crashes, String owner) {
    rows.add(new Object[] {UuidByteArrayConverter.toBytes(new UUID(0, id)), 7L, crashes, owner});
  }

  private SqlserverJobDeleteOperations operations() {
    when(em.createNativeQuery(anyString()))
        .thenAnswer(
            invocation -> {
              String sql = invocation.<String>getArgument(0).replaceAll("\s+", " ").trim();
              Map<Integer, Object> parameters = new HashMap<>();
              Query query = mock(Query.class, Answers.RETURNS_SELF);
              statements.add(new Statement(sql, query, parameters));
              when(query.setParameter(anyInt(), any()))
                  .thenAnswer(
                      binding -> {
                        parameters.put(binding.getArgument(0), binding.getArgument(1));
                        return query;
                      });
              when(query.getResultList())
                  .thenAnswer(
                      read -> {
                        boolean lock = sql.contains("UPDLOCK");
                        List<Object[]> selected =
                            rows.stream()
                                .filter(
                                    row ->
                                        !lock
                                            || !rejectedIds.contains(
                                                SqlserverJobRowMapper.uuidOrNull(row[0])))
                                .filter(
                                    row -> {
                                      List<UUID> ids =
                                          parameters.values().stream()
                                              .filter(byte[].class::isInstance)
                                              .map(value -> SqlserverJobRowMapper.uuidOrNull(value))
                                              .toList();
                                      return ids.isEmpty()
                                          || ids.contains(SqlserverJobRowMapper.uuidOrNull(row[0]));
                                    })
                                .filter(
                                    row -> {
                                      if (lock)
                                        return true; // Candidate predicates apply on the unlocked
                                      // read.
                                      boolean below = sql.matches(".*crash_count[^?]*<[^=].*");
                                      boolean exhausted = sql.matches(".*crash_count[^?]*>=.*");
                                      return (!below || ((Number) row[2]).intValue() < 1)
                                          && (!exhausted || ((Number) row[2]).intValue() >= 1);
                                    })
                                .toList();
                        if (!lock) {
                          // The page limit must be bound, or appear in the dialect's LIMIT/TOP
                          // clause.
                          int limit =
                              parameters.values().stream()
                                  .filter(Integer.class::isInstance)
                                  .map(Integer.class::cast)
                                  .filter(value -> value > 1)
                                  .findFirst()
                                  .orElse(500);
                          selected = selected.stream().limit(limit).toList();
                          return selected.stream().map(row -> row[0]).toList();
                        }
                        return selected.stream()
                            .map(
                                row ->
                                    new Object[] {
                                      row[0],
                                      row[1],
                                      row[2],
                                      row[3],
                                      row[3] != null
                                              && liveOwners.stream()
                                                  .anyMatch(
                                                      owner ->
                                                          owner.equalsIgnoreCase(row[3].toString()))
                                          ? 1L
                                          : 0L
                                    })
                            .toList();
                      });
              when(query.executeUpdate())
                  .thenAnswer(
                      write ->
                          (int)
                              parameters.values().stream()
                                  .filter(byte[].class::isInstance)
                                  .count());
              return query;
            });
    SqlserverStoreContext ctx = new SqlserverStoreContext(em);
    return new SqlserverJobDeleteOperations(ctx, new SqlserverBusinessKeyReservations(ctx));
  }

  @Test
  void noCandidatesTakesNoLocks() {
    assertEquals(0, operations().resetOrphanJobsBefore(Instant.now(), 1, 500).scanned());
    assertEquals(1, statements.size());
    assertFalse(statements.get(0).sql().contains("UPDLOCK"));
    assertFalse(statements.get(0).sql().contains("HOLDLOCK"));
    assertFalse(statements.get(0).sql().contains("ORDER BY job_id"));
    assertTrue(statements.stream().noneMatch(statement -> statement.sql().startsWith("UPDATE")));
  }

  @Test
  void candidatesAreLockedAndSplitUsingCurrentCrashCounts() {
    for (int i = 1; i <= 4; i++) seed(i, i % 2, "dead");
    var operations = operations();
    var reset = operations.resetOrphanJobsBefore(Instant.now(), 1, 500);
    assertEquals(2, reset.scanned());
    assertEquals(2, reset.reset());
    assertTrue(statements.get(1).sql().contains("FORCESEEK(pk_scheduler_job_queue(job_id))"));
    assertTrue(statements.get(1).sql().contains("scheduler_node"));
    assertTrue(statements.get(1).sql().contains("HOLDLOCK"));
    assertUpdatesUseIds();
    statements.clear();
    var exhausted = operations.recoverExhaustedOrphans(Instant.now(), 1, 500);
    assertEquals(
        Set.of(new UUID(0, 1), new UUID(0, 3)),
        exhausted.stream().map(row -> row.jobId()).collect(Collectors.toSet()));
    assertTrue(
        exhausted.stream()
            .allMatch(
                row ->
                    row.claimSeq() == 7 && row.crashCount() == 1 && "dead".equals(row.pickedBy())));
    assertTrue(statements.get(0).sql().contains("ORDER BY picked_at"));
    assertTrue(statements.stream().noneMatch(statement -> isQueueUpdate(statement.sql())));
    assertExecutionClose();
  }

  @Test
  void pageLimitBoundsCandidateReadAndMutations() {
    for (int i = 1; i <= 6; i++) seed(i, 0, "dead");
    var result = operations().resetOrphanJobsBefore(Instant.now(), 1, 2);
    assertEquals(2, result.scanned());
    assertEquals(2, result.reset());
    assertEquals(1, candidateReads());
    assertTrue(statements.stream().noneMatch(statement -> statement.sql().contains("job_id > ?")));
    assertUpdatesUseIds();
  }

  @Test
  void exhaustedLimitBoundsSelectionAndExecutionClose() {
    for (int i = 1; i <= 6; i++) seed(i, 1, "dead");
    var result = operations().recoverExhaustedOrphans(Instant.now(), 1, 2);
    assertEquals(2, result.size());
    assertEquals(1, candidateReads());
    assertTrue(statements.stream().noneMatch(statement -> isQueueUpdate(statement.sql())));
    assertExecutionClose();
  }

  @Test
  void startupReleasesExhaustedOwnersWithinPageOnly() {
    for (int i = 1; i <= 6; i++) seed(i, i % 2, "dead");
    var result = operations().resetOrphanJobsForNode("dead", 1, 2);
    assertEquals(2, result.scanned());
    assertEquals(1, result.reset());
    assertEquals(1, result.released());
    String lock = statements.get(1).sql();
    assertTrue(lock.contains("FORCESEEK(pk_scheduler_job_queue(job_id))"));
    assertFalse(lock.contains("HOLDLOCK"));
    assertTrue(lock.contains("picked_by = ?"));
    assertEquals(1, candidateReads());
    assertUpdatesUseIds();
    assertExecutionClose();
  }

  @Test
  void nonPositiveLimitTouchesNothing() {
    seed(1, 0, "dead");
    seed(2, 1, "dead");
    var operations = operations();
    for (int limit : List.of(0, -1)) {
      var periodic = operations.resetOrphanJobsBefore(Instant.now(), 1, limit);
      var startup = operations.resetOrphanJobsForNode("dead", 1, limit);
      assertEquals(0, periodic.scanned());
      assertEquals(0, periodic.reset());
      assertEquals(0, startup.scanned());
      assertEquals(0, startup.released());
      assertTrue(operations.recoverExhaustedOrphans(Instant.now(), 1, limit).isEmpty());
    }
    assertTrue(statements.isEmpty());
  }

  @Test
  void lockReadRejectionsNeverReachUpdates() {
    for (int i = 1; i <= 4; i++) seed(i, i % 2, "dead");
    rejectedIds.addAll(Set.of(new UUID(0, 1), new UUID(0, 2)));
    var result = operations().resetOrphanJobsForNode("dead", 1, 500);
    assertEquals(4, result.scanned());
    assertEquals(1, result.reset());
    assertEquals(1, result.released());
    assertUpdatesExcludeRejected();
    assertUpdatesUseIds();
  }

  @Test
  void emptyLockReadIssuesNoUpdates() {
    seed(1, 0, "dead");
    rejectedIds.add(new UUID(0, 1));
    var result = operations().resetOrphanJobsBefore(Instant.now(), 1, 500);
    assertEquals(1, result.scanned());
    assertEquals(0, result.reset());
    assertTrue(statements.stream().noneMatch(statement -> statement.sql().startsWith("UPDATE")));
  }

  @Test
  void fullPageDoesNotTriggerAnotherCandidateRead() {
    for (int i = 1; i <= 500; i++) seed(i, 0, "dead");
    assertEquals(500, operations().resetOrphanJobsBefore(Instant.now(), 1, 500).scanned());
    assertEquals(1, candidateReads());
  }

  @Test
  void entirelyRejectedPageDoesNotScanPastLimit() {
    for (int i = 1; i <= 501; i++) {
      seed(i, 0, "dead");
      if (i <= 500) rejectedIds.add(new UUID(0, i));
    }
    var result = operations().resetOrphanJobsBefore(Instant.now(), 1, 500);
    assertEquals(500, result.scanned());
    assertEquals(0, result.reset());
    assertEquals(1, candidateReads());
    assertTrue(statements.stream().noneMatch(statement -> statement.sql().startsWith("UPDATE")));
  }

  @Test
  void periodicScanSkipsRowsWhoseOwnerIsLiveUnderLock() {
    seed(1, 0, "Back");
    liveOwners.add("back");
    var result = operations().resetOrphanJobsBefore(Instant.now(), 1, 500);
    assertEquals(1, result.scanned());
    assertEquals(0, result.reset());
    assertTrue(statements.stream().noneMatch(statement -> statement.sql().startsWith("UPDATE")));
  }

  @Test
  void exhaustedScanSkipsRowsWhoseOwnerIsLiveUnderLock() {
    seed(1, 1, "Back");
    liveOwners.add("back");
    assertTrue(operations().recoverExhaustedOrphans(Instant.now(), 1, 500).isEmpty());
    assertTrue(statements.stream().noneMatch(statement -> statement.sql().startsWith("UPDATE")));
  }

  @Test
  void periodicScanProcessesRowsWithoutOwner() {
    seed(1, 0, null);
    var result = operations().resetOrphanJobsBefore(Instant.now(), 1, 500);
    assertEquals(1, result.reset());
    assertExecutionClose();
    assertUpdatesUseIds();
  }

  private long candidateReads() {
    return statements.stream()
        .filter(
            statement ->
                statement.sql().startsWith("SELECT") && !statement.sql().contains("UPDLOCK"))
        .count();
  }

  private boolean isQueueUpdate(String sql) {
    return sql.startsWith("UPDATE") && !sql.contains("scheduler_job_execution");
  }

  private void assertExecutionClose() {
    Statement close =
        statements.stream()
            .filter(
                statement ->
                    statement.sql().startsWith("UPDATE")
                        && statement.sql().contains("scheduler_job_execution"))
            .findFirst()
            .orElseThrow();
    assertTrue(close.parameters().containsValue(ORPHAN_RECOVERY_ERROR_MESSAGE));
  }

  private void assertUpdatesExcludeRejected() {
    for (Statement statement : statements) {
      if (!statement.sql().startsWith("UPDATE")) continue;
      for (UUID rejected : rejectedIds) {
        for (int parameter = 1; parameter <= 8; parameter++) {
          verify(statement.query(), never())
              .setParameter(parameter, UuidByteArrayConverter.toBytes(rejected));
        }
      }
    }
  }

  private void assertUpdatesUseIds() {
    for (Statement statement : statements) {
      String sql = statement.sql();
      if (!sql.startsWith("UPDATE")) continue;
      assertFalse(sql.contains("scheduler_node"));
      assertFalse(sql.contains("SELECT"));
      assertTrue(sql.contains("job_id IN ("));
      assertFalse(sql.contains("status = 'RUNNING' AND picked_by = ?"));
    }
  }
}
