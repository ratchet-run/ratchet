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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import jakarta.persistence.EntityManager;
import jakarta.persistence.NoResultException;
import jakarta.persistence.Query;
import java.lang.reflect.Proxy;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import run.ratchet.store.entity.ResourceLimitEntity;
import run.ratchet.store.entity.ResourcePermitEntity;
import run.ratchet.store.mysql.converter.UuidByteArrayConverter;
import run.ratchet.store.spi.ResourcePermitStore.AcquireResult;

class MysqlAuxiliaryOperationsTest {

  @Test
  void cleanupLocksUnionAcrossChunksBeforeDeletesForBothCallers() {
    for (boolean missingNodes : List.of(false, true)) {
      List<String> nodes = new ArrayList<>();
      for (int i = 0; i < 501; i++) nodes.add("stale-" + i);
      List<String> events = new ArrayList<>();
      EntityManager em = mock(EntityManager.class);
      when(em.createNativeQuery(any(String.class)))
          .thenAnswer(
              invocation -> {
                String sql = invocation.getArgument(0);
                Query query = mock(Query.class);
                Map<Integer, Object> parameters = new HashMap<>();
                when(query.setParameter(anyInt(), any()))
                    .thenAnswer(
                        binding -> {
                          parameters.put(binding.getArgument(0), binding.getArgument(1));
                          return query;
                        });
                if (sql.contains("LEFT JOIN scheduler_node")) {
                  when(query.getResultList()).thenReturn(nodes);
                } else if (sql.contains("SELECT DISTINCT resource_name")) {
                  when(query.getResultList())
                      .thenAnswer(
                          read -> {
                            events.add("discover");
                            return List.of(parameters.get(1).equals("stale-0") ? "r-z" : "r-a");
                          });
                } else if (sql.contains("SELECT p.id")) {
                  assertTrue(!sql.contains("FOR UPDATE"));
                  when(query.getResultList())
                      .thenAnswer(
                          read -> {
                            events.add("deletable");
                            if (missingNodes) {
                              assertTrue(sql.contains("AND p.acquired_at < ?"));
                              assertEquals(DUE, parameters.get(parameters.size() - 2));
                            }
                            return List.of(new byte[16]);
                          });
                } else if (sql.contains("WHERE resource_name IN")) {
                  when(query.getResultList())
                      .thenAnswer(
                          read -> {
                            events.add("keys");
                            return List.of("r-z", "r-a");
                          });
                } else if (sql.contains("FOR UPDATE")) {
                  assertTrue(!sql.contains("scheduler_node"));
                  when(query.getResultList())
                      .thenAnswer(
                          read -> {
                            events.add("lock:" + parameters.get(1));
                            return List.of(parameters.get(1));
                          });
                } else {
                  when(query.executeUpdate())
                      .thenAnswer(
                          delete -> {
                            events.add("delete");
                            assertTrue(
                                sql.startsWith(
                                    "DELETE FROM scheduler_resource_permit WHERE id IN"));
                            assertEquals(2, parameters.size());
                            return 1;
                          });
                }
                return query;
              });
      var operations = new MysqlAuxiliaryOperations(new MysqlStoreContext(em, null));
      assertEquals(
          1,
          missingNodes
              ? operations.cleanupPermitsOfMissingNodes(DUE.toInstant())
              : operations.cleanupOrphanedPermits(nodes));
      assertEquals(
          List.of(
              "discover",
              "discover",
              "keys",
              "lock:r-a",
              "lock:r-z",
              "deletable",
              "deletable",
              "delete"),
          events);
    }
  }

  @Test
  void tryAcquirePermit_sameJobExistingPermitIsIdempotent() {
    UUID jobId = UUID.fromString("01900000-0000-7000-8000-000000000001");
    MysqlAuxiliaryOperations operations =
        new MysqlAuxiliaryOperations(entityManagerWithExistingPermit(jobId, 0L));

    assertEquals(AcquireResult.ACQUIRED, operations.tryAcquirePermit("res", jobId, 0L, "node-1"));
  }

  @Test
  void tryAcquirePermit_olderClaimCountsAgainstCapacity() {
    UUID jobId = UUID.fromString("01900000-0000-7000-8000-000000000001");
    MysqlAuxiliaryOperations operations =
        new MysqlAuxiliaryOperations(entityManagerWithExistingPermit(jobId, 1L));

    assertEquals(
        AcquireResult.AT_CAPACITY, operations.tryAcquirePermit("res", jobId, 1L, "node-1"));
  }

  @Test
  void getPermitRetryDelayUsesDefaultForMissingResource() {
    MysqlAuxiliaryOperations operations =
        new MysqlAuxiliaryOperations(new MysqlStoreContext(entityManagerWithNoResult(), null));

    assertEquals(
        ResourceLimitEntity.DEFAULT_RETRY_DELAY_MS, operations.getPermitRetryDelay("missing"));
  }

  @Test
  void cleanupLocksStoredMixedCaseKeysBeforeDeletingDiscoveredNames() {
    byte[] first = new byte[16];
    byte[] second = new byte[16];
    var sequence =
        new NativeSequence(
            List.of("a", "B"),
            List.of("b", "A"),
            List.of("A"),
            List.of("b"),
            List.of(first, second),
            2);
    assertEquals(2, sequence.operations().cleanupOrphanedPermits(List.of("stale")));
    assertTrue(sequence.sql.get(0).contains("SELECT DISTINCT"));
    assertTrue(sequence.sql.get(1).contains("WHERE resource_name IN (?,?)"));
    assertEquals("A", sequence.parameters.get(2).get(1));
    assertEquals("b", sequence.parameters.get(3).get(1));
    assertTrue(sequence.sql.get(2).contains("FOR UPDATE"));
    assertTrue(sequence.sql.get(3).contains("FOR UPDATE"));
    assertTrue(sequence.sql.get(4).contains("LEFT JOIN scheduler_job_queue"));
    assertTrue(sequence.sql.get(4).contains("AND p.resource_name IN (?,?) AND q.job_id IS NULL"));
    assertEquals(Map.of(1, "stale", 2, "B", 3, "a"), sequence.parameters.get(4));
    assertEquals(
        "DELETE FROM scheduler_resource_permit WHERE id IN (?,?)", sequence.sql.get(5).strip());
    assertArrayEquals(first, (byte[]) sequence.parameters.get(5).get(1));
    assertArrayEquals(second, (byte[]) sequence.parameters.get(5).get(2));
    sequence.assertConsumed();
  }

  @Test
  void cleanupWithoutDiscoveredResourcesSkipsStoredKeyQuery() {
    EntityManager em = mock(EntityManager.class);
    Query discovery = mock(Query.class);
    when(em.createNativeQuery(any(String.class))).thenReturn(discovery);
    when(discovery.setParameter(anyInt(), any())).thenReturn(discovery);
    when(discovery.getResultList()).thenReturn(List.of());
    var operations = new MysqlAuxiliaryOperations(new MysqlStoreContext(em, null));

    assertEquals(0, operations.cleanupOrphanedPermits(List.of("stale")));

    verify(em, times(1)).createNativeQuery(any(String.class));
  }

  @Test
  void cleanupDeletesPermitsWithoutLimitRows() {
    byte[] id = new byte[16];
    var sequence = new NativeSequence(List.of("unconfigured"), List.of(), List.of(id), 1);
    assertEquals(1, sequence.operations().cleanupOrphanedPermits(List.of("stale")));
    assertEquals("unconfigured", sequence.parameters.get(2).get(2));
    assertTrue(sequence.sql.get(2).contains("AND p.resource_name IN (?)"));
    assertEquals(
        "DELETE FROM scheduler_resource_permit WHERE id IN (?)", sequence.sql.get(3).strip());
    assertArrayEquals(id, (byte[]) sequence.parameters.get(3).get(1));
    sequence.assertConsumed();
  }

  @Test
  void cleanupExcludesRunningPermitAfterLockingItsResource() {
    byte[] kept = UuidByteArrayConverter.toBytes(UUID.randomUUID());
    byte[] deletable = UuidByteArrayConverter.toBytes(UUID.randomUUID());
    for (boolean missing : List.of(false, true)) {
      for (boolean hasDeletable : List.of(false, true)) {
        List<Object> replies = new ArrayList<>();
        if (missing) replies.add(List.of("stale"));
        replies.addAll(List.of(List.of("res"), List.of("res"), List.of("res")));
        replies.add(hasDeletable ? List.of(deletable) : List.of());
        if (hasDeletable) replies.add(1);
        var sequence = new NativeSequence(replies.toArray());
        assertEquals(
            hasDeletable ? 1 : 0,
            missing
                ? sequence.operations().cleanupPermitsOfMissingNodes(DUE.toInstant())
                : sequence.operations().cleanupOrphanedPermits(List.of("stale")));
        int readIndex = missing ? 4 : 3;
        assertTrue(sequence.sql.get(readIndex - 1).contains("FOR UPDATE"));
        String read = sequence.sql.get(readIndex);
        assertTrue(read.contains("LEFT JOIN scheduler_job_queue"));
        assertTrue(read.contains("q.status = 'RUNNING' AND q.picked_by = p.node_id"));
        assertTrue(read.contains("q.job_id IS NULL"));
        assertTrue(!read.contains("FOR UPDATE"));
        if (hasDeletable) {
          assertEquals(
              "DELETE FROM scheduler_resource_permit WHERE id IN (?)",
              sequence.sql.get(readIndex + 1).strip());
          assertEquals(1, sequence.parameters.get(readIndex + 1).size());
          assertArrayEquals(deletable, (byte[]) sequence.parameters.get(readIndex + 1).get(1));
          assertTrue(!Arrays.equals(kept, (byte[]) sequence.parameters.get(readIndex + 1).get(1)));
        } else {
          assertEquals(readIndex + 1, sequence.sql.size());
        }
        sequence.assertConsumed();
      }
    }
  }

  @Test
  void cleanupDeletesIdsInBoundedBatches() {
    List<byte[]> ids = new ArrayList<>();
    for (int i = 0; i < 501; i++) ids.add(UuidByteArrayConverter.toBytes(UUID.randomUUID()));
    var sequence = new NativeSequence(List.of("res"), List.of("res"), List.of("res"), ids, 499, 1);
    assertEquals(500, sequence.operations().cleanupOrphanedPermits(List.of("stale")));
    assertEquals(500, sequence.parameters.get(4).size());
    assertEquals(1, sequence.parameters.get(5).size());
    for (int i = 0; i < 500; i++) {
      assertArrayEquals(ids.get(i), (byte[]) sequence.parameters.get(4).get(i + 1));
    }
    assertArrayEquals(ids.get(500), (byte[]) sequence.parameters.get(5).get(1));
    sequence.assertConsumed();
  }

  @Test
  void wakeWithoutLiveWaitersStopsBeforePermitCount() {
    var sequence = new NativeSequence(List.<Object[]>of(new Object[] {2, 1000}), List.of());
    assertTrue(sequence.operations().wakeWaiters("res").isEmpty());
    assertEquals(2, sequence.sql.size());
    sequence.assertConsumed();
  }

  @Test
  void runningHeadDoesNotLockQueueRows() {
    var sequence =
        new NativeSequence(
            List.<Object[]>of(new Object[] {1, 1000}),
            List.<Object[]>of(new Object[] {UUID.randomUUID(), "RUNNING", 2, null, DUE, DUE}),
            0);
    assertTrue(sequence.operations().wakeWaiters("res").isEmpty());
    assertEquals(3, sequence.sql.size());
    assertTrue(sequence.sql.get(2).contains("SELECT COUNT(*)"));
    assertTrue(!sequence.sql.get(2).contains("FOR UPDATE"));
    sequence.assertConsumed();
  }

  @Test
  void wakeUpdatesOnlyTheHeadReturnedBySkipLocked() {
    UUID first = UUID.randomUUID();
    UUID second = UUID.randomUUID();
    byte[] firstBytes = UuidByteArrayConverter.toBytes(first);
    byte[] secondBytes = UuidByteArrayConverter.toBytes(second);
    var sequence =
        new NativeSequence(
            List.<Object[]>of(new Object[] {2, 1000}),
            List.of(
                new Object[] {firstBytes, "PENDING", 4, "target", DUE, DUE},
                new Object[] {secondBytes, "PENDING", 3, null, DUE, DUE}),
            0,
            List.<Object[]>of(new Object[] {secondBytes, DUE, "res"}),
            1);
    var result = sequence.operations().wakeWaiters("res");
    assertEquals(List.of(second), result.stream().map(w -> w.jobId()).toList());
    assertEquals(5, sequence.sql.size());
    assertTrue(
        sequence.sql.get(3).contains("IN (?, ?) AND q.status = 'PENDING' FOR UPDATE SKIP LOCKED"));
    assertTrue(Arrays.equals(firstBytes, (byte[]) sequence.parameters.get(3).get(1)));
    assertTrue(Arrays.equals(secondBytes, (byte[]) sequence.parameters.get(3).get(2)));
    assertTrue(Arrays.equals(secondBytes, (byte[]) sequence.parameters.get(4).get(2)));
    sequence.assertConsumed();
  }

  @Test
  void wakeReturnsUpdatedHeadsInWaiterOrder() {
    UUID first = UUID.randomUUID();
    UUID second = UUID.randomUUID();
    var sequence =
        new NativeSequence(
            List.<Object[]>of(new Object[] {2, 1000}),
            List.of(
                new Object[] {first, "PENDING", 4, "target", DUE, DUE},
                new Object[] {second, "PENDING", 3, null, DUE, DUE}),
            0,
            List.<Object[]>of(new Object[] {second, DUE, "res"}, new Object[] {first, DUE, "res"}),
            1,
            1);
    assertEquals(
        List.of(first, second),
        sequence.operations().wakeWaiters("res").stream().map(w -> w.jobId()).toList());
    assertEquals(6, sequence.sql.size());
    sequence.assertConsumed();
  }

  @Test
  void parkWithoutCurrentResourceWritesNoWaiter() {
    for (List<String> resources : List.of(List.of("other"), List.<String>of())) {
      var sequence =
          new NativeSequence(
              List.<Object[]>of(new Object[] {1, 1000}),
              List.<Object[]>of(new Object[] {"RUNNING", 7L, 3, DUE, null, null, null, "node"}),
              List.of(UUID.randomUUID()),
              List.of(),
              1,
              resources);
      assertEquals(
          AcquireResult.AT_CAPACITY,
          sequence.operations().tryAcquirePermit("res", UUID.randomUUID(), 7L, "node"));
      assertTrue(
          sequence.sql.stream()
              .noneMatch(
                  sql ->
                      sql.startsWith("INSERT INTO scheduler_resource_waiter")
                          || sql.startsWith("UPDATE scheduler_resource_waiter")));
      sequence.assertConsumed();
    }
  }

  @Test
  void successfulAcquireDeletesOnlyItsResourceWaiter() {
    UUID jobId = UUID.randomUUID();
    var sequence =
        new NativeSequence(
            List.<Object[]>of(new Object[] {1, 1000}),
            List.<Object[]>of(new Object[] {"RUNNING", 7L, 3, DUE, 4, DUE, "res", "node"}),
            List.of(),
            List.of(),
            1);
    assertEquals(
        AcquireResult.ACQUIRED, sequence.operations().tryAcquirePermit("res", jobId, 7L, "node"));
    assertEquals(5, sequence.sql.size());
    assertEquals(
        """
        SELECT id FROM scheduler_resource_permit
        WHERE resource_name = ?
        """,
        sequence.sql.get(2));
    assertEquals(1, sequence.flushes);
    assertEquals(
        "DELETE FROM scheduler_resource_waiter WHERE job_id = ? AND resource_name = ?",
        sequence.sql.get(4).strip());
    assertArrayEquals(
        UuidByteArrayConverter.toBytes(jobId), (byte[]) sequence.parameters.get(4).get(1));
    assertEquals("res", sequence.parameters.get(4).get(2));
    assertEquals(2, sequence.parameters.get(4).size());
    sequence.assertConsumed();
  }

  private static final Timestamp DUE = Timestamp.from(Instant.parse("2026-01-01T00:00:00Z"));

  private static final class NativeSequence {
    private int flushes;
    private final Deque<Object> replies = new ArrayDeque<>();
    private final List<String> sql = new ArrayList<>();
    private final List<Map<Integer, Object>> parameters = new ArrayList<>();

    NativeSequence(Object... replies) {
      this.replies.addAll(Arrays.asList(replies));
    }

    MysqlAuxiliaryOperations operations() {
      EntityManager em =
          (EntityManager)
              Proxy.newProxyInstance(
                  EntityManager.class.getClassLoader(),
                  new Class<?>[] {EntityManager.class},
                  (proxy, method, args) -> {
                    if ("persist".equals(method.getName())) return null;
                    if ("flush".equals(method.getName())) {
                      flushes++;
                      return null;
                    }
                    if (!"createNativeQuery".equals(method.getName())) {
                      throw new UnsupportedOperationException(method.getName());
                    }
                    sql.add((String) args[0]);
                    Map<Integer, Object> binds = new HashMap<>();
                    parameters.add(binds);
                    return Proxy.newProxyInstance(
                        Query.class.getClassLoader(),
                        new Class<?>[] {Query.class},
                        (query, operation, values) ->
                            switch (operation.getName()) {
                              case "setParameter" -> {
                                binds.put((Integer) values[0], values[1]);
                                yield query;
                              }
                              case "getResultList", "getSingleResult", "executeUpdate" ->
                                  replies.removeFirst();
                              default ->
                                  throw new UnsupportedOperationException(operation.getName());
                            });
                  });
      return new MysqlAuxiliaryOperations(new MysqlStoreContext(em, null));
    }

    void assertConsumed() {
      assertTrue(replies.isEmpty());
    }
  }

  private static EntityManager entityManagerWithNoResult() {
    Query query =
        (Query)
            Proxy.newProxyInstance(
                Query.class.getClassLoader(),
                new Class<?>[] {Query.class},
                (proxy, method, args) -> {
                  return switch (method.getName()) {
                    case "setParameter" -> proxy;
                    case "getSingleResult" -> throw new NoResultException();
                    default -> throw new UnsupportedOperationException(method.getName());
                  };
                });
    return (EntityManager)
        Proxy.newProxyInstance(
            EntityManager.class.getClassLoader(),
            new Class<?>[] {EntityManager.class},
            (proxy, method, args) -> {
              if ("createNativeQuery".equals(method.getName())) {
                return query;
              }
              throw new UnsupportedOperationException(method.getName());
            });
  }

  private static MysqlStoreContext entityManagerWithExistingPermit(
      UUID expectedJobId, long claimSeq) {
    EntityManager em =
        (EntityManager)
            Proxy.newProxyInstance(
                EntityManager.class.getClassLoader(),
                new Class<?>[] {EntityManager.class},
                (proxy, method, args) -> {
                  return switch (method.getName()) {
                    case "createNativeQuery" ->
                        queryReturningExistingPermit(expectedJobId, claimSeq, (String) args[0]);
                    case "persist" ->
                        throw new AssertionError("existing permit must not insert another row");
                    default -> throw new UnsupportedOperationException(method.getName());
                  };
                });
    return new MysqlStoreContext(em, null);
  }

  private static Query queryReturningExistingPermit(UUID expectedJobId, long claimSeq, String sql) {
    return (Query)
        Proxy.newProxyInstance(
            Query.class.getClassLoader(),
            new Class<?>[] {Query.class},
            (proxy, method, args) -> {
              return switch (method.getName()) {
                case "setParameter" -> proxy;
                case "executeUpdate" -> 1;
                case "getResultList" ->
                    sql.contains("SELECT status FROM scheduler_job_queue")
                        ? List.of()
                        : sql.contains("scheduler_job_queue")
                            ? Collections.singletonList(
                                new Object[] {
                                  "RUNNING",
                                  claimSeq,
                                  2,
                                  Timestamp.from(Instant.now()),
                                  null,
                                  null,
                                  null,
                                  "node-1"
                                })
                            : sql.startsWith("SELECT resource_name FROM scheduler_job")
                                ? List.of("res")
                                : sql.contains("max_concurrent")
                                    ? Collections.singletonList(new Object[] {1, 1000})
                                    : Collections.singletonList(
                                        UuidByteArrayConverter.toBytes(
                                            ResourcePermitEntity.idFor("res", expectedJobId, 0L)));
                default -> throw new UnsupportedOperationException(method.getName());
              };
            });
  }
}
