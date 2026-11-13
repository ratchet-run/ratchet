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
package run.ratchet.store.postgresql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import java.lang.reflect.Proxy;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import run.ratchet.store.spi.ResourcePermitStore.AcquireResult;
import run.ratchet.store.spi.ResourceWaiterLease;

class PostgresqlResourceWaiterOperationsTest {

  @Test
  void wakeWithoutLiveWaitersStopsBeforePermitCount() {
    var sequence = new NativeSequence(List.<Object[]>of(new Object[] {2, 1000}), List.of());
    assertTrue(sequence.operations().wakeWaiters("res").isEmpty());
    assertEquals(2, sequence.sql.size());
    sequence.assertConsumed();
  }

  @Test
  void wakeSkipsHeldHeadAndPreservesWaiterOrder() {
    UUID first = UUID.randomUUID();
    UUID second = UUID.randomUUID();
    UUID third = UUID.randomUUID();
    var sequence =
        new NativeSequence(
            List.<Object[]>of(new Object[] {3, 1000}),
            List.of(
                new Object[] {id(first), "PENDING", 4, "target", DUE, DUE},
                new Object[] {id(second), "PENDING", 3, null, DUE, DUE},
                new Object[] {id(third), "PENDING", 2, null, DUE, DUE}),
            0,
            List.<Object[]>of(
                new Object[] {id(third), DUE, "res"}, new Object[] {id(second), DUE, "res"}),
            1,
            1);
    assertEquals(
        List.of(second, third),
        sequence.operations().wakeWaiters("res").stream().map(w -> w.jobId()).toList());
    assertEquals(6, sequence.sql.size());
    assertTrue(sequence.sql.get(0).contains("scheduler_resource_limit"));
    assertTrue(sequence.sql.get(3).contains("IN (?, ?, ?) AND q.status = 'PENDING'"));
    assertTrue(
        sequence.sql.get(3).contains("FOR UPDATE OF q SKIP LOCKED FOR SHARE OF c SKIP LOCKED"));
    assertTrue(Objects.deepEquals(id(second), sequence.parameters.get(4).get(2)));
    assertTrue(Objects.deepEquals(id(third), sequence.parameters.get(5).get(2)));
    assertEquals(DUE, sequence.parameters.get(4).get(1));
    sequence.assertConsumed();
  }

  @Test
  void runningHeadConsumesCapacityWithoutLockingQueueRows() {
    var sequence =
        new NativeSequence(
            List.<Object[]>of(new Object[] {1, 1000}),
            List.<Object[]>of(new Object[] {id(UUID.randomUUID()), "RUNNING", 2, null, DUE, DUE}),
            0);
    assertTrue(sequence.operations().wakeWaiters("res").isEmpty());
    assertEquals(3, sequence.sql.size());
    assertTrue(!sequence.sql.get(2).contains("FOR UPDATE"));
    sequence.assertConsumed();
  }

  @Test
  void claimChangeAfterCheckParksNothing() {
    UUID jobId = UUID.randomUUID();
    var sequence =
        new NativeSequence(
            List.<Object[]>of(new Object[] {1, 1000}),
            List.<Object[]>of(new Object[] {"RUNNING", 7L, 3, DUE, null, null, "res"}),
            List.of(id(UUID.randomUUID())),
            List.of(),
            0);
    assertEquals(
        AcquireResult.SUPERSEDED, sequence.operations().tryAcquirePermit("res", jobId, 7L, "node"));
    assertEquals(5, sequence.sql.size());
    assertTrue(!sequence.sql.get(1).contains("FOR UPDATE"));
    assertTrue(!sequence.sql.get(2).contains("FOR UPDATE"));
    assertTrue(sequence.sql.get(4).contains("status = 'RUNNING' AND claim_seq = ?"));
    sequence.assertConsumed();
  }

  @Test
  void capacityMissRenewsLeaseWithoutReplacingOriginalOrder() {
    UUID jobId = UUID.randomUUID();
    var sequence =
        new NativeSequence(
            List.<Object[]>of(new Object[] {1, 1000}),
            List.<Object[]>of(new Object[] {"RUNNING", 7L, 3, DUE, 4, DUE, "res"}),
            List.of(id(UUID.randomUUID())),
            List.of("PAUSED"),
            1,
            List.of("res"),
            1);
    assertEquals(
        AcquireResult.AT_CAPACITY,
        sequence.operations().tryAcquirePermit("res", jobId, 7L, "node"));
    assertEquals(7, sequence.sql.size());
    assertEquals("PAUSED", sequence.parameters.get(4).get(1));
    assertEquals("PENDING", sequence.parameters.get(4).get(2));
    assertEquals("Waiting for resource: res", sequence.parameters.get(4).get(3));
    assertEquals(1000, sequence.parameters.get(4).get(4));
    assertEquals(ResourceWaiterLease.leaseMillis(1000), sequence.parameters.get(6).get(1));
    assertTrue(!sequence.sql.get(4).contains("attempts ="));
    assertTrue(sequence.sql.get(6).contains("wait_since ="));
    assertEquals("res", sequence.parameters.get(6).get(2));
    assertEquals(4, sequence.parameters.get(6).get(3));
    assertEquals(DUE, sequence.parameters.get(6).get(4));
    sequence.assertConsumed();
  }

  @Test
  void acquireWithoutWaitersUsesFiveStatements() {
    UUID jobId = UUID.randomUUID();
    var sequence =
        new NativeSequence(
            List.<Object[]>of(new Object[] {1, 1000}),
            List.<Object[]>of(new Object[] {"RUNNING", 7L, 3, DUE, null, null, "res"}),
            List.of(),
            List.of(),
            1);
    assertEquals(
        AcquireResult.ACQUIRED, sequence.operations().tryAcquirePermit("res", jobId, 7L, "node"));
    assertEquals(5, sequence.sql.size());
    assertTrue(sequence.sql.get(0).contains("FOR UPDATE"));
    assertTrue(!sequence.sql.get(2).contains("FOR UPDATE"));
    sequence.assertConsumed();
  }

  @Test
  void parkWithoutCurrentResourceWritesNoWaiter() {
    for (List<String> resources : List.of(List.of("other"), List.<String>of())) {
      var sequence =
          new NativeSequence(
              List.<Object[]>of(new Object[] {1, 1000}),
              List.<Object[]>of(new Object[] {"RUNNING", 7L, 3, DUE, null, null, null}),
              List.of(id(UUID.randomUUID())),
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

  private static final Timestamp DUE = Timestamp.from(Instant.parse("2026-01-01T00:00:00Z"));

  private static final class NativeSequence {
    private final Deque<Object> replies = new ArrayDeque<>();
    private final List<String> sql = new ArrayList<>();
    private final List<Map<Integer, Object>> parameters = new ArrayList<>();

    NativeSequence(Object... replies) {
      this.replies.addAll(Arrays.asList(replies));
    }

    PostgresqlAuxiliaryOperations operations() {
      EntityManager em =
          (EntityManager)
              Proxy.newProxyInstance(
                  EntityManager.class.getClassLoader(),
                  new Class<?>[] {EntityManager.class},
                  (proxy, method, args) -> {
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
      return new PostgresqlAuxiliaryOperations(new PostgresqlStoreContext(em));
    }

    void assertConsumed() {
      assertTrue(replies.isEmpty());
    }
  }

  private static Object id(UUID value) {
    return value;
  }
}
