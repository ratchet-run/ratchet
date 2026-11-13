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
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.persistence.EntityManager;
import jakarta.persistence.NoResultException;
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
import java.util.UUID;
import org.junit.jupiter.api.Test;
import run.ratchet.store.entity.ResourceLimitEntity;
import run.ratchet.store.spi.ResourcePermitStore.AcquireResult;
import run.ratchet.store.spi.ResourceWaiterLease;
import run.ratchet.store.sqlserver.converter.UuidByteArrayConverter;

class SqlserverAuxiliaryOperationsTest {

  @Test
  void getPermitRetryDelayUsesDefaultForMissingResource() {
    SqlserverAuxiliaryOperations operations =
        new SqlserverAuxiliaryOperations(
            new SqlserverStoreContext(entityManagerThrowingNoResult()));

    assertEquals(
        ResourceLimitEntity.DEFAULT_RETRY_DELAY_MS,
        operations.getPermitRetryDelay("missing-resource"));
  }

  @Test
  void acquireKeepsCapacityGuardAfterPlainCountAndGate() {
    NativeSequence sequence = acquireSequence(0, 1);
    assertEquals(
        AcquireResult.ACQUIRED, sequence.operations().tryAcquirePermit("res", JOB, 7, "node"));
    assertEquals(6, sequence.sql.size());
    assertTrue(sequence.sql.get(0).contains("WITH (UPDLOCK, HOLDLOCK, ROWLOCK)"));
    assertTrue(sequence.sql.get(3).contains("SELECT COUNT(*)"));
    assertTrue(sequence.sql.get(4).contains("w.lease_until > SYSUTCDATETIME()"));
    assertTrue(sequence.sql.get(5).contains(") < max_concurrent"));
    sequence.assertConsumed();
  }

  @Test
  void fullAcquireParksAndCreatesLeaseWithoutChangingAttempts() {
    NativeSequence sequence = acquireSequence(1, null);
    sequence.replies.add(List.of()); // Batch parent lookup.
    sequence.replies.add(1); // Fenced park.
    sequence.replies.add(1); // Waiter insert.
    assertEquals(
        AcquireResult.AT_CAPACITY, sequence.operations().tryAcquirePermit("res", JOB, 7, "node"));
    assertEquals(7, sequence.sql.size());
    assertTrue(sequence.sql.get(4).contains("WITH (UPDLOCK, ROWLOCK)"));
    assertTrue(sequence.sql.get(5).contains("claim_seq = ?"));
    assertTrue(sequence.sql.get(5).contains("DATEADD(MILLISECOND, ?, SYSUTCDATETIME())"));
    assertEquals(DUE, sequence.parameters.get(6).get(4));
    long lease = ResourceWaiterLease.leaseMillis(1000);
    assertEquals((int) (lease % 1000), sequence.parameters.get(6).get(5));
    assertEquals((int) (lease / 1000), sequence.parameters.get(6).get(6));
    sequence.assertConsumed();
  }

  @Test
  void waiterAheadParksAndRenewsOnlyTheLease() {
    NativeSequence sequence =
        new NativeSequence(
            List.<Object[]>of(new Object[] {1, 1000}),
            List.<Object[]>of(new Object[] {"RUNNING", 7L, 2, DUE, 2, DUE}),
            0,
            0,
            List.of(JOB),
            List.of("PAUSED"),
            1,
            1);
    assertEquals(
        AcquireResult.AT_CAPACITY, sequence.operations().tryAcquirePermit("res", JOB, 7, "node"));
    assertEquals(8, sequence.sql.size());
    assertEquals("PAUSED", sequence.parameters.get(6).get(1));
    assertEquals("PENDING", sequence.parameters.get(6).get(2));
    assertTrue(sequence.sql.get(7).contains("SET lease_until = DATEADD"));
    assertEquals(3, sequence.parameters.get(7).size());
    sequence.assertConsumed();
  }

  @Test
  void successfulAcquireRemovesExistingWaiter() {
    NativeSequence sequence =
        new NativeSequence(
            List.<Object[]>of(new Object[] {1, 1000}),
            List.<Object[]>of(new Object[] {"RUNNING", 7L, 2, DUE, 2, DUE}),
            0,
            0,
            List.of(),
            1,
            1);
    assertEquals(
        AcquireResult.ACQUIRED, sequence.operations().tryAcquirePermit("res", JOB, 7, "node"));
    assertEquals(7, sequence.sql.size());
    assertTrue(
        sequence.sql.get(6).contains("DELETE FROM scheduler_resource_waiter WHERE job_id = ?"));
    sequence.assertConsumed();
  }

  @Test
  void sameClaimReturnsBeforeCountOrGate() {
    NativeSequence sequence =
        new NativeSequence(
            List.<Object[]>of(new Object[] {1, 1000}),
            List.<Object[]>of(new Object[] {"RUNNING", 7L, 2, DUE, null, null}),
            1);
    assertEquals(
        AcquireResult.ACQUIRED, sequence.operations().tryAcquirePermit("res", JOB, 7, "node"));
    assertEquals(3, sequence.sql.size());
    sequence.assertConsumed();
  }

  @Test
  void runningHeadUsesFreeSlotWithoutWakingTheNextWaiter() {
    NativeSequence sequence =
        new NativeSequence(
            List.<Object[]>of(new Object[] {1, 1000}),
            List.<Object[]>of(new Object[] {JOB, "RUNNING", 2, null, DUE, DUE}),
            0);
    assertTrue(sequence.operations().wakeWaiters("res").isEmpty());
    assertEquals(3, sequence.sql.size());
    sequence.assertConsumed();
  }

  @Test
  void guardedInsertMissAlsoParks() {
    NativeSequence sequence = acquireSequence(0, 0);
    sequence.replies.add(List.of());
    sequence.replies.add(1);
    sequence.replies.add(1);
    assertEquals(
        AcquireResult.AT_CAPACITY, sequence.operations().tryAcquirePermit("res", JOB, 7, "node"));
    assertEquals(9, sequence.sql.size());
    sequence.assertConsumed();
  }

  @Test
  void supersededParkCreatesNoWaiter() {
    NativeSequence sequence = acquireSequence(1, null);
    sequence.replies.add(List.of());
    sequence.replies.add(0);
    assertEquals(
        AcquireResult.SUPERSEDED, sequence.operations().tryAcquirePermit("res", JOB, 7, "node"));
    assertEquals(6, sequence.sql.size());
    sequence.assertConsumed();
  }

  @Test
  void wakeWithoutLiveWaitersStopsBeforePermitCount() {
    NativeSequence sequence =
        new NativeSequence(List.<Object[]>of(new Object[] {2, 1000}), List.of());
    assertTrue(sequence.operations().wakeWaiters("res").isEmpty());
    assertEquals(2, sequence.sql.size());
    sequence.assertConsumed();
  }

  @Test
  void wakeSkipsLockedRowsAndReturnsWaiterOrder() {
    UUID second = UUID.fromString("00000000-0000-0000-0000-000000000002");
    NativeSequence sequence =
        new NativeSequence(
            List.<Object[]>of(new Object[] {2, 1000}),
            List.of(
                new Object[] {JOB, "PENDING", 4, "target", DUE, DUE},
                new Object[] {second, "PENDING", 3, null, DUE, DUE}),
            0,
            1,
            1);
    var woken = sequence.operations().wakeWaiters("res");
    assertEquals(List.of(JOB, second), woken.stream().map(w -> w.jobId()).toList());
    assertTrue(
        sequence.sql.get(1).contains("ORDER BY w.priority DESC, w.wait_since ASC, w.job_id ASC"));
    assertTrue(sequence.sql.get(3).contains("WITH (UPDLOCK, READPAST, ROWLOCK)"));
    assertTrue(sequence.sql.get(4).contains("WITH (UPDLOCK, READPAST, ROWLOCK)"));
    assertTrue(
        Arrays.equals(
            UuidByteArrayConverter.toBytes(JOB), (byte[]) sequence.parameters.get(3).get(2)));
    assertTrue(
        Arrays.equals(
            UuidByteArrayConverter.toBytes(second), (byte[]) sequence.parameters.get(4).get(2)));
    sequence.assertConsumed();
  }

  @Test
  void wakeDoesNotReturnAHeadSkippedByTheUpdate() {
    NativeSequence sequence =
        new NativeSequence(
            List.<Object[]>of(new Object[] {1, 1000}),
            List.<Object[]>of(new Object[] {JOB, "PENDING", 4, "target", DUE, DUE}),
            0,
            0);
    assertTrue(sequence.operations().wakeWaiters("res").isEmpty());
    assertTrue(sequence.sql.get(3).contains("WITH (UPDLOCK, READPAST, ROWLOCK)"));
    sequence.assertConsumed();
  }

  private static final UUID JOB = UUID.fromString("00000000-0000-0000-0000-000000000001");
  private static final Timestamp DUE = Timestamp.from(Instant.parse("2026-01-01T00:00:00Z"));

  private static NativeSequence acquireSequence(int held, Integer inserted) {
    NativeSequence sequence =
        new NativeSequence(
            List.<Object[]>of(new Object[] {1, 1000}),
            List.<Object[]>of(new Object[] {"RUNNING", 7L, 2, DUE, null, null}),
            0,
            held);
    if (inserted != null) {
      sequence.replies.add(List.of()); // No waiters ahead.
      sequence.replies.add(inserted);
    }
    return sequence;
  }

  private static final class NativeSequence {
    private final Deque<Object> replies = new ArrayDeque<>();
    private final List<String> sql = new ArrayList<>();
    private final List<Map<Integer, Object>> parameters = new ArrayList<>();

    NativeSequence(Object... replies) {
      this.replies.addAll(Arrays.asList(replies));
    }

    SqlserverAuxiliaryOperations operations() {
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
      return new SqlserverAuxiliaryOperations(new SqlserverStoreContext(em));
    }

    void assertConsumed() {
      assertTrue(replies.isEmpty());
    }
  }

  private static EntityManager entityManagerThrowingNoResult() {
    Query query =
        (Query)
            Proxy.newProxyInstance(
                Query.class.getClassLoader(),
                new Class<?>[] {Query.class},
                (proxy, method, args) -> {
                  return switch (method.getName()) {
                    case "setParameter" -> proxy;
                    case "getSingleResult" -> throw new NoResultException("missing");
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
}
