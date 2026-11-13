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
            Collections.singletonList(secondBytes),
            1);
    var result = sequence.operations().wakeWaiters("res");
    assertEquals(List.of(second), result.stream().map(w -> w.jobId()).toList());
    assertEquals(5, sequence.sql.size());
    assertTrue(
        sequence.sql.get(3).contains("IN (?, ?) AND status = 'PENDING' FOR UPDATE SKIP LOCKED"));
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
            List.of(second, first),
            1,
            1);
    assertEquals(
        List.of(first, second),
        sequence.operations().wakeWaiters("res").stream().map(w -> w.jobId()).toList());
    assertEquals(6, sequence.sql.size());
    sequence.assertConsumed();
  }

  private static final Timestamp DUE = Timestamp.from(Instant.parse("2026-01-01T00:00:00Z"));

  private static final class NativeSequence {
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
                                  "RUNNING", claimSeq, 2, Timestamp.from(Instant.now()), null, null
                                })
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
