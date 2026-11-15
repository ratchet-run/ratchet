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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.persistence.EntityManager;
import jakarta.persistence.NoResultException;
import jakarta.persistence.Query;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import run.ratchet.store.entity.BatchEntity;
import run.ratchet.store.sqlserver.converter.UuidByteArrayConverter;

class SqlserverBatchOperationsTest {

  private static final UUID BATCH_ID = UUID.fromString("019ae3d1-3f82-7e18-9f09-a9f000000007");

  @Test
  void repairSweepsLockAndRevalidateParentBeforeSelectingChildren() {
    for (String operation : List.of("pause", "resume")) {
      for (String parentStatus : List.of("PAUSED", "PENDING")) {
        List<String> statements = new ArrayList<>();
        Query parent = statusQuery(List.of(parentStatus));
        Query children = statusQuery(List.of());
        EntityManager em =
            (EntityManager)
                Proxy.newProxyInstance(
                    EntityManager.class.getClassLoader(),
                    new Class<?>[] {EntityManager.class},
                    (proxy, method, args) -> {
                      if (method.getName().equals("createNativeQuery")) {
                        String sql = (String) args[0];
                        statements.add(sql);
                        return sql.contains("SELECT status") ? parent : children;
                      }
                      throw new UnsupportedOperationException(method.getName());
                    });
        var operations = new SqlserverBatchOperations(new SqlserverStoreContext(em));
        assertEquals(
            0,
            operation.equals("pause")
                ? operations.pauseBatchChildren(BATCH_ID)
                : operations.resumeBatchChildren(BATCH_ID));
        boolean matches = parentStatus.equals(operation.equals("pause") ? "PAUSED" : "PENDING");
        assertEquals(matches ? 2 : 1, statements.size());
        assertTrue(statements.get(0).contains("SELECT status"));
        assertTrue(statements.get(0).contains("UPDLOCK"));
        if (matches) {
          assertTrue(statements.get(1).contains("INDEX(idx_job_depends_on)"));
          assertTrue(statements.get(1).contains("INNER LOOP JOIN"));
          assertTrue(statements.get(1).contains("FORCESEEK (pk_scheduler_job_queue (job_id))"));
          assertTrue(statements.get(1).contains("OPTION (FORCE ORDER)"));
        }
      }
    }
  }

  @Test
  void sweepsAdvanceCursorBeforeEachPage() {
    for (String operation : List.of("pause", "resume", "cancel")) {
      UUID first = new UUID(1L, 1L);
      UUID last = new UUID(1L, 2L);
      UUID next = new UUID(1L, 3L);
      List<Object> cursors = new ArrayList<>();
      AtomicInteger page = new AtomicInteger();
      Query children =
          (Query)
              Proxy.newProxyInstance(
                  Query.class.getClassLoader(),
                  new Class<?>[] {Query.class},
                  (proxy, method, args) ->
                      switch (method.getName()) {
                        case "setParameter" -> {
                          if (args[0].equals(2)) cursors.add(args[1]);
                          yield proxy;
                        }
                        case "getResultList" ->
                            switch (page.getAndIncrement()) {
                              case 0 ->
                                  List.of(
                                      UuidByteArrayConverter.toBytes(first),
                                      UuidByteArrayConverter.toBytes(last));
                              case 1 -> List.of(UuidByteArrayConverter.toBytes(next));
                              default -> List.of();
                            };
                        default -> throw new UnsupportedOperationException(method.getName());
                      });
      Query writes =
          (Query)
              Proxy.newProxyInstance(
                  Query.class.getClassLoader(),
                  new Class<?>[] {Query.class},
                  (proxy, method, args) ->
                      switch (method.getName()) {
                        case "setParameter" -> proxy;
                        case "executeUpdate" -> 1;
                        default -> throw new UnsupportedOperationException(method.getName());
                      });
      Query parent =
          (Query)
              Proxy.newProxyInstance(
                  Query.class.getClassLoader(),
                  new Class<?>[] {Query.class},
                  (proxy, method, args) ->
                      switch (method.getName()) {
                        case "setParameter" -> proxy;
                        case "getResultList" ->
                            List.of(operation.equals("pause") ? "PAUSED" : "PENDING");
                        default -> throw new UnsupportedOperationException(method.getName());
                      });
      EntityManager em =
          (EntityManager)
              Proxy.newProxyInstance(
                  EntityManager.class.getClassLoader(),
                  new Class<?>[] {EntityManager.class},
                  (proxy, method, args) -> {
                    if (method.getName().equals("createNativeQuery")) {
                      String sql = (String) args[0];
                      if (sql.contains("SELECT status")) return parent;
                      if (sql.stripLeading().startsWith("SELECT")) {
                        assertTrue(sql.contains("c.job_id > ?"));
                        return children;
                      }
                      return writes;
                    }
                    throw new UnsupportedOperationException(method.getName());
                  });
      var operations = new SqlserverBatchOperations(new SqlserverStoreContext(em));
      UUID batchId = new UUID(1L, 0L);
      switch (operation) {
        case "pause" -> operations.pauseBatchChildren(batchId);
        case "resume" -> operations.resumeBatchChildren(batchId);
        default -> operations.cancelBatchChildren(batchId);
      }
      assertEquals(3, cursors.size());
      assertArrayEquals(new byte[16], (byte[]) cursors.get(0));
      assertArrayEquals(UuidByteArrayConverter.toBytes(last), (byte[]) cursors.get(1));
      assertArrayEquals(UuidByteArrayConverter.toBytes(next), (byte[]) cursors.get(2));
    }
  }

  private static Query statusQuery(List<?> rows) {
    return (Query)
        Proxy.newProxyInstance(
            Query.class.getClassLoader(),
            new Class<?>[] {Query.class},
            (proxy, method, args) ->
                switch (method.getName()) {
                  case "setParameter" -> proxy;
                  case "getResultList" -> rows;
                  default -> throw new UnsupportedOperationException(method.getName());
                });
  }

  @Test
  void mapIncrementResultRejectsScalarResult() {
    IllegalStateException thrown =
        assertThrows(
            IllegalStateException.class,
            () -> SqlserverBatchOperations.mapIncrementResult(BATCH_ID, 1, ignored -> null));

    assertTrue(thrown.getMessage().contains("Object[]"));
  }

  @Test
  void mapIncrementResultRejectsShortRow() {
    IllegalStateException thrown =
        assertThrows(
            IllegalStateException.class,
            () ->
                SqlserverBatchOperations.mapIncrementResult(
                    BATCH_ID, new Object[] {1, 0, 2}, ignored -> null));

    assertTrue(thrown.getMessage().contains("at least 4 columns"));
  }

  @Test
  void mapIncrementResultRejectsNonNumericCounters() {
    IllegalStateException thrown =
        assertThrows(
            IllegalStateException.class,
            () ->
                SqlserverBatchOperations.mapIncrementResult(
                    BATCH_ID, new Object[] {"1", 0, 2, null}, ignored -> null));

    assertTrue(thrown.getMessage().contains("completed_items"));
    assertTrue(thrown.getMessage().contains("numeric"));
  }

  @Test
  void mapIncrementResultMapsValidRow() {
    var progress =
        SqlserverBatchOperations.mapIncrementResult(
            BATCH_ID, new Object[] {1L, 2L, 5L, null}, ignored -> null);

    assertEquals(BATCH_ID, progress.batchId());
    assertEquals(5, progress.totalItems());
    assertEquals(1, progress.completedItems());
    assertEquals(2, progress.failedItems());
  }

  @Test
  void incrementAtomicThrowsClearErrorWhenBatchIsMissing() {
    SqlserverBatchOperations operations =
        new SqlserverBatchOperations(new SqlserverStoreContext(entityManagerThrowingNoResult()));

    IllegalStateException thrown =
        assertThrows(
            IllegalStateException.class, () -> operations.incrementCompletedAtomic(BATCH_ID));

    assertTrue(thrown.getMessage().contains("Batch not found"));
    assertTrue(thrown.getMessage().contains(BATCH_ID.toString()));
  }

  @Test
  void saveBatchUsesReturningRowWithoutFollowUpSelect() {
    AtomicInteger createNativeQueryCalls = new AtomicInteger();
    Query query =
        (Query)
            Proxy.newProxyInstance(
                Query.class.getClassLoader(),
                new Class<?>[] {Query.class},
                (proxy, method, args) -> {
                  return switch (method.getName()) {
                    case "setParameter" -> proxy;
                    case "getSingleResult" ->
                        new Object[] {BATCH_ID, 4, 1, 2, false, 7, null, null, null, null, null};
                    default -> throw new UnsupportedOperationException(method.getName());
                  };
                });
    EntityManager em =
        (EntityManager)
            Proxy.newProxyInstance(
                EntityManager.class.getClassLoader(),
                new Class<?>[] {EntityManager.class},
                (proxy, method, args) -> {
                  if ("createNativeQuery".equals(method.getName())) {
                    createNativeQueryCalls.incrementAndGet();
                    return query;
                  }
                  throw new UnsupportedOperationException(method.getName());
                });
    BatchEntity batch = new BatchEntity();
    batch.setId(BATCH_ID);
    batch.setTotalItems(4);
    batch.setCompletedItems(1);
    batch.setFailedItems(2);

    BatchEntity saved =
        new SqlserverBatchOperations(new SqlserverStoreContext(em)).saveBatch(batch);

    assertEquals(1, createNativeQueryCalls.get());
    assertEquals(BATCH_ID, saved.getId());
    assertEquals(7, saved.getVersion());
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
