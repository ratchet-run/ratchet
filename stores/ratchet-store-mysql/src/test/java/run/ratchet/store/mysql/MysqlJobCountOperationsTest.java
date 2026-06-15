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
import static org.junit.jupiter.api.Assertions.assertThrows;

import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import java.lang.reflect.Proxy;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class MysqlJobCountOperationsTest {

  @Test
  void doubleStatsReturnZeroWhenNativeAggregateReturnsNull() {
    MysqlJobCountOperations counts =
        new MysqlJobCountOperations(new MysqlStoreContext(entityManagerReturningNull(), null));
    Instant since = Instant.parse("2026-01-01T00:00:00Z");

    assertEquals(0.0, counts.getRetryRateStats(since));
    assertEquals(0.0, counts.getAverageProcessingTime(since));
    assertEquals(0.0, counts.getAverageBatchSize(since));
  }

  @Test
  void countActiveNodesUsesNativeSchedulerNodeCount() {
    AtomicReference<String> sql = new AtomicReference<>();
    MysqlJobCountOperations counts =
        new MysqlJobCountOperations(
            new MysqlStoreContext(entityManagerReturningCount(sql, 3L), null));

    assertEquals(3L, counts.countActiveNodes());
    assertEquals("SELECT COUNT(*) FROM scheduler_node", sql.get());
  }

  @Test
  void queueWaitPercentileUsesWindowPercentileQuery() {
    AtomicReference<Object> percentile = new AtomicReference<>();
    MysqlJobCountOperations counts =
        new MysqlJobCountOperations(
            new MysqlStoreContext(entityManagerForPercentile(percentile), null));

    assertEquals(900L, counts.getQueueWaitTimePercentile(1.0));
    assertEquals(1.0, percentile.get());
  }

  @Test
  void queueWaitPercentileRejectsOutsideRange() {
    MysqlJobCountOperations counts =
        new MysqlJobCountOperations(new MysqlStoreContext(entityManagerReturningNull(), null));

    assertThrows(IllegalArgumentException.class, () -> counts.getQueueWaitTimePercentile(-0.01));
    assertThrows(IllegalArgumentException.class, () -> counts.getQueueWaitTimePercentile(1.01));
    assertThrows(
        IllegalArgumentException.class, () -> counts.getQueueWaitTimePercentile(Double.NaN));
  }

  private static EntityManager entityManagerReturningNull() {
    return entityManagerReturningCount(new AtomicReference<>(), null);
  }

  private static EntityManager entityManagerReturningCount(
      AtomicReference<String> sql, Number countResult) {
    Query query =
        (Query)
            Proxy.newProxyInstance(
                Query.class.getClassLoader(),
                new Class<?>[] {Query.class},
                (proxy, method, args) -> {
                  if (method.getName().equals("setParameter")) {
                    return proxy;
                  }
                  if (method.getName().equals("getSingleResult")) {
                    return countResult;
                  }
                  throw new UnsupportedOperationException(method.getName());
                });

    return (EntityManager)
        Proxy.newProxyInstance(
            EntityManager.class.getClassLoader(),
            new Class<?>[] {EntityManager.class},
            (proxy, method, args) -> {
              if (method.getName().equals("createNativeQuery")) {
                sql.set((String) args[0]);
                return query;
              }
              throw new UnsupportedOperationException(method.getName());
            });
  }

  private static EntityManager entityManagerForPercentile(AtomicReference<Object> percentile) {
    Query percentileQuery =
        (Query)
            Proxy.newProxyInstance(
                Query.class.getClassLoader(),
                new Class<?>[] {Query.class},
                (proxy, method, args) -> {
                  if (method.getName().equals("setParameter")) {
                    percentile.set(args[1]);
                    return proxy;
                  }
                  if (method.getName().equals("getResultList")) {
                    return List.of(900L);
                  }
                  throw new UnsupportedOperationException(method.getName());
                });

    return (EntityManager)
        Proxy.newProxyInstance(
            EntityManager.class.getClassLoader(),
            new Class<?>[] {EntityManager.class},
            (proxy, method, args) -> {
              if (method.getName().equals("createNativeQuery")) {
                return percentileQuery;
              }
              throw new UnsupportedOperationException(method.getName());
            });
  }
}
