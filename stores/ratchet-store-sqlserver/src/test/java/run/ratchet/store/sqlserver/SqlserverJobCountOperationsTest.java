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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobPriority;

class SqlserverJobCountOperationsTest {

  @Test
  void queueWaitPercentileRejectsOutOfRangeValueBeforeQuerying() {
    SqlserverJobCountOperations operations =
        new SqlserverJobCountOperations(new SqlserverStoreContext(entityManagerThatMustNotRun()));

    IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class, () -> operations.getQueueWaitTimePercentile(1.5));

    assertTrue(thrown.getMessage().contains("[0.0, 1.0]"));
  }

  @Test
  void countPendingJobsByPriorityBindsStablePersistedCode() {
    AtomicReference<Object> parameter = new AtomicReference<>();
    SqlserverJobCountOperations operations =
        new SqlserverJobCountOperations(
            new SqlserverStoreContext(entityManagerCapturingParameter(parameter)));

    assertEquals(0L, operations.countPendingJobsByPriority(JobPriority.HIGH));
    assertEquals(JobPriority.HIGH.persistedCode(), parameter.get());
  }

  private static EntityManager entityManagerThatMustNotRun() {
    return (EntityManager)
        Proxy.newProxyInstance(
            EntityManager.class.getClassLoader(),
            new Class<?>[] {EntityManager.class},
            (proxy, method, args) -> {
              throw new AssertionError("EntityManager should not be called");
            });
  }

  private static EntityManager entityManagerCapturingParameter(AtomicReference<Object> parameter) {
    Query query =
        (Query)
            Proxy.newProxyInstance(
                Query.class.getClassLoader(),
                new Class<?>[] {Query.class},
                (proxy, method, args) -> {
                  if (method.getName().equals("setParameter")) {
                    parameter.set(args[1]);
                    return proxy;
                  }
                  if (method.getName().equals("getSingleResult")) {
                    return 0L;
                  }
                  throw new UnsupportedOperationException(method.getName());
                });

    return (EntityManager)
        Proxy.newProxyInstance(
            EntityManager.class.getClassLoader(),
            new Class<?>[] {EntityManager.class},
            (proxy, method, args) -> {
              if (method.getName().equals("createNativeQuery")) {
                return query;
              }
              throw new UnsupportedOperationException(method.getName());
            });
  }
}
