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

import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import java.lang.reflect.Proxy;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class SqlserverNodeLockOperationsTest {

  @Test
  void upsertHeartbeatRejectsNullArgumentsBeforeSqlExecution() {
    SqlserverNodeLockOperations operations =
        new SqlserverNodeLockOperations(new SqlserverStoreContext(noopEntityManager()));

    NullPointerException nodeId =
        assertThrows(
            NullPointerException.class, () -> operations.upsertHeartbeat(null, Instant.EPOCH));
    NullPointerException timestamp =
        assertThrows(NullPointerException.class, () -> operations.upsertHeartbeat("node-1", null));

    assertEquals("nodeId", nodeId.getMessage());
    assertEquals("ts", timestamp.getMessage());
  }

  private static EntityManager noopEntityManager() {
    return (EntityManager)
        Proxy.newProxyInstance(
            EntityManager.class.getClassLoader(),
            new Class<?>[] {EntityManager.class},
            (proxy, method, args) -> {
              if ("createNativeQuery".equals(method.getName())) {
                return noopQuery();
              }
              throw new UnsupportedOperationException(method.getName());
            });
  }

  private static Query noopQuery() {
    return (Query)
        Proxy.newProxyInstance(
            Query.class.getClassLoader(),
            new Class<?>[] {Query.class},
            (proxy, method, args) -> {
              if ("executeUpdate".equals(method.getName())) {
                return 1;
              }
              if ("setParameter".equals(method.getName())) {
                return proxy;
              }
              throw new UnsupportedOperationException(method.getName());
            });
  }
}
