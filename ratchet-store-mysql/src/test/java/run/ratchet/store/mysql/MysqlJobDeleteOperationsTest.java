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
import jakarta.persistence.Query;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class MysqlJobDeleteOperationsTest {

  @Test
  void resetOrphanJobsIncludesNullPickedByRows() {
    AtomicReference<String> sql = new AtomicReference<>();
    MysqlStoreContext ctx = new MysqlStoreContext(entityManagerCapturingSql(sql), null);
    MysqlJobDeleteOperations deletes =
        new MysqlJobDeleteOperations(ctx, new MysqlBusinessKeyReservations(ctx));

    assertEquals(3, deletes.resetOrphanJobs(Duration.ofSeconds(30)));
    assertTrue(sql.get().contains("picked_by IS NULL OR picked_by NOT IN"));
  }

  private static EntityManager entityManagerCapturingSql(AtomicReference<String> sql) {
    return (EntityManager)
        Proxy.newProxyInstance(
            EntityManager.class.getClassLoader(),
            new Class<?>[] {EntityManager.class},
            (proxy, method, args) -> {
              if ("createNativeQuery".equals(method.getName())) {
                sql.set(((String) args[0]).replaceAll("\\s+", " "));
                return queryReturningUpdateCount();
              }
              throw new UnsupportedOperationException(method.getName());
            });
  }

  private static Query queryReturningUpdateCount() {
    return (Query)
        Proxy.newProxyInstance(
            Query.class.getClassLoader(),
            new Class<?>[] {Query.class},
            (proxy, method, args) -> {
              return switch (method.getName()) {
                case "setParameter" -> proxy;
                case "executeUpdate" -> 3;
                default -> throw new UnsupportedOperationException(method.getName());
              };
            });
  }
}
