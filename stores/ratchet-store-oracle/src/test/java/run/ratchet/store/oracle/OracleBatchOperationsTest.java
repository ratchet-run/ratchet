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
package run.ratchet.store.oracle;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import run.ratchet.store.oracle.converter.UuidRawConverter;

class OracleBatchOperationsTest {

  @Test
  void sweepsAdvanceCursorAndRevisitEarlierIds() {
    for (String operation : List.of("pause", "resume", "cancel")) {
      UUID returned = new UUID(0L, 1L);
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
                          if (args[0].equals(3)) cursors.add(args[1]);
                          yield proxy;
                        }
                        case "getResultList" ->
                            switch (page.getAndIncrement()) {
                              case 0 ->
                                  List.of(
                                      UuidRawConverter.toBytes(first),
                                      UuidRawConverter.toBytes(last));
                              case 1 -> List.of(UuidRawConverter.toBytes(next));
                              case 3 -> List.of(UuidRawConverter.toBytes(returned));
                              default -> List.of();
                            };
                        default -> throw new UnsupportedOperationException(method.getName());
                      });
      AtomicInteger metricUpdates = new AtomicInteger();
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
                        assertEquals(
                            page.get() == 1 || page.get() == 2 || page.get() == 4,
                            sql.contains("h.job_id > ?"));
                        return children;
                      }
                      if (sql.contains("failed_items = failed_items +")) {
                        metricUpdates.incrementAndGet();
                        return (Query)
                            Proxy.newProxyInstance(
                                Query.class.getClassLoader(),
                                new Class<?>[] {Query.class},
                                (p, m, a) -> {
                                  if (m.getName().equals("setParameter")) {
                                    if (a[0].equals(1)) assertEquals(3, a[1]);
                                    return p;
                                  }
                                  if (m.getName().equals("executeUpdate")) return 1;
                                  throw new UnsupportedOperationException(m.getName());
                                });
                      }
                      return writes;
                    }
                    throw new UnsupportedOperationException(method.getName());
                  });
      var operations = new OracleBatchOperations(new OracleStoreContext(em, null));
      UUID batchId = new UUID(1L, 0L);
      assertEquals(
          3,
          switch (operation) {
            case "pause" -> operations.pauseBatchChildren(batchId);
            case "resume" -> operations.resumeBatchChildren(batchId);
            default -> operations.cancelBatchChildren(batchId);
          });
      assertEquals(operation.equals("cancel") ? 1 : 0, metricUpdates.get());
      assertEquals(6, page.get());
      assertEquals(3, cursors.size());
      assertArrayEquals(UuidRawConverter.toBytes(last), (byte[]) cursors.get(0));
      assertArrayEquals(UuidRawConverter.toBytes(next), (byte[]) cursors.get(1));
      assertArrayEquals(UuidRawConverter.toBytes(returned), (byte[]) cursors.get(2));
    }
  }
}
