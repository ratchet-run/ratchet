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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static run.ratchet.store.entity.JobExecutionEntity.ORPHAN_RECOVERY_ERROR_MESSAGE;

import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import java.lang.reflect.Proxy;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class OracleJobDeleteOperationsTest {

  @Test
  void resetOrphanJobsIncludesNullPickedByRows() {
    List<String> sql = new ArrayList<>();
    OracleStoreContext ctx = new OracleStoreContext(entityManagerCapturingSql(sql), null);
    OracleJobDeleteOperations deletes =
        new OracleJobDeleteOperations(ctx, new OracleBusinessKeyReservations(ctx));

    assertEquals(3, deletes.resetOrphanJobs(Duration.ofSeconds(30), 3, 0).reset());
    assertEquals(2, sql.size());
    assertTrue(sql.get(0).startsWith("UPDATE scheduler_job_execution"));
    assertTrue(sql.get(0).contains("picked_by IS NULL OR picked_by NOT IN"));
    assertTrue(sql.get(0).contains("WHERE status = 'RUNNING' AND job_id IN"));
    assertFalse(sql.get(0).contains("crash_count"));
    assertTrue(sql.get(1).startsWith("UPDATE scheduler_job_queue"));
    assertTrue(sql.get(1).contains("picked_by IS NULL OR picked_by NOT IN"));
  }

  private static EntityManager entityManagerCapturingSql(List<String> sql) {
    return (EntityManager)
        Proxy.newProxyInstance(
            EntityManager.class.getClassLoader(),
            new Class<?>[] {EntityManager.class},
            (proxy, method, args) -> {
              if ("createNativeQuery".equals(method.getName())) {
                String statement = ((String) args[0]).replaceAll("\\s+", " ");
                return queryReturningUpdateCount(statement, sql);
              }
              if ("clear".equals(method.getName())) {
                return null;
              }
              throw new UnsupportedOperationException(method.getName());
            });
  }

  private static Query queryReturningUpdateCount(String statement, List<String> sql) {
    Map<Integer, Object> parameters = new HashMap<>();
    return (Query)
        Proxy.newProxyInstance(
            Query.class.getClassLoader(),
            new Class<?>[] {Query.class},
            (proxy, method, args) -> {
              return switch (method.getName()) {
                case "setParameter" -> {
                  parameters.put((Integer) args[0], args[1]);
                  yield proxy;
                }
                case "executeUpdate" -> {
                  sql.add(statement);
                  assertEquals(3, parameters.size());
                  if (statement.startsWith("UPDATE scheduler_job_execution")) {
                    assertEquals(ORPHAN_RECOVERY_ERROR_MESSAGE, parameters.get(1));
                    assertTrue(parameters.get(2) instanceof Timestamp);
                    assertEquals(parameters.get(2), parameters.get(3));
                    yield 7; // The close count must not become the recovery reset count.
                  }
                  assertTrue(parameters.get(1) instanceof Timestamp);
                  assertEquals(parameters.get(1), parameters.get(2));
                  assertEquals(3, parameters.get(3));
                  yield 3;
                }
                default -> throw new UnsupportedOperationException(method.getName());
              };
            });
  }
}
