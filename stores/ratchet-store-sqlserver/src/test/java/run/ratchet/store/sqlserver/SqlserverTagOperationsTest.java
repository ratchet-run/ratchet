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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

class SqlserverTagOperationsTest {

  @Test
  void insertTagsChunksLargeInputsIntoBoundedStatements() {
    NativeSqlRecorder recorder = new NativeSqlRecorder();
    SqlserverTagOperations operations =
        new SqlserverTagOperations(new SqlserverStoreContext(recorder.entityManager()));
    List<String> tags = IntStream.range(0, 600).mapToObj(i -> "tag-" + i).toList();

    operations.insertTags(UUID.fromString("019ae3d1-3f82-7e18-9f09-a9f000000465"), tags);

    assertEquals(3, recorder.sql.size(), "Large tag lists should be inserted in chunks");
    assertEquals(List.of(500, 500, 200), recorder.parameterCounts);
  }

  @Test
  void countJobsByExecutionNodeForTagUsesAttemptOrderForLatestExecution() {
    NativeSqlRecorder recorder = new NativeSqlRecorder();
    SqlserverTagOperations operations =
        new SqlserverTagOperations(new SqlserverStoreContext(recorder.entityManager()));

    operations.countJobsByExecutionNodeForTag("run-tag");

    String sql = recorder.lastSql();
    assertFalse(sql.contains("MAX(e2.id::text)"), "Latest execution must not depend on UUID text");
    assertTrue(sql.contains("ORDER BY e2.attempt DESC"), "Attempt number defines execution order");
    assertTrue(sql.contains("TOP 1"), "Latest execution lookup should return one row");
  }

  private static final class NativeSqlRecorder {
    private final List<String> sql = new ArrayList<>();
    private final List<Integer> parameterCounts = new ArrayList<>();

    EntityManager entityManager() {
      return (EntityManager)
          Proxy.newProxyInstance(
              EntityManager.class.getClassLoader(),
              new Class<?>[] {EntityManager.class},
              (proxy, method, args) -> {
                if ("createNativeQuery".equals(method.getName())) {
                  sql.add((String) args[0]);
                  return query();
                }
                throw new UnsupportedOperationException(method.getName());
              });
    }

    String lastSql() {
      assertFalse(sql.isEmpty(), "Expected at least one native query");
      return sql.get(sql.size() - 1);
    }

    private Query query() {
      return (Query)
          Proxy.newProxyInstance(
              Query.class.getClassLoader(),
              new Class<?>[] {Query.class},
              new java.lang.reflect.InvocationHandler() {
                private int parameterCount;

                @Override
                public Object invoke(Object proxy, java.lang.reflect.Method method, Object[] args) {
                  return switch (method.getName()) {
                    case "setParameter" -> {
                      parameterCount = Math.max(parameterCount, ((Number) args[0]).intValue());
                      yield proxy;
                    }
                    case "executeUpdate" -> {
                      parameterCounts.add(parameterCount);
                      yield 1;
                    }
                    case "getResultList" -> List.of();
                    default -> throw new UnsupportedOperationException(method.getName());
                  };
                }
              });
    }
  }
}
