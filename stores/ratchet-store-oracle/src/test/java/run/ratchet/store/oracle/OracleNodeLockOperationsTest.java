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

import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.junit.jupiter.api.Test;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.store.entity.NodeEntity;

class OracleNodeLockOperationsTest {

  @Test
  void tryLock_returnsFalseFromMutationCountsWithoutOwnerSelect() {
    List<String> sqlStatements = new ArrayList<>();
    OracleNodeLockOperations locks = newLocks(sqlStatements, 0, 0);

    assertFalse(locks.tryLock("held-lock", Duration.ofMinutes(5), "node-B"));

    assertTrue(
        sqlStatements.stream().noneMatch(sql -> sql.toUpperCase(Locale.ROOT).startsWith("SELECT")),
        "tryLock must not perform a post-mutation owner SELECT");
  }

  @Test
  void tryLock_returnsTrueFromSuccessfulMutationWithoutOwnerSelect() {
    List<String> sqlStatements = new ArrayList<>();
    OracleNodeLockOperations locks = newLocks(sqlStatements, 1);

    assertTrue(locks.tryLock("new-lock", Duration.ofMinutes(5), "node-A"));

    assertTrue(
        sqlStatements.stream().noneMatch(sql -> sql.toUpperCase(Locale.ROOT).startsWith("SELECT")),
        "tryLock must not perform a post-mutation owner SELECT");
  }

  @Test
  void tryLock_preservesSubSecondTtl() {
    List<String> sqlStatements = new ArrayList<>();
    List<Object> parameters = new ArrayList<>();
    OracleNodeLockOperations locks = newLocksCapturingParams(sqlStatements, parameters, 1);

    assertTrue(locks.tryLock("short-lock", Duration.ofMillis(500), "node-A"));

    assertTrue(sqlStatements.get(0).contains("NUMTODSINTERVAL(? / 1000000, 'SECOND')"));
    assertEquals(500_000L, parameters.get(1));
  }

  @Test
  void findInactiveNodesSince_usesNativeSchedulerNodeQuery() {
    List<String> sqlStatements = new ArrayList<>();
    NodeEntity inactive = new NodeEntity();
    OracleNodeLockOperations locks = newLocksReturningRows(sqlStatements, List.of(inactive));

    List<NodeEntity> result = locks.findInactiveNodesSince(Instant.parse("2026-05-09T12:00:00Z"));

    assertEquals(List.of(inactive), result);
    assertEquals(
        List.of("SELECT * FROM scheduler_node WHERE heartbeat_ts < ? FETCH FIRST ? ROWS ONLY"),
        sqlStatements);
  }

  @Test
  void deleteInactiveNodesSince_usesNativeSchedulerNodeQuery() {
    List<String> sqlStatements = new ArrayList<>();
    OracleNodeLockOperations locks = newLocks(sqlStatements, 7);

    int deleted = locks.deleteInactiveNodesSince(Instant.parse("2026-05-09T12:00:00Z"));

    assertEquals(7, deleted);
    assertEquals(List.of("DELETE FROM scheduler_node WHERE heartbeat_ts < ?"), sqlStatements);
  }

  @Test
  void renewLock_usesMicrosecondTimestampPrecision() {
    List<String> sqlStatements = new ArrayList<>();
    OracleNodeLockOperations locks = newLocks(sqlStatements, 1);

    assertTrue(locks.renewLock("held-lock", Duration.ofSeconds(30), "node-A"));

    assertTrue(sqlStatements.get(0).contains("SYS_EXTRACT_UTC(SYSTIMESTAMP)"));
    assertTrue(sqlStatements.get(0).contains("NUMTODSINTERVAL(? / 1000000, 'SECOND')"));
  }

  private static OracleNodeLockOperations newLocks(
      List<String> sqlStatements, int... updateCounts) {
    EntityManager em =
        (EntityManager)
            Proxy.newProxyInstance(
                EntityManager.class.getClassLoader(),
                new Class<?>[] {EntityManager.class},
                (proxy, method, args) -> {
                  if ("createNativeQuery".equals(method.getName()) && args != null) {
                    String sql = (String) args[0];
                    sqlStatements.add(sql.stripLeading());
                    int updateCount = updateCounts[sqlStatements.size() - 1];
                    return queryReturning(updateCount);
                  }
                  throw new UnsupportedOperationException(method.getName());
                });
    return new OracleNodeLockOperations(new OracleStoreContext(em, noopMetrics()));
  }

  private static OracleNodeLockOperations newLocksCapturingParams(
      List<String> sqlStatements, List<Object> parameters, int... updateCounts) {
    EntityManager em =
        (EntityManager)
            Proxy.newProxyInstance(
                EntityManager.class.getClassLoader(),
                new Class<?>[] {EntityManager.class},
                (proxy, method, args) -> {
                  if ("createNativeQuery".equals(method.getName()) && args != null) {
                    String sql = (String) args[0];
                    sqlStatements.add(sql.stripLeading());
                    int updateCount = updateCounts[sqlStatements.size() - 1];
                    return queryReturning(updateCount, parameters);
                  }
                  throw new UnsupportedOperationException(method.getName());
                });
    return new OracleNodeLockOperations(new OracleStoreContext(em, noopMetrics()));
  }

  private static OracleNodeLockOperations newLocksReturningRows(
      List<String> sqlStatements, List<NodeEntity> rows) {
    EntityManager em =
        (EntityManager)
            Proxy.newProxyInstance(
                EntityManager.class.getClassLoader(),
                new Class<?>[] {EntityManager.class},
                (proxy, method, args) -> {
                  if ("createNativeQuery".equals(method.getName()) && args != null) {
                    sqlStatements.add(((String) args[0]).stripLeading());
                    assertEquals(NodeEntity.class, args[1]);
                    return queryReturningRows(rows);
                  }
                  throw new UnsupportedOperationException(method.getName());
                });
    return new OracleNodeLockOperations(new OracleStoreContext(em, noopMetrics()));
  }

  private static Query queryReturning(int updateCount) {
    return queryReturning(updateCount, null);
  }

  private static Query queryReturning(int updateCount, List<Object> parameters) {
    return (Query)
        Proxy.newProxyInstance(
            Query.class.getClassLoader(),
            new Class<?>[] {Query.class},
            (proxy, method, args) -> {
              return switch (method.getName()) {
                case "setParameter" -> {
                  if (parameters != null) {
                    parameters.add(args[1]);
                  }
                  yield proxy;
                }
                case "executeUpdate" -> updateCount;
                case "getSingleResult" ->
                    throw new AssertionError("tryLock must not SELECT owner_node after mutation");
                default -> throw new UnsupportedOperationException(method.getName());
              };
            });
  }

  private static Query queryReturningRows(List<NodeEntity> rows) {
    return (Query)
        Proxy.newProxyInstance(
            Query.class.getClassLoader(),
            new Class<?>[] {Query.class},
            (proxy, method, args) -> {
              return switch (method.getName()) {
                case "setParameter" -> proxy;
                case "getResultList" -> rows;
                default -> throw new UnsupportedOperationException(method.getName());
              };
            });
  }

  private static MetricsCollector noopMetrics() {
    return (MetricsCollector)
        Proxy.newProxyInstance(
            MetricsCollector.class.getClassLoader(),
            new Class<?>[] {MetricsCollector.class},
            (proxy, method, args) -> null);
  }
}
