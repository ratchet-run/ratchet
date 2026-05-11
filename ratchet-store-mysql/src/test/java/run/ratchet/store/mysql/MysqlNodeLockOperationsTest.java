package run.ratchet.store.mysql;

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

class MysqlNodeLockOperationsTest {

  @Test
  void tryLock_returnsFalseFromMutationCountsWithoutOwnerSelect() {
    List<String> sqlStatements = new ArrayList<>();
    MysqlNodeLockOperations locks = newLocks(sqlStatements, 0, 0);

    assertFalse(locks.tryLock("held-lock", Duration.ofMinutes(5), "node-B"));

    assertTrue(
        sqlStatements.stream().noneMatch(sql -> sql.toUpperCase(Locale.ROOT).startsWith("SELECT")),
        "tryLock must not perform a post-mutation owner SELECT");
  }

  @Test
  void tryLock_returnsTrueFromSuccessfulMutationWithoutOwnerSelect() {
    List<String> sqlStatements = new ArrayList<>();
    MysqlNodeLockOperations locks = newLocks(sqlStatements, 1);

    assertTrue(locks.tryLock("new-lock", Duration.ofMinutes(5), "node-A"));

    assertTrue(
        sqlStatements.stream().noneMatch(sql -> sql.toUpperCase(Locale.ROOT).startsWith("SELECT")),
        "tryLock must not perform a post-mutation owner SELECT");
  }

  @Test
  void findInactiveNodesSince_usesNativeSchedulerNodeQuery() {
    List<String> sqlStatements = new ArrayList<>();
    NodeEntity inactive = new NodeEntity();
    MysqlNodeLockOperations locks = newLocksReturningRows(sqlStatements, List.of(inactive));

    List<NodeEntity> result = locks.findInactiveNodesSince(Instant.parse("2026-05-09T12:00:00Z"));

    assertEquals(List.of(inactive), result);
    assertEquals(List.of("SELECT * FROM scheduler_node WHERE heartbeat_ts < ?"), sqlStatements);
  }

  @Test
  void deleteInactiveNodesSince_usesNativeSchedulerNodeQuery() {
    List<String> sqlStatements = new ArrayList<>();
    MysqlNodeLockOperations locks = newLocks(sqlStatements, 7);

    int deleted = locks.deleteInactiveNodesSince(Instant.parse("2026-05-09T12:00:00Z"));

    assertEquals(7, deleted);
    assertEquals(List.of("DELETE FROM scheduler_node WHERE heartbeat_ts < ?"), sqlStatements);
  }

  @Test
  void renewLock_usesMicrosecondTimestampPrecision() {
    List<String> sqlStatements = new ArrayList<>();
    MysqlNodeLockOperations locks = newLocks(sqlStatements, 1);

    assertTrue(locks.renewLock("held-lock", Duration.ofSeconds(30), "node-A"));

    assertTrue(sqlStatements.get(0).contains("NOW(6)"));
    assertFalse(sqlStatements.get(0).contains("NOW(3)"));
  }

  private static MysqlNodeLockOperations newLocks(List<String> sqlStatements, int... updateCounts) {
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
    return new MysqlNodeLockOperations(new MysqlStoreContext(em, noopMetrics()));
  }

  private static MysqlNodeLockOperations newLocksReturningRows(
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
    return new MysqlNodeLockOperations(new MysqlStoreContext(em, noopMetrics()));
  }

  private static Query queryReturning(int updateCount) {
    return (Query)
        Proxy.newProxyInstance(
            Query.class.getClassLoader(),
            new Class<?>[] {Query.class},
            (proxy, method, args) -> {
              return switch (method.getName()) {
                case "setParameter" -> proxy;
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
