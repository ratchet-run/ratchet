package run.ratchet.store.mysql;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.junit.jupiter.api.Test;
import run.ratchet.spi.MetricsCollector;

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

  private static MetricsCollector noopMetrics() {
    return (MetricsCollector)
        Proxy.newProxyInstance(
            MetricsCollector.class.getClassLoader(),
            new Class<?>[] {MetricsCollector.class},
            (proxy, method, args) -> null);
  }
}
