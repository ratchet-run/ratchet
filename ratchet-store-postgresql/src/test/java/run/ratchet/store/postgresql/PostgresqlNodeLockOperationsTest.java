package run.ratchet.store.postgresql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import java.lang.reflect.Proxy;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class PostgresqlNodeLockOperationsTest {

  @Test
  void upsertHeartbeatRejectsNullArgumentsBeforeSqlExecution() {
    PostgresqlNodeLockOperations operations =
        new PostgresqlNodeLockOperations(new PostgresqlStoreContext(noopEntityManager()));

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
