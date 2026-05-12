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
