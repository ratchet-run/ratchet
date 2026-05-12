package run.ratchet.store.postgresql;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class PostgresqlJobReadOperationsTest {

  private static final UUID JOB_ID = UUID.fromString("019ae3d1-3f82-7e18-9f09-a9f000000469");

  @Test
  void getJobStatusRejectsRowsWithNoEffectiveStatus() {
    PostgresqlStoreContext ctx =
        new PostgresqlStoreContext(
            entityManagerReturningRows(Collections.singletonList(new Object[] {null, null, null})));
    PostgresqlJobReadOperations operations =
        new PostgresqlJobReadOperations(ctx, new PostgresqlTagOperations(ctx));

    IllegalStateException thrown =
        assertThrows(IllegalStateException.class, () -> operations.getJobStatus(JOB_ID));

    assertTrue(thrown.getMessage().contains("no live, recurring, or terminal status"));
  }

  private static EntityManager entityManagerReturningRows(List<?> rows) {
    Query query =
        (Query)
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
    return (EntityManager)
        Proxy.newProxyInstance(
            EntityManager.class.getClassLoader(),
            new Class<?>[] {EntityManager.class},
            (proxy, method, args) -> {
              if ("createNativeQuery".equals(method.getName())) {
                return query;
              }
              throw new UnsupportedOperationException(method.getName());
            });
  }
}
