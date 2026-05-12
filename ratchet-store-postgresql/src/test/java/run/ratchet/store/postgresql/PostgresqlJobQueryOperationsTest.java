package run.ratchet.store.postgresql;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobFilter;
import run.ratchet.api.JobQuerySortField;
import run.ratchet.store.query.JobQueryCursor;

class PostgresqlJobQueryOperationsTest {

  @Test
  void searchJobsIgnoresCursorWithMalformedInstantSortValue() {
    String cursor =
        new JobQueryCursor(
                JobQuerySortField.CREATED_AT,
                "not-an-instant",
                UUID.fromString("019ae3d1-3f82-7e18-9f09-a9f000000465"))
            .encode();
    PostgresqlStoreContext ctx = new PostgresqlStoreContext(emptyResultEntityManager());
    PostgresqlJobQueryOperations operations =
        new PostgresqlJobQueryOperations(ctx, new PostgresqlTagOperations(ctx));

    assertDoesNotThrow(
        () -> operations.searchJobs(JobFilter.builder().cursor(cursor).build(), 10, 0));
  }

  private static EntityManager emptyResultEntityManager() {
    return (EntityManager)
        Proxy.newProxyInstance(
            EntityManager.class.getClassLoader(),
            new Class<?>[] {EntityManager.class},
            (proxy, method, args) -> {
              if ("createNativeQuery".equals(method.getName())) {
                return emptyResultQuery();
              }
              throw new UnsupportedOperationException(method.getName());
            });
  }

  private static Query emptyResultQuery() {
    return (Query)
        Proxy.newProxyInstance(
            Query.class.getClassLoader(),
            new Class<?>[] {Query.class},
            (proxy, method, args) -> {
              if ("getResultList".equals(method.getName())) {
                return List.of();
              }
              return switch (method.getName()) {
                case "setParameter", "setFirstResult", "setMaxResults" -> proxy;
                default -> throw new UnsupportedOperationException(method.getName());
              };
            });
  }
}
