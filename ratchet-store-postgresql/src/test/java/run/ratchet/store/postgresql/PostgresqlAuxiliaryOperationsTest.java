package run.ratchet.store.postgresql;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.persistence.EntityManager;
import jakarta.persistence.NoResultException;
import jakarta.persistence.Query;
import java.lang.reflect.Proxy;
import org.junit.jupiter.api.Test;

class PostgresqlAuxiliaryOperationsTest {

  @Test
  void getPermitRetryDelayRejectsUnknownResource() {
    PostgresqlAuxiliaryOperations operations =
        new PostgresqlAuxiliaryOperations(
            new PostgresqlStoreContext(entityManagerThrowingNoResult()));

    IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class,
            () -> operations.getPermitRetryDelay("missing-resource"));

    assertTrue(thrown.getMessage().contains("Resource is not configured"));
    assertInstanceOf(NoResultException.class, thrown.getCause());
  }

  private static EntityManager entityManagerThrowingNoResult() {
    Query query =
        (Query)
            Proxy.newProxyInstance(
                Query.class.getClassLoader(),
                new Class<?>[] {Query.class},
                (proxy, method, args) -> {
                  return switch (method.getName()) {
                    case "setParameter" -> proxy;
                    case "getSingleResult" -> throw new NoResultException("missing");
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
