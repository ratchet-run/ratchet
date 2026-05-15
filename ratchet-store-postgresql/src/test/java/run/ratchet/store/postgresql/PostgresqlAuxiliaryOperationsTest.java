package run.ratchet.store.postgresql;

import static org.junit.jupiter.api.Assertions.assertEquals;

import jakarta.persistence.EntityManager;
import jakarta.persistence.NoResultException;
import jakarta.persistence.Query;
import java.lang.reflect.Proxy;
import org.junit.jupiter.api.Test;
import run.ratchet.store.entity.ResourceLimitEntity;

class PostgresqlAuxiliaryOperationsTest {

  @Test
  void getPermitRetryDelayUsesDefaultForMissingResource() {
    PostgresqlAuxiliaryOperations operations =
        new PostgresqlAuxiliaryOperations(
            new PostgresqlStoreContext(entityManagerThrowingNoResult()));

    assertEquals(
        ResourceLimitEntity.DEFAULT_RETRY_DELAY_MS,
        operations.getPermitRetryDelay("missing-resource"));
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
