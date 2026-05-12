package run.ratchet.store.mysql;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.persistence.EntityManager;
import jakarta.persistence.NoResultException;
import jakarta.persistence.Query;
import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import run.ratchet.store.mysql.converter.UuidByteArrayConverter;

class MysqlAuxiliaryOperationsTest {

  @Test
  void tryAcquirePermit_sameJobExistingPermitIsIdempotent() {
    UUID jobId = UUID.fromString("01900000-0000-7000-8000-000000000001");
    MysqlAuxiliaryOperations operations =
        new MysqlAuxiliaryOperations(entityManagerWithExistingPermit(jobId));

    assertTrue(operations.tryAcquirePermit("res", jobId, "node-1"));
  }

  @Test
  void getPermitRetryDelayRejectsMissingResource() {
    MysqlAuxiliaryOperations operations =
        new MysqlAuxiliaryOperations(new MysqlStoreContext(entityManagerWithNoResult(), null));

    assertThrows(IllegalArgumentException.class, () -> operations.getPermitRetryDelay("missing"));
  }

  private static EntityManager entityManagerWithNoResult() {
    Query query =
        (Query)
            Proxy.newProxyInstance(
                Query.class.getClassLoader(),
                new Class<?>[] {Query.class},
                (proxy, method, args) -> {
                  return switch (method.getName()) {
                    case "setParameter" -> proxy;
                    case "getSingleResult" -> throw new NoResultException();
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

  private static MysqlStoreContext entityManagerWithExistingPermit(UUID expectedJobId) {
    EntityManager em =
        (EntityManager)
            Proxy.newProxyInstance(
                EntityManager.class.getClassLoader(),
                new Class<?>[] {EntityManager.class},
                (proxy, method, args) -> {
                  return switch (method.getName()) {
                    case "createNativeQuery" -> queryReturningExistingPermit(expectedJobId);
                    case "persist" ->
                        throw new AssertionError("existing permit must not insert another row");
                    default -> throw new UnsupportedOperationException(method.getName());
                  };
                });
    return new MysqlStoreContext(em, null);
  }

  private static Query queryReturningExistingPermit(UUID expectedJobId) {
    return (Query)
        Proxy.newProxyInstance(
            Query.class.getClassLoader(),
            new Class<?>[] {Query.class},
            (proxy, method, args) -> {
              return switch (method.getName()) {
                case "setParameter" -> {
                  if ((int) args[0] == 3) {
                    byte[] expected = UuidByteArrayConverter.toBytes(expectedJobId);
                    byte[] actual = (byte[]) args[1];
                    if (!java.util.Arrays.equals(expected, actual)) {
                      throw new AssertionError("job_id parameter should use MySQL UUID bytes");
                    }
                  }
                  yield proxy;
                }
                case "getResultList" -> Collections.singletonList(new Object[] {2, 1, 1});
                default -> throw new UnsupportedOperationException(method.getName());
              };
            });
  }
}
