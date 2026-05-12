package run.ratchet.store.postgresql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.persistence.EntityManager;
import jakarta.persistence.NoResultException;
import jakarta.persistence.Query;
import java.lang.reflect.Proxy;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class PostgresqlBatchOperationsTest {

  private static final UUID BATCH_ID = UUID.fromString("019ae3d1-3f82-7e18-9f09-a9f000000007");

  @Test
  void mapIncrementResultRejectsScalarResult() {
    IllegalStateException thrown =
        assertThrows(
            IllegalStateException.class,
            () -> PostgresqlBatchOperations.mapIncrementResult(BATCH_ID, 1, ignored -> null));

    assertTrue(thrown.getMessage().contains("Object[]"));
  }

  @Test
  void mapIncrementResultRejectsShortRow() {
    IllegalStateException thrown =
        assertThrows(
            IllegalStateException.class,
            () ->
                PostgresqlBatchOperations.mapIncrementResult(
                    BATCH_ID, new Object[] {1, 0, 2}, ignored -> null));

    assertTrue(thrown.getMessage().contains("at least 4 columns"));
  }

  @Test
  void mapIncrementResultRejectsNonNumericCounters() {
    IllegalStateException thrown =
        assertThrows(
            IllegalStateException.class,
            () ->
                PostgresqlBatchOperations.mapIncrementResult(
                    BATCH_ID, new Object[] {"1", 0, 2, null}, ignored -> null));

    assertTrue(thrown.getMessage().contains("completed_items"));
    assertTrue(thrown.getMessage().contains("numeric"));
  }

  @Test
  void mapIncrementResultMapsValidRow() {
    var progress =
        PostgresqlBatchOperations.mapIncrementResult(
            BATCH_ID, new Object[] {1L, 2L, 5L, null}, ignored -> null);

    assertEquals(BATCH_ID, progress.batchId());
    assertEquals(5, progress.totalItems());
    assertEquals(1, progress.completedItems());
    assertEquals(2, progress.failedItems());
  }

  @Test
  void incrementAtomicThrowsClearErrorWhenBatchIsMissing() {
    PostgresqlBatchOperations operations =
        new PostgresqlBatchOperations(new PostgresqlStoreContext(entityManagerThrowingNoResult()));

    IllegalStateException thrown =
        assertThrows(
            IllegalStateException.class, () -> operations.incrementCompletedAtomic(BATCH_ID));

    assertTrue(thrown.getMessage().contains("Batch not found"));
    assertTrue(thrown.getMessage().contains(BATCH_ID.toString()));
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
