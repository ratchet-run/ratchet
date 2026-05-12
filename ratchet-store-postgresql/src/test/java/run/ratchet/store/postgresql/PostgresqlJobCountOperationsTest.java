package run.ratchet.store.postgresql;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.persistence.EntityManager;
import java.lang.reflect.Proxy;
import org.junit.jupiter.api.Test;

class PostgresqlJobCountOperationsTest {

  @Test
  void queueWaitPercentileRejectsOutOfRangeValueBeforeQuerying() {
    PostgresqlJobCountOperations operations =
        new PostgresqlJobCountOperations(new PostgresqlStoreContext(entityManagerThatMustNotRun()));

    IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class, () -> operations.getQueueWaitTimePercentile(1.5));

    assertTrue(thrown.getMessage().contains("[0.0, 1.0]"));
  }

  private static EntityManager entityManagerThatMustNotRun() {
    return (EntityManager)
        Proxy.newProxyInstance(
            EntityManager.class.getClassLoader(),
            new Class<?>[] {EntityManager.class},
            (proxy, method, args) -> {
              throw new AssertionError("EntityManager should not be called");
            });
  }
}
