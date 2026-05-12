package run.ratchet.store.postgresql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import jakarta.transaction.Transactional;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class PostgresqlJobStoreImplTransactionTest {

  @Test
  void unlockUsesIndependentTransactionBoundary() throws NoSuchMethodException {
    Transactional transactional =
        PostgresqlJobStoreImpl.class
            .getMethod("unlock", String.class, String.class)
            .getAnnotation(Transactional.class);

    assertNotNull(transactional);
    assertEquals(Transactional.TxType.REQUIRES_NEW, transactional.value());
  }

  @Test
  void tryLockKeepsClassLevelRequiredBoundary() throws NoSuchMethodException {
    Transactional transactional =
        PostgresqlJobStoreImpl.class
            .getMethod("tryLock", String.class, Duration.class, String.class)
            .getAnnotation(Transactional.class);

    assertNull(transactional);
  }
}
