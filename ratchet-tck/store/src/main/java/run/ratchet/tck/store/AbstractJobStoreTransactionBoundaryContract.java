package run.ratchet.tck.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import jakarta.transaction.Transactional;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import org.junit.jupiter.api.Test;

/**
 * Contract for JPA-backed {@code JobStore} implementations that express their lock and node-liveness
 * transaction boundaries through Jakarta Transactions.
 *
 * <p>Lock acquisition and release, lease renewal, heartbeat upserts, and inactive-node cleanup must
 * commit in a transaction independent of any caller transaction. That way a distributed lock or a
 * liveness write becomes visible to other nodes as soon as the operation returns and is not undone
 * if the caller later rolls back. JPA stores satisfy this with {@code @Transactional(REQUIRES_NEW)}
 * on those methods, and this contract pins the attribute so the SQL stores cannot silently drift
 * apart again (one store once carried the annotation while its sibling did not).
 *
 * <p>Stores that achieve independent commit through single-operation atomicity rather than Jakarta
 * Transactions (for example document stores) are exempt and do not extend this contract.
 */
public abstract class AbstractJobStoreTransactionBoundaryContract {

  /** The concrete {@code JobStore} implementation class whose annotations are under test. */
  protected abstract Class<?> jobStoreImplClass();

  @Test
  void tryLockCommitsInItsOwnTransaction() throws NoSuchMethodException {
    assertRequiresNew("tryLock", String.class, Duration.class, String.class);
  }

  @Test
  void unlockCommitsInItsOwnTransaction() throws NoSuchMethodException {
    assertRequiresNew("unlock", String.class, String.class);
  }

  @Test
  void renewLockCommitsInItsOwnTransaction() throws NoSuchMethodException {
    assertRequiresNew("renewLock", String.class, Duration.class, String.class);
  }

  @Test
  void upsertHeartbeatCommitsInItsOwnTransaction() throws NoSuchMethodException {
    assertRequiresNew("upsertHeartbeat", String.class, Instant.class);
  }

  @Test
  void deleteInactiveNodesSinceCommitsInItsOwnTransaction() throws NoSuchMethodException {
    assertRequiresNew("deleteInactiveNodesSince", Instant.class);
  }

  @Test
  void deleteInactiveNodesByIdsCommitsInItsOwnTransaction() throws NoSuchMethodException {
    assertRequiresNew("deleteInactiveNodesByIds", Collection.class);
  }

  @Test
  void classLevelBoundaryIsRequired() {
    Transactional classLevel = jobStoreImplClass().getAnnotation(Transactional.class);
    assertNotNull(classLevel, "the store impl must carry a class-level @Transactional default");
    assertEquals(
        Transactional.TxType.REQUIRED,
        classLevel.value(),
        "every store method without its own attribute must default to a REQUIRED tx boundary");
  }

  private void assertRequiresNew(String method, Class<?>... parameterTypes)
      throws NoSuchMethodException {
    Transactional tx =
        jobStoreImplClass().getMethod(method, parameterTypes).getAnnotation(Transactional.class);
    assertNotNull(tx, method + " must declare @Transactional so it gets its own transaction");
    assertEquals(
        Transactional.TxType.REQUIRES_NEW,
        tx.value(),
        method + " must commit in a transaction independent of the caller (REQUIRES_NEW)");
  }
}
