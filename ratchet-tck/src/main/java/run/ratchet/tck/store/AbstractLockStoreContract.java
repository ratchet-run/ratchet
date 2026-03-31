package run.ratchet.tck.store;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** Base contract tests for {@code LockStore}. */
public abstract class AbstractLockStoreContract implements JobStoreContractFixture {

  @AfterEach
  void cleanupLockFixture() {
    cleanupStore();
  }

  @Test
  void tryLock_acquiresAndReleasesLock() {
    assertTrue(
        store().tryLock("lock1", Duration.ofMinutes(5), "node-A"), "First tryLock should succeed");

    store().unlock("lock1", "node-A");

    assertTrue(
        store().tryLock("lock1", Duration.ofMinutes(5), "node-A"),
        "tryLock should succeed after unlock");
  }

  @Test
  void tryLock_failsWhenAlreadyHeld() {
    assertTrue(
        store().tryLock("lock1", Duration.ofMinutes(5), "node-A"), "First tryLock should succeed");

    assertFalse(
        store().tryLock("lock1", Duration.ofMinutes(5), "node-B"),
        "Second tryLock by different node should fail");
  }

  @Test
  void renewLock_extendsExistingLock() {
    store().tryLock("lock1", Duration.ofMinutes(1), "node-A");

    boolean renewed = store().renewLock("lock1", Duration.ofMinutes(5), "node-A");

    assertTrue(renewed, "renewLock should succeed for the lock owner");
  }
}
