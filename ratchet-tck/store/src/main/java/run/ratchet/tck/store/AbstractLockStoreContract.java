package run.ratchet.tck.store;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import run.ratchet.tck.util.ConcurrentTestRunner;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
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

  @Test
  void unlock_releasesForOtherNode() {
    store().tryLock("lock1", Duration.ofMinutes(5), "node-A");
    store().unlock("lock1", "node-A");

    assertTrue(
        store().tryLock("lock1", Duration.ofMinutes(5), "node-B"),
        "Node-B should acquire the lock after Node-A unlocks");
  }

  @Test
  void renewLock_byDifferentNode_fails() {
    store().tryLock("lock1", Duration.ofMinutes(5), "node-A");

    boolean renewed = store().renewLock("lock1", Duration.ofMinutes(5), "node-B");

    assertFalse(renewed, "renewLock by a non-owning node should fail");
  }

  @Test
  void renewLock_nonExistent_returnsFalse() {
    boolean renewed = store().renewLock("never-acquired", Duration.ofMinutes(5), "node-A");

    assertFalse(renewed, "renewLock on a never-acquired lock should return false");
  }

  @Test
  void unlock_nonHeldLock_isNoOp() {
    store().unlock("never-acquired", "node-A");
  }

  @Test
  void tryLock_concurrent_atMostOneSucceeds() {
    AtomicInteger successCount = new AtomicInteger();

    ConcurrentTestRunner.runAll(
        Duration.ofSeconds(10),
        () -> {
          if (store().tryLock("race-lock", Duration.ofMinutes(5), "node-A")) {
            successCount.incrementAndGet();
          }
        },
        () -> {
          if (store().tryLock("race-lock", Duration.ofMinutes(5), "node-B")) {
            successCount.incrementAndGet();
          }
        },
        () -> {
          if (store().tryLock("race-lock", Duration.ofMinutes(5), "node-C")) {
            successCount.incrementAndGet();
          }
        });

    assertTrue(
        successCount.get() <= 1,
        "at most one thread should acquire the lock; got " + successCount.get());
  }

  @Test
  void tryLock_expiredLock_isReacquirable() throws InterruptedException {
    store().tryLock("ttl-lock", Duration.ofMillis(100), "node-A");

    Thread.sleep(250);

    assertTrue(
        store().tryLock("ttl-lock", Duration.ofMinutes(5), "node-B"),
        "Lock should be reacquirable after TTL expires");
  }
}
