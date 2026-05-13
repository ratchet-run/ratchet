package run.ratchet.tck.store;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.tck.util.ConcurrentTestRunner;

/** Base contract tests for {@code LockStore}. */
public abstract class AbstractLockStoreContract implements JobStoreContractFixture {

  @BeforeEach
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
  void tryLock_rejectsNullParameters() {
    Duration ttl = Duration.ofMinutes(5);

    assertThrows(NullPointerException.class, () -> store().tryLock(null, ttl, "node-A"));
    assertThrows(NullPointerException.class, () -> store().tryLock("lock1", null, "node-A"));
    assertThrows(NullPointerException.class, () -> store().tryLock("lock1", ttl, null));
  }

  @Test
  public void tryLock_rejectsNonPositiveTtl() {
    assertThrows(
        IllegalArgumentException.class, () -> store().tryLock("lock1", Duration.ZERO, "node-A"));
    assertThrows(
        IllegalArgumentException.class,
        () -> store().tryLock("lock1", Duration.ofMillis(-1), "node-A"));
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
  void renewLock_rejectsNullParameters() {
    Duration extension = Duration.ofMinutes(5);

    assertThrows(NullPointerException.class, () -> store().renewLock(null, extension, "node-A"));
    assertThrows(NullPointerException.class, () -> store().renewLock("lock1", null, "node-A"));
    assertThrows(NullPointerException.class, () -> store().renewLock("lock1", extension, null));
  }

  @Test
  public void renewLock_rejectsNonPositiveExtension() {
    assertThrows(
        IllegalArgumentException.class, () -> store().renewLock("lock1", Duration.ZERO, "node-A"));
    assertThrows(
        IllegalArgumentException.class,
        () -> store().renewLock("lock1", Duration.ofMillis(-1), "node-A"));
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
  void unlock_rejectsNullParameters() {
    assertThrows(NullPointerException.class, () -> store().unlock(null, "node-A"));
    assertThrows(NullPointerException.class, () -> store().unlock("lock1", null));
  }

  @Test
  void unlock_byNonOwner_doesNotReleaseHeldLock() {
    store().tryLock("lock1", Duration.ofMinutes(5), "node-A");

    store().unlock("lock1", "node-B");

    assertFalse(
        store().tryLock("lock1", Duration.ofMinutes(5), "node-B"),
        "Lock held by node-A must survive an unlock attempt by node-B");
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
    // Use a 1s TTL with a 2s sleep (2x margin). The previous 100ms / 250ms (2.5x margin) was
    // tight enough to flake under CI GC pauses or scheduler jitter on shared hardware.
    store().tryLock("ttl-lock", Duration.ofSeconds(1), "node-A");

    Thread.sleep(2000);

    assertTrue(
        store().tryLock("ttl-lock", Duration.ofMinutes(5), "node-B"),
        "Lock should be reacquirable after TTL expires");
  }
}
