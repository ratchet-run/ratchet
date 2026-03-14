package run.ratchet.store.mongodb;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for distributed lock operations.
 *
 * <p>Validates acquire, release, renewal, and expiry-based re-acquisition of MongoDB-backed
 * distributed locks.
 */
class LockStoreIT extends BaseDocumentStoreIT {

  @Test
  void acquireAndRelease() {
    assertTrue(store().tryLock("test-lock", Duration.ofMinutes(5), "node-1"));

    // Same node can't re-acquire a non-expired lock
    assertFalse(store().tryLock("test-lock", Duration.ofMinutes(5), "node-2"));

    // Release and re-acquire by another node
    store().unlock("test-lock", "node-1");
    assertTrue(store().tryLock("test-lock", Duration.ofMinutes(5), "node-2"));
  }

  @Test
  void expiredLock_canBeReacquired() {
    // Acquire with very short TTL
    assertTrue(store().tryLock("expiring-lock", Duration.ofMillis(1), "node-1"));

    // Wait for expiry
    try {
      Thread.sleep(50);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    // Another node should be able to acquire the expired lock
    assertTrue(store().tryLock("expiring-lock", Duration.ofMinutes(5), "node-2"));
  }

  @Test
  void renewLock_extendsExpiry() {
    assertTrue(store().tryLock("renew-lock", Duration.ofMillis(50), "node-1"));

    // Renew before expiry
    assertTrue(store().renewLock("renew-lock", Duration.ofMinutes(5), "node-1"));

    // Another node should still be blocked (lock was renewed)
    assertFalse(store().tryLock("renew-lock", Duration.ofMinutes(5), "node-2"));
  }

  @Test
  void renewLock_failsForWrongOwner() {
    assertTrue(store().tryLock("owner-lock", Duration.ofMinutes(5), "node-1"));

    // node-2 can't renew node-1's lock
    assertFalse(store().renewLock("owner-lock", Duration.ofMinutes(5), "node-2"));
  }

  @Test
  void unlock_onlyAffectsOwner() {
    assertTrue(store().tryLock("owner-unlock", Duration.ofMinutes(5), "node-1"));

    // node-2 can't unlock node-1's lock
    store().unlock("owner-unlock", "node-2");

    // Lock should still be held by node-1
    assertFalse(store().tryLock("owner-unlock", Duration.ofMinutes(5), "node-2"));
  }
}
