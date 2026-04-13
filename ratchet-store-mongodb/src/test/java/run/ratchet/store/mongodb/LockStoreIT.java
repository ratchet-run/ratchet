package run.ratchet.store.mongodb;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class LockStoreIT extends BaseDocumentStoreIT {

  @Test
  void acquireAndRelease() {
    assertTrue(store().tryLock("test-lock", Duration.ofMinutes(5), "node-1"));

    assertFalse(store().tryLock("test-lock", Duration.ofMinutes(5), "node-2"));

    store().unlock("test-lock", "node-1");
    assertTrue(store().tryLock("test-lock", Duration.ofMinutes(5), "node-2"));
  }

  @Test
  void expiredLock_canBeReacquired() {
    assertTrue(store().tryLock("expiring-lock", Duration.ofMillis(1), "node-1"));

    try {
      Thread.sleep(50);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    assertTrue(store().tryLock("expiring-lock", Duration.ofMinutes(5), "node-2"));
  }

  @Test
  void renewLock_extendsExpiry() {
    assertTrue(store().tryLock("renew-lock", Duration.ofMillis(50), "node-1"));

    assertTrue(store().renewLock("renew-lock", Duration.ofMinutes(5), "node-1"));

    assertFalse(store().tryLock("renew-lock", Duration.ofMinutes(5), "node-2"));
  }

  @Test
  void renewLock_failsForWrongOwner() {
    assertTrue(store().tryLock("owner-lock", Duration.ofMinutes(5), "node-1"));

    assertFalse(store().renewLock("owner-lock", Duration.ofMinutes(5), "node-2"));
  }

  @Test
  void unlock_onlyAffectsOwner() {
    assertTrue(store().tryLock("owner-unlock", Duration.ofMinutes(5), "node-1"));

    store().unlock("owner-unlock", "node-2");

    assertFalse(store().tryLock("owner-unlock", Duration.ofMinutes(5), "node-2"));
  }
}
