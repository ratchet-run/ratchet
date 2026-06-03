/*
 * Copyright 2026 Ratchet Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
        lockStore().tryLock("lock1", Duration.ofMinutes(5), "node-A"),
        "First tryLock should succeed");

    lockStore().unlock("lock1", "node-A");

    assertTrue(
        lockStore().tryLock("lock1", Duration.ofMinutes(5), "node-A"),
        "tryLock should succeed after unlock");
  }

  @Test
  void tryLock_failsWhenAlreadyHeld() {
    assertTrue(
        lockStore().tryLock("lock1", Duration.ofMinutes(5), "node-A"),
        "First tryLock should succeed");

    assertFalse(
        lockStore().tryLock("lock1", Duration.ofMinutes(5), "node-B"),
        "Second tryLock by different node should fail");
  }

  @Test
  void tryLock_sameOwnerLiveLock_reacquiresIdempotently() {
    assertTrue(
        lockStore().tryLock("same-owner-lock", Duration.ofMinutes(5), "node-A"),
        "First tryLock should succeed");

    assertTrue(
        lockStore().tryLock("same-owner-lock", Duration.ofMinutes(5), "node-A"),
        "Same owner should be able to refresh a live lock");
  }

  @Test
  void tryLock_subSecondTtlRemainsLiveUntilExpiry() throws InterruptedException {
    assertTrue(
        lockStore().tryLock("subsecond-lock", Duration.ofMillis(500), "node-A"),
        "Sub-second tryLock should succeed");

    assertFalse(
        lockStore().tryLock("subsecond-lock", Duration.ofMinutes(5), "node-B"),
        "Sub-second TTL must not be rounded down to an immediately expired lease");

    Thread.sleep(800);

    assertTrue(
        lockStore().tryLock("subsecond-lock", Duration.ofMinutes(5), "node-B"),
        "Lock should be reacquirable after the sub-second TTL expires");
  }

  @Test
  void tryLock_rejectsNullParameters() {
    Duration ttl = Duration.ofMinutes(5);

    assertThrows(NullPointerException.class, () -> lockStore().tryLock(null, ttl, "node-A"));
    assertThrows(NullPointerException.class, () -> lockStore().tryLock("lock1", null, "node-A"));
    assertThrows(NullPointerException.class, () -> lockStore().tryLock("lock1", ttl, null));
  }

  @Test
  public void tryLock_rejectsNonPositiveTtl() {
    assertThrows(
        IllegalArgumentException.class,
        () -> lockStore().tryLock("lock1", Duration.ZERO, "node-A"));
    assertThrows(
        IllegalArgumentException.class,
        () -> lockStore().tryLock("lock1", Duration.ofMillis(-1), "node-A"));
  }

  @Test
  void renewLock_extendsExistingLock() {
    lockStore().tryLock("lock1", Duration.ofMinutes(1), "node-A");

    boolean renewed = lockStore().renewLock("lock1", Duration.ofMinutes(5), "node-A");

    assertTrue(renewed, "renewLock should succeed for the lock owner");
  }

  @Test
  void unlock_releasesForOtherNode() {
    lockStore().tryLock("lock1", Duration.ofMinutes(5), "node-A");
    lockStore().unlock("lock1", "node-A");

    assertTrue(
        lockStore().tryLock("lock1", Duration.ofMinutes(5), "node-B"),
        "Node-B should acquire the lock after Node-A unlocks");
  }

  @Test
  void renewLock_byDifferentNode_fails() {
    lockStore().tryLock("lock1", Duration.ofMinutes(5), "node-A");

    boolean renewed = lockStore().renewLock("lock1", Duration.ofMinutes(5), "node-B");

    assertFalse(renewed, "renewLock by a non-owning node should fail");
  }

  @Test
  void renewLock_rejectsNullParameters() {
    Duration extension = Duration.ofMinutes(5);

    assertThrows(
        NullPointerException.class, () -> lockStore().renewLock(null, extension, "node-A"));
    assertThrows(NullPointerException.class, () -> lockStore().renewLock("lock1", null, "node-A"));
    assertThrows(NullPointerException.class, () -> lockStore().renewLock("lock1", extension, null));
  }

  @Test
  public void renewLock_rejectsNonPositiveExtension() {
    assertThrows(
        IllegalArgumentException.class,
        () -> lockStore().renewLock("lock1", Duration.ZERO, "node-A"));
    assertThrows(
        IllegalArgumentException.class,
        () -> lockStore().renewLock("lock1", Duration.ofMillis(-1), "node-A"));
  }

  @Test
  void renewLock_nonExistent_returnsFalse() {
    boolean renewed = lockStore().renewLock("never-acquired", Duration.ofMinutes(5), "node-A");

    assertFalse(renewed, "renewLock on a never-acquired lock should return false");
  }

  @Test
  void unlock_nonHeldLock_isNoOp() {
    lockStore().unlock("never-acquired", "node-A");
  }

  @Test
  void unlock_rejectsNullParameters() {
    assertThrows(NullPointerException.class, () -> lockStore().unlock(null, "node-A"));
    assertThrows(NullPointerException.class, () -> lockStore().unlock("lock1", null));
  }

  @Test
  void unlock_byNonOwner_doesNotReleaseHeldLock() {
    lockStore().tryLock("lock1", Duration.ofMinutes(5), "node-A");

    lockStore().unlock("lock1", "node-B");

    assertFalse(
        lockStore().tryLock("lock1", Duration.ofMinutes(5), "node-B"),
        "Lock held by node-A must survive an unlock attempt by node-B");
  }

  @Test
  void tryLock_concurrent_atMostOneSucceeds() {
    AtomicInteger successCount = new AtomicInteger();

    ConcurrentTestRunner.runAll(
        Duration.ofSeconds(10),
        () -> {
          if (lockStore().tryLock("race-lock", Duration.ofMinutes(5), "node-A")) {
            successCount.incrementAndGet();
          }
        },
        () -> {
          if (lockStore().tryLock("race-lock", Duration.ofMinutes(5), "node-B")) {
            successCount.incrementAndGet();
          }
        },
        () -> {
          if (lockStore().tryLock("race-lock", Duration.ofMinutes(5), "node-C")) {
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
    lockStore().tryLock("ttl-lock", Duration.ofSeconds(1), "node-A");

    Thread.sleep(2000);

    assertTrue(
        lockStore().tryLock("ttl-lock", Duration.ofMinutes(5), "node-B"),
        "Lock should be reacquirable after TTL expires");
  }
}
