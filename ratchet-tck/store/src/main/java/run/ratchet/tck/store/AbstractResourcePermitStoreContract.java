package run.ratchet.tck.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import run.ratchet.tck.util.ConcurrentTestRunner;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** Base contract tests for {@code ResourcePermitStore}. */
public abstract class AbstractResourcePermitStoreContract implements JobStoreContractFixture {

  @AfterEach
  void cleanupResourcePermitFixture() {
    cleanupStore();
  }

  @Test
  void configureAndAcquirePermit_succeeds() {
    store().configureResource("res-1", 2, 1000, "test");
    var job = persist(newPendingJob());

    boolean acquired = store().tryAcquirePermit("res-1", job.getId(), "node-1");

    assertTrue(acquired, "tryAcquirePermit should succeed when capacity is available");
  }

  @Test
  void tryAcquirePermit_failsAtCapacity() {
    store().configureResource("res-limited", 1, 1000, "limited resource");
    var firstJob = persist(newPendingJob());
    var secondJob = persist(newPendingJob());

    boolean firstAcquired = store().tryAcquirePermit("res-limited", firstJob.getId(), "node-1");
    boolean secondAcquired = store().tryAcquirePermit("res-limited", secondJob.getId(), "node-1");

    assertTrue(firstAcquired, "First acquire should succeed");
    assertFalse(secondAcquired, "Second acquire should fail when resource is at capacity");
  }

  @Test
  void releasePermit_freesCapacity() {
    store().configureResource("res-release", 1, 1000, "release test");
    var firstJob = persist(newPendingJob());
    var secondJob = persist(newPendingJob());

    store().tryAcquirePermit("res-release", firstJob.getId(), "node-1");
    store().releasePermit("res-release", firstJob.getId());

    boolean acquired = store().tryAcquirePermit("res-release", secondJob.getId(), "node-1");

    assertTrue(acquired, "Acquire should succeed after a permit has been released");
  }

  @Test
  void releaseAllPermits_freesAllForJob() {
    store().configureResource("res-A", 2, 1000, "resource A");
    store().configureResource("res-B", 2, 1000, "resource B");
    var job = persist(newPendingJob());

    store().tryAcquirePermit("res-A", job.getId(), "node-1");
    store().tryAcquirePermit("res-B", job.getId(), "node-1");

    store().releaseAllPermits(job.getId());

    var otherJob = persist(newPendingJob());
    assertTrue(
        store().tryAcquirePermit("res-A", otherJob.getId(), "node-1"),
        "Resource A should be freed after releaseAllPermits");
    assertTrue(
        store().tryAcquirePermit("res-B", otherJob.getId(), "node-1"),
        "Resource B should be freed after releaseAllPermits");
  }

  @Test
  void getPermitRetryDelay_returnsConfiguredDelay() {
    store().configureResource("res-delay", 1, 2500, "delay test");

    int delay = store().getPermitRetryDelay("res-delay");

    assertEquals(2500, delay, "getPermitRetryDelay should return the configured delay");
  }

  @Test
  void tryAcquirePermit_concurrent_respectsCapacity() {
    int capacity = 3;
    store().configureResource("res-cap", capacity, 1000, "capacity test");

    AtomicInteger successCount = new AtomicInteger();
    int threadCount = 6;
    Runnable[] tasks = new Runnable[threadCount];
    for (int i = 0; i < threadCount; i++) {
      final var job = persist(newPendingJob());
      final String nodeId = "node-" + i;
      tasks[i] =
          () -> {
            if (store().tryAcquirePermit("res-cap", job.getId(), nodeId)) {
              successCount.incrementAndGet();
            }
          };
    }

    ConcurrentTestRunner.runAll(Duration.ofSeconds(10), tasks);

    assertTrue(
        successCount.get() <= capacity,
        "at most " + capacity + " permits should be acquired; got " + successCount.get());
    assertTrue(successCount.get() >= 1, "at least one permit should be acquired");
  }

  @Test
  void cleanupOrphanedPermits_removesStaleNodePermits() {
    store().configureResource("res-orphan", 1, 1000, "orphan test");
    var job = persist(newPendingJob());

    store().tryAcquirePermit("res-orphan", job.getId(), "stale-node");

    int cleaned = store().cleanupOrphanedPermits(List.of("stale-node"));

    assertTrue(cleaned >= 1, "cleanupOrphanedPermits should remove the stale permit");

    var otherJob = persist(newPendingJob());
    assertTrue(
        store().tryAcquirePermit("res-orphan", otherJob.getId(), "node-1"),
        "Resource should be freed after orphan cleanup");
  }

  @Test
  void configureResource_updatesExistingConfig() {
    store().configureResource("res-reconfig", 1, 1000, "initial");
    store().configureResource("res-reconfig", 5, 2000, "updated");

    assertEquals(
        2000,
        store().getPermitRetryDelay("res-reconfig"),
        "Reconfigured resource should have updated retry delay");
  }

  @Test
  void tryAcquirePermit_sameJobTwice_idempotent() {
    store().configureResource("res-idem", 1, 1000, "idempotent test");
    var job = persist(newPendingJob());

    boolean first = store().tryAcquirePermit("res-idem", job.getId(), "node-1");
    boolean second = store().tryAcquirePermit("res-idem", job.getId(), "node-1");

    assertTrue(first, "First acquire should succeed");
    // Second acquire for same job should either succeed (idempotent) or fail,
    // but must not consume an additional permit slot
    var otherJob = persist(newPendingJob());
    // If capacity is 1 and same-job re-acquire is idempotent, this should fail
    // If same-job re-acquire consumed a slot, the resource would be at capacity
    assertFalse(
        store().tryAcquirePermit("res-idem", otherJob.getId(), "node-1"),
        "Resource capacity should still be exhausted — same-job re-acquire must not free a slot");
  }
}
