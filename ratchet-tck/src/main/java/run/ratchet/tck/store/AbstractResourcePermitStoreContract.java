package run.ratchet.tck.store;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
}
