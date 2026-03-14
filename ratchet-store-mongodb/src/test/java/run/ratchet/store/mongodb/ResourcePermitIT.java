package run.ratchet.store.mongodb;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import run.ratchet.store.entity.JobEntity;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for resource permit (concurrency limit) operations.
 *
 * <p>Validates permit acquisition, release, and enforcement of configured concurrency limits.
 */
class ResourcePermitIT extends BaseDocumentStoreIT {

  @Test
  void acquireAndRelease_withinLimit() {
    store().configureResource("gpu", 2, 5000, "GPU resource pool");

    JobEntity j1 = store().save(newPendingJob());
    JobEntity j2 = store().save(newPendingJob());
    JobEntity j3 = store().save(newPendingJob());

    assertTrue(store().tryAcquirePermit("gpu", j1.getId(), "node-1"));
    assertTrue(store().tryAcquirePermit("gpu", j2.getId(), "node-1"));

    // Third should be rejected — at limit
    assertFalse(store().tryAcquirePermit("gpu", j3.getId(), "node-1"));

    // Release one and try again
    store().releasePermit("gpu", j1.getId());
    assertTrue(store().tryAcquirePermit("gpu", j3.getId(), "node-1"));
  }

  @Test
  void releaseAllPermits_clearsForJob() {
    store().configureResource("db-conn", 3, 5000, "DB connection pool");

    JobEntity j1 = store().save(newPendingJob());
    store().tryAcquirePermit("db-conn", j1.getId(), "node-1");

    store().releaseAllPermits(j1.getId());

    // Permit slot should now be available
    JobEntity j2 = store().save(newPendingJob());
    assertTrue(store().tryAcquirePermit("db-conn", j2.getId(), "node-1"));
  }

  @Test
  void unconfiguredResource_deniesPermit() {
    JobEntity job = store().save(newPendingJob());
    assertFalse(store().tryAcquirePermit("nonexistent-resource", job.getId(), "node-1"));
  }
}
