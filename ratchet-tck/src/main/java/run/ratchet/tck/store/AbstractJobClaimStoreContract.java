package run.ratchet.tck.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import run.ratchet.store.entity.JobStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** Base contract tests for {@code JobClaimStore}. */
public abstract class AbstractJobClaimStoreContract implements JobStoreContractFixture {

  @AfterEach
  void cleanupClaimFixture() {
    cleanupStore();
  }

  @Test
  void claimNextBatch_claimsPendingJobs() {
    persist(newPendingJob());
    persist(newPendingJob());

    var claimed = store().claimNextBatch(10, "node-1");

    assertEquals(2, claimed.size(), "claimNextBatch should return both pending jobs");
    for (var job : claimed) {
      assertEquals(JobStatus.RUNNING, job.getStatus(), "Claimed job should be RUNNING");
      assertEquals("node-1", job.getPickedBy(), "Claimed job should record the claiming node");
    }
  }

  @Test
  void claimNextBatch_respectsLimit() {
    persist(newPendingJob());
    persist(newPendingJob());
    persist(newPendingJob());

    var claimed = store().claimNextBatch(2, "node-1");

    assertEquals(2, claimed.size(), "claimNextBatch should respect the limit parameter");
  }

  @Test
  void claimNextBatch_skipsAlreadyClaimedJobs() {
    persist(newPendingJob());
    persist(newPendingJob());

    store().claimNextBatch(10, "node-1");
    var secondClaim = store().claimNextBatch(10, "node-2");

    assertTrue(secondClaim.isEmpty(), "Second claim should return empty when all jobs are taken");
  }
}
