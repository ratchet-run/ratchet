package run.ratchet.tck.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import run.ratchet.store.entity.JobStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** Base contract tests for {@code JobStatusStore}. */
public abstract class AbstractJobStatusStoreContract implements JobStoreContractFixture {

  @AfterEach
  void cleanupStatusFixture() {
    cleanupStore();
  }

  @Test
  void compareAndSwapStatus_updatesExpectedState() {
    var saved = persist(newPendingJob());

    boolean updated =
        store().compareAndSwapStatus(saved.getId(), JobStatus.PENDING, JobStatus.RUNNING, null);

    assertTrue(updated, "Pending job should transition to RUNNING");
    assertEquals(JobStatus.RUNNING, store().getJobStatus(saved.getId()));
  }

  @Test
  void incrementRetryAttempt_requiresRunningStatus() {
    var saved = persist(newPendingJob());

    assertEquals(
        -1,
        store().incrementRetryAttempt(saved.getId()),
        "Retry attempts should not increment for non-running jobs");

    store().compareAndSwapStatus(saved.getId(), JobStatus.PENDING, JobStatus.RUNNING, null);
    assertEquals(1, store().incrementRetryAttempt(saved.getId()));
  }
}
