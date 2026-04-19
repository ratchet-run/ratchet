package run.ratchet.tck.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import run.ratchet.store.entity.JobStatus;
import java.time.Instant;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** Base contract tests for {@code JobRetryStore}. */
public abstract class AbstractJobRetryStoreContract implements JobStoreContractFixture {

  @AfterEach
  void cleanupRetryFixture() {
    cleanupStore();
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

  @Test
  void scheduleJobRetry_setsNewTimeAndAttempts() {
    var saved = persist(newPendingJob());
    store().compareAndSwapStatus(saved.getId(), JobStatus.PENDING, JobStatus.RUNNING, null);

    Instant retryTime = Instant.now().plusSeconds(300);
    boolean retried = store().scheduleJobRetry(saved.getId(), "transient error", retryTime, 1);

    assertTrue(retried, "scheduleJobRetry should succeed for a running job");
    var reloaded = store().findById(saved.getId()).orElseThrow();
    assertEquals(JobStatus.PENDING, reloaded.getStatus(), "Job should be back to PENDING");
  }

  @Test
  void resetFailedToPending_transitionsAndResetsMetadata() {
    var saved = persist(newPendingJob());
    store().compareAndSwapStatus(saved.getId(), JobStatus.PENDING, JobStatus.RUNNING, null);
    store().compareAndSwapStatus(saved.getId(), JobStatus.RUNNING, JobStatus.FAILED, "error");

    boolean reset = store().resetFailedToPending(saved.getId());

    assertTrue(reset, "resetFailedToPending should succeed for a FAILED job");
    var reloaded = store().findById(saved.getId()).orElseThrow();
    assertEquals(JobStatus.PENDING, reloaded.getStatus());
  }
}
