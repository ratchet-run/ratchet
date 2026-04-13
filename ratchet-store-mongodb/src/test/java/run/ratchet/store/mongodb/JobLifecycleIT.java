package run.ratchet.store.mongodb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobStatus;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class JobLifecycleIT extends BaseDocumentStoreIT {

  @Test
  void fullLifecycle_createClaimSucceed() {
    JobEntity job = store().save(newPendingJob());
    assertNotNull(job.getId());
    assertEquals(JobStatus.PENDING, job.getStatus());

    List<JobEntity> claimed = store().claimNextBatch(1, "node-1");
    assertEquals(1, claimed.size());
    assertEquals(job.getId(), claimed.get(0).getId());
    assertEquals(JobStatus.RUNNING, claimed.get(0).getStatus());
    assertEquals("node-1", claimed.get(0).getPickedBy());

    boolean swapped =
        store().compareAndSwapStatus(job.getId(), JobStatus.RUNNING, JobStatus.SUCCEEDED, null);
    assertTrue(swapped);

    Optional<JobEntity> completed = store().findById(job.getId());
    assertTrue(completed.isPresent());
    assertEquals(JobStatus.SUCCEEDED, completed.get().getStatus());
  }

  @Test
  void fullLifecycle_createClaimFail() {
    JobEntity job = store().save(newPendingJob());

    List<JobEntity> claimed = store().claimNextBatch(1, "node-1");
    assertEquals(1, claimed.size());

    boolean swapped =
        store()
            .compareAndSwapStatus(
                job.getId(), JobStatus.RUNNING, JobStatus.FAILED, "Something went wrong");
    assertTrue(swapped);

    Optional<JobEntity> failed = store().findById(job.getId());
    assertTrue(failed.isPresent());
    assertEquals(JobStatus.FAILED, failed.get().getStatus());
  }

  @Test
  void retryFlow_failIncrementRetryReclaimSucceed() {
    JobEntity job = newPendingJob();
    job.setMaxRetries(3);
    job = store().save(job);

    store().claimNextBatch(1, "node-1");
    store().incrementRetryAttempt(job.getId());
    store().compareAndSwapStatus(job.getId(), JobStatus.RUNNING, JobStatus.FAILED, "first attempt");
    store().compareAndSwapStatus(job.getId(), JobStatus.FAILED, JobStatus.PENDING, null);

    List<JobEntity> reclaimed = store().claimNextBatch(1, "node-1");
    assertEquals(1, reclaimed.size());

    store().compareAndSwapStatus(job.getId(), JobStatus.RUNNING, JobStatus.SUCCEEDED, null);

    Optional<JobEntity> result = store().findById(job.getId());
    assertTrue(result.isPresent());
    assertEquals(JobStatus.SUCCEEDED, result.get().getStatus());
    assertEquals(1, result.get().getAttempts());
  }

  @Test
  void casRejectsStaleTransition() {
    JobEntity job = store().save(newPendingJob());
    store().claimNextBatch(1, "node-1");

    // Try to transition from PENDING (stale) — should fail because it's now RUNNING
    boolean swapped =
        store().compareAndSwapStatus(job.getId(), JobStatus.PENDING, JobStatus.SUCCEEDED, null);
    assertFalse(swapped);

    Optional<JobEntity> current = store().findById(job.getId());
    assertTrue(current.isPresent());
    assertEquals(JobStatus.RUNNING, current.get().getStatus());
  }

  @Test
  void claimRespectsScheduledTime() {
    JobEntity futureJob = newPendingJob();
    futureJob.setScheduledTime(Instant.now().plusSeconds(3600));
    store().save(futureJob);

    JobEntity nowJob = store().save(newPendingJob());

    List<JobEntity> claimed = store().claimNextBatch(10, "node-1");
    assertEquals(1, claimed.size());
    assertEquals(nowJob.getId(), claimed.get(0).getId());
  }
}
