package run.ratchet.tck.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import run.ratchet.store.entity.JobStatus;
import run.ratchet.tck.util.ConcurrentTestRunner;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;
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

  @Test
  void compareAndSwapStatus_failsOnStatusMismatch() {
    var saved = persist(newPendingJob());

    boolean updated =
        store().compareAndSwapStatus(saved.getId(), JobStatus.RUNNING, JobStatus.CANCELED, null);

    assertFalse(updated, "CAS from wrong expected status should return false");
    assertEquals(
        JobStatus.PENDING,
        store().getJobStatus(saved.getId()),
        "Status should remain PENDING after failed CAS");
  }

  /**
   * Three threads race to CAS the same job from PENDING → RUNNING. At most one should succeed — the
   * CAS is atomic. We assert "at most one" rather than "exactly one" because thread scheduling on
   * low-core CI machines may serialize execution such that no true contention occurs.
   */
  @Test
  void compareAndSwapStatus_concurrent_atMostOneSucceeds() {
    var saved = persist(newPendingJob());
    long id = saved.getId();

    AtomicInteger successCount = new AtomicInteger();

    ConcurrentTestRunner.runAll(
        Duration.ofSeconds(10),
        () -> {
          if (store().compareAndSwapStatus(id, JobStatus.PENDING, JobStatus.RUNNING, null)) {
            successCount.incrementAndGet();
          }
        },
        () -> {
          if (store().compareAndSwapStatus(id, JobStatus.PENDING, JobStatus.RUNNING, null)) {
            successCount.incrementAndGet();
          }
        },
        () -> {
          if (store().compareAndSwapStatus(id, JobStatus.PENDING, JobStatus.RUNNING, null)) {
            successCount.incrementAndGet();
          }
        });

    assertTrue(
        successCount.get() <= 1, "at most one CAS should succeed; got " + successCount.get());
    assertEquals(JobStatus.RUNNING, store().getJobStatus(id), "Job should be RUNNING after CAS");
  }

  @Test
  void tryPickUpJob_setsStatusAndPickedBy() {
    var saved = persist(newPendingJob());

    boolean picked = store().tryPickUpJob(saved.getId(), "node-1");

    assertTrue(picked, "tryPickUpJob should succeed on a PENDING job");
    var reloaded = store().findById(saved.getId()).orElseThrow();
    assertEquals(JobStatus.RUNNING, reloaded.getStatus());
    assertEquals("node-1", reloaded.getPickedBy());
  }

  @Test
  void tryPickUpJob_failsOnAlreadyRunning() {
    var saved = persist(newPendingJob());
    store().tryPickUpJob(saved.getId(), "node-1");

    boolean secondPick = store().tryPickUpJob(saved.getId(), "node-2");

    assertFalse(secondPick, "tryPickUpJob should fail on an already-running job");
  }

  @Test
  void markJobSucceeded_updatesStatusAndResult() {
    var saved = persist(newPendingJob());
    store().compareAndSwapStatus(saved.getId(), JobStatus.PENDING, JobStatus.RUNNING, null);

    Instant start = Instant.now().minusSeconds(5);
    Instant end = Instant.now();
    boolean marked =
        store()
            .markJobSucceeded(
                saved.getId(), "{\"ok\":true}", "java.lang.String", start, end, 5000L, 100L);

    assertTrue(marked, "markJobSucceeded should return true for a running job");
    var reloaded = store().findById(saved.getId()).orElseThrow();
    assertEquals(JobStatus.SUCCEEDED, reloaded.getStatus());
    assertNotNull(reloaded.getJobResult(), "Result JSON should be persisted");
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

  @Test
  void transitionToPaused_andBack_preservesOriginalStatus() {
    var saved = persist(newPendingJob());

    boolean paused = store().transitionToPaused(saved.getId(), JobStatus.PENDING);
    assertTrue(paused, "transitionToPaused should succeed for a PENDING job");

    var pausedJob = store().findById(saved.getId()).orElseThrow();
    assertEquals(JobStatus.PAUSED, pausedJob.getStatus());
    assertEquals(
        JobStatus.PENDING,
        pausedJob.getPausedFromStatus(),
        "pausedFromStatus should record the original status");

    boolean resumed = store().transitionFromPaused(saved.getId(), JobStatus.PENDING);
    assertTrue(resumed, "transitionFromPaused should succeed for a PAUSED job");

    var resumedJob = store().findById(saved.getId()).orElseThrow();
    assertEquals(JobStatus.PENDING, resumedJob.getStatus());
  }
}
