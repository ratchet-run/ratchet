package run.ratchet.tck.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobStatus;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;

/** Base contract tests for {@code JobPauseStore}. */
public abstract class AbstractJobPauseStoreContract implements JobStoreContractFixture {

  @BeforeEach
  @AfterEach
  void cleanupPauseFixture() {
    cleanupStore();
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

  @Test
  void transitionFromPausedAtomic_restoresStoredStatusAndClearsPauseMetadata() {
    var saved = persist(newPendingJob());

    assertTrue(store().transitionToPaused(saved.getId(), JobStatus.PENDING));

    JobStatus restored = store().transitionFromPausedAtomic(saved.getId());

    assertEquals(JobStatus.PENDING, restored);
    var resumed = store().findById(saved.getId()).orElseThrow();
    assertEquals(JobStatus.PENDING, resumed.getStatus());
    assertNull(resumed.getPausedFromStatus(), "resume must clear pausedFromStatus");
  }

  @Test
  void transitionFromPausedAtomic_nonPausedJob_returnsNull() {
    var saved = persist(newPendingJob());

    JobStatus restored = store().transitionFromPausedAtomic(saved.getId());

    assertNull(restored, "non-PAUSED rows should not be resumed atomically");
    assertEquals(JobStatus.PENDING, store().getJobStatus(saved.getId()));
  }

  @Test
  void transitionFromPausedAtomic_unknownJob_returnsNull() {
    assertNull(store().transitionFromPausedAtomic(new UUID(0L, Long.MAX_VALUE)));
  }

  @Test
  void transitionToPaused_rejectsAlreadyPausedExpectedStatus() {
    var saved = persist(newPendingJob());
    assertTrue(store().transitionToPaused(saved.getId(), JobStatus.PENDING));

    assertThrows(
        IllegalArgumentException.class,
        () -> store().transitionToPaused(saved.getId(), JobStatus.PAUSED));
  }

  @Test
  void transitionToPaused_returnsFalseForNonPausableExpectedStatuses() {
    var waiting = newPendingJob();
    waiting.setStatus(JobStatus.WAITING);
    waiting = persist(waiting);

    assertFalse(store().transitionToPaused(waiting.getId(), JobStatus.WAITING));

    var terminal = persist(newPendingJob());
    store().compareAndSwapStatus(terminal.getId(), JobStatus.PENDING, JobStatus.RUNNING, null);
    store().compareAndSwapStatus(terminal.getId(), JobStatus.RUNNING, JobStatus.FAILED, "boom");

    assertFalse(store().transitionToPaused(terminal.getId(), JobStatus.FAILED));
  }

  @Test
  void transitionFromPaused_rejectsPausedWaitingAndTerminalTargets() {
    var saved = persist(newPendingJob());
    assertTrue(store().transitionToPaused(saved.getId(), JobStatus.PENDING));

    assertThrows(
        IllegalArgumentException.class,
        () -> store().transitionFromPaused(saved.getId(), JobStatus.PAUSED));
    assertThrows(
        IllegalArgumentException.class,
        () -> store().transitionFromPaused(saved.getId(), JobStatus.WAITING));
    assertThrows(
        IllegalArgumentException.class,
        () -> store().transitionFromPaused(saved.getId(), JobStatus.FAILED));
  }

  @Test
  @org.junit.jupiter.api.Disabled(
      "Superseded by AbstractRecurringJobStoreContract after CP2: recurring masters no longer"
          + " live in scheduler_job. The legacy semantics are obsolete.")
  void pauseRecurring_andResumeRecurring_onlyOperateOnRecurringMasters() {
    var recurring = persist(recurringJob());
    var oneShot = persist(newPendingJob());

    assertFalse(
        store().pauseRecurring(oneShot.getId()), "one-shot jobs must not use recurring pause");
    assertTrue(store().pauseRecurring(recurring.getId()), "recurring master should pause");
    assertEquals(JobStatus.PAUSED, store().getJobStatus(recurring.getId()));

    assertFalse(
        store().resumeRecurring(oneShot.getId()), "one-shot jobs must not use recurring resume");
    assertTrue(store().resumeRecurring(recurring.getId()), "paused recurring master should resume");
    assertEquals(JobStatus.PENDING, store().getJobStatus(recurring.getId()));
  }

  @Test
  @org.junit.jupiter.api.Disabled("Superseded by AbstractRecurringJobStoreContract after CP2.")
  void pauseRecurring_resumeRecurring_areIdempotentForWrongState() {
    var recurring = persist(recurringJob());

    assertTrue(store().pauseRecurring(recurring.getId()));
    assertFalse(
        store().pauseRecurring(recurring.getId()), "already-paused master should not pause");

    assertTrue(store().resumeRecurring(recurring.getId()));
    assertFalse(
        store().resumeRecurring(recurring.getId()), "already-pending master should not resume");
  }

  private JobEntity recurringJob() {
    JobEntity job = newPendingJob();
    job.setJobType(JobExecutionType.RECURRING);
    job.setCronExpr("0 * * * *");
    job.setNextFire(Instant.now().plusSeconds(60));
    return job;
  }
}
