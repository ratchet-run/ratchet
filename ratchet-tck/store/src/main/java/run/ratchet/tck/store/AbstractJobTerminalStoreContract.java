package run.ratchet.tck.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobStatus;

/** Base contract tests for {@code JobTerminalStore}. */
public abstract class AbstractJobTerminalStoreContract implements JobStoreContractFixture {

  @BeforeEach
  @AfterEach
  void cleanupTerminalFixture() {
    cleanupStore();
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
  void markJobSucceededMinimal_updatesStatusWithoutResult() {
    var saved = persist(newPendingJob());
    store().compareAndSwapStatus(saved.getId(), JobStatus.PENDING, JobStatus.RUNNING, null);

    Instant start = Instant.now().minusSeconds(5);
    Instant end = Instant.now();
    boolean marked = store().markJobSucceededMinimal(saved.getId(), start, end, 5000L, 100L);

    assertTrue(marked, "markJobSucceededMinimal should return true for a running job");
    var reloaded = store().findById(saved.getId()).orElseThrow();
    assertEquals(JobStatus.SUCCEEDED, reloaded.getStatus());
    assertNull(reloaded.getJobResult(), "Minimal success should not persist result JSON");
  }

  @Test
  void markJobSucceededAndUpdateBatch_updatesJobAndBatchAtomically() {
    var parent = persist(newBatchParentJob());
    persistBatch(parent.getId(), 1);
    var saved = persist(newPendingJob());
    store().compareAndSwapStatus(saved.getId(), JobStatus.PENDING, JobStatus.RUNNING, null);

    Instant start = Instant.now().minusSeconds(5);
    Instant end = Instant.now();
    boolean marked =
        store()
            .markJobSucceededAndUpdateBatch(
                saved.getId(),
                "{\"ok\":true}",
                "java.lang.String",
                start,
                end,
                5000L,
                100L,
                parent.getId());

    assertTrue(marked, "markJobSucceededAndUpdateBatch should return true for a running job");
    var reloaded = store().findById(saved.getId()).orElseThrow();
    assertEquals(JobStatus.SUCCEEDED, reloaded.getStatus());
    assertNotNull(reloaded.getJobResult(), "Result JSON should be persisted");
    assertEquals(1, store().findBatchById(parent.getId()).orElseThrow().getCompletedItems());
  }

  @Test
  void markJobFailedTerminal_usesCallerAttemptsAndPersistsTimingFields() {
    var saved = persist(newPendingJob());
    store().compareAndSwapStatus(saved.getId(), JobStatus.PENDING, JobStatus.RUNNING, null);

    boolean marked = store().markJobFailedTerminal(saved.getId(), "permanent", 3);

    assertTrue(marked, "markJobFailedTerminal should return true for a running job");
    var reloaded = store().findById(saved.getId()).orElseThrow();
    assertEquals(JobStatus.FAILED, reloaded.getStatus());
    assertEquals(3, reloaded.getAttempts(), "Caller totalAttempts must be persisted");
    assertTerminalTiming(reloaded.getExecutionStartTime(), reloaded.getExecutionEndTime());
    assertNotNull(reloaded.getExecutionDurationMs(), "Terminal duration should be persisted");
  }

  @Test
  void compareAndSwapStatus_runningToFailedPreservesHotAttemptsAndTimingFields() {
    var saved = persist(newPendingJob());
    store().compareAndSwapStatus(saved.getId(), JobStatus.PENDING, JobStatus.RUNNING, null);
    assertEquals(1, store().incrementRetryAttempt(saved.getId()));

    boolean marked =
        store().compareAndSwapStatus(saved.getId(), JobStatus.RUNNING, JobStatus.FAILED, "boom");

    assertTrue(marked, "RUNNING to FAILED CAS should succeed");
    var reloaded = store().findById(saved.getId()).orElseThrow();
    assertEquals(JobStatus.FAILED, reloaded.getStatus());
    assertEquals(1, reloaded.getAttempts(), "Terminal row must preserve hot-row attempts");
    assertTerminalTiming(reloaded.getExecutionStartTime(), reloaded.getExecutionEndTime());
    assertNotNull(reloaded.getExecutionDurationMs(), "Terminal duration should be persisted");
  }

  @Test
  void cancelJob_runningJobPersistsTimingFields() {
    var saved = persist(newPendingJob());
    store().compareAndSwapStatus(saved.getId(), JobStatus.PENDING, JobStatus.RUNNING, null);

    boolean canceled = store().cancelJob(saved.getId());

    assertTrue(canceled, "cancelJob should return true for a running job");
    var reloaded = store().findById(saved.getId()).orElseThrow();
    assertEquals(JobStatus.CANCELED, reloaded.getStatus());
    assertTerminalTiming(reloaded.getExecutionStartTime(), reloaded.getExecutionEndTime());
    assertNotNull(reloaded.getExecutionDurationMs(), "Terminal duration should be persisted");
  }

  @Test
  void compareAndSwapStatus_runningToCanceledPersistsTimingFields() {
    var saved = persist(newPendingJob());
    store().compareAndSwapStatus(saved.getId(), JobStatus.PENDING, JobStatus.RUNNING, null);

    boolean canceled =
        store().compareAndSwapStatus(saved.getId(), JobStatus.RUNNING, JobStatus.CANCELED, null);

    assertTrue(canceled, "RUNNING to CANCELED CAS should succeed");
    var reloaded = store().findById(saved.getId()).orElseThrow();
    assertEquals(JobStatus.CANCELED, reloaded.getStatus());
    assertTerminalTiming(reloaded.getExecutionStartTime(), reloaded.getExecutionEndTime());
    assertNotNull(reloaded.getExecutionDurationMs(), "Terminal duration should be persisted");
  }

  private static void assertTerminalTiming(Instant start, Instant end) {
    assertNotNull(start, "Terminal start time should be persisted");
    assertNotNull(end, "Terminal end time should be persisted");
    assertFalse(end.isBefore(start), "Terminal end time should not precede start time");
  }
}
