package run.ratchet.tck.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobStatus;

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
}
