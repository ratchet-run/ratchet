package run.ratchet.tck.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
}
