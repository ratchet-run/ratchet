package run.ratchet.tck.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobStatus;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Pins the read-side semantics of {@code findActiveByBusinessKey} across the full job lifecycle.
 *
 * <p>The complementary contracts focus on different concerns:
 *
 * <ul>
 *   <li>{@link AbstractJobCrudStoreContract} — generic CRUD plus the PENDING happy path and a
 *       SUCCEEDED-ignored sanity check.
 *   <li>{@link AbstractDualWriteInvariantContract} — terminal-release, reset re-reservation, and
 *       concurrent-enqueue invariants on stores that physically split queue state from cold
 *       metadata.
 * </ul>
 *
 * <p>This contract fills the remaining status matrix — RUNNING, PAUSED, FAILED-terminal, and
 * CANCELED — and locks the pause/resume cycle plus uniqueness while a peer is in any active state.
 * It is independent of physical layout: SQL stores satisfy it via the {@code
 * scheduler_business_key_reservation} table, Mongo via a partial unique index over {@code
 * (business_key, status IN PENDING/RUNNING/PAUSED)}.
 */
public abstract class AbstractActiveBusinessKeyContract implements JobStoreContractFixture {

  private static String uniqueBusinessKey() {
    return "abk-" + UUID.randomUUID();
  }

  @AfterEach
  void cleanupActiveBusinessKeyFixture() {
    cleanupStore();
  }

  // ───────────────────────── status visibility matrix ─────────────────────────

  @Test
  void findActiveByBusinessKey_runningJob_returnsJob() {
    String bk = uniqueBusinessKey();
    JobEntity saved = persist(jobWithBusinessKey(bk));
    assertTrue(
        store().compareAndSwapStatus(saved.getId(), JobStatus.PENDING, JobStatus.RUNNING, null),
        "PENDING→RUNNING CAS precondition");

    var result = store().findActiveByBusinessKey(bk);

    assertTrue(result.isPresent(), "RUNNING job should be visible to findActiveByBusinessKey");
    assertEquals(saved.getId(), result.get().getId());
  }

  @Test
  void findActiveByBusinessKey_pausedJob_returnsJob() {
    String bk = uniqueBusinessKey();
    JobEntity saved = persist(jobWithBusinessKey(bk));
    assertTrue(
        store().transitionToPaused(saved.getId(), JobStatus.PENDING),
        "PENDING→PAUSED transition precondition");

    var result = store().findActiveByBusinessKey(bk);

    assertTrue(
        result.isPresent(),
        "PAUSED job should remain visible to findActiveByBusinessKey — PAUSED is an active status");
    assertEquals(saved.getId(), result.get().getId());
  }

  @Test
  void findActiveByBusinessKey_failedTerminal_returnsEmpty() {
    String bk = uniqueBusinessKey();
    JobEntity saved = persist(jobWithBusinessKey(bk));
    long id = saved.getId();
    store().compareAndSwapStatus(id, JobStatus.PENDING, JobStatus.RUNNING, null);
    assertTrue(
        store().markJobFailedTerminal(id, "permanent error", 3),
        "markJobFailedTerminal precondition");

    var result = store().findActiveByBusinessKey(bk);

    assertFalse(
        result.isPresent(), "FAILED-terminal job should NOT be visible to findActiveByBusinessKey");
  }

  @Test
  void findActiveByBusinessKey_canceledJob_returnsEmpty() {
    String bk = uniqueBusinessKey();
    JobEntity saved = persist(jobWithBusinessKey(bk));
    assertTrue(store().cancelJob(saved.getId()), "cancelJob precondition");

    var result = store().findActiveByBusinessKey(bk);

    assertFalse(
        result.isPresent(), "CANCELED job should NOT be visible to findActiveByBusinessKey");
  }

  // ───────────────────────── pause/resume cycle ─────────────────────────

  @Test
  void pauseFromRunning_keepsBusinessKeyReserved() {
    String bk = uniqueBusinessKey();
    JobEntity saved = persist(jobWithBusinessKey(bk));
    long id = saved.getId();
    store().compareAndSwapStatus(id, JobStatus.PENDING, JobStatus.RUNNING, null);

    assertTrue(
        store().transitionToPaused(id, JobStatus.RUNNING),
        "RUNNING→PAUSED transition must succeed");

    var result = store().findActiveByBusinessKey(bk);
    assertTrue(result.isPresent(), "BK should still be reserved after RUNNING→PAUSED");
    assertEquals(id, result.get().getId());
  }

  @Test
  void pauseThenResume_keepsBusinessKeyReservedThroughout() {
    String bk = uniqueBusinessKey();
    JobEntity saved = persist(jobWithBusinessKey(bk));
    long id = saved.getId();

    store().transitionToPaused(id, JobStatus.PENDING);
    assertTrue(store().findActiveByBusinessKey(bk).isPresent(), "BK reserved while PAUSED");

    JobStatus resumed = store().transitionFromPausedAtomic(id);
    assertEquals(JobStatus.PENDING, resumed, "Resumed back to original PENDING status");

    var result = store().findActiveByBusinessKey(bk);
    assertTrue(result.isPresent(), "BK still reserved after PAUSED→PENDING resume");
    assertEquals(id, result.get().getId());
  }

  @Test
  void cancelFromPaused_releasesBusinessKey() {
    String bk = uniqueBusinessKey();
    JobEntity saved = persist(jobWithBusinessKey(bk));
    long id = saved.getId();
    store().transitionToPaused(id, JobStatus.PENDING);

    assertTrue(store().cancelJob(id), "cancelJob from PAUSED must succeed");

    assertEquals(JobStatus.CANCELED, store().getJobStatus(id));
    assertFalse(
        store().findActiveByBusinessKey(bk).isPresent(),
        "BK must be released when a PAUSED job is canceled");

    // Re-enqueue with the same BK should now succeed
    JobEntity replacement = persist(jobWithBusinessKey(bk));
    assertNotEquals(id, replacement.getId(), "Replacement must be a distinct job");
  }

  // ───────────────────────── uniqueness across active statuses ─────────────────────────

  @Test
  void duplicateEnqueueWhileRunning_fails() {
    String bk = uniqueBusinessKey();
    JobEntity first = persist(jobWithBusinessKey(bk));
    store().compareAndSwapStatus(first.getId(), JobStatus.PENDING, JobStatus.RUNNING, null);

    try {
      persist(jobWithBusinessKey(bk));
      fail("Duplicate enqueue while RUNNING peer holds the business key must throw");
    } catch (RuntimeException expected) {
      // expected
    }
  }

  @Test
  void duplicateEnqueueWhilePaused_fails() {
    String bk = uniqueBusinessKey();
    JobEntity first = persist(jobWithBusinessKey(bk));
    store().transitionToPaused(first.getId(), JobStatus.PENDING);

    try {
      persist(jobWithBusinessKey(bk));
      fail("Duplicate enqueue while PAUSED peer holds the business key must throw");
    } catch (RuntimeException expected) {
      // expected
    }
  }

  // ───────────────────────── identity / lookup correctness ─────────────────────────

  @Test
  void unknownBusinessKey_returnsEmpty() {
    persist(jobWithBusinessKey(uniqueBusinessKey()));

    var result = store().findActiveByBusinessKey("definitely-not-a-real-key-" + UUID.randomUUID());

    assertFalse(result.isPresent(), "Unknown business key must yield empty");
  }

  @Test
  void multipleDistinctKeys_returnsMatchingJobOnly() {
    String bkA = uniqueBusinessKey();
    String bkB = uniqueBusinessKey();
    JobEntity jobA = persist(jobWithBusinessKey(bkA));
    JobEntity jobB = persist(jobWithBusinessKey(bkB));

    var resultA = store().findActiveByBusinessKey(bkA);
    var resultB = store().findActiveByBusinessKey(bkB);

    assertTrue(resultA.isPresent());
    assertEquals(jobA.getId(), resultA.get().getId(), "BK A must resolve to job A");
    assertTrue(resultB.isPresent());
    assertEquals(jobB.getId(), resultB.get().getId(), "BK B must resolve to job B");
  }

  @Test
  void terminalSucceeded_thenReenqueueSameKey_returnsReplacement() {
    String bk = uniqueBusinessKey();
    JobEntity first = persist(jobWithBusinessKey(bk));
    long firstId = first.getId();
    store().compareAndSwapStatus(firstId, JobStatus.PENDING, JobStatus.RUNNING, null);
    store().markJobSucceededMinimal(firstId, Instant.now(), Instant.now(), 0L, 0L);

    JobEntity replacement = persist(jobWithBusinessKey(bk));

    var result = store().findActiveByBusinessKey(bk);
    assertTrue(result.isPresent(), "Replacement must take ownership of the BK");
    assertEquals(
        replacement.getId(),
        result.get().getId(),
        "findActiveByBusinessKey must return the live replacement, not the SUCCEEDED predecessor");
  }

  private JobEntity jobWithBusinessKey(String bk) {
    JobEntity job = newPendingJob();
    job.setBusinessKey(bk);
    return job;
  }
}
