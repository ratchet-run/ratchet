package run.ratchet.tck.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobStatus;
import run.ratchet.tck.util.ConcurrentTestRunner;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
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
    UUID id = saved.getId();
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
    UUID id = saved.getId();
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
    UUID id = saved.getId();

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
    UUID id = saved.getId();
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
    UUID firstId = first.getId();
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

  @Test
  void terminalFailed_thenReenqueueSameKey_returnsReplacement() {
    String bk = uniqueBusinessKey();
    JobEntity first = persist(jobWithBusinessKey(bk));
    UUID firstId = first.getId();
    store().compareAndSwapStatus(firstId, JobStatus.PENDING, JobStatus.RUNNING, null);
    store().markJobFailedTerminal(firstId, "permanent", 3);

    JobEntity replacement = persist(jobWithBusinessKey(bk));

    var result = store().findActiveByBusinessKey(bk);
    assertTrue(result.isPresent(), "Replacement must take ownership of the BK after FAILED");
    assertEquals(
        replacement.getId(),
        result.get().getId(),
        "findActiveByBusinessKey must return the live replacement, not the FAILED predecessor");
  }

  @Test
  void canceled_thenReenqueueSameKey_returnsReplacement() {
    String bk = uniqueBusinessKey();
    JobEntity first = persist(jobWithBusinessKey(bk));
    UUID firstId = first.getId();
    store().cancelJob(firstId);

    JobEntity replacement = persist(jobWithBusinessKey(bk));

    var result = store().findActiveByBusinessKey(bk);
    assertTrue(result.isPresent(), "Replacement must take ownership of the BK after CANCELED");
    assertEquals(
        replacement.getId(),
        result.get().getId(),
        "findActiveByBusinessKey must return the live replacement, not the CANCELED predecessor");
  }

  // ───────────────────────── retry / reset cycles ─────────────────────────

  /**
   * Full happy-path retry cycle: enqueue → run → fail → reset → run → succeed. The BK is reserved
   * for every active state and released exactly once at terminal SUCCEEDED.
   */
  @Test
  void resetFailedToPending_thenSucceed_releasesBusinessKey() {
    String bk = uniqueBusinessKey();
    JobEntity job = persist(jobWithBusinessKey(bk));
    UUID id = job.getId();
    store().compareAndSwapStatus(id, JobStatus.PENDING, JobStatus.RUNNING, null);
    store().markJobFailedTerminal(id, "transient", 1);
    assertTrue(store().resetFailedToPending(id), "resetFailedToPending precondition");
    assertTrue(
        store().findActiveByBusinessKey(bk).isPresent(),
        "BK must remain reserved through the retry path");

    store().compareAndSwapStatus(id, JobStatus.PENDING, JobStatus.RUNNING, null);
    assertTrue(
        store().markJobSucceededMinimal(id, Instant.now(), Instant.now(), 0L, 0L),
        "markJobSucceededMinimal precondition");

    assertEquals(JobStatus.SUCCEEDED, store().getJobStatus(id));
    assertFalse(
        store().findActiveByBusinessKey(bk).isPresent(),
        "BK must be released exactly once at terminal SUCCEEDED");
  }

  // ───────────────────────── concurrency races ─────────────────────────

  /**
   * The retry path (background timer in {@code RetryBufferDrainer}) can race with an operator-
   * triggered {@code cancelJob}. Either outcome — RESET-wins (PENDING, BK held) or CANCEL-wins
   * (CANCELED, BK released) — is correct, but the BK state MUST match the winner. A torn write (job
   * reset to PENDING but BK released by cancel, or job CANCELED but BK still reserved) leaks the BK
   * and silently blocks future enqueues.
   */
  @Test
  void concurrentResetAndCancel_BKStateMatchesWinner() {
    String bk = uniqueBusinessKey();
    JobEntity job = persist(jobWithBusinessKey(bk));
    UUID id = job.getId();
    store().compareAndSwapStatus(id, JobStatus.PENDING, JobStatus.RUNNING, null);
    store().markJobFailedTerminal(id, "transient", 1);

    AtomicBoolean resetWon = new AtomicBoolean();
    AtomicBoolean cancelWon = new AtomicBoolean();
    List<Throwable> failures =
        ConcurrentTestRunner.runAll(
            Duration.ofSeconds(15),
            () -> {
              if (store().resetFailedToPending(id)) {
                resetWon.set(true);
              }
            },
            () -> {
              if (store().cancelJob(id)) {
                cancelWon.set(true);
              }
            });

    long unexpectedFailures =
        failures.stream().filter(t -> t != null && !isStaleWriteException(t)).count();
    assertEquals(0L, unexpectedFailures, "no thread should fail with a non-stale-write exception");

    JobStatus finalStatus = store().getJobStatus(id);
    boolean bkActive = store().findActiveByBusinessKey(bk).isPresent();

    if (finalStatus == JobStatus.PENDING) {
      assertTrue(resetWon.get(), "PENDING outcome implies reset won the race");
      assertTrue(bkActive, "BK must be reserved when reset wins (PENDING is an active status)");
    } else if (finalStatus == JobStatus.CANCELED) {
      assertTrue(cancelWon.get(), "CANCELED outcome implies cancel won the race");
      assertFalse(bkActive, "BK must be released when cancel wins (CANCELED is a terminal status)");
    } else {
      fail("unexpected terminal status after reset/cancel race: " + finalStatus);
    }
  }

  /**
   * Pause and cancel target the same job concurrently. {@code cancelJob} legitimately accepts any
   * non-terminal status, so the legitimate sequential outcomes are:
   *
   * <ul>
   *   <li>cancel beats pause to PENDING → CANCELED, pause's CAS sees CANCELED and no-ops
   *   <li>pause runs first → PAUSED, then cancel runs against PAUSED → CANCELED
   *   <li>pause runs and cancel never gets a turn → PAUSED
   * </ul>
   *
   * <p>The contract isn't "exactly one transition wins" — it's "the final state is well-defined and
   * the BK reservation matches it." A torn write (CANCELED-but-BK-still-reserved, or
   * PAUSED-but-BK-released) leaks the BK and silently blocks future enqueues.
   */
  @Test
  void concurrentPauseAndCancel_finalStateIsConsistent() {
    String bk = uniqueBusinessKey();
    JobEntity job = persist(jobWithBusinessKey(bk));
    UUID id = job.getId();

    List<Throwable> failures =
        ConcurrentTestRunner.runAll(
            Duration.ofSeconds(15),
            () -> store().transitionToPaused(id, JobStatus.PENDING),
            () -> store().cancelJob(id));

    long unexpectedFailures =
        failures.stream().filter(t -> t != null && !isStaleWriteException(t)).count();
    assertEquals(0L, unexpectedFailures, "no non-stale-write failures expected");

    JobStatus finalStatus = store().getJobStatus(id);
    boolean bkActive = store().findActiveByBusinessKey(bk).isPresent();
    if (finalStatus == JobStatus.PAUSED) {
      assertTrue(bkActive, "BK must remain reserved when final state is PAUSED");
    } else if (finalStatus == JobStatus.CANCELED) {
      assertFalse(bkActive, "BK must be released when final state is CANCELED");
    } else {
      fail("unexpected final status after pause/cancel race: " + finalStatus);
    }
  }

  // ───────────────────────── isolation between keys ─────────────────────────

  /**
   * Mutating one job's status MUST NOT touch a peer's BK reservation. Trivial-looking, but a
   * regression in any reservation-table JOIN or partial-index predicate could break this without
   * showing up in the per-key tests above.
   */
  @Test
  void unrelatedKeyTransitions_doNotAffectOtherKeyOwnership() {
    String bkA = uniqueBusinessKey();
    String bkB = uniqueBusinessKey();
    JobEntity jobA = persist(jobWithBusinessKey(bkA));
    JobEntity jobB = persist(jobWithBusinessKey(bkB));

    // Run job A through a full terminal cycle while job B sits idle.
    store().compareAndSwapStatus(jobA.getId(), JobStatus.PENDING, JobStatus.RUNNING, null);
    store().markJobSucceededMinimal(jobA.getId(), Instant.now(), Instant.now(), 0L, 0L);

    var resultA = store().findActiveByBusinessKey(bkA);
    var resultB = store().findActiveByBusinessKey(bkB);
    assertFalse(resultA.isPresent(), "BK A must be released after job A SUCCEEDED");
    assertTrue(resultB.isPresent(), "BK B must be untouched by job A's lifecycle");
    assertEquals(jobB.getId(), resultB.get().getId(), "BK B still owned by job B");
  }

  private JobEntity jobWithBusinessKey(String bk) {
    JobEntity job = newPendingJob();
    job.setBusinessKey(bk);
    return job;
  }
}
