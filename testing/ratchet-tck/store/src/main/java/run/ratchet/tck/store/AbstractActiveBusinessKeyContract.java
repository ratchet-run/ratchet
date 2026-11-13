/*
 * Copyright 2026 Ratchet Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package run.ratchet.tck.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opentest4j.TestAbortedException;
import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.BusinessKeyHolder;
import run.ratchet.api.JobStatus;
import run.ratchet.api.JobType;
import run.ratchet.api.RecurringMisfirePolicy;
import run.ratchet.store.BusinessKeyReservationConflictException;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.spi.RecurringJobDefinition;
import run.ratchet.store.spi.RecurringJobStore.ArchiveReason;
import run.ratchet.store.util.BusinessKeyReservations;
import run.ratchet.tck.util.ConcurrentTestRunner;

/**
 * Verifies reservation reads for jobs and recurring masters through findBusinessKeyHolder. Jobs
 * return their public type and live status: PENDING, RUNNING, PAUSED or WAITING. Recurring masters
 * return RECURRING with PAUSED when paused and PENDING otherwise. Missing owners and released
 * reservations return empty. Pause, resume and concurrent writes must preserve reservation
 * ownership consistently.
 */
public abstract class AbstractActiveBusinessKeyContract implements JobStoreContractFixture {

  private static String uniqueBusinessKey() {
    return "abk-" + UUID.randomUUID();
  }

  @BeforeEach
  @AfterEach
  void cleanupActiveBusinessKeyFixture() {
    cleanupStore();
  }

  // ───────────────────────── status visibility matrix ─────────────────────────

  @Test
  void findBusinessKeyHolder_runningJob_returnsJob() {
    String bk = uniqueBusinessKey();
    JobEntity saved = persist(jobWithBusinessKey(bk));
    var claimed = store().claimNextBatch(1, "business-key-reader");
    assertEquals(List.of(saved.getId()), claimed.stream().map(JobEntity::getId).toList());

    var result = store().findBusinessKeyHolder(bk);
    assertEquals(JobType.SINGLE, result.orElseThrow().type());
    assertEquals(JobStatus.RUNNING, result.orElseThrow().status());

    assertTrue(result.isPresent(), "RUNNING job should be visible to findBusinessKeyHolder");
    assertEquals(saved.getId(), result.get().id());
  }

  @Test
  void findBusinessKeyHolder_pausedJob_returnsJob() {
    String bk = uniqueBusinessKey();
    JobEntity saved = persist(jobWithBusinessKey(bk));
    assertTrue(
        store().transitionToPaused(saved.getId(), JobStatus.PENDING),
        "PENDING→PAUSED transition precondition");

    var result = store().findBusinessKeyHolder(bk);
    assertEquals(JobType.SINGLE, result.orElseThrow().type());
    assertEquals(JobStatus.PAUSED, result.orElseThrow().status());

    assertTrue(
        result.isPresent(),
        "PAUSED job should remain visible to findBusinessKeyHolder. PAUSED is an active status");
    assertEquals(saved.getId(), result.get().id());
  }

  @Test
  void findBusinessKeyHolder_waitingJob_returnsJob() {
    String bk = uniqueBusinessKey();
    JobEntity waiting = jobWithBusinessKey(bk);
    waiting.setStatus(JobStatus.WAITING);
    waiting.setSignalKey("approval");
    waiting.setSignalTimeout(Instant.now().plusSeconds(600));
    JobEntity saved = persist(waiting);

    var result = store().findBusinessKeyHolder(bk);
    assertEquals(JobType.SINGLE, result.orElseThrow().type());
    assertEquals(JobStatus.WAITING, result.orElseThrow().status());

    assertTrue(
        result.isPresent(),
        "WAITING job should remain visible to findBusinessKeyHolder. WAITING is an active"
            + " status");
    assertEquals(saved.getId(), result.get().id());
  }

  @Test
  void findBusinessKeyHolder_failedTerminal_returnsEmpty() {
    String bk = uniqueBusinessKey();
    JobEntity saved = persist(jobWithBusinessKey(bk));
    UUID id = saved.getId();
    store().compareAndSwapStatus(id, JobStatus.PENDING, JobStatus.RUNNING, null);
    assertTrue(
        store().markJobFailedTerminal(id, "permanent error", 3, null),
        "markJobFailedTerminal precondition");

    var result = store().findBusinessKeyHolder(bk);

    assertFalse(
        result.isPresent(), "FAILED-terminal job should NOT be visible to findBusinessKeyHolder");
  }

  @Test
  void findBusinessKeyHolder_canceledJob_returnsEmpty() {
    String bk = uniqueBusinessKey();
    JobEntity saved = persist(jobWithBusinessKey(bk));
    assertTrue(store().cancelJob(saved.getId()), "cancelJob precondition");

    var result = store().findBusinessKeyHolder(bk);

    assertFalse(result.isPresent(), "CANCELED job should NOT be visible to findBusinessKeyHolder");
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

    var result = store().findBusinessKeyHolder(bk);
    assertEquals(JobType.SINGLE, result.orElseThrow().type());
    assertEquals(JobStatus.PAUSED, result.orElseThrow().status());
    assertTrue(result.isPresent(), "BK should still be reserved after RUNNING→PAUSED");
    assertEquals(id, result.get().id());
  }

  @Test
  void pauseThenResume_keepsBusinessKeyReservedThroughout() {
    String bk = uniqueBusinessKey();
    JobEntity saved = persist(jobWithBusinessKey(bk));
    UUID id = saved.getId();

    store().transitionToPaused(id, JobStatus.PENDING);
    assertEquals(
        new BusinessKeyHolder(id, JobType.SINGLE, JobStatus.PAUSED),
        store().findBusinessKeyHolder(bk).orElseThrow(),
        "BK reserved while PAUSED");

    JobStatus resumed = store().transitionFromPausedAtomic(id);
    assertEquals(JobStatus.PENDING, resumed, "Resumed back to original PENDING status");

    var result = store().findBusinessKeyHolder(bk);
    assertEquals(JobType.SINGLE, result.orElseThrow().type());
    assertEquals(JobStatus.PENDING, result.orElseThrow().status());
    assertTrue(result.isPresent(), "BK still reserved after PAUSED→PENDING resume");
    assertEquals(id, result.get().id());
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
        store().findBusinessKeyHolder(bk).isPresent(),
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
      assertReservationConflict(expected);
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
      assertReservationConflict(expected);
    }
  }

  @Test
  void duplicateEnqueueWhileWaiting_fails() {
    String bk = uniqueBusinessKey();
    JobEntity waiting = jobWithBusinessKey(bk);
    waiting.setStatus(JobStatus.WAITING);
    waiting.setSignalKey("approval");
    waiting.setSignalTimeout(Instant.now().plusSeconds(600));
    persist(waiting);

    try {
      persist(jobWithBusinessKey(bk));
      fail("Duplicate enqueue while WAITING peer holds the business key must throw");
    } catch (RuntimeException expected) {
      assertReservationConflict(expected);
    }
  }

  /** The runtime maps only this store signal to a duplicate business key on submit. */
  private static void assertReservationConflict(Throwable failure) {
    for (Throwable t = failure; t != null; t = t.getCause()) {
      if (t instanceof BusinessKeyReservationConflictException) {
        return;
      }
    }
    fail("Expected BusinessKeyReservationConflictException in the cause chain", failure);
  }

  // ───────────────────────── identity / lookup correctness ─────────────────────────

  @Test
  void unknownBusinessKey_returnsEmpty() {
    persist(jobWithBusinessKey(uniqueBusinessKey()));

    var result = store().findBusinessKeyHolder("definitely-not-a-real-key-" + UUID.randomUUID());

    assertFalse(result.isPresent(), "Unknown business key must yield empty");
  }

  @Test
  void multipleDistinctKeys_returnsMatchingJobOnly() {
    String bkA = uniqueBusinessKey();
    String bkB = uniqueBusinessKey();
    JobEntity jobA = persist(jobWithBusinessKey(bkA));
    JobEntity jobB = persist(jobWithBusinessKey(bkB));

    var resultA = store().findBusinessKeyHolder(bkA);
    var resultB = store().findBusinessKeyHolder(bkB);

    assertTrue(resultA.isPresent());
    assertEquals(
        new BusinessKeyHolder(jobA.getId(), JobType.SINGLE, JobStatus.PENDING),
        resultA.orElseThrow(),
        "BK A must resolve to job A");
    assertTrue(resultB.isPresent());
    assertEquals(
        new BusinessKeyHolder(jobB.getId(), JobType.SINGLE, JobStatus.PENDING),
        resultB.orElseThrow(),
        "BK B must resolve to job B");
  }

  @Test
  void terminalSucceeded_thenReenqueueSameKey_returnsReplacement() {
    String bk = uniqueBusinessKey();
    JobEntity first = persist(jobWithBusinessKey(bk));
    UUID firstId = first.getId();
    store().compareAndSwapStatus(firstId, JobStatus.PENDING, JobStatus.RUNNING, null);
    store().markJobSucceededMinimal(firstId, Instant.now(), Instant.now(), 0L, 0L);

    JobEntity replacement = persist(jobWithBusinessKey(bk));

    var result = store().findBusinessKeyHolder(bk);
    assertEquals(JobType.SINGLE, result.orElseThrow().type());
    assertEquals(JobStatus.PENDING, result.orElseThrow().status());
    assertTrue(result.isPresent(), "Replacement must take ownership of the BK");
    assertEquals(
        replacement.getId(),
        result.get().id(),
        "findBusinessKeyHolder must return the live replacement, not the SUCCEEDED predecessor");
  }

  @Test
  void terminalFailed_thenReenqueueSameKey_returnsReplacement() {
    String bk = uniqueBusinessKey();
    JobEntity first = persist(jobWithBusinessKey(bk));
    UUID firstId = first.getId();
    store().compareAndSwapStatus(firstId, JobStatus.PENDING, JobStatus.RUNNING, null);
    store().markJobFailedTerminal(firstId, "permanent", 3, null);

    JobEntity replacement = persist(jobWithBusinessKey(bk));

    var result = store().findBusinessKeyHolder(bk);
    assertEquals(JobType.SINGLE, result.orElseThrow().type());
    assertEquals(JobStatus.PENDING, result.orElseThrow().status());
    assertTrue(result.isPresent(), "Replacement must take ownership of the BK after FAILED");
    assertEquals(
        replacement.getId(),
        result.get().id(),
        "findBusinessKeyHolder must return the live replacement, not the FAILED predecessor");
  }

  @Test
  void canceled_thenReenqueueSameKey_returnsReplacement() {
    String bk = uniqueBusinessKey();
    JobEntity first = persist(jobWithBusinessKey(bk));
    UUID firstId = first.getId();
    store().cancelJob(firstId);

    JobEntity replacement = persist(jobWithBusinessKey(bk));

    var result = store().findBusinessKeyHolder(bk);
    assertEquals(JobType.SINGLE, result.orElseThrow().type());
    assertEquals(JobStatus.PENDING, result.orElseThrow().status());
    assertTrue(result.isPresent(), "Replacement must take ownership of the BK after CANCELED");
    assertEquals(
        replacement.getId(),
        result.get().id(),
        "findBusinessKeyHolder must return the live replacement, not the CANCELED predecessor");
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
    store().markJobFailedTerminal(id, "transient", 1, null);
    assertTrue(store().resetFailedToPending(id), "resetFailedToPending precondition");
    assertTrue(
        store().findBusinessKeyHolder(bk).isPresent(),
        "BK must remain reserved through the retry path");

    store().compareAndSwapStatus(id, JobStatus.PENDING, JobStatus.RUNNING, null);
    assertTrue(
        store().markJobSucceededMinimal(id, Instant.now(), Instant.now(), 0L, 0L),
        "markJobSucceededMinimal precondition");

    assertEquals(JobStatus.SUCCEEDED, store().getJobStatus(id));
    assertFalse(
        store().findBusinessKeyHolder(bk).isPresent(),
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
    store().markJobFailedTerminal(id, "transient", 1, null);

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
    boolean bkActive = store().findBusinessKeyHolder(bk).isPresent();

    if (finalStatus == JobStatus.PENDING) {
      assertTrue(resetWon.get(), "PENDING outcome implies reset won the race");
      assertTrue(bkActive, "BK must be reserved when reset wins (PENDING is an active status)");
      assertEquals(
          new BusinessKeyHolder(id, JobType.SINGLE, JobStatus.PENDING),
          store().findBusinessKeyHolder(bk).orElseThrow());
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
    boolean bkActive = store().findBusinessKeyHolder(bk).isPresent();
    if (finalStatus == JobStatus.PAUSED) {
      assertTrue(bkActive, "BK must remain reserved when final state is PAUSED");
      assertEquals(
          new BusinessKeyHolder(id, JobType.SINGLE, JobStatus.PAUSED),
          store().findBusinessKeyHolder(bk).orElseThrow());
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

    var resultA = store().findBusinessKeyHolder(bkA);
    var resultB = store().findBusinessKeyHolder(bkB);
    assertFalse(resultA.isPresent(), "BK A must be released after job A SUCCEEDED");
    assertTrue(resultB.isPresent(), "BK B must be untouched by job A's lifecycle");
    assertEquals(
        new BusinessKeyHolder(jobB.getId(), JobType.SINGLE, JobStatus.PENDING),
        resultB.orElseThrow(),
        "BK B still owned by job B");
  }

  protected void insertReservationWithoutOwner(
      String businessKey, UUID ownerId, String ownerTable) {
    throw new TestAbortedException("store does not expose raw reservation writes");
  }

  @Test
  void findBusinessKeyHolder_pendingJob_returnsHolder() {
    String key = uniqueBusinessKey();
    JobEntity saved = persist(jobWithBusinessKey(key));
    assertEquals(
        new BusinessKeyHolder(saved.getId(), JobType.SINGLE, JobStatus.PENDING),
        store().findBusinessKeyHolder(key).orElseThrow());
  }

  @Test
  void findBusinessKeyHolder_succeededJob_returnsEmpty() {
    String key = uniqueBusinessKey();
    JobEntity saved = persist(jobWithBusinessKey(key));
    store().compareAndSwapStatus(saved.getId(), JobStatus.PENDING, JobStatus.RUNNING, null);
    assertTrue(
        store().markJobSucceededMinimal(saved.getId(), Instant.now(), Instant.now(), 0L, 0L));
    assertTrue(store().findBusinessKeyHolder(key).isEmpty());
  }

  @Test
  void findBusinessKeyHolder_deletedJob_returnsEmpty() {
    String key = uniqueBusinessKey();
    JobEntity saved = persist(jobWithBusinessKey(key));
    store().delete(saved.getId());
    assertTrue(store().findBusinessKeyHolder(key).isEmpty());
  }

  @Test
  void findBusinessKeyHolder_missingOwners_returnsEmpty() {
    String queueKey = uniqueBusinessKey();
    String recurringKey = uniqueBusinessKey();
    insertReservationWithoutOwner(
        queueKey, UUID.randomUUID(), BusinessKeyReservations.OWNER_TABLE_QUEUE);
    insertReservationWithoutOwner(
        recurringKey, UUID.randomUUID(), BusinessKeyReservations.OWNER_TABLE_RECURRING);
    assertTrue(store().findBusinessKeyHolder(queueKey).isEmpty());
    assertTrue(store().findBusinessKeyHolder(recurringKey).isEmpty());
  }

  @Test
  void findBusinessKeyHolder_recurringMaster_returnsPendingOrPaused() {
    String key = uniqueBusinessKey();
    UUID id = createRecurringHolder(key);
    assertEquals(
        new BusinessKeyHolder(id, JobType.RECURRING, JobStatus.PENDING),
        store().findBusinessKeyHolder(key).orElseThrow());
    assertTrue(recurringStore().pauseRecurring(id));
    assertEquals(
        new BusinessKeyHolder(id, JobType.RECURRING, JobStatus.PAUSED),
        store().findBusinessKeyHolder(key).orElseThrow());
  }

  @Test
  void findBusinessKeyHolder_exhaustedMaster_returnsEmpty() {
    String key = uniqueBusinessKey();
    UUID id = createRecurringHolder(key);
    assertTrue(recurringStore().cancelRecurringAndArchive(id, ArchiveReason.EXHAUSTED));
    assertTrue(store().findBusinessKeyHolder(key).isEmpty());
  }

  @Test
  void findBusinessKeyHolder_canceledMaster_returnsEmpty() {
    String key = uniqueBusinessKey();
    UUID id = createRecurringHolder(key);
    assertTrue(recurringStore().cancelRecurringAndArchive(id, ArchiveReason.CANCELED));
    assertTrue(store().findBusinessKeyHolder(key).isEmpty());
  }

  @Test
  void findBusinessKeyHolder_masterCanceledByKey_returnsEmpty() {
    String key = uniqueBusinessKey();
    createRecurringHolder(key);
    assertTrue(recurringStore().cancelRecurringJobByBusinessKey(key));
    assertTrue(store().findBusinessKeyHolder(key).isEmpty());
  }

  // ───── hold after a running cancel ─────

  @Test
  void cancelRunningJob_withHold_keepsKeyReserved() {
    // A running cancel becomes terminal while its key stays reserved.
    String bk = uniqueBusinessKey();
    var job = persist(jobWithBusinessKey(bk));
    var claim = store().claimNextBatch(1, "hold-node").get(0);
    assertEquals(job.getId(), claim.getId());
    assertTrue(store().cancelRunningJob(job.getId(), Instant.now().plus(Duration.ofMinutes(10))));
    assertEquals(JobStatus.CANCELED, store().getJobStatus(job.getId()));
    assertReservationConflict(
        assertThrows(RuntimeException.class, () -> persist(jobWithBusinessKey(bk))));
  }

  @Test
  void cancelRunningJob_withHold_lookupReturnsCanceledHolder() {
    // Lookup exposes the canceled owner of a held key.
    String bk = uniqueBusinessKey();
    var job = persist(jobWithBusinessKey(bk));
    var claim = store().claimNextBatch(1, "hold-node").get(0);
    assertEquals(job.getId(), claim.getId());
    assertTrue(store().cancelRunningJob(job.getId(), Instant.now().plus(Duration.ofMinutes(10))));
    assertEquals(
        new BusinessKeyHolder(job.getId(), JobType.SINGLE, JobStatus.CANCELED),
        store().findBusinessKeyHolder(bk).orElseThrow());
  }

  @Test
  void cancelRunningJob_expiredHoldBeforeSweep_stillReturnsCanceledHolder() {
    // Expiry alone does not release a hold.
    String bk = uniqueBusinessKey();
    var job = persist(jobWithBusinessKey(bk));
    var claim = store().claimNextBatch(1, "hold-node").get(0);
    assertEquals(job.getId(), claim.getId());
    assertTrue(store().cancelRunningJob(job.getId(), Instant.now().minusSeconds(60)));
    assertEquals(
        new BusinessKeyHolder(job.getId(), JobType.SINGLE, JobStatus.CANCELED),
        store().findBusinessKeyHolder(bk).orElseThrow());
    assertReservationConflict(
        assertThrows(RuntimeException.class, () -> persist(jobWithBusinessKey(bk))));
  }

  @Test
  void cancelRunningJob_withoutHold_releasesKey() {
    // A null expiry cancels and releases the reservation.
    String bk = uniqueBusinessKey();
    var job = persist(jobWithBusinessKey(bk));
    var claim = store().claimNextBatch(1, "hold-node").get(0);
    assertEquals(job.getId(), claim.getId());
    assertTrue(store().cancelRunningJob(job.getId(), null));
    assertTrue(store().findBusinessKeyHolder(bk).isEmpty());
    persist(jobWithBusinessKey(bk));
  }

  @Test
  void cancelRunningJob_notRunning_returnsFalse() {
    // A pending reservation survives an unmatched running cancel.
    String bk = uniqueBusinessKey();
    var job = persist(jobWithBusinessKey(bk));
    assertFalse(store().cancelRunningJob(job.getId(), Instant.now().plus(Duration.ofMinutes(10))));
    assertEquals(JobStatus.PENDING, store().getJobStatus(job.getId()));
    assertEquals(
        new BusinessKeyHolder(job.getId(), JobType.SINGLE, JobStatus.PENDING),
        store().findBusinessKeyHolder(bk).orElseThrow());
  }

  @Test
  void cancelRunningJob_jobWithoutKey_cancels() {
    // A keyless running job can be canceled without a reservation.
    var job = persist(newPendingJob());
    assertEquals(job.getId(), store().claimNextBatch(1, "hold-node").get(0).getId());
    assertTrue(store().cancelRunningJob(job.getId(), Instant.now().plus(Duration.ofMinutes(10))));
    assertEquals(JobStatus.CANCELED, store().getJobStatus(job.getId()));
  }

  @Test
  void releaseBusinessKeyHold_matchingClaim_freesKey() {
    // Only the matching canceled claim releases its hold.
    String bk = uniqueBusinessKey();
    var job = persist(jobWithBusinessKey(bk));
    var claim = store().claimNextBatch(1, "hold-node").get(0);
    assertEquals(job.getId(), claim.getId());
    assertTrue(store().cancelRunningJob(job.getId(), Instant.now().plus(Duration.ofMinutes(10))));
    assertTrue(store().releaseBusinessKeyHold(job.getId(), claim.getClaimSeq()));
    assertTrue(store().findBusinessKeyHolder(bk).isEmpty());
    persist(jobWithBusinessKey(bk));
  }

  @Test
  void releaseBusinessKeyHold_otherClaim_keepsHold() {
    // A different claim cannot release the hold.
    String bk = uniqueBusinessKey();
    var job = persist(jobWithBusinessKey(bk));
    var claim = store().claimNextBatch(1, "hold-node").get(0);
    assertEquals(job.getId(), claim.getId());
    assertTrue(store().cancelRunningJob(job.getId(), Instant.now().plus(Duration.ofMinutes(10))));
    assertFalse(store().releaseBusinessKeyHold(job.getId(), claim.getClaimSeq() + 1));
    assertEquals(
        new BusinessKeyHolder(job.getId(), JobType.SINGLE, JobStatus.CANCELED),
        store().findBusinessKeyHolder(bk).orElseThrow());
  }

  @Test
  void releaseBusinessKeyHold_liveReservation_isNotTouched() {
    // Claim release never removes a normal running reservation.
    String bk = uniqueBusinessKey();
    var job = persist(jobWithBusinessKey(bk));
    var claim = store().claimNextBatch(1, "hold-node").get(0);
    assertEquals(job.getId(), claim.getId());
    assertFalse(store().releaseBusinessKeyHold(job.getId(), claim.getClaimSeq()));
    assertEquals(
        new BusinessKeyHolder(job.getId(), JobType.SINGLE, JobStatus.RUNNING),
        store().findBusinessKeyHolder(bk).orElseThrow());
  }

  @Test
  void releaseBusinessKeyHolds_deletesOnlyNamedNodes() {
    // Node cleanup removes only named holds and preserves normal reservations.
    String keyA = uniqueBusinessKey();
    var jobA = persist(jobWithBusinessKey(keyA));
    assertEquals(jobA.getId(), store().claimNextBatch(1, "hold-node-a").get(0).getId());
    assertTrue(store().cancelRunningJob(jobA.getId(), Instant.now().plus(Duration.ofMinutes(10))));
    String keyB = uniqueBusinessKey();
    var jobB = persist(jobWithBusinessKey(keyB));
    assertEquals(jobB.getId(), store().claimNextBatch(1, "hold-node-b").get(0).getId());
    assertTrue(store().cancelRunningJob(jobB.getId(), Instant.now().plus(Duration.ofMinutes(10))));
    String keyC = uniqueBusinessKey();
    var jobC = persist(jobWithBusinessKey(keyC));
    assertEquals(jobC.getId(), store().claimNextBatch(1, "hold-node-a").get(0).getId());
    assertEquals(1, store().releaseBusinessKeyHolds(List.of("hold-node-a")));
    assertTrue(store().findBusinessKeyHolder(keyA).isEmpty());
    persist(jobWithBusinessKey(keyA));
    assertEquals(
        new BusinessKeyHolder(jobB.getId(), JobType.SINGLE, JobStatus.CANCELED),
        store().findBusinessKeyHolder(keyB).orElseThrow());
    assertEquals(
        new BusinessKeyHolder(jobC.getId(), JobType.SINGLE, JobStatus.RUNNING),
        store().findBusinessKeyHolder(keyC).orElseThrow());
  }

  @Test
  void deleteExpiredBusinessKeyHolds_deletesOnlyExpired() {
    // Expiry cleanup preserves live holds and normal pending reservations.
    String expiredKey = uniqueBusinessKey();
    var expired = persist(jobWithBusinessKey(expiredKey));
    assertEquals(expired.getId(), store().claimNextBatch(1, "hold-node").get(0).getId());
    assertTrue(store().cancelRunningJob(expired.getId(), Instant.now().minusSeconds(60)));
    String liveKey = uniqueBusinessKey();
    var live = persist(jobWithBusinessKey(liveKey));
    assertEquals(live.getId(), store().claimNextBatch(1, "hold-node").get(0).getId());
    assertTrue(store().cancelRunningJob(live.getId(), Instant.now().plus(Duration.ofMinutes(10))));
    String pendingKey = uniqueBusinessKey();
    var pending = persist(jobWithBusinessKey(pendingKey));
    assertEquals(1, store().deleteExpiredBusinessKeyHolds(Instant.now(), 100));
    assertTrue(store().findBusinessKeyHolder(expiredKey).isEmpty());
    persist(jobWithBusinessKey(expiredKey));
    assertEquals(
        new BusinessKeyHolder(live.getId(), JobType.SINGLE, JobStatus.CANCELED),
        store().findBusinessKeyHolder(liveKey).orElseThrow());
    assertEquals(
        new BusinessKeyHolder(pending.getId(), JobType.SINGLE, JobStatus.PENDING),
        store().findBusinessKeyHolder(pendingKey).orElseThrow());
  }

  @Test
  void deleteExpiredBusinessKeyHolds_respectsLimit() {
    // An expiry sweep deletes no more than its limit.
    for (int i = 0; i < 3; i++) {
      var job = persist(jobWithBusinessKey(uniqueBusinessKey()));
      assertEquals(job.getId(), store().claimNextBatch(1, "hold-node").get(0).getId());
      assertTrue(store().cancelRunningJob(job.getId(), Instant.now().minusSeconds(60)));
    }
    assertEquals(2, store().deleteExpiredBusinessKeyHolds(Instant.now(), 2));
    assertEquals(1, store().deleteExpiredBusinessKeyHolds(Instant.now(), 2));
  }

  @Test
  void delete_heldJob_freesKey() {
    // Deleting the canceled job also deletes its hold.
    String bk = uniqueBusinessKey();
    var job = persist(jobWithBusinessKey(bk));
    var claim = store().claimNextBatch(1, "hold-node").get(0);
    assertEquals(job.getId(), claim.getId());
    assertTrue(store().cancelRunningJob(job.getId(), Instant.now().plus(Duration.ofMinutes(10))));
    store().delete(job.getId());
    assertTrue(store().findBusinessKeyHolder(bk).isEmpty());
    persist(jobWithBusinessKey(bk));
  }

  private UUID createRecurringHolder(String key) {
    var recurring = recurringStore();
    UUID id = UUID.randomUUID();
    recurring.createRecurring(
        new RecurringJobDefinition(
            id,
            "0 * * * * ?",
            "UTC",
            Instant.now().plusSeconds(60),
            false,
            null,
            2,
            0,
            BackoffPolicy.NONE,
            0,
            0,
            newPendingJob().getPayload(),
            null,
            null,
            key,
            null,
            null,
            Instant.now(),
            null,
            false,
            RecurringMisfirePolicy.defaults()));
    return id;
  }

  private JobEntity jobWithBusinessKey(String bk) {
    JobEntity job = newPendingJob();
    job.setBusinessKey(bk);
    return job;
  }
}
