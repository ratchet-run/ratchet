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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.BusinessKeyHolder;
import run.ratchet.api.JobStatus;
import run.ratchet.api.JobType;
import run.ratchet.api.RecurringMisfirePolicy;
import run.ratchet.api.exception.DuplicateIdempotencyKeyException;
import run.ratchet.store.BusinessKeyReservationConflictException;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.id.UuidV7Factory;
import run.ratchet.store.spi.RecurringJobDefinition;

/**
 * Verifies write-free reservation conflicts and the caller's ability to continue its transaction.
 */
public abstract class AbstractBusinessKeyReservationContract implements JobStoreContractFixture {

  private static final String NEEDS_OWNER_TRANSACTION =
      """
      supportsOwnerTransaction() is false on this contract class. A store whose create joins \
      the caller's transaction overrides it and inOwnerTransaction(Runnable) here.""";

  private static final String NEEDS_LOCK_WAIT_HOOKS =
      """
      awaitBlockedSession(Object, Object, Duration) returned false on this contract class. \
      Override it and ownerSessionId() here to observe database lock waits.""";

  // Each step the main thread runs before it releases a held session waits at most 30 s, and a
  // test runs at most four of them, so a held session waits longer than their sum.
  private static final long RELEASE_TIMEOUT_SECONDS = 150;

  @BeforeEach
  @AfterEach
  void cleanupBusinessKeyReservationFixture() {
    cleanupStore();
  }

  protected boolean plainReadsUseTransactionSnapshot() {
    return false;
  }

  @Test
  void conflict_writesNothingForTheLoser() {
    String key = uniqueKey();
    JobEntity holder = create(jobWithBusinessKey(key));
    JobEntity loser = jobWithBusinessKey(key);

    assertThrows(BusinessKeyReservationConflictException.class, () -> create(loser));

    assertAbsent(loser);
    assertOwner(key, holder);
  }

  @Test
  void conflict_leavesOwnerTransactionUsable() {
    assumeTrue(supportsOwnerTransaction(), NEEDS_OWNER_TRANSACTION);
    String key = uniqueKey();
    JobEntity holder = create(jobWithBusinessKey(key));
    JobEntity loser = jobWithBusinessKey(key);
    JobEntity unrelated = jobWithBusinessKey(null);

    inOwnerTransaction(
        () -> {
          assertThrows(BusinessKeyReservationConflictException.class, () -> create(loser));
          create(unrelated);
        });

    assertTrue(store().findById(unrelated.getId()).isPresent());
    assertAbsent(loser);
    assertOwner(key, holder);
  }

  @Test
  void recurringHolder_conflictWritesNothing() {
    var recurring = recurringStore();
    String key = uniqueKey();
    UUID holderId = UUID.randomUUID();
    recurring.createRecurring(
        new RecurringJobDefinition(
            holderId,
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
    JobEntity loser = jobWithBusinessKey(key);

    assertThrows(BusinessKeyReservationConflictException.class, () -> create(loser));

    assertAbsent(loser);
    assertEquals(
        new BusinessKeyHolder(holderId, JobType.RECURRING, JobStatus.PENDING),
        store().findBusinessKeyHolder(key).orElseThrow());
    // Recurring definitions live outside the one-off job table.
    assertEquals(holderId, recurring.findRecurringByBusinessKey(key).orElseThrow().id());
  }

  @Test
  void lateCommittedHolder_conflictLeavesOwnerTransactionUsable() throws Exception {
    assumeTrue(supportsOwnerTransaction(), NEEDS_OWNER_TRANSACTION);
    String key = uniqueKey();
    JobEntity holder = jobWithBusinessKey(key);
    JobEntity loser = jobWithBusinessKey(key);
    JobEntity unrelated = jobWithBusinessKey(null);
    var worker = Executors.newSingleThreadExecutor();
    try {
      inOwnerTransaction(
          () -> {
            store().findById(UUID.randomUUID());
            var committed = worker.submit(() -> create(holder));
            try {
              committed.get(30, TimeUnit.SECONDS);
            } catch (Exception e) {
              throw new IllegalStateException("Late holder did not commit", e);
            }
            if (plainReadsUseTransactionSnapshot()) {
              assertTrue(store().findBusinessKeyHolder(key).isEmpty());
            }
            assertThrows(BusinessKeyReservationConflictException.class, () -> create(loser));
            create(unrelated);
          });
      assertTrue(store().findById(unrelated.getId()).isPresent());
      assertAbsent(loser);
      assertOwner(key, holder);
    } finally {
      worker.shutdownNow();
      assertTrue(worker.awaitTermination(30, TimeUnit.SECONDS));
    }
  }

  @Test
  void failureAfterReserve_neverCommitsTheReservation() {
    JobEntity original = create(jobWithBusinessKey(uniqueKey()));
    // With both keys held the business-key conflict wins, so create reserves the business key
    // before it writes the idempotency key.
    JobEntity bothHeld = jobWithBusinessKey(original.getBusinessKey());
    bothHeld.setIdempotencyKey(original.getIdempotencyKey());
    assertThrows(BusinessKeyReservationConflictException.class, () -> create(bothHeld));

    String key = uniqueKey();
    JobEntity duplicate = jobWithBusinessKey(key);
    duplicate.setIdempotencyKey(original.getIdempotencyKey());
    try {
      inOwnerTransaction(
          () -> assertThrows(DuplicateIdempotencyKeyException.class, () -> create(duplicate)));
    } catch (RuntimeException expectedCommitFailure) {
      // Providers may mark the owner transaction rollback-only after the idempotency conflict.
    }

    assertTrue(store().findBusinessKeyHolder(key).isEmpty());
    JobEntity replacement = create(jobWithBusinessKey(key));
    assertOwner(key, replacement);
  }

  @Test
  void reservationIsHeldWhileIdempotencyWriteWaits() throws Exception {
    assumeTrue(supportsOwnerTransaction(), NEEDS_OWNER_TRANSACTION);
    String idempotencyKey = UUID.randomUUID().toString();
    String key = uniqueKey();
    JobEntity holder = jobWithBusinessKey(null);
    holder.setIdempotencyKey(idempotencyKey);
    JobEntity duplicate = jobWithBusinessKey(key);
    duplicate.setIdempotencyKey(idempotencyKey);
    JobEntity probe = jobWithBusinessKey(key);
    var holderSession = new AtomicReference<Object>();
    var duplicateSession = new AtomicReference<Object>();
    var probeSession = new AtomicReference<Object>();
    var holderWritten = new CountDownLatch(1);
    var commitHolder = new CountDownLatch(1);
    var duplicateStarted = new CountDownLatch(1);
    var probeStarted = new CountDownLatch(1);
    var workers = Executors.newFixedThreadPool(3);
    try {
      var holding =
          workers.submit(
              () ->
                  inOwnerTransaction(
                      () -> {
                        create(holder);
                        holderSession.set(ownerSessionId());
                        holderWritten.countDown();
                        await(commitHolder);
                      }));
      assertTrue(holderWritten.await(30, TimeUnit.SECONDS), "Holder did not write its key");
      var duplicating =
          workers.submit(
              () -> {
                try {
                  inOwnerTransaction(
                      () -> {
                        duplicateSession.set(ownerSessionId());
                        duplicateStarted.countDown();
                        assertThrows(
                            DuplicateIdempotencyKeyException.class, () -> create(duplicate));
                      });
                } catch (RuntimeException expectedCommitFailure) {
                  // Providers may mark the owner transaction rollback-only after the
                  // idempotency conflict.
                }
              });
      assertTrue(duplicateStarted.await(30, TimeUnit.SECONDS), "Duplicate did not start");
      assumeTrue(
          observeBlocked(duplicateSession.get(), holderSession.get()), NEEDS_LOCK_WAIT_HOOKS);
      var probing =
          workers.submit(
              () ->
                  inOwnerTransaction(
                      () -> {
                        probeSession.set(ownerSessionId());
                        probeStarted.countDown();
                        create(probe);
                      }));
      assertTrue(probeStarted.await(30, TimeUnit.SECONDS), "Probe did not start");
      // The duplicate is still waiting on the idempotency key, so it can block the probe only if
      // it reserved the business key before it wrote the idempotency key.
      assertTrue(
          observeBlocked(probeSession.get(), duplicateSession.get()),
          "Probe must be blocked by the duplicate's business-key reservation");
      commitHolder.countDown();
      holding.get(30, TimeUnit.SECONDS);
      duplicating.get(30, TimeUnit.SECONDS);
      probing.get(30, TimeUnit.SECONDS);

      assertTrue(store().findById(holder.getId()).isPresent());
      assertTrue(store().findById(duplicate.getId()).isEmpty());
      assertOwner(key, probe);
    } finally {
      commitHolder.countDown();
      workers.shutdownNow();
      assertTrue(workers.awaitTermination(30, TimeUnit.SECONDS));
    }
  }

  @Test
  void bulkReservationIsHeldWhileIdempotencyWriteWaits() throws Exception {
    assumeTrue(supportsOwnerTransaction(), NEEDS_OWNER_TRANSACTION);
    String idempotencyKey = UUID.randomUUID().toString();
    String key = uniqueKey();
    JobEntity holder = jobWithBusinessKey(null);
    holder.setIdempotencyKey(idempotencyKey);
    JobEntity duplicate = jobWithBusinessKey(key);
    duplicate.setIdempotencyKey(idempotencyKey);
    duplicate.setId(UuidV7Factory.create());
    JobEntity probe = jobWithBusinessKey(key);
    var holderSession = new AtomicReference<Object>();
    var duplicateSession = new AtomicReference<Object>();
    var probeSession = new AtomicReference<Object>();
    var holderWritten = new CountDownLatch(1);
    var commitHolder = new CountDownLatch(1);
    var duplicateStarted = new CountDownLatch(1);
    var probeStarted = new CountDownLatch(1);
    var workers = Executors.newFixedThreadPool(3);
    try {
      var holding =
          workers.submit(
              () ->
                  inOwnerTransaction(
                      () -> {
                        create(holder);
                        holderSession.set(ownerSessionId());
                        holderWritten.countDown();
                        await(commitHolder);
                      }));
      assertTrue(holderWritten.await(30, TimeUnit.SECONDS), "Holder did not write its key");
      var duplicating =
          workers.submit(
              () ->
                  assertThrows(
                      RuntimeException.class,
                      () ->
                          inOwnerTransaction(
                              () -> {
                                duplicateSession.set(ownerSessionId());
                                duplicateStarted.countDown();
                                store().bulkInsert(List.of(duplicate));
                              })));
      assertTrue(duplicateStarted.await(30, TimeUnit.SECONDS), "Duplicate did not start");
      assumeTrue(
          observeBlocked(duplicateSession.get(), holderSession.get()), NEEDS_LOCK_WAIT_HOOKS);
      var probing =
          workers.submit(
              () ->
                  inOwnerTransaction(
                      () -> {
                        probeSession.set(ownerSessionId());
                        probeStarted.countDown();
                        create(probe);
                      }));
      assertTrue(probeStarted.await(30, TimeUnit.SECONDS), "Probe did not start");
      // The duplicate is still waiting on the idempotency key, so it can block the probe only if
      // bulkInsert reserved the business key before it wrote the idempotency key.
      assertTrue(
          observeBlocked(probeSession.get(), duplicateSession.get()),
          "Probe must be blocked by the bulk insert's business-key reservation");
      commitHolder.countDown();
      holding.get(30, TimeUnit.SECONDS);
      duplicating.get(30, TimeUnit.SECONDS);
      probing.get(30, TimeUnit.SECONDS);

      assertTrue(store().findById(holder.getId()).isPresent());
      assertTrue(store().findById(duplicate.getId()).isEmpty());
      assertOwner(key, probe);
    } finally {
      commitHolder.countDown();
      workers.shutdownNow();
      assertTrue(workers.awaitTermination(30, TimeUnit.SECONDS));
    }
  }

  /**
   * A create and a bulk insert that share both keys must not deadlock. A gate holds the bulk job's
   * id, so the bulk insert stops after its key writes until the create waits on it. The create must
   * then see the business-key conflict.
   */
  @Test
  void createAndBulkInsertOnBothKeys_createSeesConflict() throws Exception {
    assumeTrue(supportsOwnerTransaction(), NEEDS_OWNER_TRANSACTION);
    String key = uniqueKey();
    String idempotencyKey = UUID.randomUUID().toString();
    JobEntity gate = jobWithBusinessKey(null);
    gate.setId(UuidV7Factory.create());
    JobEntity bulk = jobWithBusinessKey(key);
    bulk.setId(gate.getId());
    bulk.setIdempotencyKey(idempotencyKey);
    JobEntity single = jobWithBusinessKey(key);
    single.setId(UuidV7Factory.create());
    single.setIdempotencyKey(idempotencyKey);
    var gateSession = new AtomicReference<Object>();
    var bulkSession = new AtomicReference<Object>();
    var singleSession = new AtomicReference<Object>();
    var gateWritten = new CountDownLatch(1);
    var releaseGate = new CountDownLatch(1);
    var bulkStarted = new CountDownLatch(1);
    var singleStarted = new CountDownLatch(1);
    var conflict = new AtomicBoolean();
    var rollbackMarker = new RuntimeException("Roll back the gate");
    var workers = Executors.newFixedThreadPool(3);
    try {
      var gating =
          workers.submit(
              () -> {
                try {
                  inOwnerTransaction(
                      () -> {
                        create(gate);
                        gateSession.set(ownerSessionId());
                        gateWritten.countDown();
                        await(releaseGate);
                        throw rollbackMarker;
                      });
                } catch (RuntimeException e) {
                  if (e != rollbackMarker) {
                    throw e;
                  }
                }
              });
      assertTrue(gateWritten.await(30, TimeUnit.SECONDS), "Gate did not write its job");
      var inserting =
          workers.submit(
              () ->
                  inOwnerTransaction(
                      () -> {
                        bulkSession.set(ownerSessionId());
                        bulkStarted.countDown();
                        store().bulkInsert(List.of(bulk));
                      }));
      assertTrue(bulkStarted.await(30, TimeUnit.SECONDS), "Bulk insert did not start");
      assumeTrue(observeBlocked(bulkSession.get(), gateSession.get()), NEEDS_LOCK_WAIT_HOOKS);
      // The bulk insert is waiting on the gate's job row, after its key writes.
      var creating =
          workers.submit(
              () ->
                  inOwnerTransaction(
                      () -> {
                        singleSession.set(ownerSessionId());
                        singleStarted.countDown();
                        try {
                          create(single);
                        } catch (BusinessKeyReservationConflictException e) {
                          conflict.set(true);
                        }
                      }));
      assertTrue(singleStarted.await(30, TimeUnit.SECONDS), "Create did not start");
      assertTrue(
          observeBlocked(singleSession.get(), bulkSession.get()),
          "The create must wait for the bulk insert's key writes");
      releaseGate.countDown();
      gating.get(30, TimeUnit.SECONDS);
      inserting.get(30, TimeUnit.SECONDS);
      creating.get(30, TimeUnit.SECONDS);

      assertTrue(conflict.get(), "The create must see the bulk job's business key");
      assertTrue(store().findById(bulk.getId()).isPresent());
      assertTrue(store().findById(single.getId()).isEmpty());
      assertEquals(
          Optional.of(bulk.getId()), store().findOriginalJobIdByIdempotencyKey(idempotencyKey));
      assertOwner(key, bulk);
    } finally {
      releaseGate.countDown();
      workers.shutdownNow();
      assertTrue(workers.awaitTermination(30, TimeUnit.SECONDS));
    }
  }

  @Test
  void race_winnerCommits_loserSeesConflict() throws Exception {
    race(false);
  }

  @Test
  void race_winnerRollsBack_loserSucceeds() throws Exception {
    race(true);
  }

  private void race(boolean rollback) throws Exception {
    assumeTrue(supportsOwnerTransaction(), NEEDS_OWNER_TRANSACTION);
    String key = uniqueKey();
    JobEntity winner = jobWithBusinessKey(key);
    JobEntity loser = jobWithBusinessKey(key);
    JobEntity unrelated = jobWithBusinessKey(null);
    var created = new CountDownLatch(1);
    var release = new CountDownLatch(1);
    var attempting = new CountDownLatch(1);
    var conflict = new AtomicBoolean();
    var winnerSession = new AtomicReference<Object>();
    var loserSession = new AtomicReference<Object>();
    var rollbackMarker = new RuntimeException("Roll back the winner");
    var workers = Executors.newFixedThreadPool(2);
    try {
      var first =
          workers.submit(
              () -> {
                try {
                  inOwnerTransaction(
                      () -> {
                        create(winner);
                        winnerSession.set(ownerSessionId());
                        created.countDown();
                        await(release);
                        if (rollback) {
                          throw rollbackMarker;
                        }
                      });
                  assertFalse(rollback, "Winner must roll back when requested");
                } catch (RuntimeException e) {
                  assertTrue(rollback, "Winner should have committed");
                  assertSame(rollbackMarker, e);
                }
              });
      assertTrue(created.await(30, TimeUnit.SECONDS), "Winner did not reserve the key");
      var second =
          workers.submit(
              () ->
                  inOwnerTransaction(
                      () -> {
                        loserSession.set(ownerSessionId());
                        attempting.countDown();
                        try {
                          create(loser);
                        } catch (BusinessKeyReservationConflictException e) {
                          conflict.set(true);
                        }
                        create(unrelated);
                      }));
      assertTrue(attempting.await(30, TimeUnit.SECONDS), "Loser did not start");
      // The hook confirms the loser is blocked by the winner before the winner ends.
      assumeTrue(observeBlocked(loserSession.get(), winnerSession.get()), NEEDS_LOCK_WAIT_HOOKS);
      assertThrows(
          TimeoutException.class,
          () -> second.get(500, TimeUnit.MILLISECONDS),
          "Loser must wait for the uncommitted reservation");
      release.countDown();
      first.get(30, TimeUnit.SECONDS);
      second.get(30, TimeUnit.SECONDS);

      assertTrue(store().findById(unrelated.getId()).isPresent());
      assertEquals(!rollback, conflict.get());
      if (rollback) {
        assertTrue(store().findById(winner.getId()).isEmpty());
        assertTrue(store().findById(loser.getId()).isPresent());
        assertOwner(key, loser);
      } else {
        assertAbsent(loser);
        assertOwner(key, winner);
      }
    } finally {
      release.countDown();
      workers.shutdownNow();
      assertTrue(workers.awaitTermination(30, TimeUnit.SECONDS));
    }
  }

  private static void await(CountDownLatch latch) {
    try {
      if (!latch.await(RELEASE_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
        throw new IllegalStateException("Timed out waiting for release");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted waiting for release", e);
    }
  }

  /** Returns false only when the fixture cannot observe lock waits; the caller then skips. */
  private boolean observeBlocked(Object waiter, Object holder) {
    boolean observed = awaitBlockedSession(waiter, holder, Duration.ofSeconds(30));
    if (observed && (waiter == null || holder == null)) {
      // A hook that reports a wait without both session ids has not checked who is waiting.
      throw new IllegalStateException(
          "awaitBlockedSession reported a lock wait, but ownerSessionId() returned null;"
              + " override ownerSessionId() on this contract class");
    }
    return observed;
  }

  private JobEntity create(JobEntity job) {
    return store().create(job);
  }

  private JobEntity jobWithBusinessKey(String key) {
    JobEntity job = newPendingJob();
    job.setBusinessKey(key);
    job.setIdempotencyKey(UUID.randomUUID().toString());
    return job;
  }

  private void assertAbsent(JobEntity loser) {
    assertTrue(store().findById(loser.getId()).isEmpty(), "No loser job row");
    assertTrue(
        store().findOriginalJobIdByIdempotencyKey(loser.getIdempotencyKey()).isEmpty(),
        "No loser idempotency reservation");
  }

  private void assertOwner(String key, JobEntity holder) {
    assertEquals(holder.getId(), store().findBusinessKeyHolder(key).orElseThrow().id());
  }

  private static String uniqueKey() {
    return "bkres-" + UUID.randomUUID();
  }
}
