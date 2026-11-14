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
import run.ratchet.api.exception.DuplicateIdempotencyKeyException;
import run.ratchet.store.dto.PartitionGenerationWrite;
import run.ratchet.store.entity.JobEntity;

/**
 * Verifies idempotency-ledger conflicts write nothing and leave the caller's transaction usable.
 */
public abstract class AbstractIdempotencyKeyReservationContract implements JobStoreContractFixture {

  private static final String NEEDS_OWNER_TRANSACTION =
      """
      supportsOwnerTransaction() is false on this contract class. A store whose create joins \
      the caller's transaction overrides it and inOwnerTransaction(Runnable) here.\
      """;

  private static final String NEEDS_LOCK_WAIT_HOOKS =
      """
      awaitBlockedSession(Object, Object, Duration) returned false on this contract class. \
      Override it and ownerSessionId() here to observe database lock waits.\
      """;

  // Each step the main thread runs before it releases a held session waits at most 30 s, and a
  // test runs at most four of them, so a held session waits longer than their sum.
  private static final long RELEASE_TIMEOUT_SECONDS = 150;

  @BeforeEach
  @AfterEach
  void cleanupIdempotencyKeyReservationFixture() {
    cleanupStore();
  }

  protected boolean plainReadsUseTransactionSnapshot() {
    return false;
  }

  @Test
  void conflict_writesNothingForTheLoser() {
    String key = uniqueKey();
    JobEntity holder = create(jobWithIdempotencyKey(key));
    JobEntity loser = jobWithIdempotencyKey(key);

    assertThrows(DuplicateIdempotencyKeyException.class, () -> create(loser));

    assertAbsent(loser);
    assertOwner(key, holder);
  }

  @Test
  void conflict_leavesOwnerTransactionUsable() {
    assumeTrue(supportsOwnerTransaction(), NEEDS_OWNER_TRANSACTION);
    String key = uniqueKey();
    JobEntity holder = create(jobWithIdempotencyKey(key));
    JobEntity loser = jobWithIdempotencyKey(key);
    JobEntity unrelated = jobWithIdempotencyKey(null);

    inOwnerTransaction(
        () -> {
          assertThrows(DuplicateIdempotencyKeyException.class, () -> create(loser));
          create(unrelated);
        });

    assertTrue(store().findById(unrelated.getId()).isPresent());
    assertAbsent(loser);
    assertOwner(key, holder);
  }

  @Test
  void partitionConflict_writesNothing() {
    String key = uniqueKey();
    JobEntity holder = create(jobWithIdempotencyKey(key));
    var write = AbstractPartitionStoreContract.generation(this, uniqueKey(), 2);
    write.parent().setIdempotencyKey(key);

    assertThrows(
        DuplicateIdempotencyKeyException.class, () -> partitionStore().createOrAdvance(write));

    assertPartitionAbsent(write);
    assertOwner(key, holder);
  }

  @Test
  void partitionConflict_leavesOwnerTransactionUsable() {
    assumeTrue(supportsOwnerTransaction(), NEEDS_OWNER_TRANSACTION);
    String key = uniqueKey();
    JobEntity holder = create(jobWithIdempotencyKey(key));
    var write = AbstractPartitionStoreContract.generation(this, uniqueKey(), 2);
    write.parent().setIdempotencyKey(key);
    JobEntity unrelated = jobWithIdempotencyKey(null);

    inOwnerTransaction(
        () -> {
          assertThrows(
              DuplicateIdempotencyKeyException.class,
              () -> partitionStore().createOrAdvance(write));
          create(unrelated);
        });

    assertTrue(store().findById(unrelated.getId()).isPresent());
    assertPartitionAbsent(write);
    assertOwner(key, holder);
  }

  @Test
  void partitionSuccess_reservesParentKeyOnce() {
    var write = AbstractPartitionStoreContract.generation(this, uniqueKey(), 2);
    String key = uniqueKey();
    write.parent().setIdempotencyKey(key);

    assertTrue(partitionStore().createOrAdvance(write));

    assertOwner(key, write.parent());
    assertTrue(store().findById(write.parent().getId()).isPresent());
    assertEquals(2, partitionStore().findPartitions(write.set().getSetId(), -1, 10).size());
    assertThrows(DuplicateIdempotencyKeyException.class, () -> create(jobWithIdempotencyKey(key)));
    assertOwner(key, write.parent());
  }

  @Test
  void rejectedPartitionVersion_doesNotLeaveAReservedKeyAfterRollback() {
    var original = AbstractPartitionStoreContract.generation(this, uniqueKey(), 2);
    original.parent().setIdempotencyKey(uniqueKey());
    assertTrue(partitionStore().createOrAdvance(original));
    var fresh = AbstractPartitionStoreContract.generation(this, uniqueKey(), 0);
    fresh.parent().setIdempotencyKey(uniqueKey());
    var stale =
        new PartitionGenerationWrite(
            original.set(), -1, fresh.rows(), fresh.parent(), fresh.batch());
    var rollback = new IllegalStateException("Rejected generation must roll back");
    Runnable reject =
        () -> {
          assertFalse(partitionStore().createOrAdvance(stale));
          if (supportsOwnerTransaction()) throw rollback;
        };
    if (supportsOwnerTransaction()) {
      assertSame(
          rollback, assertThrows(IllegalStateException.class, () -> inOwnerTransaction(reject)));
    } else {
      reject.run();
    }
    assertTrue(
        store().findOriginalJobIdByIdempotencyKey(fresh.parent().getIdempotencyKey()).isEmpty());
    assertTrue(store().findById(fresh.parent().getId()).isEmpty());
    assertOwner(original.parent().getIdempotencyKey(), original.parent());
  }

  private void assertPartitionAbsent(PartitionGenerationWrite write) {
    assertTrue(partitionStore().findPartitionSet(write.set().getSetKey()).isEmpty());
    assertTrue(partitionStore().findPartitionSetById(write.set().getSetId()).isEmpty());
    assertTrue(partitionStore().findPartitions(write.set().getSetId(), -1, 10).isEmpty());
    assertTrue(store().findById(write.parent().getId()).isEmpty());
    assertTrue(batchStore().findBatchById(write.parent().getId()).isEmpty());
  }

  @Test
  void lateCommittedHolder_conflictLeavesOwnerTransactionUsable() throws Exception {
    assumeTrue(supportsOwnerTransaction(), NEEDS_OWNER_TRANSACTION);
    String key = uniqueKey();
    JobEntity holder = jobWithIdempotencyKey(key);
    JobEntity loser = jobWithIdempotencyKey(key);
    JobEntity unrelated = jobWithIdempotencyKey(null);
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
              assertTrue(store().findOriginalJobIdByIdempotencyKey(key).isEmpty());
            }
            assertThrows(DuplicateIdempotencyKeyException.class, () -> create(loser));
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
    JobEntity winner = jobWithIdempotencyKey(key);
    JobEntity loser = jobWithIdempotencyKey(key);
    JobEntity unrelated = jobWithIdempotencyKey(null);
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
                        } catch (DuplicateIdempotencyKeyException e) {
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

  private JobEntity jobWithIdempotencyKey(String key) {
    JobEntity job = newPendingJob();
    job.setBusinessKey("bk-" + UUID.randomUUID());
    job.setIdempotencyKey(key == null ? uniqueKey() : key);
    return job;
  }

  private void assertAbsent(JobEntity loser) {
    assertTrue(store().findById(loser.getId()).isEmpty(), "No loser job row");
    assertTrue(
        store().findBusinessKeyHolder(loser.getBusinessKey()).isEmpty(),
        "No loser business-key reservation");
  }

  private void assertOwner(String key, JobEntity holder) {
    assertEquals(holder.getId(), store().findOriginalJobIdByIdempotencyKey(key).orElseThrow());
  }

  private static String uniqueKey() {
    return UUID.randomUUID().toString();
  }
}
