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

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobStatus;
import run.ratchet.api.WorkflowCondition;
import run.ratchet.store.converter.EncryptionHolder;
import run.ratchet.store.dto.JobCompletionPlan;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.WorkflowConditionEntity;
import run.ratchet.store.spi.JobStateStore.StateWriteResult;
import run.ratchet.store.spi.JobStateStore.VersionedState;
import run.ratchet.store.spi.TerminalOutcome;
import run.ratchet.store.util.ExtensionValidation;
import run.ratchet.tck.store.AbstractPayloadEncryptionStoreContract.RecordingEngine;
import run.ratchet.tck.store.AbstractPayloadEncryptionStoreContract.SingleKeyProvider;

/** Contract for resolving transactional completion and reading its current planning inputs. */
public abstract class AbstractTransactionalCompletionStoreContract
    implements JobStoreContractFixture {

  @BeforeEach
  @AfterEach
  void cleanStore() {
    cleanupStore();
  }

  @Test
  void runningClaimIsRolledBackButAnotherClaimIsSuperseded() {
    var capability = transactionalCompletionStore();
    JobEntity job = runningJob();
    assertTrue(job.getClaimSeq() > 0);
    assertEquals(
        TerminalOutcome.ROLLED_BACK, capability.terminalOutcome(job.getId(), job.getClaimSeq()));
    assertEquals(
        TerminalOutcome.SUPERSEDED, capability.terminalOutcome(job.getId(), job.getClaimSeq() + 1));
  }

  @Test
  void resetAndReclaimedAttemptsAreSuperseded() {
    var capability = transactionalCompletionStore();
    JobEntity first = runningJob();
    store().resetRunningJobs(first.getPickedBy());
    assertEquals(JobStatus.PENDING, store().findById(first.getId()).orElseThrow().getStatus());
    assertEquals(
        TerminalOutcome.SUPERSEDED, capability.terminalOutcome(first.getId(), first.getClaimSeq()));
    assertTrue(store().tryPickUpJob(first.getId(), "completion-node"));
    JobEntity next = store().findById(first.getId()).orElseThrow();
    assertTrue(next.getClaimSeq() > first.getClaimSeq());
    assertEquals(
        TerminalOutcome.SUPERSEDED, capability.terminalOutcome(first.getId(), first.getClaimSeq()));
    assertEquals(
        TerminalOutcome.ROLLED_BACK, capability.terminalOutcome(next.getId(), next.getClaimSeq()));
  }

  @Test
  void fencedSuccessWithResultCopiesClaimToColdRow() {
    assertSuccessCopiesClaim("\"result\"");
  }

  @Test
  void fencedSuccessWithoutResultCopiesClaimToColdRow() {
    assertSuccessCopiesClaim(null);
  }

  @Test
  void minimalSuccessCopiesClaimToColdRow() {
    var capability = transactionalCompletionStore();
    JobEntity job = runningJob();
    Instant now = Instant.now();
    assertTrue(store().markJobSucceededMinimal(job.getId(), now, now, 0L, 0L));
    assertTrue(store().currentClaim(job.getId()).isEmpty());
    assertEquals(
        TerminalOutcome.COMMITTED, capability.terminalOutcome(job.getId(), job.getClaimSeq()));
    assertEquals(
        TerminalOutcome.SUPERSEDED, capability.terminalOutcome(job.getId(), job.getClaimSeq() + 1));
  }

  private void assertSuccessCopiesClaim(String result) {
    var capability = transactionalCompletionStore();
    JobEntity first = runningJob();
    store().resetRunningJobs(first.getPickedBy());
    assertTrue(store().tryPickUpJob(first.getId(), "completion-node"));
    JobEntity job = store().findById(first.getId()).orElseThrow();
    assertTrue(job.getClaimSeq() > first.getClaimSeq());
    assertTrue(store().commitCompletion(plan(job, JobStatus.SUCCEEDED, result)).committed());
    assertTrue(store().currentClaim(job.getId()).isEmpty(), "success removes the hot row");
    // findById reads claim_seq from the queue row, which success deletes; terminalOutcome reads the
    // cold row, so COMMITTED below proves the copy.
    assertEquals(
        TerminalOutcome.COMMITTED, capability.terminalOutcome(job.getId(), job.getClaimSeq()));
    assertEquals(
        TerminalOutcome.SUPERSEDED, capability.terminalOutcome(job.getId(), first.getClaimSeq()));
  }

  @Test
  void failedAttemptIsSuperseded() {
    var capability = transactionalCompletionStore();
    JobEntity job = runningJob();
    assertTrue(store().commitCompletion(plan(job, JobStatus.FAILED, null)).committed());
    assertTrue(store().currentClaim(job.getId()).isEmpty());
    assertEquals(
        TerminalOutcome.SUPERSEDED, capability.terminalOutcome(job.getId(), job.getClaimSeq()));
  }

  @Test
  void canceledJobIsCanceledOnlyForTheClaimItEnded() {
    var capability = transactionalCompletionStore();
    JobEntity job = runningJob();
    assertTrue(store().cancelJob(job.getId()));
    assertTrue(store().currentClaim(job.getId()).isEmpty());
    assertEquals(
        TerminalOutcome.CANCELED, capability.terminalOutcome(job.getId(), job.getClaimSeq()));
    assertEquals(
        TerminalOutcome.SUPERSEDED, capability.terminalOutcome(job.getId(), job.getClaimSeq() + 1));
  }

  @Test
  void pendingAndMissingJobsAreSuperseded() {
    var capability = transactionalCompletionStore();
    JobEntity pending = persist(newPendingJob());
    assertEquals(
        TerminalOutcome.SUPERSEDED,
        capability.terminalOutcome(pending.getId(), pending.getClaimSeq()));
    assertEquals(TerminalOutcome.SUPERSEDED, capability.terminalOutcome(UUID.randomUUID(), 0));
  }

  @Test
  void planningJobReadsMatchPlainReadsIncludingEmptyResults() {
    var capability = transactionalCompletionStore();
    JobEntity job = persist(newPendingJob("completion-tag"));
    JobEntity other = persist(newPendingJob());
    UUID missing = UUID.randomUUID();
    assertEquals(store().findById(job.getId()), capability.findByIdForCompletion(job.getId()));
    assertEquals(
        store().findById(job.getId()).orElseThrow().getTags(),
        capability.findByIdForCompletion(job.getId()).orElseThrow().getTags());
    assertEquals(store().findById(missing), capability.findByIdForCompletion(missing));
    List<UUID> ids = List.of(job.getId(), other.getId(), missing);
    assertEquals(
        new HashSet<>(store().findByIds(ids)),
        new HashSet<>(capability.findByIdsForCompletion(ids)));
    assertEquals(store().findByIds(List.of()), capability.findByIdsForCompletion(List.of()));
    assertEquals(
        store().findByIds(List.of(missing)), capability.findByIdsForCompletion(List.of(missing)));
  }

  @Test
  void planningDependantsMatchPlainPagesAndEmptyResults() {
    var capability = transactionalCompletionStore();
    JobEntity parent = persist(newPendingJob());
    for (int i = 0; i < 3; i++) {
      JobEntity child = newPendingJob("child-tag");
      child.setDependsOn(parent.getId());
      child.setStatus(JobStatus.WAITING);
      persist(child);
    }
    assertEquals(3, capability.findDependantsForCompletion(parent.getId(), 10, 0).size());
    for (int offset : List.of(0, 1, 2, 3)) {
      assertEquals(
          store().findDependants(parent.getId(), 2, offset),
          capability.findDependantsForCompletion(parent.getId(), 2, offset));
    }
    UUID missing = UUID.randomUUID();
    assertEquals(
        store().findDependants(missing, 2, 0),
        capability.findDependantsForCompletion(missing, 2, 0));
  }

  @Test
  void planningConditionsMatchPlainOrderingAndEmptyResults() {
    var capability = transactionalCompletionStore();
    JobEntity parent = persist(newPendingJob());
    for (int order : List.of(2, 0, 1)) {
      JobEntity child = persist(newPendingJob());
      WorkflowConditionEntity condition = new WorkflowConditionEntity();
      condition.setId(UUID.randomUUID());
      condition.setParentJobId(parent.getId());
      condition.setChildJobId(child.getId());
      condition.setConditionType(WorkflowCondition.ConditionType.SUCCESS);
      condition.setDefinitionOrder(order);
      condition.setCreatedAt(Instant.now());
      workflowConditionStore().saveCondition(condition);
    }
    assertEquals(3, capability.findConditionsForCompletion(parent.getId()).size());
    assertEquals(
        workflowConditionStore().findConditionsByParentJobId(parent.getId()),
        capability.findConditionsForCompletion(parent.getId()));
    UUID missing = UUID.randomUUID();
    assertEquals(
        workflowConditionStore().findConditionsByParentJobId(missing),
        capability.findConditionsForCompletion(missing));
  }

  private static final String NAMESPACE = "ratchet.checkpoint";
  private static final String A = "{\"value\":\"a\"}";
  private static final String B = "{\"value\":\"b\"}";

  private void requireOwnerTransaction() {
    if (!supportsOwnerTransaction()) {
      throw new AssertionError(
          "Fixture must override inOwnerTransaction, supportsOwnerTransaction, ownerSessionId"
              + " and awaitBlockedSession");
    }
  }

  private StateWriteResult write(JobEntity job, String value, Integer version) {
    AtomicReference<StateWriteResult> result = new AtomicReference<>();
    inOwnerTransaction(() -> result.set(claimStateWrite(job, value, version)));
    return result.get();
  }

  private StateWriteResult claimStateWrite(JobEntity job, String value, Integer version) {
    return transactionalCompletionStore()
        .writeClaimState(job.getId(), NAMESPACE, value, job.getClaimSeq(), version);
  }

  /**
   * Runs work in an owner transaction, then rolls that transaction back. A caller must roll back
   * after VERSION_CONFLICT, and the store may already have marked the transaction rollback-only.
   */
  private void inRolledBackOwnerTransaction(Runnable work) {
    RollBack rollBack = new RollBack();
    assertSame(
        rollBack,
        assertThrows(
            RollBack.class,
            () ->
                inOwnerTransaction(
                    () -> {
                      work.run();
                      throw rollBack;
                    })));
  }

  private static final class RollBack extends RuntimeException {
    RollBack() {
      super("Roll back the owner transaction", null, false, false);
    }
  }

  private void assertState(JobEntity job, String value, int version) {
    assertEquals(
        new VersionedState(value, version),
        stateStore().getState(job.getId(), NAMESPACE).orElseThrow());
  }

  @Test
  void claimStateWriteCreatesVersionZeroOnFirstWrite() {
    requireOwnerTransaction();
    JobEntity job = runningJob();
    assertEquals(StateWriteResult.UPDATED, write(job, A, null));
    assertState(job, A, 0);
  }

  @Test
  void claimStateWriteReplacesValueAtExpectedVersion() {
    requireOwnerTransaction();
    JobEntity job = runningJob();
    assertEquals(StateWriteResult.UPDATED, write(job, A, null));
    assertEquals(StateWriteResult.UPDATED, write(job, B, 0));
    assertState(job, B, 1);
    assertEquals(StateWriteResult.UPDATED, write(job, A, 1));
    assertState(job, A, 2);
  }

  @Test
  void claimStateWriteReturnsVersionConflictForStaleVersion() {
    requireOwnerTransaction();
    JobEntity job = runningJob();
    assertEquals(StateWriteResult.UPDATED, write(job, A, null));
    assertEquals(StateWriteResult.UPDATED, write(job, B, 0));
    inRolledBackOwnerTransaction(
        () -> assertEquals(StateWriteResult.VERSION_CONFLICT, claimStateWrite(job, A, 0)));
    assertState(job, B, 1);
  }

  @Test
  void claimStateWriteExpectingNoRowConflictsWithExistingRow() {
    requireOwnerTransaction();
    JobEntity job = runningJob();
    assertEquals(StateWriteResult.UPDATED, write(job, A, null));
    inRolledBackOwnerTransaction(
        () -> assertEquals(StateWriteResult.VERSION_CONFLICT, claimStateWrite(job, B, null)));
    assertState(job, A, 0);
  }

  @Test
  void claimStateWriteExpectingVersionConflictsWithMissingRow() {
    requireOwnerTransaction();
    JobEntity job = runningJob();
    inRolledBackOwnerTransaction(
        () -> assertEquals(StateWriteResult.VERSION_CONFLICT, claimStateWrite(job, A, 0)));
    assertTrue(stateStore().getState(job.getId(), NAMESPACE).isEmpty());
  }

  @Test
  void claimStateWriteRollsBackWithOwnerTransaction() {
    requireOwnerTransaction();
    JobEntity job = runningJob();
    assertEquals(StateWriteResult.UPDATED, write(job, A, null));
    assertThrows(
        IllegalStateException.class,
        () ->
            inOwnerTransaction(
                () -> {
                  assertEquals(
                      StateWriteResult.UPDATED,
                      transactionalCompletionStore()
                          .writeClaimState(job.getId(), NAMESPACE, B, job.getClaimSeq(), 0));
                  throw new IllegalStateException("Roll back owner");
                }));
    assertState(job, A, 0);
  }

  @Test
  void claimStateWriteIsSupersededForAnotherClaim() {
    requireOwnerTransaction();
    JobEntity job = runningJob();
    inOwnerTransaction(
        () ->
            assertEquals(
                StateWriteResult.SUPERSEDED,
                transactionalCompletionStore()
                    .writeClaimState(job.getId(), NAMESPACE, A, job.getClaimSeq() + 1, null)));
    assertTrue(stateStore().getState(job.getId(), NAMESPACE).isEmpty());
  }

  @Test
  void claimStateWriteIsSupersededAfterResetToPending() {
    requireOwnerTransaction();
    JobEntity job = runningJob();
    store().resetRunningJobs(job.getPickedBy());
    JobEntity pending = store().findById(job.getId()).orElseThrow();
    assertEquals(JobStatus.PENDING, pending.getStatus());
    assertEquals(job.getClaimSeq(), pending.getClaimSeq());
    assertEquals(StateWriteResult.SUPERSEDED, write(job, A, null));
    assertTrue(stateStore().getState(job.getId(), NAMESPACE).isEmpty());
  }

  @Test
  void claimStateWriteIsSupersededForCanceledOrMissingJob() {
    requireOwnerTransaction();
    JobEntity job = runningJob();
    assertTrue(store().cancelJob(job.getId()));
    assertEquals(StateWriteResult.SUPERSEDED, write(job, A, null));
    assertTrue(stateStore().getState(job.getId(), NAMESPACE).isEmpty());
    UUID missing = UUID.randomUUID();
    inOwnerTransaction(
        () ->
            assertEquals(
                StateWriteResult.SUPERSEDED,
                transactionalCompletionStore().writeClaimState(missing, NAMESPACE, A, 1, null)));
    assertTrue(stateStore().getState(missing, NAMESPACE).isEmpty());
    JobEntity completed = runningJob();
    assertTrue(store().commitCompletion(plan(completed, JobStatus.SUCCEEDED, null)).committed());
    assertEquals(StateWriteResult.SUPERSEDED, write(completed, A, null));
    assertTrue(stateStore().getState(completed.getId(), NAMESPACE).isEmpty());
  }

  @Test
  void claimStateWriteHoldsUpdateLockOnQueueRowUntilCommit() throws Exception {
    requireOwnerTransaction();
    for (boolean update : List.of(false, true)) {
      JobEntity job = runningJob();
      if (update) assertEquals(StateWriteResult.UPDATED, write(job, A, null));
      assertFalse(queueRowUpdateLockRefused(job.getId()));
      ExecutorService pool = Executors.newSingleThreadExecutor();
      CountDownLatch written = new CountDownLatch(1);
      CountDownLatch release = new CountDownLatch(1);
      try {
        var owner =
            pool.submit(
                () ->
                    inOwnerTransaction(
                        () -> {
                          assertEquals(
                              StateWriteResult.UPDATED,
                              transactionalCompletionStore()
                                  .writeClaimState(
                                      job.getId(),
                                      NAMESPACE,
                                      update ? B : A,
                                      job.getClaimSeq(),
                                      update ? 0 : null));
                          written.countDown();
                          await(release);
                        }));
        assertTrue(written.await(30, TimeUnit.SECONDS), "Owner did not write the checkpoint");
        assertTrue(
            queueRowUpdateLockRefused(job.getId()),
            "Claim state writer must refuse a conflicting queue-row lock; update=" + update);
        release.countDown();
        owner.get(30, TimeUnit.SECONDS);
        assertFalse(queueRowUpdateLockRefused(job.getId()));
        assertState(job, update ? B : A, update ? 1 : 0);
      } finally {
        release.countDown();
        pool.shutdownNow();
        terminate(pool);
      }
    }
  }

  @Test
  void concurrentFirstClaimStateWritesForDifferentJobsAllSucceed() {
    requireOwnerTransaction();
    ExecutorService pool = Executors.newFixedThreadPool(8);
    List<JobEntity> allJobs = new ArrayList<>();
    List<StateWriteResult> results = new ArrayList<>();
    List<Throwable> failures = new ArrayList<>();
    try {
      for (int round = 0; round < 10; round++) {
        List<JobEntity> jobs = new ArrayList<>();
        for (int index = 0; index < 8; index++) jobs.add(runningJob());
        allJobs.addAll(jobs);
        CyclicBarrier start = new CyclicBarrier(8);
        List<Future<StateWriteResult>> writes = new ArrayList<>();
        for (JobEntity job : jobs) {
          writes.add(
              pool.submit(
                  () -> {
                    AtomicReference<StateWriteResult> result = new AtomicReference<>();
                    inOwnerTransaction(
                        () -> {
                          cross(start);
                          result.set(
                              transactionalCompletionStore()
                                  .writeClaimState(
                                      job.getId(), NAMESPACE, A, job.getClaimSeq(), null));
                        });
                    return result.get();
                  }));
        }
        for (Future<StateWriteResult> future : writes) {
          try {
            results.add(future.get(30, TimeUnit.SECONDS));
          } catch (Exception failure) {
            failures.add(new AssertionError("Round " + round, failure));
          }
        }
      }
      String detail =
          "First-write failures: "
              + failures.stream()
                  .limit(3)
                  .map(failure -> failure + ": " + failure.getCause())
                  .toList();
      assertTrue(failures.isEmpty(), detail);
      assertEquals(80, results.size(), detail);
      for (StateWriteResult result : results)
        assertEquals(StateWriteResult.UPDATED, result, detail);
      for (JobEntity job : allJobs) assertState(job, A, 0);
    } finally {
      pool.shutdownNow();
      terminate(pool);
    }
  }

  @Test
  void claimStateWriteHoldsQueueLockUntilCommit() throws Exception {
    requireOwnerTransaction();
    claimStateWriteHoldsQueueLockUntilCommit(false);
    claimStateWriteHoldsQueueLockUntilCommit(true);
  }

  private void claimStateWriteHoldsQueueLockUntilCommit(boolean update) throws Exception {
    JobEntity job = runningJob();
    if (update) assertEquals(StateWriteResult.UPDATED, write(job, A, null));
    ExecutorService pool = Executors.newFixedThreadPool(2);
    CountDownLatch written = new CountDownLatch(1);
    CountDownLatch started = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    AtomicReference<Object> holder = new AtomicReference<>();
    AtomicReference<Object> waiter = new AtomicReference<>();
    AtomicReference<Boolean> canceled = new AtomicReference<>();
    try {
      var owner =
          pool.submit(
              () ->
                  inOwnerTransaction(
                      () -> {
                        holder.set(ownerSessionId());
                        assertEquals(
                            StateWriteResult.UPDATED,
                            transactionalCompletionStore()
                                .writeClaimState(
                                    job.getId(),
                                    NAMESPACE,
                                    update ? B : A,
                                    job.getClaimSeq(),
                                    update ? 0 : null));
                        written.countDown();
                        await(release);
                      }));
      assertTrue(written.await(30, TimeUnit.SECONDS));
      var cancel =
          pool.submit(
              () ->
                  inOwnerTransaction(
                      () -> {
                        waiter.set(ownerSessionId());
                        started.countDown();
                        canceled.set(store().cancelJob(job.getId()));
                      }));
      assertTrue(started.await(30, TimeUnit.SECONDS));
      assertTrue(
          awaitBlockedSession(waiter.get(), holder.get(), Duration.ofSeconds(30)),
          "Cancel must be blocked by the claim state writer");
      release.countDown();
      owner.get(30, TimeUnit.SECONDS);
      cancel.get(30, TimeUnit.SECONDS);
      assertEquals(Boolean.TRUE, canceled.get());
      assertEquals(JobStatus.CANCELED, store().findById(job.getId()).orElseThrow().getStatus());
      assertState(job, update ? B : A, update ? 1 : 0);
    } finally {
      release.countDown();
      pool.shutdownNow();
      assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));
    }
  }

  @Test
  void claimStateWriteSeesCurrentRowsAfterEarlierSnapshot() {
    requireOwnerTransaction();
    for (boolean update : List.of(false, true)) {
      JobEntity canceled = runningJob();
      if (update) assertEquals(StateWriteResult.UPDATED, write(canceled, A, null));
      inOwnerTransaction(
          () -> {
            stateStore().getState(canceled.getId(), NAMESPACE);
            store().findById(canceled.getId());
            elsewhere(() -> assertTrue(store().cancelJob(canceled.getId())));
            assertEquals(
                StateWriteResult.SUPERSEDED,
                transactionalCompletionStore()
                    .writeClaimState(
                        canceled.getId(),
                        NAMESPACE,
                        update ? B : A,
                        canceled.getClaimSeq(),
                        update ? 0 : null));
          });
      if (update) assertState(canceled, A, 0);
      else assertTrue(stateStore().getState(canceled.getId(), NAMESPACE).isEmpty());
    }
    JobEntity job = runningJob();
    assertEquals(StateWriteResult.UPDATED, write(job, A, null));
    // The caller rolls back after a conflict, so each stale snapshot gets its own transaction.
    inRolledBackOwnerTransaction(
        () -> {
          assertState(job, A, 0);
          elsewhere(() -> updateElsewhere(job, B, 0));
          assertEquals(StateWriteResult.VERSION_CONFLICT, claimStateWrite(job, A, 0));
        });
    inOwnerTransaction(
        () -> {
          assertState(job, B, 1);
          elsewhere(() -> updateElsewhere(job, A, 1));
          assertEquals(StateWriteResult.UPDATED, claimStateWrite(job, B, 2));
        });
    assertState(job, B, 3);
  }

  private void updateElsewhere(JobEntity job, String value, int version) {
    assertEquals(
        StateWriteResult.UPDATED,
        stateStore().updateState(job.getId(), NAMESPACE, value, version, job.getClaimSeq()));
  }

  @Test
  void claimStateWriteAndCancelNeverDeadlock() {
    requireOwnerTransaction();
    ExecutorService pool = Executors.newFixedThreadPool(2);
    try {
      for (int round = 0; round < 25; round++) {
        JobEntity job = runningJob();
        boolean update = round % 2 != 0;
        if (update) assertEquals(StateWriteResult.UPDATED, write(job, A, null));
        CyclicBarrier start = new CyclicBarrier(2);
        var writer =
            pool.submit(
                () -> {
                  cross(start);
                  return write(job, update ? B : A, update ? 0 : null);
                });
        var cancel =
            pool.submit(
                () -> {
                  cross(start);
                  return store().cancelJob(job.getId());
                });
        try {
          StateWriteResult result = writer.get(30, TimeUnit.SECONDS);
          assertTrue(cancel.get(30, TimeUnit.SECONDS), "Cancel in round " + round);
          assertEquals(JobStatus.CANCELED, store().findById(job.getId()).orElseThrow().getStatus());
          assertTrue(result == StateWriteResult.UPDATED || result == StateWriteResult.SUPERSEDED);
          if (update) {
            assertState(
                job,
                result == StateWriteResult.UPDATED ? B : A,
                result == StateWriteResult.UPDATED ? 1 : 0);
          } else {
            assertEquals(
                result == StateWriteResult.UPDATED,
                stateStore().getState(job.getId(), NAMESPACE).isPresent());
          }
        } catch (Throwable e) {
          throw new AssertionError("Claim state/cancel failed in round " + round, e);
        }
      }
    } finally {
      pool.shutdownNow();
      terminate(pool);
    }
  }

  @Test
  void staleFencedUpdateAfterClaimStateWriteChangesNothing() {
    requireOwnerTransaction();
    JobEntity first = runningJob();
    long oldClaim = first.getClaimSeq();
    assertEquals(
        StateWriteResult.UPDATED, stateStore().initState(first.getId(), NAMESPACE, A, oldClaim));
    store().resetRunningJobs(first.getPickedBy());
    assertTrue(store().tryPickUpJob(first.getId(), "completion-node"));
    JobEntity next = store().findById(first.getId()).orElseThrow();
    assertTrue(next.getClaimSeq() > oldClaim);
    assertEquals(StateWriteResult.UPDATED, write(next, B, 0));
    for (int version : List.of(0, 1)) {
      assertEquals(
          StateWriteResult.SUPERSEDED,
          stateStore().updateState(next.getId(), NAMESPACE, A, version, oldClaim));
    }
    assertEquals(
        StateWriteResult.SUPERSEDED, stateStore().initState(next.getId(), NAMESPACE, A, oldClaim));
    assertState(next, B, 1);
    JobEntity fresh = runningJob();
    assertEquals(StateWriteResult.UPDATED, write(fresh, B, null));
    assertEquals(
        StateWriteResult.SUPERSEDED,
        stateStore().initState(fresh.getId(), NAMESPACE, A, fresh.getClaimSeq() - 1));
    assertState(fresh, B, 0);
  }

  @Test
  void claimStateWriteEncryptsWhenPayloadEncrypted() {
    requireOwnerTransaction();
    JobEntity job = runningJob();
    RecordingEngine engine = new RecordingEngine();
    EncryptionHolder.install(
        List.of(engine), RecordingEngine.ALGORITHM_ID, new SingleKeyProvider(), true);
    try {
      assertEquals(StateWriteResult.UPDATED, write(job, A, null));
      assertEquals(StateWriteResult.UPDATED, write(job, B, 0));
      assertState(job, B, 1);
      assertTrue(engine.encryptedPlaintexts.containsAll(List.of(A, B)));
      assertTrue(engine.decryptCount.get() > 0);
    } finally {
      EncryptionHolder.disable();
    }
  }

  @Test
  void claimStateWriteRoundTripsStateAtTheSizeLimit() {
    requireOwnerTransaction();
    JobEntity job = runningJob();
    String value = "\"" + "é".repeat((ExtensionValidation.MAX_STATE_BYTES - 2) / 2) + "\"";
    assertEquals(StateWriteResult.UPDATED, write(job, value, null));
    assertState(job, value, 0);
    assertEquals(StateWriteResult.UPDATED, write(job, value, 0));
    assertState(job, value, 1);
  }

  @Test
  void claimStateWriteRejectsInvalidArgumentsBeforeWriting() {
    requireOwnerTransaction();
    JobEntity job = runningJob();
    String oversized = "\"" + "é".repeat(ExtensionValidation.MAX_STATE_BYTES / 2) + "\"";
    for (Runnable invalid :
        List.<Runnable>of(
            () ->
                transactionalCompletionStore()
                    .writeClaimState(job.getId(), NAMESPACE, oversized, job.getClaimSeq(), null),
            () ->
                transactionalCompletionStore()
                    .writeClaimState(job.getId(), "x".repeat(65), A, job.getClaimSeq(), null),
            () ->
                transactionalCompletionStore()
                    .writeClaimState(job.getId(), NAMESPACE, A, job.getClaimSeq(), -1))) {
      inOwnerTransaction(
          () ->
              assertEquals(
                  0,
                  countStoreStatements(
                      () -> assertThrows(IllegalArgumentException.class, invalid::run))));
    }
    assertTrue(stateStore().getState(job.getId(), NAMESPACE).isEmpty());
  }

  @Test
  void claimStateWriteFailsClosedUnderSnapshotIsolationAfterCancel() {
    requireOwnerTransaction();
    claimStateWriteFailsClosedUnderSnapshotIsolationAfterCancel(false);
    claimStateWriteFailsClosedUnderSnapshotIsolationAfterCancel(true);
  }

  private void claimStateWriteFailsClosedUnderSnapshotIsolationAfterCancel(boolean update) {
    JobEntity job = runningJob();
    if (update) assertEquals(StateWriteResult.UPDATED, write(job, A, null));
    AtomicReference<StateWriteResult> result = new AtomicReference<>();
    AtomicReference<Throwable> writeFailure = new AtomicReference<>();
    Throwable ownerFailure = null;
    try {
      inOwnerTransactionAtSnapshotIsolation(
          () -> {
            stateStore().getState(job.getId(), NAMESPACE);
            store().findById(job.getId());
            elsewhere(() -> assertTrue(store().cancelJob(job.getId())));
            try {
              result.set(
                  transactionalCompletionStore()
                      .writeClaimState(
                          job.getId(),
                          NAMESPACE,
                          update ? B : A,
                          job.getClaimSeq(),
                          update ? 0 : null));
            } catch (RuntimeException e) {
              writeFailure.set(e);
              throw e;
            }
          });
    } catch (RuntimeException e) {
      ownerFailure = e;
    }
    String outcome =
        "Write result: "
            + result.get()
            + "; write failure: "
            + writeFailure.get()
            + "; owner failure: "
            + ownerFailure;
    assertTrue(
        result.get() == StateWriteResult.SUPERSEDED
            || (writeFailure.get() != null && isSerializationFailure(writeFailure.get())),
        outcome);
    assertEquals(
        JobStatus.CANCELED, store().findById(job.getId()).orElseThrow().getStatus(), outcome);
    if (update) assertState(job, A, 0);
    else assertTrue(stateStore().getState(job.getId(), NAMESPACE).isEmpty(), outcome);
  }

  private static void await(CountDownLatch latch) {
    try {
      assertTrue(latch.await(30, TimeUnit.SECONDS), "Timed out waiting for transaction");
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError(e);
    }
  }

  private static void cross(CyclicBarrier barrier) {
    try {
      barrier.await(30, TimeUnit.SECONDS);
    } catch (Exception e) {
      throw new AssertionError(e);
    }
  }

  private static void terminate(ExecutorService pool) {
    try {
      assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError(e);
    }
  }

  private static void elsewhere(Runnable work) {
    ExecutorService pool = Executors.newSingleThreadExecutor();
    try {
      pool.submit(work).get(30, TimeUnit.SECONDS);
    } catch (Exception e) {
      throw new AssertionError("Independent transaction failed", e);
    } finally {
      pool.shutdownNow();
      terminate(pool);
    }
  }

  private JobEntity runningJob() {
    JobEntity job = persist(newPendingJob());
    assertTrue(store().tryPickUpJob(job.getId(), "completion-node"));
    return store().findById(job.getId()).orElseThrow();
  }

  private static JobCompletionPlan plan(JobEntity job, JobStatus terminal, String result) {
    Instant now = Instant.now();
    return new JobCompletionPlan(
        job.getId(),
        JobStatus.RUNNING,
        terminal,
        result,
        null,
        "completion",
        job.getAttempts(),
        now,
        now,
        0L,
        0L,
        null,
        null,
        List.of(),
        job.getClaimSeq(),
        null,
        List.of());
  }
}
