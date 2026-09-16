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

  /**
   * SQL adapters open separate transactions before the contender barrier. Mongo opens its own
   * sessions.
   */
  protected void inCompletionTransaction(Runnable work) {
    work.run();
  }

  @Test
  void competingCompletionsApplyBatchAndDependencyEffectsOnce() throws Exception {
    var batch = persist(newBatchParentJob());
    persistBatch(batch.getId(), 1);
    var child = persist(newPendingJob());
    var dependent = newPendingJob();
    dependent.setScheduledTime(Instant.parse("2099-01-01T00:00:00Z"));
    dependent = persist(dependent);
    var snapshot = store().findById(dependent.getId()).orElseThrow();
    store().compareAndSwapStatus(child.getId(), JobStatus.PENDING, JobStatus.RUNNING, null);
    Instant now = Instant.now();
    var transition =
        new run.ratchet.store.dto.JobCompletionPlan.DependencyTransition(
            snapshot.getId(),
            snapshot.getStatus(),
            snapshot.getVersion(),
            snapshot.getScheduledTime(),
            JobStatus.PENDING,
            Instant.parse("2026-01-01T00:00:00Z"),
            snapshot.getJobType());
    var plan =
        new run.ratchet.store.dto.JobCompletionPlan(
            child.getId(),
            JobStatus.RUNNING,
            JobStatus.SUCCEEDED,
            null,
            null,
            null,
            1,
            now,
            now,
            1L,
            0L,
            batch.getId(),
            null,
            java.util.List.of(transition));
    var bothInTransaction = new java.util.concurrent.CountDownLatch(2);
    var executor = java.util.concurrent.Executors.newFixedThreadPool(2);
    try {
      java.util.concurrent.Callable<Boolean> complete =
          () -> {
            var committed = new java.util.concurrent.atomic.AtomicBoolean();
            inCompletionTransaction(
                () -> {
                  bothInTransaction.countDown();
                  try {
                    assertTrue(
                        bothInTransaction.await(20, java.util.concurrent.TimeUnit.SECONDS),
                        "both contenders must reach the transaction barrier");
                  } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(interrupted);
                  }
                  committed.set(store().commitCompletion(plan).committed());
                });
            return committed.get();
          };
      var first = executor.submit(complete);
      var second = executor.submit(complete);
      assertEquals(
          1,
          (first.get(30, java.util.concurrent.TimeUnit.SECONDS) ? 1 : 0)
              + (second.get(30, java.util.concurrent.TimeUnit.SECONDS) ? 1 : 0));
    } finally {
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(10, java.util.concurrent.TimeUnit.SECONDS));
    }
    assertEquals(1, batchStore().findBatchById(batch.getId()).orElseThrow().getCompletedItems());
    var after = store().findById(snapshot.getId()).orElseThrow();
    assertEquals(transition.scheduledTime(), after.getScheduledTime());
    assertEquals(snapshot.getVersion() + 1, after.getVersion());
    assertEquals(JobStatus.SUCCEEDED, store().findById(child.getId()).orElseThrow().getStatus());
  }

  @Test
  void completionPreservesBothUnchangedAndChangedAttemptCounts() {
    for (JobStatus terminal : java.util.List.of(JobStatus.SUCCEEDED, JobStatus.FAILED)) {
      for (int plannedAttempts : new int[] {2, 5}) {
        var job = newPendingJob();
        job.setAttempts(2);
        job = persist(job);
        store().compareAndSwapStatus(job.getId(), JobStatus.PENDING, JobStatus.RUNNING, null);
        Instant now = Instant.now();
        var plan =
            new run.ratchet.store.dto.JobCompletionPlan(
                job.getId(),
                JobStatus.RUNNING,
                terminal,
                null,
                null,
                "test completion",
                plannedAttempts,
                now,
                now,
                0L,
                0L,
                null,
                null,
                java.util.List.of());

        assertTrue(store().commitCompletion(plan).committed());

        var completed = store().findById(job.getId()).orElseThrow();
        assertEquals(terminal, completed.getStatus());
        assertEquals(
            plannedAttempts,
            completed.getAttempts(),
            "Terminal history must retain the planned attempt count");
      }
    }
  }

  @Test
  void completionCommitsDependencyUnlockAndIsIdempotent() {
    var parent = persist(newPendingJob());
    var child = newPendingJob();
    child.setScheduledTime(Instant.parse("2099-01-01T00:00:00Z"));
    child = persist(child);
    store().compareAndSwapStatus(parent.getId(), JobStatus.PENDING, JobStatus.RUNNING, null);
    var snapshot = store().findById(child.getId()).orElseThrow();
    Instant now = Instant.now();
    var transition =
        new run.ratchet.store.dto.JobCompletionPlan.DependencyTransition(
            child.getId(),
            snapshot.getStatus(),
            snapshot.getVersion(),
            snapshot.getScheduledTime(),
            JobStatus.PENDING,
            Instant.parse("2026-01-01T00:00:00Z"),
            snapshot.getJobType());
    var plan =
        new run.ratchet.store.dto.JobCompletionPlan(
            parent.getId(),
            JobStatus.RUNNING,
            JobStatus.SUCCEEDED,
            null,
            null,
            null,
            1,
            now,
            now,
            0L,
            0L,
            null,
            null,
            java.util.List.of(transition));
    assertTrue(store().commitCompletion(plan).committed());
    assertEquals(JobStatus.SUCCEEDED, store().findById(parent.getId()).orElseThrow().getStatus());
    assertEquals(
        transition.scheduledTime(),
        store().findById(child.getId()).orElseThrow().getScheduledTime());
    assertFalse(store().commitCompletion(plan).committed());
  }

  @Test
  void completionRollsBackTerminalMutationWhenBatchAccountingFails() {
    var job = persist(newPendingJob());
    store().compareAndSwapStatus(job.getId(), JobStatus.PENDING, JobStatus.RUNNING, null);
    Instant now = Instant.now();
    var plan =
        new run.ratchet.store.dto.JobCompletionPlan(
            job.getId(),
            JobStatus.RUNNING,
            JobStatus.SUCCEEDED,
            null,
            null,
            null,
            1,
            now,
            now,
            0L,
            0L,
            java.util.UUID.randomUUID(),
            null,
            java.util.List.of());
    org.junit.jupiter.api.Assertions.assertThrows(
        RuntimeException.class, () -> store().commitCompletion(plan));
    assertEquals(
        JobStatus.RUNNING,
        store().findById(job.getId()).orElseThrow().getStatus(),
        "Failure after the primary write must roll back the terminal transition");
  }

  @Test
  void completionRejectsStaleDependentWithoutFinishingParent() {
    var parent = persist(newPendingJob());
    var child = persist(newPendingJob());
    store().compareAndSwapStatus(parent.getId(), JobStatus.PENDING, JobStatus.RUNNING, null);
    Instant now = Instant.now();
    var transition =
        new run.ratchet.store.dto.JobCompletionPlan.DependencyTransition(
            child.getId(),
            JobStatus.PAUSED,
            child.getVersion(),
            child.getScheduledTime(),
            JobStatus.PENDING,
            now,
            child.getJobType());
    var plan =
        new run.ratchet.store.dto.JobCompletionPlan(
            parent.getId(),
            JobStatus.RUNNING,
            JobStatus.FAILED,
            null,
            null,
            "failed",
            1,
            now,
            now,
            0L,
            0L,
            null,
            null,
            java.util.List.of(transition));
    org.junit.jupiter.api.Assertions.assertThrows(
        RuntimeException.class, () -> store().commitCompletion(plan));
    assertEquals(JobStatus.RUNNING, store().findById(parent.getId()).orElseThrow().getStatus());
  }

  @Test
  void completionCountsBatchChildOnceAndCompletesSyntheticParentAtomically() {
    var parent = persist(newBatchParentJob());
    persistBatch(parent.getId(), 1);
    var child = persist(newPendingJob());
    store().compareAndSwapStatus(child.getId(), JobStatus.PENDING, JobStatus.RUNNING, null);
    Instant now = Instant.now();
    var childPlan =
        new run.ratchet.store.dto.JobCompletionPlan(
            child.getId(),
            JobStatus.RUNNING,
            JobStatus.SUCCEEDED,
            null,
            null,
            null,
            1,
            now,
            now,
            0L,
            0L,
            parent.getId(),
            null,
            java.util.List.of());
    var completed = store().commitCompletion(childPlan);
    assertTrue(completed.committed());
    assertEquals(1, completed.batchProgress().completedItems());
    assertFalse(store().commitCompletion(childPlan).committed());
    assertEquals(1, batchStore().findBatchById(parent.getId()).orElseThrow().getCompletedItems());
    var parentPlan =
        new run.ratchet.store.dto.JobCompletionPlan(
            parent.getId(),
            JobStatus.PENDING,
            JobStatus.SUCCEEDED,
            null,
            null,
            null,
            0,
            now,
            now,
            0L,
            0L,
            null,
            new run.ratchet.store.dto.JobCompletionPlan.BatchCompletion(1, 1, 0),
            java.util.List.of());
    assertTrue(store().commitCompletion(parentPlan).committed());
    assertEquals(JobStatus.SUCCEEDED, store().findById(parent.getId()).orElseThrow().getStatus());
    assertTrue(batchStore().findBatchById(parent.getId()).orElseThrow().getCompletionProcessed());
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
