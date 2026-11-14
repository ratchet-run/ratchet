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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static run.ratchet.store.entity.JobExecutionEntity.ORPHAN_RECOVERY_ERROR_MESSAGE;
import static run.ratchet.store.entity.JobExecutionEntity.SHUTDOWN_RELEASE_ERROR_MESSAGE;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobStatus;
import run.ratchet.api.exception.RatchetOptimisticLockException;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionEntity;
import run.ratchet.store.entity.JobExecutionEntity.ExecutionStatus;
import run.ratchet.store.entity.JobPayload;
import run.ratchet.store.id.UuidV7Factory;
import run.ratchet.store.spi.ExhaustedOrphan;
import run.ratchet.store.spi.OrphanRecovery;

/** Base contract tests for {@code JobBulkStore}. */
public abstract class AbstractJobBulkStoreContract implements JobStoreContractFixture {

  @BeforeEach
  @AfterEach
  void cleanupBulkFixture() {
    cleanupStore();
  }

  @Test
  void bulkInsert_keepsPresetIds() {
    var first = newPendingJob();
    var second = newPendingJob();
    UUID firstId = UuidV7Factory.create();
    UUID secondId = UuidV7Factory.create();
    first.setId(firstId);
    second.setId(secondId);
    store().bulkInsert(List.of(first, second));
    assertEquals(firstId, first.getId());
    assertEquals(secondId, second.getId());
    assertEquals(firstId, store().findById(firstId).orElseThrow().getId());
    assertEquals(secondId, store().findById(secondId).orElseThrow().getId());
  }

  @Test
  void bulkInsert_roundTripsLargeMultibytePayloads() {
    String value = "é".repeat(128 * 1024);
    var first = newPendingJob();
    first.setId(UuidV7Factory.create());
    first.setPayload(
        new JobPayload(
            "java.lang.String",
            "valueOf",
            "(Ljava/lang/Object;)Ljava/lang/String;",
            true,
            List.of(value),
            null));
    var second = newPendingJob();
    second.setId(UuidV7Factory.create());
    second.setPayload(first.getPayload());
    store().bulkInsert(List.of(first, second));
    for (var job : List.of(first, second)) {
      assertEquals(job.getPayload(), store().findById(job.getId()).orElseThrow().getPayload());
    }
  }

  @Test
  void bulkInsert_duplicateInMiddleRollsBackEveryNewJob() {
    var existing = persist(newPendingJob());
    var first = newPendingJob();
    first.setId(UuidV7Factory.create());
    var duplicate = newPendingJob();
    duplicate.setId(UuidV7Factory.create());
    duplicate.setIdempotencyKey(existing.getIdempotencyKey());
    var last = newPendingJob();
    last.setId(UuidV7Factory.create());
    assertThrows(RuntimeException.class, () -> store().bulkInsert(List.of(first, duplicate, last)));
    assertTrue(
        store().findById(first.getId()).isEmpty(),
        "A failed bulk insert must not retain its prefix");
    assertTrue(store().findById(last.getId()).isEmpty());
    assertTrue(store().findOriginalJobIdByIdempotencyKey(first.getIdempotencyKey()).isEmpty());
    assertTrue(store().findById(existing.getId()).isPresent());
  }

  @Test
  void bulkInsert_persistsAllJobs() {
    var job1 = newPendingJob();
    job1.setId(UuidV7Factory.create());
    var job2 = newPendingJob();
    job2.setId(UuidV7Factory.create());
    var job3 = newPendingJob();
    job3.setId(UuidV7Factory.create());

    store().bulkInsert(List.of(job1, job2, job3));

    var found = store().findByIds(List.of(job1.getId(), job2.getId(), job3.getId()));
    assertEquals(3, found.size(), "bulkInsert should persist all 3 jobs");
  }

  @Test
  void findByIds_returnsEveryRowPastTheChunkBoundary() {
    // The SQL stores read IN-lists in 500-id chunks; 501 ids forces a second, partial
    // chunk. This catches a store that builds one unbounded IN-list (large batch
    // recovery then exceeds the database's bind-parameter cap) and a chunk loop that
    // drops the trailing partial chunk.
    List<JobEntity> jobs = new ArrayList<>(501);
    for (int i = 0; i < 501; i++) {
      var job = newPendingJob();
      job.setId(UuidV7Factory.create());
      jobs.add(job);
    }
    store().bulkInsert(jobs);

    List<UUID> ids = new ArrayList<>(jobs.size() + 1);
    for (JobEntity job : jobs) {
      ids.add(job.getId());
    }
    ids.add(UuidV7Factory.create()); // an unknown id is skipped, not an error

    var found = store().findByIds(ids);

    Set<UUID> expected = jobs.stream().map(JobEntity::getId).collect(Collectors.toSet());
    Set<UUID> actual = found.stream().map(JobEntity::getId).collect(Collectors.toSet());
    assertEquals(
        expected,
        actual,
        "findByIds must return exactly the persisted ids past the chunk boundary");
    assertEquals(501, found.size(), "findByIds must not duplicate rows across chunks");
  }

  @Test
  void resetOrphanJobs_honorsSubMinuteGrace() {
    var job = newPendingJob();
    job = persist(job);
    job.setStatus(JobStatus.RUNNING);
    // Picked 45s ago by a phantom node that does not exist in scheduler_node
    job.setPickedBy("phantom-node-" + job.getId());
    job.setPickedAt(Instant.now().minusSeconds(45));
    store().save(job);

    // Grace = 15s → picked 45s ago IS orphaned → should be reset
    int reset = store().resetOrphanJobsBefore(Instant.now().minusSeconds(15), 3, 500).reset();
    assertTrue(reset >= 1, "Job picked 45s ago with 15s grace should be reset");

    var reloaded = store().findById(job.getId()).orElseThrow();
    assertEquals(
        JobStatus.PENDING, reloaded.getStatus(), "Orphan job should be reset to PENDING status");
  }

  @Test
  void resetOrphanJobs_preservesRecentlyPickedJobs() {
    var job = newPendingJob();
    job = persist(job);
    job.setStatus(JobStatus.RUNNING);
    job.setPickedBy("phantom-node-" + job.getId());
    // Picked 10s ago — well within any reasonable grace period
    job.setPickedAt(Instant.now().minusSeconds(10));
    store().save(job);

    // Grace = 30s → picked 10s ago is NOT orphaned → should be preserved
    store().resetOrphanJobsBefore(Instant.now().minusSeconds(30), 3, 500);

    var reloaded = store().findById(job.getId()).orElseThrow();
    assertEquals(
        JobStatus.RUNNING, reloaded.getStatus(), "Recently-picked job should remain RUNNING");
  }

  @Test
  void resetOrphanJobsBefore_usesExactCutoff() {
    var old = persist(newPendingJob());
    old.setStatus(JobStatus.RUNNING);
    old.setPickedBy("phantom-node-" + old.getId());
    old.setPickedAt(Instant.now().minusSeconds(45));
    store().save(old);

    var recent = persist(newPendingJob());
    recent.setStatus(JobStatus.RUNNING);
    recent.setPickedBy("phantom-node-" + recent.getId());
    recent.setPickedAt(Instant.now().minusSeconds(10));
    store().save(recent);

    int reset = store().resetOrphanJobsBefore(Instant.now().minusSeconds(30), 3, 500).reset();

    assertEquals(1, reset, "Only rows picked before the cutoff should be reset");
    assertEquals(JobStatus.PENDING, store().findById(old.getId()).orElseThrow().getStatus());
    assertEquals(JobStatus.RUNNING, store().findById(recent.getId()).orElseThrow().getStatus());
  }

  @Test
  void resetOrphanJobsBefore_sparesJobWhosePickedByNodeIsStillAlive() {
    // Two RUNNING jobs both picked long before the cutoff ("slow jobs"). One's owning node is
    // still heartbeating; the other's node is gone. Only the dead-node job is an orphan — a live
    // node whose job is merely slow must NOT be reclaimed, even past the picked_at cutoff. Every
    // other orphan test uses phantom node ids absent from scheduler_node, so this is the only test
    // that drives the heartbeat-join branch (PG NOT EXISTS / MySQL NOT IN / Mongo nin) to true.
    Instant now = Instant.now();

    var liveOwned = persist(newPendingJob());
    liveOwned.setStatus(JobStatus.RUNNING);
    liveOwned.setPickedBy("live-node");
    liveOwned.setPickedAt(now.minusSeconds(120));
    store().save(liveOwned);

    var deadOwned = persist(newPendingJob());
    deadOwned.setStatus(JobStatus.RUNNING);
    deadOwned.setPickedBy("dead-node");
    deadOwned.setPickedAt(now.minusSeconds(120));
    store().save(deadOwned);

    // live-node has a fresh heartbeat; dead-node is never registered in scheduler_node.
    store().upsertHeartbeat("live-node", now);

    int reset = store().resetOrphanJobsBefore(now.minusSeconds(30), 3, 500).reset();

    assertEquals(1, reset, "Only the dead-node's slow job should be reclaimed");
    assertEquals(
        JobStatus.PENDING,
        store().findById(deadOwned.getId()).orElseThrow().getStatus(),
        "Dead-node's job must be reset to PENDING");
    assertEquals(
        JobStatus.RUNNING,
        store().findById(liveOwned.getId()).orElseThrow().getStatus(),
        "Live-node's slow job must stay RUNNING despite an old picked_at");
  }

  @Test
  void deleteJobsByIds_removesSpecifiedJobs() {
    var first = persist(newPendingJob());
    var second = persist(newPendingJob());
    var third = persist(newPendingJob());

    int deleted = store().deleteJobsByIds(List.of(first.getId(), second.getId()));

    assertEquals(2, deleted, "deleteJobsByIds should report 2 rows deleted");
    assertTrue(store().findById(first.getId()).isEmpty(), "Deleted job should not be found");
    assertTrue(store().findById(second.getId()).isEmpty(), "Deleted job should not be found");
    assertTrue(store().findById(third.getId()).isPresent(), "Non-deleted job should remain");
  }

  @Test
  void bulkInsert_emptyList_isNoOp() {
    store().bulkInsert(List.of());

    assertEquals(0, store().countPendingJobs(), "Empty bulk insert should not create any jobs");
  }

  @Test
  void deleteJobsByIds_emptyList_returnsZero() {
    persist(newPendingJob());

    int deleted = store().deleteJobsByIds(List.of());

    assertEquals(0, deleted, "deleteJobsByIds with empty list should return 0");
    assertEquals(1, store().countPendingJobs(), "Existing job should not be affected");
  }

  @Test
  void deleteJobsByIds_unknownIds_returnsZero() {
    int deleted =
        store()
            .deleteJobsByIds(
                List.of(new UUID(0L, Long.MAX_VALUE), new UUID(0L, Long.MAX_VALUE - 1)));

    assertEquals(0, deleted, "deleteJobsByIds with unknown IDs should return 0");
  }

  @Test
  void deleteDlqOlderThan_removesAllTerminalFailures() {
    JobEntity exhausted = newPendingJob();
    exhausted.setMaxRetries(1);
    exhausted = persist(exhausted);
    store().compareAndSwapStatus(exhausted.getId(), JobStatus.PENDING, JobStatus.RUNNING, null);
    store().markJobFailedTerminal(exhausted.getId(), "boom", 1, null);

    JobEntity retryable = newPendingJob();
    retryable.setMaxRetries(3);
    retryable = persist(retryable);
    store().compareAndSwapStatus(retryable.getId(), JobStatus.PENDING, JobStatus.RUNNING, null);
    store().markJobFailedTerminal(retryable.getId(), "retry later", 1, null);

    var pending = persist(newPendingJob());

    int deleted = store().deleteDlqOlderThan(Instant.now().plusSeconds(1));

    assertEquals(2, deleted, "All aged terminal failures are DLQ entries");
    assertTrue(store().findById(exhausted.getId()).isEmpty(), "Exhausted failure is deleted");
    assertTrue(
        store().findById(retryable.getId()).isEmpty(), "Failure below retry limit is deleted");
    assertTrue(store().findById(pending.getId()).isPresent(), "Pending job remains");
  }

  @Test
  void resetOrphanJobsForNode_reclaimsOwnRunningRowsUnconditionally() {
    // Startup self-recovery: our node had two RUNNING rows picked a moment ago. Even though
    // their picked_at is well inside any reasonable grace window, they must be reclaimed on
    // restart because this node is not trustworthy about those rows anymore.
    var a = newPendingJob();
    a = persist(a);
    a.setStatus(JobStatus.RUNNING);
    a.setPickedBy("node-self");
    a.setPickedAt(Instant.now()); // fresh — steady-state grace would preserve this
    store().save(a);

    var b = newPendingJob();
    b = persist(b);
    b.setStatus(JobStatus.RUNNING);
    b.setPickedBy("node-self");
    b.setPickedAt(Instant.now().minusSeconds(5));
    store().save(b);

    // Another node's fresh row — must NOT be touched
    var other = newPendingJob();
    other = persist(other);
    other.setStatus(JobStatus.RUNNING);
    other.setPickedBy("node-other");
    other.setPickedAt(Instant.now());
    store().save(other);

    int reset = store().resetOrphanJobsForNode("node-self", 3, 500).reset();
    assertEquals(2, reset, "resetOrphanJobsForNode should reclaim both self-owned RUNNING rows");

    assertEquals(JobStatus.PENDING, store().findById(a.getId()).orElseThrow().getStatus());
    assertEquals(JobStatus.PENDING, store().findById(b.getId()).orElseThrow().getStatus());
    assertEquals(
        JobStatus.RUNNING,
        store().findById(other.getId()).orElseThrow().getStatus(),
        "Other node's rows must not be reclaimed");
  }

  @Test
  void resetOrphanJobsForNode_ignoresNonRunningRows() {
    var pending = persist(newPendingJob());

    int reset = store().resetOrphanJobsForNode("node-self", 3, 500).reset();
    assertEquals(0, reset);
    assertEquals(JobStatus.PENDING, store().findById(pending.getId()).orElseThrow().getStatus());
  }

  @Test
  void cancelJobsByTag_cancelsActiveOneShotJobsWithMatchingTag() {
    String tag = "axon-deadline";

    var pending1 = persist(newPendingJob(tag));
    var pending2 = persist(newPendingJob(tag));
    var pending3 = persist(newPendingJob(tag));

    var paused = persist(newPendingJob(tag));
    store().transitionToPaused(paused.getId(), JobStatus.PENDING);

    JobEntity waiting = newPendingJob(tag);
    waiting.setStatus(JobStatus.WAITING);
    waiting = persist(waiting);
    UUID waitingId = waiting.getId();

    var running = persist(newPendingJob(tag));
    store().tryPickUpJob(running.getId(), "node-1");

    var untagged = persist(newPendingJob());

    int count = store().cancelJobsByTag(tag);

    assertEquals(5, count, "Should cancel 3 PENDING + 1 PAUSED + 1 WAITING tagged one-shot jobs");
    assertEquals(JobStatus.CANCELED, store().getJobStatus(pending1.getId()));
    assertEquals(JobStatus.CANCELED, store().getJobStatus(pending2.getId()));
    assertEquals(JobStatus.CANCELED, store().getJobStatus(pending3.getId()));
    assertEquals(JobStatus.CANCELED, store().getJobStatus(paused.getId()));
    assertEquals(JobStatus.CANCELED, store().getJobStatus(waitingId));
    assertEquals(
        JobStatus.RUNNING,
        store().getJobStatus(running.getId()),
        "RUNNING jobs are not affected — executor observes their natural termination");
    assertEquals(
        JobStatus.PENDING,
        store().getJobStatus(untagged.getId()),
        "Untagged jobs are not affected");
    // Recurring masters now live in scheduler_recurring_job; the bulk JobBatchStatusStore tag
    // cancel only walks scheduler_job_queue, so recurring masters are filtered by virtue of not
    // existing on the executable path — covered by AbstractRecurringJobStoreContract.
  }

  @Test
  void cancelJobsByTag_returnsZeroWhenNoMatchingJobs() {
    persist(newPendingJob("other-tag"));

    int count = store().cancelJobsByTag("nonexistent");

    assertEquals(0, count, "No matching tag should produce zero cancellations");
  }

  @Test
  void resetOrphanJobs_ignoresNonRunningJobs() {
    // PENDING job — should not be touched by orphan reset
    var pending = persist(newPendingJob());

    // CANCELED job — also not touched
    var canceled = persist(newPendingJob());
    store().compareAndSwapStatus(canceled.getId(), JobStatus.PENDING, JobStatus.RUNNING, null);
    store().compareAndSwapStatus(canceled.getId(), JobStatus.RUNNING, JobStatus.CANCELED, null);

    store().resetOrphanJobsBefore(Instant.now().minusSeconds(1), 3, 500);

    assertEquals(
        JobStatus.PENDING,
        store().findById(pending.getId()).orElseThrow().getStatus(),
        "PENDING job should remain PENDING");
    assertEquals(
        JobStatus.CANCELED,
        store().findById(canceled.getId()).orElseThrow().getStatus(),
        "CANCELED job should remain CANCELED");
  }

  @Test
  void orphanBudgetCountsRedeliveriesAndReturnsFencedExhaustedClaims() {
    JobEntity job = persist(newPendingJob());
    assertTrue(store().tryPickUpJob(job.getId(), "dead"));
    OrphanRecovery first = store().resetOrphanJobsBefore(Instant.now().plusSeconds(1), 1, 500);
    var firstExhausted = first.exhausted();
    assertEquals(1, first.reset());
    assertTrue(firstExhausted.isEmpty());
    assertEquals(JobStatus.PENDING, store().getJobStatus(job.getId()));
    assertEquals(0, store().findById(job.getId()).orElseThrow().getAttempts());
    assertTrue(store().tryPickUpJob(job.getId(), "dead"));
    long claimSeq = store().findById(job.getId()).orElseThrow().getClaimSeq();

    OrphanRecovery second = store().resetOrphanJobsBefore(Instant.now().plusSeconds(1), 1, 500);
    var secondExhausted = second.exhausted();

    assertEquals(0, second.reset());
    assertEquals(List.of(new ExhaustedOrphan(job.getId(), claimSeq, 1, "dead")), secondExhausted);
    assertEquals(JobStatus.RUNNING, store().getJobStatus(job.getId()));
    assertEquals(
        OrphanRecovery.none(),
        store().resetOrphanJobsBefore(Instant.now().minusSeconds(1), 1, 500));
  }

  @Test
  void graceRecoveryChargesBudgetAndStopsAtLimit() {
    JobEntity job = persist(newPendingJob());
    assertTrue(store().tryPickUpJob(job.getId(), "dead"));
    ageClaim(job.getId());
    assertEquals(1, store().resetOrphanJobsBefore(Instant.now().minusSeconds(30), 1, 500).reset());
    assertTrue(store().tryPickUpJob(job.getId(), "dead"));
    ageClaim(job.getId());
    OrphanRecovery exhausted =
        store().resetOrphanJobsBefore(Instant.now().minusSeconds(30), 1, 500);
    var exhaustedExhausted = exhausted.exhausted();
    assertEquals(0, exhausted.reset());
    assertEquals(1, exhaustedExhausted.size());
    assertEquals(1, exhaustedExhausted.get(0).crashCount());
    assertEquals(JobStatus.RUNNING, store().getJobStatus(job.getId()));
  }

  @Test
  void startupRecoveryChargesBudgetAndReleasesAllExhaustedOwners() {
    JobEntity first = persist(newPendingJob());
    JobEntity second = persist(newPendingJob());
    for (JobEntity job : List.of(first, second)) {
      assertTrue(store().tryPickUpJob(job.getId(), "self"));
    }
    assertEquals(2, store().resetOrphanJobsForNode("self", 1, 500).reset());
    for (JobEntity job : List.of(first, second)) {
      assertTrue(store().tryPickUpJob(job.getId(), "self"));
    }
    JobEntity before = store().findById(first.getId()).orElseThrow();
    OrphanRecovery startup = store().resetOrphanJobsForNode("self", 1, 500);
    assertEquals(0, startup.reset());
    assertEquals(2, startup.scanned());
    assertEquals(2, startup.released());
    for (JobEntity job : List.of(first, second)) {
      JobEntity after = store().findById(job.getId()).orElseThrow();
      assertEquals(JobStatus.RUNNING, after.getStatus());
      assertNull(after.getPickedBy());
    }
    JobEntity after = store().findById(first.getId()).orElseThrow();
    assertEquals(before.getPickedAt(), after.getPickedAt());
    assertEquals(before.getClaimSeq(), after.getClaimSeq());
    store().upsertHeartbeat("self", Instant.now().plusSeconds(120));
    assertEquals(0, store().resetRunningJobs("self"));
    OrphanRecovery next = store().resetOrphanJobsBefore(Instant.now().plusSeconds(1), 1, 500);
    var nextExhausted = next.exhausted();
    assertEquals(2, nextExhausted.size(), "NULL owners remain detectable after restart heartbeat");
  }

  @Test
  void exhaustedLimitBoundsSelectionAndAliveOwnersAreExcluded() {
    JobEntity live = persist(newPendingJob());
    assertTrue(store().tryPickUpJob(live.getId(), "live"));
    for (int i = 0; i < 3; i++) {
      JobEntity dead = persist(newPendingJob());
      assertTrue(store().tryPickUpJob(dead.getId(), "dead"));
    }
    Instant cutoff = Instant.now().plusSeconds(1);
    store().upsertHeartbeat("live", cutoff.plusSeconds(60));
    OrphanRecovery result = store().resetOrphanJobsBefore(cutoff, 0, 2);
    var resultExhausted = result.exhausted();
    assertEquals(0, result.reset());
    assertEquals(2, resultExhausted.size());
    assertTrue(resultExhausted.stream().noneMatch(row -> row.jobId().equals(live.getId())));
    assertTrue(resultExhausted.stream().allMatch(row -> row.crashCount() == 0));
    assertEquals(JobStatus.RUNNING, store().getJobStatus(live.getId()));
  }

  @Test
  void crashFailedJobIsPurgedBelowRetryLimit() {
    JobEntity job = newPendingJob();
    job.setMaxRetries(10);
    job = persist(job);
    assertTrue(store().tryPickUpJob(job.getId(), "dead"));
    ExhaustedOrphan orphan =
        store().resetOrphanJobsBefore(Instant.now().plusSeconds(1), 0, 500).exhausted().get(0);
    assertTrue(
        store()
            .markJobFailedTerminal(
                job.getId(), "crash redelivery limit reached", 0, orphan.claimSeq()));
    assertEquals(1, store().deleteDlqOlderThan(Instant.now().plusSeconds(1)));
    assertTrue(store().findById(job.getId()).isEmpty());
  }

  @Test
  void orphanCompletionSnapshotHasTypeDependencyAndFenceWithoutPayload() {
    JobEntity job = persist(newPendingJob());
    assertTrue(store().tryPickUpJob(job.getId(), "dead"));
    JobEntity full = store().findById(job.getId()).orElseThrow();
    JobEntity snapshot = store().findOrphanCompletionSnapshot(job.getId()).orElseThrow();
    assertEquals(full.getId(), snapshot.getId());
    assertEquals(full.getJobType(), snapshot.getJobType());
    assertEquals(full.getDependsOn(), snapshot.getDependsOn());
    assertEquals(full.getClaimSeq(), snapshot.getClaimSeq());
    assertEquals(full.getAttempts(), snapshot.getAttempts());
    assertEquals(full.getMaxRetries(), snapshot.getMaxRetries());
    assertNull(snapshot.getPayload());
  }

  @Test
  void graceRecoveryBoundsExhaustedListAndSparesAliveOwners() {
    JobEntity live = persist(newPendingJob());
    assertTrue(store().tryPickUpJob(live.getId(), "live"));
    ageClaim(live.getId());
    for (int i = 0; i < 3; i++) {
      JobEntity dead = persist(newPendingJob());
      assertTrue(store().tryPickUpJob(dead.getId(), "dead"));
      ageClaim(dead.getId());
    }
    store().upsertHeartbeat("live", Instant.now());

    OrphanRecovery result = store().resetOrphanJobsBefore(Instant.now().minusSeconds(30), 0, 2);
    var resultExhausted = result.exhausted();

    assertEquals(0, result.reset());
    assertEquals(2, resultExhausted.size());
    assertTrue(resultExhausted.stream().noneMatch(row -> row.jobId().equals(live.getId())));
    assertEquals(JobStatus.RUNNING, store().getJobStatus(live.getId()));
  }

  @Test
  void resetRunningJobs_closesEveryReleasedRunningExecution() {
    auditStore();
    List<JobEntity> released = List.of(persist(newPendingJob()), persist(newPendingJob()));
    for (JobEntity job : released) {
      assertTrue(store().tryPickUpJob(job.getId(), "self"));
      auditStore().saveExecution(JobExecutionEntity.start(job.getId(), 1, "self"));
      auditStore().saveExecution(JobExecutionEntity.start(job.getId(), 1, "self"));
    }

    assertEquals(2, store().resetRunningJobs("self"));

    for (JobEntity job : released) {
      var executions = auditStore().findExecutionsByJobId(job.getId(), 100, 0);
      assertEquals(2, executions.size());
      for (JobExecutionEntity execution : executions) {
        assertEquals(ExecutionStatus.FAILED, execution.getStatus());
        assertNotNull(execution.getEndedAt());
        assertEquals(SHUTDOWN_RELEASE_ERROR_MESSAGE, execution.getErrorMessage());
        assertNull(execution.getErrorClass());
        assertNull(execution.getDurationMs());
      }
      assertEquals(JobStatus.PENDING, store().getJobStatus(job.getId()));
    }
  }

  @Test
  void resetRunningJobs_preservesOtherOwnersNonRunningJobsAndEarlierSuccess() {
    auditStore();
    JobEntity released = persist(newPendingJob());
    JobEntity other = persist(newPendingJob());
    JobEntity pending = newPendingJob();
    pending.setPickedBy("self");
    pending = persist(pending);
    assertTrue(store().tryPickUpJob(released.getId(), "self"));
    assertTrue(store().tryPickUpJob(other.getId(), "other"));
    for (JobEntity job : List.of(released, other, pending)) {
      auditStore().saveExecution(JobExecutionEntity.start(job.getId(), 2, "self"));
    }
    JobExecutionEntity succeeded = JobExecutionEntity.start(released.getId(), 1, "self");
    succeeded.markSucceeded();
    succeeded.setEndedAt(Instant.ofEpochMilli(succeeded.getEndedAt().toEpochMilli()));
    auditStore().saveExecution(succeeded);

    assertEquals(1, store().resetRunningJobs("self"));

    for (JobEntity job : List.of(other, pending)) {
      assertRunningExecution(auditStore().findExecutionsByJobId(job.getId(), 100, 0).get(0));
    }
    JobExecutionEntity preserved =
        auditStore().findExecutionsByJobId(released.getId(), 100, 0).stream()
            .filter(row -> row.getId().equals(succeeded.getId()))
            .findFirst()
            .orElseThrow();
    assertEquals(ExecutionStatus.SUCCEEDED, preserved.getStatus());
    assertEquals(succeeded.getEndedAt(), preserved.getEndedAt());
    assertEquals(succeeded.getDurationMs(), preserved.getDurationMs());
    assertNull(preserved.getErrorClass());
    assertNull(preserved.getErrorMessage());
  }

  @Test
  void resetOrphanJobsBefore_closesLostExecutionsIncludingReusedAttemptNumbers() {
    auditStore();
    JobEntity job = persist(newPendingJob());
    assertTrue(store().tryPickUpJob(job.getId(), "dead"));
    // Crash recovery reuses attempt numbers; all RUNNING rows must be closed.
    auditStore().saveExecution(JobExecutionEntity.start(job.getId(), 1, "dead"));
    auditStore().saveExecution(JobExecutionEntity.start(job.getId(), 1, "older-dead"));
    ageClaim(job.getId());

    assertEquals(1, store().resetOrphanJobsBefore(Instant.now().minusSeconds(30), 3, 500).reset());

    var executions = auditStore().findExecutionsByJobId(job.getId(), 100, 0);
    assertEquals(2, executions.size());
    executions.forEach(this::assertLostExecution);
  }

  @Test
  void resetOrphanJobsBefore_closesExhaustedExecution() {
    auditStore();
    JobEntity job = persist(newPendingJob());
    assertTrue(store().tryPickUpJob(job.getId(), "dead"));
    auditStore().saveExecution(JobExecutionEntity.start(job.getId(), 1, "dead"));
    ageClaim(job.getId());

    var result = store().resetOrphanJobsBefore(Instant.now().minusSeconds(30), 0, 500).exhausted();

    assertEquals(1, result.size());
    assertEquals(JobStatus.RUNNING, store().getJobStatus(job.getId()));
    var executions = auditStore().findExecutionsByJobId(job.getId(), 100, 0);
    assertEquals(1, executions.size());
    assertLostExecution(executions.get(0));
  }

  @Test
  void resetOrphanJobsForNode_closesOnlyItsRunningExecutionsIncludingExhaustedJobs() {
    auditStore();
    JobEntity exhausted = persist(newPendingJob());
    assertTrue(store().tryPickUpJob(exhausted.getId(), "self"));
    assertEquals(1, store().resetOrphanJobsForNode("self", 1, 500).reset());
    JobEntity reset = persist(newPendingJob());
    JobEntity other = persist(newPendingJob());
    JobEntity pending = newPendingJob();
    pending.setPickedBy("self");
    pending = persist(pending);
    for (JobEntity job : List.of(reset, exhausted, other)) {
      assertTrue(store().tryPickUpJob(job.getId(), job == other ? "other" : "self"));
    }
    for (JobEntity job : List.of(reset, exhausted, other, pending)) {
      auditStore().saveExecution(JobExecutionEntity.start(job.getId(), 1, "self"));
    }

    assertEquals(1, store().resetOrphanJobsForNode("self", 1, 500).reset());

    assertLostExecution(auditStore().findExecutionsByJobId(reset.getId(), 100, 0).get(0));
    assertLostExecution(auditStore().findExecutionsByJobId(exhausted.getId(), 100, 0).get(0));
    for (JobEntity job : List.of(other, pending)) {
      assertRunningExecution(auditStore().findExecutionsByJobId(job.getId(), 100, 0).get(0));
    }
  }

  @Test
  void resetOrphanJobsBefore_preservesAliveFreshNonRunningAndSucceededExecutions() {
    auditStore();
    JobEntity live = persist(newPendingJob());
    JobEntity fresh = persist(newPendingJob());
    JobEntity pending = newPendingJob();
    pending.setPickedBy("dead");
    pending.setPickedAt(Instant.now().minusSeconds(120));
    pending = persist(pending);
    JobEntity dead = persist(newPendingJob());
    for (JobEntity job : List.of(live, fresh, dead)) {
      assertTrue(store().tryPickUpJob(job.getId(), job == live ? "live" : "dead"));
      if (job != fresh) {
        ageClaim(job.getId());
      }
    }
    for (JobEntity job : List.of(live, fresh, pending)) {
      auditStore().saveExecution(JobExecutionEntity.start(job.getId(), 1, "node"));
    }
    JobExecutionEntity succeeded = JobExecutionEntity.start(dead.getId(), 1, "dead");
    succeeded.markSucceeded();
    succeeded.setEndedAt(Instant.ofEpochMilli(succeeded.getEndedAt().toEpochMilli()));
    auditStore().saveExecution(succeeded);
    auditStore().saveExecution(JobExecutionEntity.start(dead.getId(), 2, "dead"));
    store().upsertHeartbeat("live", Instant.now());

    assertEquals(1, store().resetOrphanJobsBefore(Instant.now().minusSeconds(30), 3, 500).reset());

    for (JobEntity job : List.of(live, fresh, pending)) {
      assertRunningExecution(auditStore().findExecutionsByJobId(job.getId(), 100, 0).get(0));
    }
    var executions = auditStore().findExecutionsByJobId(dead.getId(), 100, 0);
    assertEquals(2, executions.size());
    JobExecutionEntity preserved =
        executions.stream()
            .filter(row -> row.getId().equals(succeeded.getId()))
            .findFirst()
            .orElseThrow();
    assertEquals(ExecutionStatus.SUCCEEDED, preserved.getStatus());
    assertEquals(succeeded.getEndedAt(), preserved.getEndedAt());
    assertEquals(succeeded.getDurationMs(), preserved.getDurationMs());
    assertNull(preserved.getErrorMessage());
    executions.stream()
        .filter(row -> !row.getId().equals(succeeded.getId()))
        .forEach(this::assertLostExecution);
  }

  @Test
  void closeStaleExecutions_closesOldRowsOfInactiveJobs() {
    Instant now = Instant.now();
    List<JobEntity> jobs = new ArrayList<>();
    for (JobStatus status :
        List.of(
            JobStatus.PENDING,
            JobStatus.WAITING,
            JobStatus.SUCCEEDED,
            JobStatus.FAILED,
            JobStatus.CANCELED)) {
      JobEntity job = staleJob(status);
      jobs.add(job);
      staleExecution(job, now.minusSeconds(7200));
    }
    assertEquals(jobs.size(), store().closeStaleExecutions(now.plusSeconds(600), 100));
    for (JobEntity job : jobs) {
      var row = auditStore().findExecutionsByJobId(job.getId(), 100, 0).get(0);
      assertEquals(ExecutionStatus.FAILED, row.getStatus());
      assertEquals(JobExecutionEntity.STALE_EXECUTION_ERROR_MESSAGE, row.getErrorMessage());
      assertNotNull(row.getEndedAt());
      assertNull(row.getErrorClass());
      assertNull(row.getDurationMs());
    }
  }

  @Test
  void closeStaleExecutions_preservesRunningJobsAndNewRows() {
    Instant now = Instant.now();
    JobEntity running = persist(newPendingJob());
    assertTrue(store().tryPickUpJob(running.getId(), "live"));
    staleExecution(running, now.minusSeconds(7200));
    JobEntity pending = persist(newPendingJob());
    staleExecution(pending, now.plusSeconds(1200));
    assertEquals(0, store().closeStaleExecutions(now.plusSeconds(600), 100));
    for (JobEntity job : List.of(running, pending)) {
      assertRunningExecution(auditStore().findExecutionsByJobId(job.getId(), 100, 0).get(0));
    }
  }

  @Test
  void closeStaleExecutions_preservesRecentlyChangedLiveAndTerminalJobs() {
    Instant now = Instant.now();
    for (JobStatus status : List.of(JobStatus.PENDING, JobStatus.SUCCEEDED)) {
      JobEntity job = staleJob(status);
      staleExecution(job, now.minusSeconds(7200));
      assertEquals(0, store().closeStaleExecutions(now.minusSeconds(600), 100));
      assertRunningExecution(auditStore().findExecutionsByJobId(job.getId(), 100, 0).get(0));
    }
  }

  @Test
  void closeStaleExecutions_preservesRecordedOutcomes() {
    Instant now = Instant.now();
    JobEntity job = persist(newPendingJob());
    for (ExecutionStatus status : List.of(ExecutionStatus.SUCCEEDED, ExecutionStatus.FAILED)) {
      JobExecutionEntity row = JobExecutionEntity.start(job.getId(), 1, "node");
      row.setStartedAt(now.minusSeconds(7200));
      row.setStatus(status);
      row.setEndedAt(Instant.ofEpochMilli(now.minusSeconds(3600).toEpochMilli()));
      row.setErrorMessage("original outcome");
      auditStore().saveExecution(row);
    }
    assertEquals(0, store().closeStaleExecutions(now.plusSeconds(600), 100));
    for (var row : auditStore().findExecutionsByJobId(job.getId(), 100, 0)) {
      assertTrue(row.getStatus() != ExecutionStatus.RUNNING);
      assertEquals("original outcome", row.getErrorMessage());
      assertEquals(
          Instant.ofEpochMilli(now.minusSeconds(3600).toEpochMilli()),
          Instant.ofEpochMilli(row.getEndedAt().toEpochMilli()));
    }
  }

  @Test
  void closeStaleExecutions_boundsEachScanAndIgnoresNonPositiveLimits() {
    Instant now = Instant.now();
    JobEntity job = persist(newPendingJob());
    for (int i = 0; i < 3; i++) {
      staleExecution(job, now.minusSeconds(7200 + i));
    }
    Instant cutoff = now.plusSeconds(600);
    assertEquals(0, store().closeStaleExecutions(cutoff, 0));
    assertEquals(0, store().closeStaleExecutions(cutoff, -1));
    auditStore().findExecutionsByJobId(job.getId(), 100, 0).forEach(this::assertRunningExecution);
    assertEquals(2, store().closeStaleExecutions(cutoff, 2));
    var rows = auditStore().findExecutionsByJobId(job.getId(), 100, 0);
    var newest =
        rows.stream().max(Comparator.comparing(JobExecutionEntity::getStartedAt)).orElseThrow();
    assertRunningExecution(newest);
    assertEquals(1, store().closeStaleExecutions(cutoff, 2));
    assertEquals(0, store().closeStaleExecutions(cutoff, 2));
  }

  @Test
  void closeStaleExecutions_runningJobDoesNotBlockEligibleRows() {
    Instant now = Instant.now();
    JobEntity running = persist(newPendingJob());
    assertTrue(store().tryPickUpJob(running.getId(), "live"));
    staleExecution(running, now.minusSeconds(10800));
    JobEntity succeeded = staleJob(JobStatus.SUCCEEDED);
    staleExecution(succeeded, now.minusSeconds(7200));
    assertEquals(1, store().closeStaleExecutions(now.plusSeconds(600), 1));
    assertRunningExecution(auditStore().findExecutionsByJobId(running.getId(), 100, 0).get(0));
    assertEquals(
        ExecutionStatus.FAILED,
        auditStore().findExecutionsByJobId(succeeded.getId(), 100, 0).get(0).getStatus());
  }

  private JobEntity staleJob(JobStatus status) {
    JobEntity job = newPendingJob();
    if (status == JobStatus.WAITING) {
      job.setStatus(status);
    }
    job = persist(job);
    if (status == JobStatus.PENDING) {
      store().updateJobStatus(job.getId(), JobStatus.PENDING, null);
    }
    if (status == JobStatus.SUCCEEDED
        || status == JobStatus.FAILED
        || status == JobStatus.CANCELED) {
      assertTrue(store().tryPickUpJob(job.getId(), "node"));
      if (status == JobStatus.SUCCEEDED) {
        Instant now = Instant.now();
        assertTrue(store().markJobSucceededMinimal(job.getId(), now, now, 0L, 0L));
      } else {
        assertTrue(store().compareAndSwapStatus(job.getId(), JobStatus.RUNNING, status, null));
      }
    }
    return job;
  }

  private void staleExecution(JobEntity job, Instant startedAt) {
    JobExecutionEntity row = JobExecutionEntity.start(job.getId(), 1, "node");
    row.setStartedAt(startedAt);
    auditStore().saveExecution(row);
  }

  private void assertLostExecution(JobExecutionEntity execution) {
    assertEquals(ExecutionStatus.FAILED, execution.getStatus());
    assertNotNull(execution.getEndedAt());
    assertEquals(ORPHAN_RECOVERY_ERROR_MESSAGE, execution.getErrorMessage());
    assertNull(execution.getErrorClass());
    assertNull(execution.getDurationMs());
  }

  private void assertRunningExecution(JobExecutionEntity execution) {
    assertEquals(ExecutionStatus.RUNNING, execution.getStatus());
    assertNull(execution.getEndedAt());
    assertNull(execution.getErrorMessage());
    assertNull(execution.getErrorClass());
    assertNull(execution.getDurationMs());
  }

  @Test
  void resetOrphanJobsBefore_pagesResettableOrphans() {
    auditStore();
    List<JobEntity> jobs = new ArrayList<>();
    for (int i = 0; i < 5; i++) jobs.add(agedOrphan("dead", false));
    Instant cutoff = Instant.now().minusSeconds(30);
    OrphanRecovery page = store().resetOrphanJobsBefore(cutoff, 1, 2);
    assertEquals(new OrphanRecovery(2, 2, 0, List.of()), page);
    assertEquals(
        3,
        jobs.stream()
            .filter(job -> store().getJobStatus(job.getId()) == JobStatus.RUNNING)
            .count());
    for (JobEntity job : jobs) {
      var executions = auditStore().findExecutionsByJobId(job.getId(), 100, 0);
      assertEquals(1, executions.size());
      if (store().getJobStatus(job.getId()) == JobStatus.RUNNING)
        assertRunningExecution(executions.get(0));
      else assertLostExecution(executions.get(0));
    }
    int scanned = page.scanned();
    int total = page.reset();
    int calls = 1;
    while (scanned == 2 && calls++ < 5) {
      page = store().resetOrphanJobsBefore(cutoff, 1, 2);
      scanned = page.scanned();
      total += page.reset();
    }
    assertEquals(5, total);
    for (JobEntity job : jobs) {
      JobEntity after = store().findById(job.getId()).orElseThrow();
      assertEquals(JobStatus.PENDING, after.getStatus());
      assertNull(after.getPickedBy());
      assertNull(after.getPickedAt());
      auditStore().findExecutionsByJobId(job.getId(), 100, 0).forEach(this::assertLostExecution);
      assertTrue(store().tryPickUpJob(job.getId(), "dead"));
      ageClaim(job.getId());
    }
    var exhausted = store().resetOrphanJobsBefore(cutoff, 1, 10).exhausted();
    assertEquals(
        jobs.stream().map(JobEntity::getId).collect(Collectors.toSet()),
        exhausted.stream().map(ExhaustedOrphan::jobId).collect(Collectors.toSet()));
    assertTrue(exhausted.stream().allMatch(row -> row.crashCount() == 1));
  }

  @Test
  void resetOrphanJobsBefore_defersExhaustedRowsAndReturnsThemOldestFirst() {
    auditStore();
    Instant now = Instant.now();
    JobEntity oldest = agedOrphan("dead", true);
    JobEntity middle = agedOrphan("dead", true);
    JobEntity youngest = agedOrphan("dead", true);
    JobEntity resettable = agedOrphan("dead", false);
    JobEntity live = agedOrphan("live", true);
    setClaimTime(oldest.getId(), now.minusSeconds(300));
    setClaimTime(middle.getId(), now.minusSeconds(200));
    setClaimTime(youngest.getId(), now.minusSeconds(100));
    setClaimTime(resettable.getId(), now.minusSeconds(400));
    store().upsertHeartbeat("live", now.plusSeconds(60));
    JobEntity liveBefore = store().findById(live.getId()).orElseThrow();
    List<JobEntity> before =
        List.of(oldest, middle, youngest).stream()
            .map(job -> store().findById(job.getId()).orElseThrow())
            .toList();
    Instant beforeCall = Instant.now();
    OrphanRecovery page = store().resetOrphanJobsBefore(now.minusSeconds(30), 1, 10);
    assertEquals(4, page.scanned());
    assertEquals(1, page.reset());
    assertEquals(0, page.released());
    assertEquals(
        before.stream()
            .map(job -> new ExhaustedOrphan(job.getId(), job.getClaimSeq(), 1, "dead"))
            .toList(),
        page.exhausted());
    for (JobEntity original : before) {
      JobEntity deferred = store().findById(original.getId()).orElseThrow();
      assertEquals(JobStatus.RUNNING, deferred.getStatus());
      assertEquals(original.getPickedBy(), deferred.getPickedBy());
      assertEquals(original.getClaimSeq(), deferred.getClaimSeq());
      assertFalse(deferred.getPickedAt().isBefore(beforeCall));
      assertEquals(deferred.getPickedAt(), deferred.getUpdatedAt());
      auditStore()
          .findExecutionsByJobId(original.getId(), 100, 0)
          .forEach(this::assertLostExecution);
    }
    assertEquals(JobStatus.PENDING, store().getJobStatus(resettable.getId()));
    auditStore()
        .findExecutionsByJobId(resettable.getId(), 100, 0)
        .forEach(this::assertLostExecution);
    JobEntity liveAfter = store().findById(live.getId()).orElseThrow();
    assertEquals(JobStatus.RUNNING, liveAfter.getStatus());
    assertEquals(liveBefore.getPickedAt(), liveAfter.getPickedAt());
    assertEquals(liveBefore.getUpdatedAt(), liveAfter.getUpdatedAt());
    assertEquals(liveBefore.getClaimSeq(), liveAfter.getClaimSeq());
    assertEquals("live", liveAfter.getPickedBy());
    assertRunningExecution(auditStore().findExecutionsByJobId(live.getId(), 100, 0).get(0));
  }

  @Test
  void resetOrphanJobsBefore_exhaustedRowsLeaveTheWindowUntilItPasses() {
    List<JobEntity> jobs =
        List.of(agedOrphan("dead", true), agedOrphan("dead", true), agedOrphan("dead", true));
    Instant cutoff = Instant.now().minusSeconds(30);
    assertEquals(3, store().resetOrphanJobsBefore(cutoff, 1, 10).exhausted().size());
    assertEquals(OrphanRecovery.none(), store().resetOrphanJobsBefore(cutoff, 1, 10));
    var again = store().resetOrphanJobsBefore(Instant.now().plusSeconds(60), 1, 10);
    assertEquals(
        jobs.stream().map(JobEntity::getId).collect(Collectors.toSet()),
        again.exhausted().stream().map(ExhaustedOrphan::jobId).collect(Collectors.toSet()));
    assertTrue(again.exhausted().stream().allMatch(row -> row.crashCount() == 1));
  }

  @Test
  void resetOrphanJobsBefore_pageOfOnlyExhaustedRowsMakesProgress() {
    List<JobEntity> jobs = new ArrayList<>();
    for (int i = 0; i < 5; i++) jobs.add(agedOrphan("dead", true));
    Instant cutoff = Instant.now().minusSeconds(30);
    List<UUID> seen = new ArrayList<>();
    for (int expected : List.of(2, 2, 1, 0)) {
      OrphanRecovery page = store().resetOrphanJobsBefore(cutoff, 1, 2);
      assertEquals(expected, page.scanned());
      assertEquals(expected, page.exhausted().size());
      assertEquals(0, page.reset());
      assertEquals(0, page.released());
      page.exhausted().forEach(row -> seen.add(row.jobId()));
    }
    assertEquals(5, seen.size());
    assertEquals(jobs.stream().map(JobEntity::getId).collect(Collectors.toSet()), Set.copyOf(seen));
  }

  @Test
  void deferredExhaustedClaimCanStillBeFailed() {
    JobEntity job = agedOrphan("dead", true);
    ExhaustedOrphan row =
        store().resetOrphanJobsBefore(Instant.now().minusSeconds(30), 1, 10).exhausted().get(0);
    assertEquals(job.getId(), row.jobId());
    assertTrue(store().markJobFailedTerminal(row.jobId(), "done", 0, row.claimSeq()));
    assertEquals(
        OrphanRecovery.none(), store().resetOrphanJobsBefore(Instant.now().plusSeconds(60), 1, 10));
  }

  @Test
  void deferredExhaustedClaimRejectsStaleSaveAndStillCompletes() {
    JobEntity job = agedOrphan("dead", true);
    JobEntity stale = store().findById(job.getId()).orElseThrow();
    OrphanRecovery recovery = store().resetOrphanJobsBefore(Instant.now().minusSeconds(30), 1, 10);
    assertEquals(1, recovery.exhausted().size());
    ExhaustedOrphan row = recovery.exhausted().get(0);
    assertEquals(job.getId(), row.jobId());
    Instant deferredPickedAt = store().findById(job.getId()).orElseThrow().getPickedAt();

    RuntimeException failure = assertThrows(RuntimeException.class, () -> store().save(stale));
    Throwable cause = failure;
    while (cause != null && !(cause instanceof RatchetOptimisticLockException)) {
      cause = cause.getCause();
    }
    assertTrue(cause instanceof RatchetOptimisticLockException);

    JobEntity deferred = store().findById(job.getId()).orElseThrow();
    assertEquals(JobStatus.RUNNING, deferred.getStatus());
    assertEquals(deferredPickedAt, deferred.getPickedAt());
    assertEquals(row.claimSeq(), deferred.getClaimSeq());
    assertEquals("dead", deferred.getPickedBy());
    assertEquals(
        OrphanRecovery.none(),
        store().resetOrphanJobsBefore(Instant.now().minusSeconds(30), 1, 10));
    assertTrue(store().markJobFailedTerminal(row.jobId(), "done", 0, row.claimSeq()));
  }

  @Test
  void resetOrphanJobsForNode_pagesOwnRowsWhileHeartbeatIsLive() {
    auditStore();
    List<JobEntity> jobs = new ArrayList<>();
    for (int i = 0; i < 5; i++) jobs.add(agedOrphan("self", i < 2));
    store().upsertHeartbeat("self", Instant.now().plusSeconds(60));
    Set<UUID> exhaustedIds =
        jobs.subList(0, 2).stream().map(JobEntity::getId).collect(Collectors.toSet());
    List<Long> sequences =
        jobs.stream()
            .map(job -> store().findById(job.getId()).orElseThrow().getClaimSeq())
            .toList();
    List<Instant> pickedTimes =
        jobs.stream()
            .map(job -> store().findById(job.getId()).orElseThrow().getPickedAt())
            .toList();
    assertEquals(
        OrphanRecovery.none(), store().resetOrphanJobsBefore(Instant.now().minusSeconds(30), 1, 2));
    int released = 0;
    int reset = 0;
    int scanned = 2;
    for (int calls = 0; scanned == 2 && calls < 5; calls++) {
      OrphanRecovery page = store().resetOrphanJobsForNode("self", 1, 2);
      assertTrue(page.scanned() <= 2);
      scanned = page.scanned();
      released += page.released();
      reset += page.reset();
    }
    assertEquals(3, reset);
    assertEquals(2, released);
    for (int i = 0; i < jobs.size(); i++) {
      JobEntity after = store().findById(jobs.get(i).getId()).orElseThrow();
      assertNull(after.getPickedBy());
      assertEquals(
          exhaustedIds.contains(after.getId()) ? JobStatus.RUNNING : JobStatus.PENDING,
          after.getStatus());
      if (exhaustedIds.contains(after.getId())) {
        assertEquals(sequences.get(i).longValue(), after.getClaimSeq());
        assertEquals(pickedTimes.get(i), after.getPickedAt());
      }
      auditStore().findExecutionsByJobId(after.getId(), 100, 0).forEach(this::assertLostExecution);
    }
  }

  @Test
  void orphanMethodsWithNonPositiveLimitTouchNothing() {
    auditStore();
    JobEntity resettable = agedOrphan("self", false);
    JobEntity exhausted = agedOrphan("self", true);
    List<JobEntity> jobs = List.of(resettable, exhausted);
    List<Instant> times =
        jobs.stream()
            .map(job -> store().findById(job.getId()).orElseThrow().getPickedAt())
            .toList();
    List<Long> sequences =
        jobs.stream()
            .map(job -> store().findById(job.getId()).orElseThrow().getClaimSeq())
            .toList();
    for (int limit : List.of(0, -1)) {
      assertEquals(
          OrphanRecovery.none(),
          store().resetOrphanJobsBefore(Instant.now().minusSeconds(30), 1, limit));
      assertEquals(OrphanRecovery.none(), store().resetOrphanJobsForNode("self", 1, limit));
      assertTrue(
          store()
              .resetOrphanJobsBefore(Instant.now().minusSeconds(30), 1, limit)
              .exhausted()
              .isEmpty());
      for (int i = 0; i < jobs.size(); i++) {
        JobEntity after = store().findById(jobs.get(i).getId()).orElseThrow();
        assertEquals(JobStatus.RUNNING, after.getStatus());
        assertEquals("self", after.getPickedBy());
        assertEquals(times.get(i), after.getPickedAt());
        assertEquals(sequences.get(i).longValue(), after.getClaimSeq());
        assertRunningExecution(auditStore().findExecutionsByJobId(after.getId(), 100, 0).get(0));
      }
    }
  }

  private JobEntity agedOrphan(String owner, boolean exhausted) {
    JobEntity job = persist(newPendingJob());
    String firstOwner = exhausted ? "seed-" + job.getId() : owner;
    assertTrue(store().tryPickUpJob(job.getId(), firstOwner));
    if (exhausted) {
      assertEquals(1, store().resetOrphanJobsForNode(firstOwner, 1, 500).reset());
      assertTrue(store().tryPickUpJob(job.getId(), owner));
    }
    ageClaim(job.getId());
    auditStore().saveExecution(JobExecutionEntity.start(job.getId(), 1, owner));
    return store().findById(job.getId()).orElseThrow();
  }

  private void setClaimTime(UUID jobId, Instant pickedAt) {
    JobEntity job = store().findById(jobId).orElseThrow();
    job.setPickedAt(pickedAt);
    store().save(job);
  }

  private void ageClaim(UUID jobId) {
    setClaimTime(jobId, Instant.now().minusSeconds(120));
  }
}
