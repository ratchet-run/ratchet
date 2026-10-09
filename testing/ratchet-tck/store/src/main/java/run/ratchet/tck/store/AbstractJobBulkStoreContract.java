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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobStatus;
import run.ratchet.store.entity.JobEntity;
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
    int reset = store().resetOrphanJobs(Duration.ofSeconds(15), 3, 100).reset();
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
    store().resetOrphanJobs(Duration.ofSeconds(30), 3, 100);

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

    int reset = store().resetOrphanJobsBefore(Instant.now().minusSeconds(30), 3, 100).reset();

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

    int reset = store().resetOrphanJobsBefore(now.minusSeconds(30), 3, 100).reset();

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

    int reset = store().resetOrphanJobsForNode("node-self", 3, 100).reset();
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

    int reset = store().resetOrphanJobsForNode("node-self", 3, 100).reset();
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

    store().resetOrphanJobs(Duration.ofSeconds(1), 3, 100);

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
    OrphanRecovery first = store().resetOrphanJobsBefore(Instant.now().plusSeconds(1), 1, 100);
    assertEquals(1, first.reset());
    assertTrue(first.exhausted().isEmpty());
    assertEquals(JobStatus.PENDING, store().getJobStatus(job.getId()));
    assertEquals(0, store().findById(job.getId()).orElseThrow().getAttempts());
    assertTrue(store().tryPickUpJob(job.getId(), "dead"));
    long claimSeq = store().findById(job.getId()).orElseThrow().getClaimSeq();

    OrphanRecovery second = store().resetOrphanJobsBefore(Instant.now().plusSeconds(1), 1, 100);

    assertEquals(0, second.reset());
    assertEquals(
        List.of(new ExhaustedOrphan(job.getId(), claimSeq, 1, "dead")), second.exhausted());
    assertEquals(JobStatus.RUNNING, store().getJobStatus(job.getId()));
    assertEquals(second, store().resetOrphanJobsBefore(Instant.now().plusSeconds(1), 1, 100));
  }

  @Test
  void graceRecoveryChargesBudgetAndStopsAtLimit() {
    JobEntity job = persist(newPendingJob());
    assertTrue(store().tryPickUpJob(job.getId(), "dead"));
    ageClaim(job.getId());
    assertEquals(1, store().resetOrphanJobs(Duration.ofSeconds(30), 1, 100).reset());
    assertTrue(store().tryPickUpJob(job.getId(), "dead"));
    ageClaim(job.getId());
    OrphanRecovery exhausted = store().resetOrphanJobs(Duration.ofSeconds(30), 1, 100);
    assertEquals(0, exhausted.reset());
    assertEquals(1, exhausted.exhausted().size());
    assertEquals(1, exhausted.exhausted().get(0).crashCount());
    assertEquals(JobStatus.RUNNING, store().getJobStatus(job.getId()));
  }

  @Test
  void startupRecoveryChargesBudgetAndReleasesAllExhaustedOwners() {
    JobEntity first = persist(newPendingJob());
    JobEntity second = persist(newPendingJob());
    for (JobEntity job : List.of(first, second)) {
      assertTrue(store().tryPickUpJob(job.getId(), "self"));
    }
    assertEquals(2, store().resetOrphanJobsForNode("self", 1, 100).reset());
    for (JobEntity job : List.of(first, second)) {
      assertTrue(store().tryPickUpJob(job.getId(), "self"));
    }
    JobEntity before = store().findById(first.getId()).orElseThrow();
    OrphanRecovery startup = store().resetOrphanJobsForNode("self", 1, 1);
    assertEquals(0, startup.reset());
    assertEquals(1, startup.exhausted().size());
    assertEquals("self", startup.exhausted().get(0).pickedBy());
    assertEquals(1, startup.exhausted().get(0).crashCount());
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
    OrphanRecovery next = store().resetOrphanJobsBefore(Instant.now().plusSeconds(1), 1, 100);
    assertEquals(
        2, next.exhausted().size(), "NULL owners remain detectable after restart heartbeat");
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
    assertEquals(0, result.reset());
    assertEquals(2, result.exhausted().size());
    assertTrue(result.exhausted().stream().noneMatch(row -> row.jobId().equals(live.getId())));
    assertTrue(result.exhausted().stream().allMatch(row -> row.crashCount() == 0));
    assertEquals(JobStatus.RUNNING, store().getJobStatus(live.getId()));
  }

  @Test
  void crashFailedJobIsPurgedBelowRetryLimit() {
    JobEntity job = newPendingJob();
    job.setMaxRetries(10);
    job = persist(job);
    assertTrue(store().tryPickUpJob(job.getId(), "dead"));
    ExhaustedOrphan orphan =
        store().resetOrphanJobsBefore(Instant.now().plusSeconds(1), 0, 100).exhausted().get(0);
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

    OrphanRecovery result = store().resetOrphanJobs(Duration.ofSeconds(30), 0, 2);

    assertEquals(0, result.reset());
    assertEquals(2, result.exhausted().size());
    assertTrue(result.exhausted().stream().noneMatch(row -> row.jobId().equals(live.getId())));
    assertEquals(JobStatus.RUNNING, store().getJobStatus(live.getId()));
  }

  private void ageClaim(UUID jobId) {
    JobEntity job = store().findById(jobId).orElseThrow();
    job.setPickedAt(Instant.now().minusSeconds(120));
    store().save(job);
  }
}
