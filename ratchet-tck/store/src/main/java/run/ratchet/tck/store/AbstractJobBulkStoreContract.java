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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobStatus;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.id.UuidV7Factory;

/** Base contract tests for {@code JobBulkStore}. */
public abstract class AbstractJobBulkStoreContract implements JobStoreContractFixture {

  @BeforeEach
  @AfterEach
  void cleanupBulkFixture() {
    cleanupStore();
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
  void resetOrphanJobs_honorsSubMinuteGrace() {
    var job = newPendingJob();
    job = persist(job);
    job.setStatus(JobStatus.RUNNING);
    // Picked 45s ago by a phantom node that does not exist in scheduler_node
    job.setPickedBy("phantom-node-" + job.getId());
    job.setPickedAt(Instant.now().minusSeconds(45));
    store().save(job);

    // Grace = 15s → picked 45s ago IS orphaned → should be reset
    int reset = store().resetOrphanJobs(Duration.ofSeconds(15));
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
    store().resetOrphanJobs(Duration.ofSeconds(30));

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

    int reset = store().resetOrphanJobsBefore(Instant.now().minusSeconds(30));

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

    int reset = store().resetOrphanJobsBefore(now.minusSeconds(30));

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
  void deleteDlqOlderThan_removesOnlyExhaustedTerminalFailures() {
    JobEntity exhausted = newPendingJob();
    exhausted.setMaxRetries(1);
    exhausted = persist(exhausted);
    store().compareAndSwapStatus(exhausted.getId(), JobStatus.PENDING, JobStatus.RUNNING, null);
    store().markJobFailedTerminal(exhausted.getId(), "boom", 1);

    JobEntity retryable = newPendingJob();
    retryable.setMaxRetries(3);
    retryable = persist(retryable);
    store().compareAndSwapStatus(retryable.getId(), JobStatus.PENDING, JobStatus.RUNNING, null);
    store().markJobFailedTerminal(retryable.getId(), "retry later", 1);

    var pending = persist(newPendingJob());

    int deleted = store().deleteDlqOlderThan(Instant.now().plusSeconds(1));

    assertEquals(1, deleted, "Only exhausted terminal failures should be purged");
    assertTrue(store().findById(exhausted.getId()).isEmpty(), "Exhausted failure is deleted");
    assertTrue(store().findById(retryable.getId()).isPresent(), "Retryable failure remains");
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

    int reset = store().resetOrphanJobsForNode("node-self");
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

    int reset = store().resetOrphanJobsForNode("node-self");
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

    store().resetOrphanJobs(Duration.ofSeconds(1));

    assertEquals(
        JobStatus.PENDING,
        store().findById(pending.getId()).orElseThrow().getStatus(),
        "PENDING job should remain PENDING");
    assertEquals(
        JobStatus.CANCELED,
        store().findById(canceled.getId()).orElseThrow().getStatus(),
        "CANCELED job should remain CANCELED");
  }
}
