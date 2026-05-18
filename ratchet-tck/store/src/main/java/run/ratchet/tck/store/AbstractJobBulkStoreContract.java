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
import run.ratchet.store.entity.JobExecutionType;
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

    JobEntity recurring = newPendingJob(tag);
    recurring.setJobType(JobExecutionType.RECURRING);
    recurring = persist(recurring);
    UUID recurringId = recurring.getId();

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
    assertEquals(
        JobStatus.PENDING,
        store().getJobStatus(recurringId),
        "Recurring jobs are not affected by cancelJobsByTag");
  }

  @Test
  void cancelJobsByTag_returnsZeroWhenNoMatchingJobs() {
    persist(newPendingJob("other-tag"));

    int count = store().cancelJobsByTag("nonexistent");

    assertEquals(0, count, "No matching tag should produce zero cancellations");
  }

  @Test
  void cancelRecurringJobsByTag_bulkUpdate() {
    String tag = "recurring-tag";

    JobEntity rec1 = newPendingJob(tag);
    rec1.setJobType(JobExecutionType.RECURRING);
    rec1 = persist(rec1);

    JobEntity rec2 = newPendingJob(tag);
    rec2.setJobType(JobExecutionType.RECURRING);
    rec2 = persist(rec2);

    JobEntity untaggedRecurring = newPendingJob();
    untaggedRecurring.setJobType(JobExecutionType.RECURRING);
    untaggedRecurring = persist(untaggedRecurring);

    int count = store().cancelRecurringJobsByTag(tag);

    assertEquals(2, count, "Should cancel both tagged recurring jobs in a single bulk operation");
    assertEquals(JobStatus.CANCELED, store().getJobStatus(rec1.getId()));
    assertEquals(JobStatus.CANCELED, store().getJobStatus(rec2.getId()));
    assertEquals(
        JobStatus.PENDING,
        store().getJobStatus(untaggedRecurring.getId()),
        "Untagged recurring job remains active");
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
