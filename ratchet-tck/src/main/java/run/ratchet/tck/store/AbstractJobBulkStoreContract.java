package run.ratchet.tck.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import run.ratchet.store.entity.JobStatus;
import run.ratchet.store.id.TsidFactory;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** Base contract tests for {@code JobBulkStore}. */
public abstract class AbstractJobBulkStoreContract implements JobStoreContractFixture {

  @AfterEach
  void cleanupBulkFixture() {
    cleanupStore();
  }

  @Test
  void bulkInsert_persistsAllJobs() {
    var job1 = newPendingJob();
    job1.setId(TsidFactory.next());
    var job2 = newPendingJob();
    job2.setId(TsidFactory.next());
    var job3 = newPendingJob();
    job3.setId(TsidFactory.next());

    store().bulkInsert(List.of(job1, job2, job3));

    var found = store().findByIds(List.of(job1.getId(), job2.getId(), job3.getId()));
    assertEquals(3, found.size(), "bulkInsert should persist all 3 jobs");
  }

  /**
   * Verifies that {@code resetOrphanJobs} honors sub-minute grace periods.
   *
   * <p>Regression for the pre-alpha unit-confusion bug: all three store impls used {@code
   * Duration.toMinutes()} which truncated sub-minute values to 0 (resetting every RUNNING job) or
   * produced non-multiple-of-60 mismatches with node heartbeats (duplicate execution race).
   */
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
}
