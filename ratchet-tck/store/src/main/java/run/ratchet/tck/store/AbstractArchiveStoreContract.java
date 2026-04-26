package run.ratchet.tck.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobStatus;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** Base contract tests for {@code ArchiveStore}. */
public abstract class AbstractArchiveStoreContract implements JobStoreContractFixture {

  @AfterEach
  void cleanupArchiveFixture() {
    cleanupStore();
  }

  @Test
  void archiveJob_createsArchiveRecord() {
    var job = persist(newPendingJob());
    store().compareAndSwapStatus(job.getId(), JobStatus.PENDING, JobStatus.RUNNING, null);
    store().markJobSucceeded(job.getId(), null, null, Instant.now(), Instant.now(), 100L, 50L);
    var completed = store().findById(job.getId()).orElseThrow();

    var archived = store().archiveJob(completed, "test", "tck");

    assertNotNull(archived, "archiveJob should return a non-null archived entity");
    assertEquals(
        completed.getId(),
        archived.getOriginalJobId(),
        "Archived entity should reference the original job ID");
  }

  @Test
  void findArchivedJobs_returnsByTargetClass() {
    var job = persist(newPendingJob());
    store().compareAndSwapStatus(job.getId(), JobStatus.PENDING, JobStatus.RUNNING, null);
    store().markJobSucceeded(job.getId(), null, null, Instant.now(), Instant.now(), 100L, 50L);
    var completed = store().findById(job.getId()).orElseThrow();
    store().archiveJob(completed, "test", "tck");

    var results = store().findArchivedJobs(completed.getPayload().target(), null, null, null, 10);

    assertFalse(results.isEmpty(), "findArchivedJobs should return the archived job");
    assertEquals(
        completed.getId(),
        results.get(0).getOriginalJobId(),
        "Archived result should match the original job");
  }

  @Test
  void archiveJobsBatch_archivesMultipleJobs() {
    var job1 = completeJob(persist(newPendingJob()));
    var job2 = completeJob(persist(newPendingJob()));

    int count = store().archiveJobsBatch(List.of(job1, job2), "batch-test", "tck");

    assertEquals(2, count, "archiveJobsBatch should archive both jobs");
  }

  @Test
  void findJobsForArchiving_excludesRecentJobs() {
    persist(newPendingJob());

    var candidates = store().findJobsForArchiving(Instant.now().minusSeconds(3600), 10);

    assertTrue(
        candidates.isEmpty(),
        "A job created just now should not be eligible for archiving with a 1-hour cutoff");
  }

  @Test
  void countJobsForArchiving_matchesFindCount() {
    persist(newPendingJob());

    Instant cutoff = Instant.now().minusSeconds(3600);
    long count = store().countJobsForArchiving(cutoff);
    var candidates = store().findJobsForArchiving(cutoff, 100);

    assertEquals(
        count, candidates.size(), "countJobsForArchiving and findJobsForArchiving should agree");
  }

  @Test
  void purgeArchivedJobs_removesOldArchives() {
    var job = completeJob(persist(newPendingJob()));
    store().archiveJob(job, "purge-test", "tck");

    // Purge with a cutoff in the future — should remove the just-archived record
    int purged = store().purgeArchivedJobs(Instant.now().plusSeconds(3600));

    assertTrue(purged >= 1, "purgeArchivedJobs should remove the archived record");
  }

  @Test
  void findArchivedJobs_filtersByBusinessKey() {
    var job1 = newPendingJob();
    job1.setBusinessKey("bk-archive-1");
    job1 = completeJob(persist(job1));
    store().archiveJob(job1, "test", "tck");

    var job2 = newPendingJob();
    job2.setBusinessKey("bk-archive-2");
    job2 = completeJob(persist(job2));
    store().archiveJob(job2, "test", "tck");

    var results = store().findArchivedJobs(null, "bk-archive-1", null, null, 10);

    assertEquals(1, results.size(), "findArchivedJobs should filter by businessKey");
    assertEquals("bk-archive-1", results.get(0).getBusinessKey());
  }

  @Test
  void findArchivedJobs_emptyStore_returnsEmptyList() {
    var results = store().findArchivedJobs(null, null, null, null, 10);

    assertTrue(results.isEmpty(), "findArchivedJobs on empty store should return empty list");
  }

  private JobEntity completeJob(JobEntity job) {
    store().compareAndSwapStatus(job.getId(), JobStatus.PENDING, JobStatus.RUNNING, null);
    store().markJobSucceeded(job.getId(), null, null, Instant.now(), Instant.now(), 100L, 50L);
    return store().findById(job.getId()).orElseThrow();
  }
}
