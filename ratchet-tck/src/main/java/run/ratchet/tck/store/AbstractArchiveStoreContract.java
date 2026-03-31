package run.ratchet.tck.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import run.ratchet.store.entity.JobStatus;
import java.time.Instant;
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
}
