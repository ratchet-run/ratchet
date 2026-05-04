package run.ratchet.store.mongodb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import run.ratchet.store.entity.ArchivedJobEntity;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.api.JobStatus;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

class ArchiveStoreIT extends BaseDocumentStoreIT {

  @Test
  void archiveJob_preservesOriginalData() {
    JobEntity job = store().save(newPendingJob());
    store().claimNextBatch(1, "node-1");
    store().compareAndSwapStatus(job.getId(), JobStatus.RUNNING, JobStatus.SUCCEEDED, null);

    job = store().findById(job.getId()).orElseThrow();

    ArchivedJobEntity archived = store().archiveJob(job, "completed", "system");

    assertNotNull(archived.getId());
    assertEquals(job.getId(), archived.getOriginalJobId());
    assertEquals(JobStatus.SUCCEEDED, archived.getFinalStatus());
    assertEquals("completed", archived.getArchiveReason());
    assertEquals("system", archived.getArchivedBy());
    assertNotNull(archived.getArchivedAt());
  }

  @Test
  void archiveJobsBatch_archivesMultiple() {
    JobEntity j1 = store().save(newPendingJob());
    JobEntity j2 = store().save(newPendingJob());

    store().claimNextBatch(2, "node-1");
    store().compareAndSwapStatus(j1.getId(), JobStatus.RUNNING, JobStatus.SUCCEEDED, null);
    store().compareAndSwapStatus(j2.getId(), JobStatus.RUNNING, JobStatus.SUCCEEDED, null);

    j1 = store().findById(j1.getId()).orElseThrow();
    j2 = store().findById(j2.getId()).orElseThrow();

    int count = store().archiveJobsBatch(List.of(j1, j2), "batch-archive", "admin");
    assertEquals(2, count);

    List<ArchivedJobEntity> all = store().findArchivedJobs(null, null, null, null, 100);
    assertEquals(2, all.size());
  }

  @Test
  void findArchivedJobs_filtersByTargetClass() {
    for (int i = 0; i < 2; i++) {
      JobEntity job = store().save(newPendingJob());
      store().claimNextBatch(1, "node-1");
      store().compareAndSwapStatus(job.getId(), JobStatus.RUNNING, JobStatus.SUCCEEDED, null);
      job = store().findById(job.getId()).orElseThrow();
      store().archiveJob(job, "done", "system");
    }

    List<ArchivedJobEntity> found =
        store().findArchivedJobs("com.example.TestJob", null, null, null, 100);
    assertEquals(2, found.size());

    List<ArchivedJobEntity> notFound =
        store().findArchivedJobs("com.example.Other", null, null, null, 100);
    assertTrue(notFound.isEmpty());
  }

  @Test
  void purgeArchivedJobs_removesOldArchives() {
    JobEntity job = store().save(newPendingJob());
    store().claimNextBatch(1, "node-1");
    store().compareAndSwapStatus(job.getId(), JobStatus.RUNNING, JobStatus.SUCCEEDED, null);
    job = store().findById(job.getId()).orElseThrow();
    store().archiveJob(job, "done", "system");

    // Purge archives older than 1 second from now (should catch the one we just created
    // since archivedAt is in the past by the time we get here)
    int purged = store().purgeArchivedJobs(Instant.now().plusSeconds(1));
    assertEquals(1, purged);
  }
}
