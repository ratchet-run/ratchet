package run.ratchet.store.mongodb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobStatus;
import run.ratchet.store.entity.ArchivedJobEntity;
import run.ratchet.store.entity.JobEntity;

class MongoArchiveOperationsIT extends BaseDocumentStoreIT {

  @Test
  void archiveJobsBatchMovesSourceJobsInsideMongoTransaction() {
    JobEntity first = complete(store().save(newPendingJob()));
    JobEntity second = complete(store().save(newPendingJob()));

    int archived = store().archiveJobsBatch(List.of(first, second), "retention", "system");

    assertEquals(2, archived);
    assertTrue(store().findById(first.getId()).isEmpty());
    assertTrue(store().findById(second.getId()).isEmpty());

    Set<UUID> archivedOriginalIds =
        store().findArchivedJobs(null, null, null, null, 10).stream()
            .map(ArchivedJobEntity::getOriginalJobId)
            .collect(Collectors.toSet());
    assertEquals(Set.of(first.getId(), second.getId()), archivedOriginalIds);
  }

  private JobEntity complete(JobEntity job) {
    store().compareAndSwapStatus(job.getId(), JobStatus.PENDING, JobStatus.RUNNING, null);
    store().markJobSucceeded(job.getId(), null, null, Instant.now(), Instant.now(), 100L, 50L);
    return store().findById(job.getId()).orElseThrow();
  }
}
