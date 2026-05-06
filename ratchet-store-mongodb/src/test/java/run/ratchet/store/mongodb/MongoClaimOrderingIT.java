package run.ratchet.store.mongodb;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobPriority;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;

class MongoClaimOrderingIT extends BaseDocumentStoreIT {

  @Test
  void optimizedClaim_prefersHigherPriorityJobs() {
    JobEntity low = newPendingJob(JobPriority.LOW);
    low.setScheduledTime(Instant.now().minusSeconds(5));
    low = store().save(low);

    JobEntity high = newPendingJob(JobPriority.HIGH);
    high.setScheduledTime(Instant.now().minusSeconds(5));
    high = store().save(high);

    List<UUID> ids =
        store().claimNextBatchOptimized(JobExecutionType.SINGLE, 10, "node-1").stream()
            .map(claim -> claim.id())
            .toList();

    assertEquals(List.of(high.getId(), low.getId()), ids);
  }

  @Test
  void optimizedClaim_boostsLongWaitingJobsAheadOfNewerWork() {
    JobEntity boostedLow = newPendingJob(JobPriority.LOWEST);
    boostedLow.setScheduledTime(Instant.now().minusSeconds(45 * 60));
    boostedLow = store().save(boostedLow);

    JobEntity normal = newPendingJob(JobPriority.NORMAL);
    normal.setScheduledTime(Instant.now().minusSeconds(30));
    normal = store().save(normal);

    List<UUID> ids =
        store().claimNextBatchOptimized(JobExecutionType.SINGLE, 10, "node-1").stream()
            .map(claim -> claim.id())
            .toList();

    assertEquals(List.of(boostedLow.getId(), normal.getId()), ids);
  }
}
