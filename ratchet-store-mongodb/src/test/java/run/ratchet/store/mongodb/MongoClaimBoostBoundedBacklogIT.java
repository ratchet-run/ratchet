package run.ratchet.store.mongodb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import run.ratchet.api.JobPriority;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Pins the documented {@code CLAIM_CANDIDATE_CEILING} contract: above a backlog of 2048
 * candidates per type, top-priority jobs still win but the oldest low-priority jobs may not
 * surface for the boost pass.
 */
class MongoClaimBoostBoundedBacklogIT extends BaseDocumentStoreIT {

  private static final int BACKLOG_ABOVE_CEILING = 2100;
  private static final int HIGH_PRIORITY_COUNT = 5;
  private static final int CLAIM_LIMIT = 300;

  @Test
  void topPriorityClaimedFirst_evenWithBacklogAboveCeiling() {
    List<Long> expectedHighIds = new ArrayList<>(HIGH_PRIORITY_COUNT);

    for (int i = 0; i < BACKLOG_ABOVE_CEILING; i++) {
      JobEntity low = newPendingJob(JobPriority.LOWEST);
      low.setScheduledTime(Instant.now().minusSeconds(3600));
      store().save(low);
    }

    for (int i = 0; i < HIGH_PRIORITY_COUNT; i++) {
      JobEntity high = newPendingJob(JobPriority.CRITICAL);
      high.setScheduledTime(Instant.now().minusSeconds(1));
      JobEntity saved = store().save(high);
      expectedHighIds.add(saved.getId());
    }

    List<Long> claimedIds =
        store().claimNextBatchOptimized(JobExecutionType.SINGLE, CLAIM_LIMIT, "node-1").stream()
            .map(claim -> claim.id())
            .toList();

    assertEquals(CLAIM_LIMIT, claimedIds.size(), "claim should fill requested batch");
    List<Long> topOfBatch = claimedIds.subList(0, HIGH_PRIORITY_COUNT);
    assertTrue(
        topOfBatch.containsAll(expectedHighIds),
        "HIGHEST-priority jobs must lead the batch even with backlog above CLAIM_CANDIDATE_CEILING; actual lead="
            + topOfBatch
            + " expected="
            + expectedHighIds);
  }
}
