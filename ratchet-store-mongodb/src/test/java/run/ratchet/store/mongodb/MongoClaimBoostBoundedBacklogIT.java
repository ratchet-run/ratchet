package run.ratchet.store.mongodb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobPriority;
import run.ratchet.api.RatchetOptions;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;

/**
 * Pins exact effective-priority ordering even when the pending backlog is larger than the former
 * Mongo claim candidate window.
 */
class MongoClaimBoostBoundedBacklogIT extends BaseDocumentStoreIT {

  private static final int BACKLOG_ABOVE_FORMER_CEILING = 2100;
  private static final int CLAIM_LIMIT = 5;

  private static Instant oldEnoughForLowestToBeatCritical() {
    int boostInterval = RatchetOptions.defaults().store().priorityBoostIntervalMinutes();
    assumeTrue(boostInterval > 0, "priority boosting is disabled");
    return Instant.now()
        .minus(Duration.ofMinutes((long) boostInterval * (JobPriority.CRITICAL.ordinal() + 1L)))
        .minusSeconds(1);
  }

  @Test
  void boostedLowPriorityClaimedFirst_evenWithBacklogAboveFormerCeiling() {
    List<UUID> expectedLowIds = new ArrayList<>(CLAIM_LIMIT);
    Instant oldEnoughToPassCritical = oldEnoughForLowestToBeatCritical();

    for (int i = 0; i < BACKLOG_ABOVE_FORMER_CEILING; i++) {
      JobEntity low = newPendingJob(JobPriority.LOWEST);
      low.setScheduledTime(oldEnoughToPassCritical);
      JobEntity saved = store().save(low);
      if (expectedLowIds.size() < CLAIM_LIMIT) {
        expectedLowIds.add(saved.getId());
      }
    }

    for (int i = 0; i < CLAIM_LIMIT; i++) {
      JobEntity high = newPendingJob(JobPriority.CRITICAL);
      high.setScheduledTime(Instant.now().minusSeconds(1));
      store().save(high);
    }

    List<UUID> claimedIds =
        store().claimNextBatchOptimized(JobExecutionType.SINGLE, CLAIM_LIMIT, "node-1").stream()
            .map(claim -> claim.id())
            .toList();

    assertEquals(
        expectedLowIds,
        claimedIds,
        "Aged LOWEST jobs should outrank fresh CRITICAL jobs once their effective priority is higher");
  }
}
