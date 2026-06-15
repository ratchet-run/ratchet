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
package run.ratchet.store.mongodb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobPriority;
import run.ratchet.api.RatchetOptions;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;

class MongoClaimOrderingIT extends BaseDocumentStoreIT {

  private static int priorityBoostIntervalMinutes() {
    int boostInterval = RatchetOptions.defaults().store().priorityBoostIntervalMinutes();
    assumeTrue(boostInterval > 0, "priority boosting is disabled");
    return boostInterval;
  }

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
  void optimizedClaim_respectsBatchLimitAcrossSubsequentClaims() {
    Instant due = Instant.now().minusSeconds(5);
    for (int i = 0; i < 12; i++) {
      JobEntity job = newPendingJob(JobPriority.NORMAL);
      job.setScheduledTime(due.plusMillis(i));
      store().save(job);
    }

    List<UUID> firstBatch =
        store().claimNextBatchOptimized(JobExecutionType.SINGLE, 10, "node-1").stream()
            .map(claim -> claim.id())
            .toList();
    List<UUID> secondBatch =
        store().claimNextBatchOptimized(JobExecutionType.SINGLE, 10, "node-2").stream()
            .map(claim -> claim.id())
            .toList();

    assertEquals(10, firstBatch.size());
    assertEquals(2, secondBatch.size());
    assertTrue(firstBatch.stream().noneMatch(secondBatch::contains));
  }

  @Test
  void optimizedClaim_boostsLongWaitingJobsAheadOfNewerWork() {
    int boostInterval = priorityBoostIntervalMinutes();

    JobEntity boostedLow = newPendingJob(JobPriority.LOWEST);
    // LOWEST needs three default 15-minute boost intervals to outrank fresh NORMAL work.
    boostedLow.setScheduledTime(
        Instant.now().minus(Duration.ofMinutes((long) boostInterval * 3)).minusSeconds(1));
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

  @Test
  void optimizedClaim_doesNotBoostBeforeFullIntervalBoundary() {
    int boostInterval = priorityBoostIntervalMinutes();

    JobEntity nearlyBoostedLow = newPendingJob(JobPriority.LOWEST);
    nearlyBoostedLow.setScheduledTime(
        Instant.now().minus(Duration.ofMinutes((long) boostInterval * 2)).plusSeconds(1));
    nearlyBoostedLow = store().save(nearlyBoostedLow);

    JobEntity normal = newPendingJob(JobPriority.NORMAL);
    normal.setScheduledTime(Instant.now().minusSeconds(30));
    normal = store().save(normal);

    List<UUID> ids =
        store().claimNextBatchOptimized(JobExecutionType.SINGLE, 10, "node-1").stream()
            .map(claim -> claim.id())
            .toList();

    assertEquals(List.of(normal.getId(), nearlyBoostedLow.getId()), ids);
  }
}
