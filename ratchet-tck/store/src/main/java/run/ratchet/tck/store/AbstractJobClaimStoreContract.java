package run.ratchet.tck.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.ExecutorTargets;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobStatus;
import run.ratchet.api.NodeTagFilter;
import run.ratchet.api.RatchetOptions;
import run.ratchet.store.dto.JobClaimDto;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.tck.util.ConcurrentTestRunner;

/** Base contract tests for {@code JobClaimStore}. */
public abstract class AbstractJobClaimStoreContract implements JobStoreContractFixture {

  private static Instant oldEnoughForLowestToBeatCritical() {
    int boostInterval = RatchetOptions.defaults().store().priorityBoostIntervalMinutes();
    assumeTrue(boostInterval > 0, "priority boosting is disabled");
    return Instant.now()
        .minus(Duration.ofMinutes((long) boostInterval * (JobPriority.CRITICAL.ordinal() + 1L)))
        .minusSeconds(1);
  }

  @BeforeEach
  @AfterEach
  void cleanupClaimFixture() {
    cleanupStore();
  }

  @Test
  void claimNextBatch_claimsPendingJobs() {
    persist(newPendingJob());
    persist(newPendingJob());

    var claimed = store().claimNextBatch(10, "node-1");

    assertEquals(2, claimed.size(), "claimNextBatch should return both pending jobs");
    for (var job : claimed) {
      assertEquals(JobStatus.RUNNING, job.getStatus(), "Claimed job should be RUNNING");
      assertEquals("node-1", job.getPickedBy(), "Claimed job should record the claiming node");
    }
  }

  @Test
  void claimNextBatch_respectsLimit() {
    persist(newPendingJob());
    persist(newPendingJob());
    persist(newPendingJob());

    var claimed = store().claimNextBatch(2, "node-1");

    assertEquals(2, claimed.size(), "claimNextBatch should respect the limit parameter");
  }

  @Test
  void claimNextBatch_skipsAlreadyClaimedJobs() {
    persist(newPendingJob());
    persist(newPendingJob());

    store().claimNextBatch(10, "node-1");
    var secondClaim = store().claimNextBatch(10, "node-2");

    assertTrue(secondClaim.isEmpty(), "Second claim should return empty when all jobs are taken");
  }

  @Test
  void claimNextBatch_emptyStore_returnsEmptyList() {
    var claimed = store().claimNextBatch(10, "node-1");

    assertTrue(claimed.isEmpty(), "claimNextBatch on empty store should return empty list");
  }

  @Test
  void claimNextBatchOptimized_usesAgeBoostedEffectivePriority() {
    JobEntity oldLow = newPendingJob();
    oldLow.setPriority(JobPriority.LOWEST);
    oldLow.setScheduledTime(oldEnoughForLowestToBeatCritical());
    oldLow = persist(oldLow);

    JobEntity freshCritical = newPendingJob();
    freshCritical.setPriority(JobPriority.CRITICAL);
    freshCritical.setScheduledTime(Instant.now().minusSeconds(1));
    persist(freshCritical);

    List<JobClaimDto> claims =
        store().claimNextBatchOptimized(JobExecutionType.SINGLE, 1, "node-1");

    assertEquals(1, claims.size(), "optimized claim should return the requested job");
    assertEquals(
        oldLow.getId(),
        claims.get(0).id(),
        "age-boosted LOWEST job should outrank a fresh CRITICAL job");
  }

  @Test
  void claimNextBatchOptimized_returnsBatchInPriorityOrder() {
    Instant due = Instant.now().minusSeconds(5);

    JobEntity low = newPendingJob();
    low.setPriority(JobPriority.LOW);
    low.setScheduledTime(due);
    low = persist(low);

    JobEntity high = newPendingJob();
    high.setPriority(JobPriority.HIGH);
    high.setScheduledTime(due);
    high = persist(high);

    List<JobClaimDto> claims = store().claimNextBatchOptimized(JobExecutionType.SINGLE, 10, "node");

    assertEquals(
        List.of(high.getId(), low.getId()),
        claims.stream().map(JobClaimDto::id).toList(),
        "optimized claim should return jobs in effective-priority order even when the batch fits"
            + " within the limit");
  }

  @Test
  void claimNextBatch_returnsBatchInPriorityOrder() {
    Instant due = Instant.now().minusSeconds(5);

    JobEntity low = newPendingJob();
    low.setPriority(JobPriority.LOW);
    low.setScheduledTime(due);
    low = persist(low);

    JobEntity high = newPendingJob();
    high.setPriority(JobPriority.HIGH);
    high.setScheduledTime(due);
    high = persist(high);

    List<JobEntity> claimed = store().claimNextBatch(10, "node");

    assertEquals(
        List.of(high.getId(), low.getId()),
        claimed.stream().map(JobEntity::getId).toList(),
        "claimNextBatch should return jobs in effective-priority order even when the batch fits"
            + " within the limit");
  }

  @Test
  void claimNextBatch_skipsNonPendingJobs() {
    var running = persist(newPendingJob());
    store().compareAndSwapStatus(running.getId(), JobStatus.PENDING, JobStatus.RUNNING, null);

    var canceled = persist(newPendingJob());
    store().compareAndSwapStatus(canceled.getId(), JobStatus.PENDING, JobStatus.RUNNING, null);
    store().compareAndSwapStatus(canceled.getId(), JobStatus.RUNNING, JobStatus.CANCELED, null);

    var claimed = store().claimNextBatch(10, "node-1");

    assertTrue(claimed.isEmpty(), "claimNextBatch should skip non-PENDING jobs");
  }

  @Test
  void claimNextBatch_setsPickedByAndPickedAt() {
    persist(newPendingJob());

    var claimed = store().claimNextBatch(10, "node-1");

    assertEquals(1, claimed.size());
    JobEntity job = claimed.get(0);
    assertEquals("node-1", job.getPickedBy(), "Claimed job should record the claiming node");
    assertNotNull(job.getPickedAt(), "Claimed job should have a non-null pickedAt timestamp");
  }

  @Test
  void claimNextBatch_concurrent_noDuplicateClaims() {
    int jobCount = 10;
    for (int i = 0; i < jobCount; i++) {
      persist(newPendingJob());
    }

    Set<UUID> allClaimedIds = ConcurrentHashMap.newKeySet();
    Set<UUID> duplicates = ConcurrentHashMap.newKeySet();

    List<Throwable> failures =
        ConcurrentTestRunner.runAll(
            Duration.ofSeconds(10),
            () -> {
              for (JobEntity job : store().claimNextBatch(jobCount, "node-A")) {
                if (!allClaimedIds.add(job.getId())) {
                  duplicates.add(job.getId());
                }
              }
            },
            () -> {
              for (JobEntity job : store().claimNextBatch(jobCount, "node-B")) {
                if (!allClaimedIds.add(job.getId())) {
                  duplicates.add(job.getId());
                }
              }
            },
            () -> {
              for (JobEntity job : store().claimNextBatch(jobCount, "node-C")) {
                if (!allClaimedIds.add(job.getId())) {
                  duplicates.add(job.getId());
                }
              }
            });

    long errorCount = failures.stream().filter(t -> t != null).count();
    assertEquals(0L, errorCount, "no thread should fail; got " + failures);
    assertTrue(duplicates.isEmpty(), "no job should be claimed by multiple nodes: " + duplicates);
    assertEquals(jobCount, allClaimedIds.size(), "all jobs should be claimed exactly once");
  }

  @Test
  void claimNextBatchOptimized_filtersByRequestedExecutionType() {
    persist(newPendingJob());

    JobEntity batchChild = newPendingJob();
    batchChild.setJobType(JobExecutionType.BATCH_CHILD);
    persist(batchChild);

    List<JobClaimDto> claims =
        store().claimNextBatchOptimized(JobExecutionType.BATCH_CHILD, 10, "node-1");

    assertEquals(1, claims.size(), "optimized claim should only return the requested job type");
    assertEquals(JobExecutionType.BATCH_CHILD, claims.get(0).jobType());
  }

  @Test
  void claimNextBatchOptimized_withRequireTags_onlyClaimsMatchingJobs() {
    persist(newPendingJob("gpu"));
    persist(newPendingJob());

    NodeTagFilter filter = new NodeTagFilter(List.of("gpu"), List.of());
    List<JobClaimDto> claimed =
        store().claimNextBatchOptimized(JobExecutionType.SINGLE, 10, "node-req", filter);

    assertEquals(1, claimed.size(), "requireTags should only claim job with matching tag");
  }

  @Test
  void claimNextBatchOptimized_withExcludeTags_skipsExcludedJobs() {
    persist(newPendingJob("gpu"));
    persist(newPendingJob());

    NodeTagFilter filter = new NodeTagFilter(List.of(), List.of("gpu"));
    List<JobClaimDto> claimed =
        store().claimNextBatchOptimized(JobExecutionType.SINGLE, 10, "node-exc", filter);

    assertEquals(1, claimed.size(), "excludeTags should skip job tagged gpu");
  }

  @Test
  void claimNextBatchOptimized_withBothFilters_appliesBoth() {
    persist(newPendingJob("gpu"));
    persist(newPendingJob("batch"));
    persist(newPendingJob("gpu", "batch"));
    persist(newPendingJob());

    NodeTagFilter filter = new NodeTagFilter(List.of("gpu"), List.of("batch"));
    List<JobClaimDto> claimed =
        store().claimNextBatchOptimized(JobExecutionType.SINGLE, 10, "node-both", filter);

    assertEquals(1, claimed.size(), "only job tagged gpu-but-not-batch should be claimed");
  }

  @Test
  void claimNextBatchOptimized_withNoneFilter_claimsAll() {
    persist(newPendingJob("gpu"));
    persist(newPendingJob("cpu"));

    List<JobClaimDto> claimed =
        store()
            .claimNextBatchOptimized(JobExecutionType.SINGLE, 10, "node-all", NodeTagFilter.NONE);

    assertEquals(2, claimed.size(), "NodeTagFilter.NONE should claim all pending jobs");
  }

  @Test
  void claimNextBatch_withRequireTags_onlyClaimsMatchingJobs() {
    persist(newPendingJob("gpu"));
    persist(newPendingJob());

    NodeTagFilter filter = new NodeTagFilter(List.of("gpu"), List.of());
    List<JobEntity> claimed = store().claimNextBatch(10, "node-nb-req", filter);

    assertEquals(1, claimed.size(), "claimNextBatch requireTags should only claim matching job");
  }

  @Test
  void claimNextBatchOptimized_roundTripsExecutionTarget() {
    JobEntity targeted = newPendingJob();
    targeted.setExecutionTarget(ExecutorTargets.VIRTUAL);
    persist(targeted);

    List<JobClaimDto> claims =
        store().claimNextBatchOptimized(JobExecutionType.SINGLE, 10, "node-1");

    assertEquals(1, claims.size());
    assertEquals(
        ExecutorTargets.VIRTUAL,
        claims.get(0).executionTarget(),
        "claim projection should carry the persisted execution_target");
  }

  @Test
  void claimNextBatchOptimized_nullExecutionTarget_claimsBackAsNull() {
    persist(newPendingJob());

    List<JobClaimDto> claims =
        store().claimNextBatchOptimized(JobExecutionType.SINGLE, 10, "node-1");

    assertEquals(1, claims.size());
    assertNull(
        claims.get(0).executionTarget(),
        "a job with no execution target should claim back as null (inherit)");
  }
}
