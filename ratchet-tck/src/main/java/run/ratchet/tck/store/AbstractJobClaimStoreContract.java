package run.ratchet.tck.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import run.ratchet.api.JobPriority;
import run.ratchet.api.RatchetOptions;
import run.ratchet.store.dto.JobClaimDto;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.entity.JobStatus;
import run.ratchet.tck.util.ConcurrentTestRunner;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** Base contract tests for {@code JobClaimStore}. */
public abstract class AbstractJobClaimStoreContract implements JobStoreContractFixture {

  private static Instant oldEnoughForLowestToBeatCritical() {
    int boostInterval = RatchetOptions.defaults().store().priorityBoostIntervalMinutes();
    assumeTrue(boostInterval > 0, "priority boosting is disabled");
    return Instant.now()
        .minus(Duration.ofMinutes((long) boostInterval * (JobPriority.CRITICAL.ordinal() + 1L)))
        .minusSeconds(1);
  }

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

    Set<Long> allClaimedIds = ConcurrentHashMap.newKeySet();
    Set<Long> duplicates = ConcurrentHashMap.newKeySet();

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
    assertTrue(allClaimedIds.size() >= 1, "at least one thread should have claimed jobs");
  }

  @Test
  void claimDueRecurring_claimsDueJobs() {
    JobEntity recurring = newPendingJob();
    recurring.setJobType(JobExecutionType.RECURRING);
    recurring.setCronExpr("0 * * * *");
    recurring.setNextFire(Instant.now().minusSeconds(60));
    persist(recurring);

    var claimed = store().claimDueRecurring(10, "node-1");

    assertEquals(1, claimed.size(), "claimDueRecurring should claim job with past nextFire");
    assertEquals(JobStatus.RUNNING, claimed.get(0).getStatus());
  }

  @Test
  void claimDueRecurring_skipsNotYetDue() {
    JobEntity recurring = newPendingJob();
    recurring.setJobType(JobExecutionType.RECURRING);
    recurring.setCronExpr("0 * * * *");
    recurring.setNextFire(Instant.now().plusSeconds(3600));
    persist(recurring);

    var claimed = store().claimDueRecurring(10, "node-1");

    assertTrue(claimed.isEmpty(), "claimDueRecurring should skip job with future nextFire");
  }

  @Test
  void claimDueRecurring_respectsLimit() {
    for (int i = 0; i < 3; i++) {
      JobEntity recurring = newPendingJob();
      recurring.setJobType(JobExecutionType.RECURRING);
      recurring.setCronExpr("0 * * * *");
      recurring.setNextFire(Instant.now().minusSeconds(60));
      persist(recurring);
    }

    var claimed = store().claimDueRecurring(2, "node-1");

    assertEquals(2, claimed.size(), "claimDueRecurring should respect the limit parameter");
  }

  @Test
  void claimDueRecurring_usesAgeBoostedEffectivePriority() {
    JobEntity oldLow = newPendingJob();
    oldLow.setJobType(JobExecutionType.RECURRING);
    oldLow.setCronExpr("0 * * * *");
    oldLow.setPriority(JobPriority.LOWEST);
    oldLow.setNextFire(oldEnoughForLowestToBeatCritical());
    oldLow = persist(oldLow);

    JobEntity freshCritical = newPendingJob();
    freshCritical.setJobType(JobExecutionType.RECURRING);
    freshCritical.setCronExpr("0 * * * *");
    freshCritical.setPriority(JobPriority.CRITICAL);
    freshCritical.setNextFire(Instant.now().minusSeconds(1));
    persist(freshCritical);

    var claimed = store().claimDueRecurring(1, "node-1");

    assertEquals(1, claimed.size(), "claimDueRecurring should return the requested job");
    assertEquals(
        oldLow.getId(),
        claimed.get(0).getId(),
        "age-boosted recurring LOWEST job should outrank a fresh CRITICAL job");
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
}
