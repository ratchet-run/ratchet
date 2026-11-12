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
package run.ratchet.tck.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static run.ratchet.store.spi.ResourcePermitStore.AcquireResult.ACQUIRED;
import static run.ratchet.store.spi.ResourcePermitStore.AcquireResult.AT_CAPACITY;
import static run.ratchet.store.spi.ResourcePermitStore.AcquireResult.SUPERSEDED;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.spi.ResourcePermitStore.AcquireResult;
import run.ratchet.tck.util.ConcurrentTestRunner;

/**
 * Base contract tests for {@code ResourcePermitStore}.
 *
 * <p>A permit belongs to the claim that took it. Only an attempt whose claim is RUNNING when the
 * store checks it can take one. That check is point in time, so these contracts change the claim
 * only before or after an acquire. An older claim's permit counts against capacity until it is
 * released or cleaned up.
 */
public abstract class AbstractResourcePermitStoreContract implements JobStoreContractFixture {

  @BeforeEach
  @AfterEach
  void cleanupResourcePermitFixture() {
    cleanupStore();
  }

  @Test
  void configureAndAcquirePermit_succeeds() {
    resourcePermitStore().configureResource("res-1", 2, 1000, "test");
    var job = claimNewJob("node-1");

    assertEquals(
        ACQUIRED,
        acquire("res-1", job, "node-1"),
        "tryAcquirePermit should succeed when capacity is available");
  }

  @Test
  void tryAcquirePermit_failsAtCapacity() {
    resourcePermitStore().configureResource("res-limited", 1, 1000, "limited resource");
    var firstJob = claimNewJob("node-1");
    var secondJob = claimNewJob("node-1");

    assertEquals(ACQUIRED, acquire("res-limited", firstJob, "node-1"), "First acquire succeeds");
    assertEquals(
        AT_CAPACITY,
        acquire("res-limited", secondJob, "node-1"),
        "Second acquire should fail when resource is at capacity");
  }

  @Test
  void tryAcquirePermit_unconfiguredResource_failsHard() {
    var job = persist(newPendingJob());

    assertThrows(
        IllegalArgumentException.class,
        () -> resourcePermitStore().tryAcquirePermit("missing-resource", job.getId(), 0L, "node-1"),
        "Unconfigured resources should fail hard, before the claim check and before capacity");
  }

  @Test
  void releasePermit_freesCapacity() {
    resourcePermitStore().configureResource("res-release", 1, 1000, "release test");
    var firstJob = claimNewJob("node-1");
    var secondJob = claimNewJob("node-1");

    assertEquals(ACQUIRED, acquire("res-release", firstJob, "node-1"));
    release("res-release", firstJob);

    assertEquals(
        ACQUIRED,
        acquire("res-release", secondJob, "node-1"),
        "Acquire should succeed after a permit has been released");
  }

  @Test
  void releaseAllPermits_freesAllForJob() {
    resourcePermitStore().configureResource("res-A", 1, 1000, "resource A");
    resourcePermitStore().configureResource("res-B", 1, 1000, "resource B");
    var job = claimNewJob("node-1");

    assertEquals(ACQUIRED, acquire("res-A", job, "node-1"));
    assertEquals(ACQUIRED, acquire("res-B", job, "node-1"));

    resourcePermitStore().releaseAllPermits(job.getId());

    var otherJob = claimNewJob("node-1");
    assertEquals(
        ACQUIRED,
        acquire("res-A", otherJob, "node-1"),
        "Resource A should be freed after releaseAllPermits");
    assertEquals(
        ACQUIRED,
        acquire("res-B", otherJob, "node-1"),
        "Resource B should be freed after releaseAllPermits");
  }

  @Test
  void getPermitRetryDelay_returnsConfiguredDelay() {
    resourcePermitStore().configureResource("res-delay", 1, 2500, "delay test");

    int delay = resourcePermitStore().getPermitRetryDelay("res-delay");

    assertEquals(2500, delay, "getPermitRetryDelay should return the configured delay");
  }

  @Test
  void getPermitRetryDelay_unknownResource_returnsDefaultDelay() {
    int delay = resourcePermitStore().getPermitRetryDelay("missing-resource");

    assertEquals(5000, delay, "Unknown resources should use the default retry delay");
  }

  @Test
  void tryAcquirePermit_concurrent_respectsCapacity() {
    int capacity = 3;
    resourcePermitStore().configureResource("res-cap", capacity, 1000, "capacity test");

    AtomicInteger successCount = new AtomicInteger();
    int threadCount = 6;
    Runnable[] tasks = new Runnable[threadCount];
    for (int i = 0; i < threadCount; i++) {
      final String nodeId = "node-" + i;
      final var job = claimNewJob(nodeId);
      tasks[i] =
          () -> {
            if (acquire("res-cap", job, nodeId) == ACQUIRED) {
              successCount.incrementAndGet();
            }
          };
    }

    ConcurrentTestRunner.runAll(Duration.ofSeconds(10), tasks);

    assertTrue(
        successCount.get() <= capacity,
        "at most " + capacity + " permits should be acquired; got " + successCount.get());
    assertTrue(successCount.get() >= 1, "at least one permit should be acquired");
  }

  @Test
  void cleanupOrphanedPermits_removesStaleNodePermits() {
    resourcePermitStore().configureResource("res-orphan", 1, 1000, "orphan test");
    var job = claimNewJob("stale-node");

    assertEquals(ACQUIRED, acquire("res-orphan", job, "stale-node"));

    int cleaned = resourcePermitStore().cleanupOrphanedPermits(List.of("stale-node"));

    assertTrue(cleaned >= 1, "cleanupOrphanedPermits should remove the stale permit");

    var otherJob = claimNewJob("node-1");
    assertEquals(
        ACQUIRED,
        acquire("res-orphan", otherJob, "node-1"),
        "Resource should be freed after orphan cleanup");
  }

  @Test
  void configureResource_updatesExistingConfig() {
    resourcePermitStore().configureResource("res-reconfig", 1, 1000, "initial");
    resourcePermitStore().configureResource("res-reconfig", 5, 2000, "updated");

    assertEquals(
        2000,
        resourcePermitStore().getPermitRetryDelay("res-reconfig"),
        "Reconfigured resource should have updated retry delay");
  }

  @Test
  void tryAcquirePermit_sameClaimTwice_idempotent() {
    resourcePermitStore().configureResource("res-idem", 1, 1000, "idempotent test");
    var job = claimNewJob("node-1");

    assertEquals(ACQUIRED, acquire("res-idem", job, "node-1"), "First acquire should succeed");
    assertEquals(
        ACQUIRED,
        acquire("res-idem", job, "node-1"),
        "Second acquire by the same claim should be idempotent");

    var otherJob = claimNewJob("node-1");
    assertEquals(
        AT_CAPACITY,
        acquire("res-idem", otherJob, "node-1"),
        "Resource capacity should remain exhausted by the one idempotent permit");
    release("res-idem", job);
    assertEquals(
        ACQUIRED,
        acquire("res-idem", otherJob, "node-1"),
        "One release frees the one permit the repeated acquire left");
  }

  @Test
  void newerClaimWaitsUntilDeadNodeCleanup() {
    resourcePermitStore().configureResource("res-reclaim", 1, 1000, "orphan reclaim");
    var first = claimNewJob("dead-node");
    assertEquals(ACQUIRED, acquire("res-reclaim", first, "dead-node"));

    assertEquals(1, store().resetOrphanJobsBefore(Instant.now().plusSeconds(60), 3, 100).reset());
    var retry = reclaim(first, "live-node");
    assertEquals(
        AT_CAPACITY,
        acquire("res-reclaim", retry, "live-node"),
        "the dead attempt's permit still counts");
    resourcePermitStore().cleanupOrphanedPermits(List.of("dead-node"));
    assertEquals(ACQUIRED, acquire("res-reclaim", retry, "live-node"));
    var other = claimNewJob("other-node");
    assertEquals(AT_CAPACITY, acquire("res-reclaim", other, "other-node"));
    resourcePermitStore().cleanupOrphanedPermits(List.of("dead-node"));
    assertEquals(
        AT_CAPACITY,
        acquire("res-reclaim", other, "other-node"),
        "cleanup of the dead node must not free the live retry's permit");
    release("res-reclaim", retry);
    assertEquals(ACQUIRED, acquire("res-reclaim", other, "other-node"));
  }

  @Test
  void newerClaimWaitsUntilOlderClaimReleases() {
    resourcePermitStore().configureResource("res-timeout", 1, 1000, "retry");
    var first = claimNewJob("node-a");
    assertEquals(ACQUIRED, acquire("res-timeout", first, "node-a"));

    scheduleImmediateRetry(first);
    var retry = reclaim(first, "node-b");
    assertEquals(AT_CAPACITY, acquire("res-timeout", retry, "node-b"));
    release("res-timeout", first);
    assertEquals(ACQUIRED, acquire("res-timeout", retry, "node-b"));
    release("res-timeout", first);
    var other = claimNewJob("node-c");
    assertEquals(
        AT_CAPACITY,
        acquire("res-timeout", other, "node-c"),
        "the older claim's release must not free the newer claim's own permit");
  }

  @Test
  void newerClaimOnSameNodeWaitsUntilOlderClaimReleases() {
    resourcePermitStore().configureResource("res-same-node", 1, 1000, "retry");
    var first = claimNewJob("node-a");
    assertEquals(ACQUIRED, acquire("res-same-node", first, "node-a"));

    scheduleImmediateRetry(first);
    var retry = reclaim(first, "node-a");
    assertEquals(AT_CAPACITY, acquire("res-same-node", retry, "node-a"));
    release("res-same-node", first);
    assertEquals(ACQUIRED, acquire("res-same-node", retry, "node-a"));
    release("res-same-node", first);
    var other = claimNewJob("node-c");
    assertEquals(
        AT_CAPACITY,
        acquire("res-same-node", other, "node-c"),
        "the older claim's release must not free the newer claim's own permit");
  }

  @Test
  void newerClaimCountsOlderClaimPermitAgainstCapacity() {
    resourcePermitStore().configureResource("res-two", 2, 1000, "two slots");
    var first = claimNewJob("node-a");
    assertEquals(ACQUIRED, acquire("res-two", first, "node-a"));
    scheduleImmediateRetry(first);
    var retry = reclaim(first, "node-b");
    assertEquals(ACQUIRED, acquire("res-two", retry, "node-b"), "a free slot admits the retry");
    assertEquals(ACQUIRED, acquire("res-two", retry, "node-b"));
    var third = claimNewJob("node-c");
    assertEquals(
        AT_CAPACITY,
        acquire("res-two", third, "node-c"),
        "the older claim's permit and the retry's permit fill both slots");
    release("res-two", first);
    assertEquals(ACQUIRED, acquire("res-two", third, "node-c"));
  }

  @Test
  void staleAttemptRelease_freesPermitTheNewerAttemptNeverTook() {
    resourcePermitStore().configureResource("res-untaken", 1, 1000, "untaken permit");
    var first = claimNewJob("node-a");
    assertEquals(ACQUIRED, acquire("res-untaken", first, "node-a"));

    scheduleImmediateRetry(first);
    // The new attempt never asks for the permit, for example because a circuit breaker is open.
    reclaim(first, "node-b");
    release("res-untaken", first);

    var other = claimNewJob("node-c");
    assertEquals(
        ACQUIRED,
        acquire("res-untaken", other, "node-c"),
        "the old attempt's release frees the permit it still owns");
  }

  @Test
  void tryAcquirePermit_rejectsAttemptWhoseJobWasReset() {
    resourcePermitStore().configureResource("res-stale", 1, 1000, "stale acquire");
    var stalled = claimNewJob("node-a");
    assertEquals(1, store().resetOrphanJobsBefore(Instant.now().plusSeconds(60), 3, 100).reset());

    assertEquals(
        SUPERSEDED,
        acquire("res-stale", stalled, "node-a"),
        "an attempt whose job is no longer RUNNING under its claim must not take a permit");
    var retry = reclaim(stalled, "node-b");
    assertEquals(
        ACQUIRED,
        acquire("res-stale", retry, "node-b"),
        "the rejected attempt must not hold capacity");
  }

  @Test
  void tryAcquirePermit_rejectsOlderClaimWhileNewerClaimRuns() {
    resourcePermitStore().configureResource("res-older", 1, 1000, "older claim");
    var first = claimNewJob("node-a");
    scheduleImmediateRetry(first);
    var retry = reclaim(first, "node-b");

    assertEquals(
        SUPERSEDED,
        acquire("res-older", first, "node-a"),
        "the older claim must not take a permit while the newer claim runs");
    assertEquals(ACQUIRED, acquire("res-older", retry, "node-b"));
    var other = claimNewJob("node-c");
    assertEquals(AT_CAPACITY, acquire("res-older", other, "node-c"));
  }

  @Test
  void tryAcquirePermit_rejectsAttemptWhoseJobEnded() {
    resourcePermitStore().configureResource("res-ended", 1, 1000, "ended job");
    var ended = claimNewJob("node-a");
    assertTrue(store().cancelJob(ended.getId()));

    assertEquals(
        SUPERSEDED,
        acquire("res-ended", ended, "node-a"),
        "an attempt whose job has ended must not take a permit");
  }

  private AcquireResult acquire(String resource, JobEntity claimed, String nodeId) {
    return resourcePermitStore()
        .tryAcquirePermit(resource, claimed.getId(), claimed.getClaimSeq(), nodeId);
  }

  private void release(String resource, JobEntity claimed) {
    resourcePermitStore().releasePermit(resource, claimed.getId(), claimed.getClaimSeq());
  }

  private JobEntity claimNewJob(String nodeId) {
    var pending = persist(newPendingJob());
    var claimed = store().claimNextBatch(1, nodeId);
    assertEquals(1, claimed.size());
    assertEquals(pending.getId(), claimed.get(0).getId());
    return claimed.get(0);
  }

  private JobEntity reclaim(JobEntity previous, String nodeId) {
    var claimed = store().claimNextBatch(1, nodeId);
    assertEquals(1, claimed.size());
    assertEquals(previous.getId(), claimed.get(0).getId());
    assertTrue(claimed.get(0).getClaimSeq() > previous.getClaimSeq());
    return claimed.get(0);
  }

  /** Puts a claimed job back to PENDING and due now, as a hard timeout with retries left does. */
  private void scheduleImmediateRetry(JobEntity claimed) {
    assertTrue(
        store()
            .scheduleJobRetry(
                claimed.getId(),
                "timed out",
                Instant.now().minusSeconds(5),
                claimed.getAttempts() + 1,
                claimed.getClaimSeq()));
  }
}
