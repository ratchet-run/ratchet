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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static run.ratchet.store.spi.ResourcePermitStore.AcquireResult.ACQUIRED;
import static run.ratchet.store.spi.ResourcePermitStore.AcquireResult.AT_CAPACITY;
import static run.ratchet.store.spi.ResourcePermitStore.AcquireResult.SUPERSEDED;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobStatus;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.spi.ResourcePermitStore.AcquireResult;
import run.ratchet.store.spi.ResourceWaiterLease;
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
    resourcePermitStore().wakeWaiters("res-idem");
    otherJob = reclaim(otherJob, "node-1");
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
    resourcePermitStore().wakeWaiters("res-reclaim");
    retry = reclaim(retry, "live-node");
    assertEquals(ACQUIRED, acquire("res-reclaim", retry, "live-node"));
    var other = claimNewJob("other-node");
    assertEquals(AT_CAPACITY, acquire("res-reclaim", other, "other-node"));
    resourcePermitStore().cleanupOrphanedPermits(List.of("dead-node"));
    // No free slot: cleanup must not wake the parked job while the live retry holds capacity.
    assertTrue(resourcePermitStore().wakeWaiters("res-reclaim").isEmpty());
    assertEquals(JobStatus.PENDING, store().getJobStatus(other.getId()));
    assertEquals(
        SUPERSEDED,
        acquire("res-reclaim", other, "other-node"),
        "cleanup of the dead node must not free the live retry's permit");
    release("res-reclaim", retry);
    resourcePermitStore().wakeWaiters("res-reclaim");
    other = reclaim(other, "other-node");
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
    resourcePermitStore().wakeWaiters("res-timeout");
    retry = reclaim(retry, "node-b");
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
    resourcePermitStore().wakeWaiters("res-same-node");
    retry = reclaim(retry, "node-a");
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
    resourcePermitStore().wakeWaiters("res-two");
    third = reclaim(third, "node-c");
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

  @Test
  void waitersAreServedInTheOrderTheyStartedWaiting() {
    resourcePermitStore().configureResource("ordered", 1, 60_000, null);
    var holder = claimNewJob("holder");
    assertEquals(ACQUIRED, acquire("ordered", holder, "holder"));
    var a = claimDueJob(Instant.now().minusSeconds(30), JobPriority.NORMAL);
    var b = claimDueJob(Instant.now().minusSeconds(20), JobPriority.NORMAL);
    Instant original = a.getScheduledTime();
    assertEquals(AT_CAPACITY, acquire("ordered", a, "node"));
    assertEquals(AT_CAPACITY, acquire("ordered", b, "node"));
    release("ordered", holder);
    assertEquals(
        List.of(a.getId()),
        resourcePermitStore().wakeWaiters("ordered").stream().map(w -> w.jobId()).toList());
    assertEquals(
        millis(original), millis(store().findById(a.getId()).orElseThrow().getScheduledTime()));
    // Claim A and a fresh job together, then let the fresh job ask first.
    var c = persist(newPendingJob());
    var claims = store().claimNextBatch(2, "node");
    assertEquals(2, claims.size());
    var fresh = claims.stream().filter(j -> j.getId().equals(c.getId())).findFirst().orElseThrow();
    var head = claims.stream().filter(j -> j.getId().equals(a.getId())).findFirst().orElseThrow();
    assertEquals(AT_CAPACITY, acquire("ordered", fresh, "node"));
    assertEquals(ACQUIRED, acquire("ordered", head, "node"));
  }

  @Test
  void higherPriorityWaiterGoesFirstThenTheOlderOne() {
    resourcePermitStore().configureResource("priority", 1, 60_000, null);
    var holder = claimNewJob("holder");
    assertEquals(ACQUIRED, acquire("priority", holder, "holder"));
    var old = claimDueJob(Instant.now().minusSeconds(40), JobPriority.NORMAL);
    var young = claimDueJob(Instant.now().minusSeconds(30), JobPriority.NORMAL);
    var high = claimDueJob(Instant.now().minusSeconds(20), JobPriority.HIGH);
    for (var job : List.of(old, young, high)) {
      assertEquals(AT_CAPACITY, acquire("priority", job, "node"));
    }
    release("priority", holder);
    assertEquals(
        List.of(high.getId()),
        resourcePermitStore().wakeWaiters("priority").stream().map(w -> w.jobId()).toList());
    high = reclaim(high, "node");
    assertEquals(ACQUIRED, acquire("priority", high, "node"));
    release("priority", high);
    assertEquals(
        List.of(old.getId()),
        resourcePermitStore().wakeWaiters("priority").stream().map(w -> w.jobId()).toList());
  }

  @Test
  void waiterWhoseLeaseRanOutStopsCounting() throws InterruptedException {
    resourcePermitStore().configureResource("expired", 1, 1000, null);
    var holder = claimNewJob("holder");
    assertEquals(ACQUIRED, acquire("expired", holder, "holder"));
    var old = claimNewJob("old");
    var young = claimNewJob("young");
    assertEquals(AT_CAPACITY, acquire("expired", old, "old"));
    long parkedAt = System.nanoTime();
    release("expired", holder);
    // The lease is measured on the database clock, so wait it out with a small margin.
    long leaseEnd =
        parkedAt + Duration.ofMillis(ResourceWaiterLease.leaseMillis(1000) + 250).toNanos();
    while (System.nanoTime() < leaseEnd) {
      Thread.sleep(Math.max(1, Duration.ofNanos(leaseEnd - System.nanoTime()).toMillis()));
    }
    assertTrue(resourcePermitStore().wakeWaiters("expired").isEmpty());
    assertEquals(ACQUIRED, acquire("expired", young, "young"));
  }

  @Test
  void pausedWaiterDoesNotCount() {
    batchStore();
    resourcePermitStore().configureResource("paused", 1, 60_000, null);
    var holder = claimNewJob("holder");
    assertEquals(ACQUIRED, acquire("paused", holder, "holder"));
    var parent = newBatchParentJob();
    parent.setScheduledTime(Instant.now().plusSeconds(3600));
    parent = persist(parent);
    persistBatch(parent.getId(), 1);
    var child = newPendingJob();
    child.setDependsOn(parent.getId());
    child.setJobType(JobExecutionType.BATCH_CHILD);
    child = persist(child);
    var running = store().claimNextBatch(1, "child").get(0);
    assertEquals(child.getId(), running.getId());
    assertTrue(store().transitionToPaused(parent.getId(), JobStatus.PENDING));
    batchStore().pauseBatchChildren(parent.getId());
    assertEquals(AT_CAPACITY, acquire("paused", running, "child"));
    assertEquals(JobStatus.PAUSED, store().getJobStatus(child.getId()));
    release("paused", holder);
    assertTrue(resourcePermitStore().wakeWaiters("paused").isEmpty());
    assertEquals(ACQUIRED, acquire("paused", claimNewJob("fresh"), "fresh"));
  }

  @Test
  void findResourcesWithWaitersPagesInAscendingOrder() {
    for (String resource : List.of("page-c", "page-a", "page-b")) {
      resourcePermitStore().configureResource(resource, 1, 60_000, null);
      var holder = claimNewJob("holder");
      assertEquals(ACQUIRED, acquire(resource, holder, "holder"));
      for (int i = 0; i < 2; i++) {
        var waiter = claimNewJob("waiter");
        assertEquals(AT_CAPACITY, acquire(resource, waiter, "waiter"));
      }
    }

    assertEquals(
        List.of("page-a", "page-b"), resourcePermitStore().findResourcesWithWaiters(null, 2));
    assertEquals(List.of("page-c"), resourcePermitStore().findResourcesWithWaiters("page-b", 2));
    assertTrue(resourcePermitStore().findResourcesWithWaiters("page-c", 2).isEmpty());
  }

  @Test
  void staleWaiterRowsAreSkippedAndRemoved() {
    resourcePermitStore().configureResource("stale", 1, 60_000, null);
    var holder = claimNewJob("holder");
    assertEquals(ACQUIRED, acquire("stale", holder, "holder"));
    var waiter = claimNewJob("waiter");
    assertEquals(AT_CAPACITY, acquire("stale", waiter, "waiter"));
    assertTrue(store().cancelJob(waiter.getId()));
    release("stale", holder);
    assertTrue(resourcePermitStore().wakeWaiters("stale").isEmpty());
    assertEquals(ACQUIRED, acquire("stale", claimNewJob("fresh"), "fresh"));
    assertEquals(List.of("stale"), resourcePermitStore().findResourcesWithWaiters(null, 10));
    assertEquals(1, resourcePermitStore().deleteStaleWaiters(10));
    assertTrue(resourcePermitStore().findResourcesWithWaiters(null, 10).isEmpty());
  }

  @Test
  void wakeUpWakesAsManyWaitersAsArePermitsFree() {
    resourcePermitStore().configureResource("many", 2, 60_000, null);
    var h1 = claimNewJob("h1");
    var h2 = claimNewJob("h2");
    assertEquals(ACQUIRED, acquire("many", h1, "h1"));
    assertEquals(ACQUIRED, acquire("many", h2, "h2"));
    var a = claimDueJob(Instant.now().minusSeconds(40), JobPriority.NORMAL);
    var b = claimDueJob(Instant.now().minusSeconds(30), JobPriority.NORMAL);
    var c = claimDueJob(Instant.now().minusSeconds(20), JobPriority.NORMAL);
    for (var job : List.of(a, b, c)) assertEquals(AT_CAPACITY, acquire("many", job, "node"));
    release("many", h1);
    release("many", h2);
    assertEquals(
        List.of(a.getId(), b.getId()),
        resourcePermitStore().wakeWaiters("many").stream().map(w -> w.jobId()).toList());
    // Running heads consume the first two free slots even before acquiring their permits.
    assertEquals(2, store().claimNextBatch(2, "node").size());
    resourcePermitStore().configureResource("many", 3, 60_000, null);
    assertEquals(
        List.of(c.getId()),
        resourcePermitStore().wakeWaiters("many").stream().map(w -> w.jobId()).toList());
  }

  @Test
  void wakeUpRestoresTheWaitersOriginalDueTime() {
    resourcePermitStore().configureResource("due", 1, 60_000, null);
    var holder = claimNewJob("holder");
    assertEquals(ACQUIRED, acquire("due", holder, "holder"));
    var waiter =
        claimDueJob(
            Instant.now().minusSeconds(10).truncatedTo(ChronoUnit.MILLIS), JobPriority.NORMAL);
    assertEquals(AT_CAPACITY, acquire("due", waiter, "node"));
    release("due", holder);
    var woken = resourcePermitStore().wakeWaiters("due");
    assertEquals(List.of(waiter.getId()), woken.stream().map(w -> w.jobId()).toList());
    assertEquals(
        millis(waiter.getScheduledTime()),
        millis(store().findById(waiter.getId()).orElseThrow().getScheduledTime()));
    // Already-due PENDING heads still produce a wake hint.
    assertEquals(woken, resourcePermitStore().wakeWaiters("due"));
  }

  @Test
  void supersededAttemptParksNothing() {
    resourcePermitStore().configureResource("superseded", 1, 60_000, null);
    var holder = claimNewJob("holder");
    assertEquals(ACQUIRED, acquire("superseded", holder, "holder"));
    var old = claimNewJob("old");
    scheduleImmediateRetry(old);
    var current = reclaim(old, "current");
    assertEquals(SUPERSEDED, acquire("superseded", old, "old"));
    var reloaded = store().findById(old.getId()).orElseThrow();
    assertEquals(JobStatus.RUNNING, reloaded.getStatus());
    assertEquals(current.getClaimSeq(), reloaded.getClaimSeq());
    assertTrue(resourcePermitStore().findResourcesWithWaiters(null, 10).isEmpty());
  }

  @Test
  void missParksTheJobInsideTheAcquire() {
    resourcePermitStore().configureResource("park", 1, 60_000, null);
    var holder = claimNewJob("holder");
    assertEquals(ACQUIRED, acquire("park", holder, "holder"));
    var waiter = claimNewJob("waiter");
    Instant before = Instant.now();
    assertEquals(AT_CAPACITY, acquire("park", waiter, "waiter"));
    Instant after = Instant.now();
    var parked = store().findById(waiter.getId()).orElseThrow();
    assertEquals(JobStatus.PENDING, parked.getStatus());
    assertNull(parked.getPickedBy());
    assertNull(parked.getPickedAt());
    assertEquals("Waiting for resource: park", parked.getLastError());
    assertEquals(waiter.getAttempts(), parked.getAttempts());
    assertTrue(parked.getScheduledTime().isAfter(before.plusSeconds(58)));
    assertTrue(parked.getScheduledTime().isBefore(after.plusSeconds(62)));
  }

  @Test
  void userRescheduleRemovesTheWaiterForBothGenericUpdatePaths() {
    resourcePermitStore().configureResource("reschedule", 1, 60_000, null);
    var holder = claimNewJob("holder");
    assertEquals(ACQUIRED, acquire("reschedule", holder, "holder"));
    for (boolean changeAnotherHotField : List.of(false, true)) {
      var waiter = claimNewJob("waiter");
      assertEquals(AT_CAPACITY, acquire("reschedule", waiter, "waiter"));
      var parked = store().findById(waiter.getId()).orElseThrow();
      var userTime = Instant.now().plusSeconds(3600).truncatedTo(ChronoUnit.MILLIS);
      parked.setScheduledTime(userTime);
      if (changeAnotherHotField) parked.setAttempts(parked.getAttempts() + 1);
      store().save(parked);
      assertTrue(resourcePermitStore().findResourcesWithWaiters(null, 10).isEmpty());
      assertEquals(
          userTime, millis(store().findById(waiter.getId()).orElseThrow().getScheduledTime()));
    }
  }

  private JobEntity claimDueJob(Instant due, JobPriority priority) {
    var job = newPendingJob();
    job.setScheduledTime(due);
    job.setPriority(priority);
    var saved = persist(job);
    return reclaim(saved, "node");
  }

  private static Instant millis(Instant value) {
    return value.truncatedTo(ChronoUnit.MILLIS);
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
