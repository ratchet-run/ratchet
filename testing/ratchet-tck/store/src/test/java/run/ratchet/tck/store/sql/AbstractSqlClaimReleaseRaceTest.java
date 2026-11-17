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
package run.ratchet.tck.store.sql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static run.ratchet.tck.store.sql.SqlRaceFailures.isConnectionFailure;
import static run.ratchet.tck.store.sql.SqlRaceFailures.isLockFailure;
import static run.ratchet.tck.store.sql.SqlRaceFailures.rootMessage;

import java.time.Instant;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobStatus;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.tck.store.JpaContainerFixture;

/** Two-writer races for the bundled SQL stores' no-charge claim release. */
public abstract class AbstractSqlClaimReleaseRaceTest {
  protected abstract JpaContainerFixture fixture();

  @BeforeEach
  @AfterEach
  void clean() {
    fixture().cleanupStore();
  }

  @Test
  void releaseClaimRacesChargeUnderSameClaim() throws Exception {
    var store = fixture().store();
    for (int round = 1; round <= 200; round++) {
      JobEntity claim = claimNewJob(null);
      var released = new AtomicBoolean();
      var charged = new AtomicInteger(-2);
      race(
          () ->
              released.set(
                  store.releaseClaim(
                      claim.getId(), "release", Instant.now(), 2, claim.getClaimSeq())),
          () -> charged.set(store.incrementRetryAttempt(claim.getId(), claim.getClaimSeq())));
      JobEntity after = store.findById(claim.getId()).orElseThrow();
      assertTrue(released.get() ^ (charged.get() == 3), "exactly one writer wins, round " + round);
      if (released.get()) {
        assertEquals(-1, charged.get());
        assertEquals(JobStatus.PENDING, after.getStatus());
        assertEquals(2, after.getAttempts());
      } else {
        assertEquals(3, charged.get());
        assertEquals(JobStatus.RUNNING, after.getStatus());
        assertEquals(3, after.getAttempts());
        assertTrue(
            store.scheduleJobRetry(
                claim.getId(), "timeout", Instant.now(), 3, claim.getClaimSeq()));
      }
    }
  }

  @Test
  void releaseClaimRacesParentPause() throws Exception {
    var store = fixture().store();
    var batches = fixture().batchStore();
    for (int round = 1; round <= 200; round++) {
      var parent = store.create(fixture().newBatchParentJob());
      fixture().persistBatch(parent.getId(), 1);
      JobEntity claim = claimNewJob(parent);
      race(
          () ->
              assertTrue(
                  store.releaseClaim(
                      claim.getId(), "release", Instant.now(), 2, claim.getClaimSeq())),
          () -> {
            assertTrue(store.transitionToPaused(parent.getId(), JobStatus.PENDING));
            batches.pauseBatchChildren(parent.getId());
          });
      assertEquals(JobStatus.PAUSED, store.getJobStatus(parent.getId()));
      JobEntity after = store.findById(claim.getId()).orElseThrow();
      assertFalse(
          after.getStatus() == JobStatus.PENDING,
          "child runnable under paused parent, round " + round);
      assertEquals(JobStatus.PAUSED, after.getStatus());
      assertEquals(JobStatus.PENDING, after.getPausedFromStatus());
      assertEquals(2, after.getAttempts());
      assertTrue(store.transitionFromPaused(parent.getId(), JobStatus.PENDING));
      assertEquals(1, batches.resumeBatchChildren(parent.getId()));
      assertEquals(JobStatus.PENDING, store.getJobStatus(claim.getId()));
    }
  }

  private JobEntity claimNewJob(JobEntity parent) {
    var store = fixture().store();
    JobEntity child = fixture().newPendingJob();
    child.setAttempts(2);
    if (parent != null) {
      child.setJobType(JobExecutionType.BATCH_CHILD);
      child.setDependsOn(parent.getId());
    }
    child = store.create(child);
    assertTrue(store.tryPickUpJob(child.getId(), "node-1"));
    return store.findById(child.getId()).orElseThrow();
  }

  private void race(Runnable first, Runnable second) throws Exception {
    var failures = new ConcurrentLinkedQueue<Throwable>();
    var start = new CountDownLatch(1);
    var executor = Executors.newFixedThreadPool(2);
    try {
      var a = executor.submit(() -> participate(start, first, failures));
      var b = executor.submit(() -> participate(start, second, failures));
      start.countDown();
      a.get(30, TimeUnit.SECONDS);
      b.get(30, TimeUnit.SECONDS);
    } finally {
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
    }
    if (!failures.isEmpty()) {
      var failure = new AssertionError("Claim release race failures: " + failures.size());
      for (Throwable cause : failures) {
        String kind =
            isConnectionFailure(cause)
                ? "connection setup"
                : isLockFailure(cause) ? "lock" : "unexpected";
        failure.addSuppressed(new AssertionError(kind + ": " + rootMessage(cause), cause));
      }
      throw failure;
    }
  }

  private static void participate(
      CountDownLatch start, Runnable operation, ConcurrentLinkedQueue<Throwable> failures) {
    try {
      assertTrue(start.await(30, TimeUnit.SECONDS), "timed out waiting for race barrier");
      operation.run();
    } catch (Throwable failure) {
      if (failure instanceof InterruptedException) Thread.currentThread().interrupt();
      failures.add(failure);
    }
  }
}
