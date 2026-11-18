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

import java.time.Instant;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobStatus;

class MongoClaimReleaseRaceTest {
  private static final MongoTestFixture fixture = new MongoTestFixture();

  @AfterAll
  static void closeFixture() {
    fixture.close();
  }

  @BeforeEach
  void clean() {
    fixture.cleanupStore();
  }

  @Test
  void releaseClaimRacesChargeUnderSameClaim() throws Exception {
    var store = fixture.store();
    var executor = Executors.newFixedThreadPool(2);
    try {
      for (int round = 1; round <= 200; round++) {
        var job = fixture.newPendingJob();
        job.setAttempts(2);
        job = store.create(job);
        assertTrue(store.tryPickUpJob(job.getId(), "node-1"));
        var claim = store.findById(job.getId()).orElseThrow();
        var released = new AtomicBoolean();
        var charged = new AtomicInteger(-2);
        var failures = new ConcurrentLinkedQueue<Throwable>();
        var start = new CountDownLatch(1);
        var a =
            executor.submit(
                () ->
                    participate(
                        start,
                        () ->
                            released.set(
                                store.releaseClaim(
                                    claim.getId(),
                                    "release",
                                    Instant.now(),
                                    2,
                                    claim.getClaimSeq())),
                        failures));
        var b =
            executor.submit(
                () ->
                    participate(
                        start,
                        () ->
                            charged.set(
                                store.incrementRetryAttempt(
                                    claim.getId(), claim.getClaimSeq(), null)),
                        failures));
        start.countDown();
        a.get(30, TimeUnit.SECONDS);
        b.get(30, TimeUnit.SECONDS);
        if (!failures.isEmpty()) {
          var failure = new AssertionError("Mongo claim release race failures, round " + round);
          failures.forEach(failure::addSuppressed);
          throw failure;
        }
        var after = store.findById(claim.getId()).orElseThrow();
        assertTrue(
            released.get() ^ (charged.get() == 3), "exactly one writer wins, round " + round);
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
    } finally {
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
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
