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

import static org.junit.jupiter.api.Assertions.assertTrue;
import static run.ratchet.tck.store.sql.SqlRaceFailures.isConnectionFailure;
import static run.ratchet.tck.store.sql.SqlRaceFailures.isLockFailure;
import static run.ratchet.tck.store.sql.SqlRaceFailures.rootMessage;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobStatus;
import run.ratchet.api.exception.RatchetOptimisticLockException;
import run.ratchet.store.BusinessKeyReservationConflictException;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.tck.store.JpaContainerFixture;

/** Exercises business key holds against queue and reservation writes in the bundled SQL stores. */
public abstract class AbstractSqlBusinessKeyHoldRaceTest {
  protected abstract JpaContainerFixture fixture();

  @BeforeEach
  @AfterEach
  void clean() {
    fixture().cleanupStore();
  }

  @Test
  void holdWritesDoNotDeadlockWithSubmitsCancelsAndCompletions() throws Exception {
    // Concurrent reservation and queue writes must not deadlock the new hold statements.
    Duration softLimit = Duration.ofSeconds(15);
    int softRounds = 500;
    Duration hardCap = Duration.ofSeconds(60);
    var store = fixture().store();
    List<String> keys = new ArrayList<>();
    for (int i = 0; i < 20; i++) keys.add("hold-race-" + UUID.randomUUID());
    var pending = new ConcurrentLinkedQueue<UUID>();
    var claimed = new ConcurrentLinkedQueue<JobEntity>();
    var canceled = new ConcurrentLinkedQueue<JobEntity>();
    var holds = new AtomicInteger();
    var releases = new AtomicInteger();
    var expired = new AtomicInteger();
    var connectionFailures = new ConcurrentLinkedQueue<Throwable>();
    var failures = new ConcurrentLinkedQueue<Throwable>();
    var escapedLockFailures = new ConcurrentHashMap<String, AtomicInteger>();
    var lastLockFailure = new ThreadLocal<String>();
    var lockFailureSamples = new ConcurrentHashMap<String, String>();
    var holdLockFailures = new ConcurrentHashMap<String, AtomicInteger>();
    var lockFailures = new ConcurrentHashMap<String, AtomicInteger>();
    var currentOperation = new ThreadLocal<String>();
    var start = new CountDownLatch(1);
    List<String> methods =
        List.of(
            "create",
            "claimNextBatch",
            "claimNextBatch",
            "cancelRunningJob",
            "releaseBusinessKeyHold",
            "releaseBusinessKeyHolds",
            "deleteExpiredBusinessKeyHolds",
            "findBusinessKeyHolder",
            "cancelJob");
    List<Runnable> roles =
        List.of(
            () -> {
              var job = fixture().newPendingJob();
              job.setBusinessKey(random(keys));
              try {
                pending.add(store.create(job).getId());
              } catch (BusinessKeyReservationConflictException expected) {
                // Another pending job, running job or canceled hold owns this key.
              }
            },
            () -> claim("stress-a", claimed),
            () -> claim("stress-b", claimed),
            () -> {
              var job = claimed.poll();
              if (job == null) return;
              if (store.cancelRunningJob(
                  job.getId(),
                  Instant.now().plusSeconds(ThreadLocalRandom.current().nextInt(0, 3)))) {
                holds.incrementAndGet();
                // Leave some holds to the expiry and node sweeps instead of releasing all by claim.
                if (ThreadLocalRandom.current().nextBoolean()) canceled.add(job);
              }
            },
            () -> {
              var job = canceled.poll();
              if (job != null && store.releaseBusinessKeyHold(job.getId(), job.getClaimSeq()))
                releases.incrementAndGet();
            },
            () -> store.releaseBusinessKeyHolds(List.of("stress-b")),
            () -> expired.addAndGet(store.deleteExpiredBusinessKeyHolds(Instant.now(), 50)),
            () -> store.findBusinessKeyHolder(random(keys)),
            () -> {
              UUID id = pending.poll();
              if (id != null && store.getJobStatus(id) == JobStatus.PENDING) store.cancelJob(id);
            });
    int sessionBound = roles.size();
    var executor = Executors.newFixedThreadPool(roles.size());
    var futures = new ArrayList<Future<?>>();
    fixture()
        .observeTransientFailures(
            (method, failure) -> {
              if (!isConnectionFailure(failure) && isLockFailure(failure)) {
                var counts = isHoldMethod(method) ? holdLockFailures : lockFailures;
                counts.computeIfAbsent(method, ignored -> new AtomicInteger()).incrementAndGet();
                lastLockFailure.set(method);
                lockFailureSamples.putIfAbsent(method, rootMessage(failure));
              }
            });
    long runStart = System.nanoTime();
    long softDeadline = runStart + softLimit.toNanos();
    long hardDeadline = runStart + hardCap.toNanos();
    try {
      for (int roleIndex = 0; roleIndex < roles.size(); roleIndex++) {
        Runnable role = roles.get(roleIndex);
        String operation = methods.get(roleIndex);
        futures.add(
            executor.submit(
                () -> {
                  awaitLatch(start);
                  for (int round = 0; System.nanoTime() < hardDeadline; round++) {
                    boolean softLimitReached =
                        round >= softRounds || System.nanoTime() >= softDeadline;
                    if (softLimitReached
                        && holds.get() > 0
                        && releases.get() > 0
                        && expired.get() > 0) break;
                    try {
                      currentOperation.set(operation);
                      lastLockFailure.remove();
                      role.run();
                    } catch (Throwable failure) {
                      if (isConnectionFailure(failure)) {
                        connectionFailures.add(failure);
                      } else if (isLockFailure(failure)) {
                        String method = lastLockFailure.get();
                        if (method == null) {
                          method = currentOperation.get();
                          var counts = isHoldMethod(method) ? holdLockFailures : lockFailures;
                          counts
                              .computeIfAbsent(method, ignored -> new AtomicInteger())
                              .incrementAndGet();
                          lockFailureSamples.putIfAbsent(method, rootMessage(failure));
                        }
                        escapedLockFailures
                            .computeIfAbsent(
                                method == null ? "unknown" : method, ignored -> new AtomicInteger())
                            .incrementAndGet();
                      } else if (!(failure instanceof RatchetOptimisticLockException)) {
                        failures.add(failure);
                      }
                    }
                    LockSupport.parkNanos(Duration.ofMillis(5).toNanos());
                  }
                }));
      }
      start.countDown();
      for (var future : futures) future.get(hardCap.plusSeconds(30).toSeconds(), TimeUnit.SECONDS);
    } finally {
      executor.shutdownNow();
      try {
        assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
      } finally {
        fixture().observeTransientFailures((method, failure) -> {});
      }
    }
    System.out.println(
        "Business key hold race: holds="
            + holds.get()
            + ", releases="
            + releases.get()
            + ", expiry deletes="
            + expired.get()
            + ", other lock failures="
            + lockFailures
            + ", escaped after retries="
            + escapedLockFailures);
    lockFailureSamples.forEach(
        (method, message) ->
            System.out.println(
                "Business key hold race lock failure in " + method + ": " + message));
    if (!connectionFailures.isEmpty()) {
      var failure =
          new AssertionError(
              "Test set-up failed: "
                  + connectionFailures.size()
                  + " database connection failures during the stress run. Its "
                  + sessionBound
                  + " threads hold at most one session each, but idle pooled connections and the"
                  + " database's own processes count against the same server limit. These are not"
                  + " store failures. Hold lock failures seen: "
                  + holdLockFailures);
      connectionFailures.stream().limit(5).forEach(failure::addSuppressed);
      throw failure;
    }
    // Lock failures in pre-existing submit, claim and completion statements are reported only.
    assertTrue(
        holdLockFailures.isEmpty(),
        "business key hold or lookup lock failures: " + holdLockFailures);
    if (!failures.isEmpty()) {
      var failure = new AssertionError("Unexpected stress failures: " + failures.size());
      failures.forEach(failure::addSuppressed);
      throw failure;
    }
    if (holds.get() == 0 || releases.get() == 0 || expired.get() == 0) {
      throw new AssertionError(
          "The stress run did not reach a hold, a release by claim and an expiry delete within "
              + hardCap.toSeconds()
              + " s: holds="
              + holds.get()
              + ", releases="
              + releases.get()
              + ", expired="
              + expired.get()
              + ", other lock failures="
              + lockFailures
              + ", escaped after retries="
              + escapedLockFailures);
    }
    assertTrue(holds.get() > 0, "cancel must create holds");
    assertTrue(releases.get() > 0, "release by claim must delete a hold");
    assertTrue(expired.get() > 0, "expiry sweep must delete a hold");
  }

  private void claim(String node, ConcurrentLinkedQueue<JobEntity> claimed) {
    var store = fixture().store();
    for (var job : store.claimNextBatch(1, node)) {
      switch (ThreadLocalRandom.current().nextInt(3)) {
        case 0 ->
            store.markJobSucceeded(job.getId(), null, null, Instant.now(), Instant.now(), 0L, 0L);
        case 1 ->
            store.scheduleJobRetry(
                job.getId(),
                "stress retry",
                Instant.now(),
                job.getAttempts() + 1,
                job.getClaimSeq());
        default -> claimed.add(job);
      }
    }
  }

  private static String random(List<String> keys) {
    return keys.get(ThreadLocalRandom.current().nextInt(keys.size()));
  }

  private static boolean isHoldMethod(String method) {
    return Set.of(
            "releaseBusinessKeyHold",
            "releaseBusinessKeyHolds",
            "deleteExpiredBusinessKeyHolds",
            "cancelRunningJob",
            "findBusinessKeyHolder",
            "release_business_key_hold",
            "release_business_key_holds",
            "delete_expired_business_key_holds",
            "cancel_running_job",
            "find_business_key_holder")
        .contains(method);
  }

  private static void awaitLatch(CountDownLatch latch) {
    try {
      assertTrue(latch.await(30, TimeUnit.SECONDS), "timed out waiting for race barrier");
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }
}
