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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static run.ratchet.tck.store.sql.SqlRaceFailures.isConnectionFailure;
import static run.ratchet.tck.store.sql.SqlRaceFailures.isLockFailure;
import static run.ratchet.tck.store.sql.SqlRaceFailures.rootMessage;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Predicate;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.BusinessKeyHolder;
import run.ratchet.api.JobStatus;
import run.ratchet.api.JobType;
import run.ratchet.api.exception.RatchetOptimisticLockException;
import run.ratchet.store.BusinessKeyReservationConflictException;
import run.ratchet.store.dto.JobCompletionPlan;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.NodeEntity;
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
            "compareAndSwapStatus");
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
              // The engine cancels a pending job with a status swap, which locks the queue row
              // first, as cancelRunningJob does.
              UUID id = pending.poll();
              if (id != null) {
                store.compareAndSwapStatus(id, JobStatus.PENDING, JobStatus.CANCELED, null);
              }
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

  @Test
  void expirySweepAndNodeReleaseDoNotDeadlock() throws Exception {
    // Both sweeps delete many holds in one transaction. The holds expire in the reverse order of
    // their keys, so sweeps that delete in different orders lock the same rows in opposite order.
    var store = fixture().store();
    String node = "sweep-node";
    var lockFailures = new ConcurrentHashMap<String, AtomicInteger>();
    fixture()
        .observeTransientFailures(
            (method, failure) -> {
              if (isLockFailure(failure)) {
                lockFailures
                    .computeIfAbsent(method, ignored -> new AtomicInteger())
                    .incrementAndGet();
              }
            });
    var executor = Executors.newFixedThreadPool(2);
    try {
      for (int round = 0; round < 60 && lockFailures.isEmpty(); round++) {
        List<UUID> ids = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
          var job = fixture().newPendingJob();
          job.setBusinessKey("sweep-" + round + "-" + (char) ('a' + i));
          ids.add(store.create(job).getId());
        }
        int claimed = 0;
        for (int tries = 0; claimed < ids.size() && tries < 20; tries++) {
          claimed += store.claimNextBatch(ids.size(), node).size();
        }
        assertEquals(ids.size(), claimed);
        Instant now = Instant.now();
        for (int i = 0; i < ids.size(); i++) {
          assertTrue(store.cancelRunningJob(ids.get(i), now.minusSeconds(i + 1L)));
        }
        var barrier = new CyclicBarrier(2);
        Future<Integer> expiry =
            executor.submit(
                () -> {
                  barrier.await();
                  return store.deleteExpiredBusinessKeyHolds(Instant.now(), 50);
                });
        Future<Integer> release =
            executor.submit(
                () -> {
                  barrier.await();
                  return store.releaseBusinessKeyHolds(List.of(node));
                });
        assertEquals(
            ids.size(),
            expiry.get(60, TimeUnit.SECONDS) + release.get(60, TimeUnit.SECONDS),
            "each hold is deleted once");
      }
    } finally {
      executor.shutdownNow();
      fixture().observeTransientFailures((method, failure) -> {});
    }
    assertTrue(lockFailures.isEmpty(), "sweep lock failures: " + lockFailures);
  }

  @Test
  void expirySweepSkipsAHoldThatAnotherTransactionLocked() throws Exception {
    // A frozen node can keep a held reservation row locked; the expiry sweep must not wait for it.
    String prefix = "locked-expiry-" + UUID.randomUUID() + "-";
    List<String> keys = createHolds(prefix, "expiry-node", Instant.now().minusSeconds(60));
    assertSweepSkipsLockedHold(
        prefix, keys, () -> fixture().store().deleteExpiredBusinessKeyHolds(Instant.now(), 50));
  }

  @Test
  void nodeReleaseSkipsAHoldThatAnotherTransactionLocked() throws Exception {
    // A frozen node can keep a held reservation row locked; the node release must not wait for it.
    String prefix = "locked-release-" + UUID.randomUUID() + "-";
    List<String> keys = createHolds(prefix, "released-node", Instant.now().plusSeconds(3600));
    assertSweepSkipsLockedHold(
        prefix, keys, () -> fixture().store().releaseBusinessKeyHolds(List.of("released-node")));
  }

  @Test
  void deadNodeKeepsItsRowUntilALaterScanReleasesItsLockedHold() throws Exception {
    // The scan keeps a dead node's row while a hold of that node is left, so the next scan
    // retries the release for a hold that the first one skipped as locked.
    var store = fixture().store();
    String node = "locked-dead-" + UUID.randomUUID();
    store.upsertHeartbeat(node, Instant.now().minus(Duration.ofHours(1)));
    String prefix = "locked-dead-node-" + UUID.randomUUID() + "-";
    List<String> keys = createHolds(prefix, node, Instant.now().plusSeconds(3600));
    String locked = keys.get(1);
    Instant cutoff = Instant.now().minusSeconds(60);
    // The orphan scan's steps for stale nodes, limited to this test's node.
    Callable<List<Integer>> scan =
        () -> {
          List<String> stale =
              store.findInactiveNodesSince(cutoff).stream()
                  .map(NodeEntity::getId)
                  .filter(node::equals)
                  .toList();
          int released = store.releaseBusinessKeyHolds(stale);
          int deleted = store.deleteInactiveNodesByIds(stale);
          return List.of(stale.size(), released, deleted);
        };
    ExecutorService executor = Executors.newSingleThreadExecutor();
    Future<List<Integer>> firstScan = null;
    try {
      try (Connection lockHolder = fixture().openConnection()) {
        lockHolder.setAutoCommit(false);
        try {
          try (Statement statement = lockHolder.createStatement()) {
            assertEquals(
                1,
                statement.executeUpdate(
                    "DELETE FROM scheduler_business_key_reservation WHERE business_key = '"
                        + locked
                        + "'"));
          }
          firstScan = executor.submit(scan);
          List<Integer> first;
          try {
            first = firstScan.get(10, TimeUnit.SECONDS);
          } catch (TimeoutException e) {
            throw new AssertionError(
                "the scan waited on a hold that another transaction locked", e);
          }
          assertEquals(List.of(1, keys.size() - 1, 0), first, "found, released, nodes deleted");
          assertEquals(List.of(locked), remainingHoldKeys(prefix));
          assertTrue(store.findNodeById(node).isPresent(), "the dead node keeps its row");
        } finally {
          lockHolder.rollback();
        }
      }
      assertEquals(
          List.of(1, 1, 1),
          executor.submit(scan).get(30, TimeUnit.SECONDS),
          "the next scan releases the skipped hold, then deletes the node row");
      assertEquals(List.of(), remainingHoldKeys(prefix));
      assertTrue(store.findNodeById(node).isEmpty());
    } finally {
      if (firstScan != null && !firstScan.isDone()) {
        try {
          firstScan.get(60, TimeUnit.SECONDS);
        } catch (Exception ignored) {
          // Let a blocked store call finish after rollback before fixture cleanup.
        }
      }
      executor.shutdownNow();
    }
  }

  @Test
  void nodeReleaseKeepsAHoldThatReplacedTheSelectedOne() throws Exception {
    assertSweepKeepsReplacement(false);
  }

  @Test
  void expirySweepKeepsAHoldThatReplacedTheSelectedOne() throws Exception {
    assertSweepKeepsReplacement(true);
  }

  private void assertSweepKeepsReplacement(boolean expiry) throws Exception {
    var store = fixture().store();
    String key = "replacement-" + UUID.randomUUID();
    String node = "replacement-node";
    Instant heldUntil = Instant.now().plusSeconds(expiry ? -60 : 3600);
    var first = fixture().newPendingJob();
    first.setBusinessKey(key);
    store.create(first);
    JobEntity claimed = store.claimNextBatch(1, node).get(0);
    assertEquals(first.getId(), claimed.getId());
    assertTrue(store.cancelRunningJob(claimed.getId(), heldUntil));
    Thread caller = Thread.currentThread();
    AtomicBoolean fired = new AtomicBoolean();
    AtomicReference<UUID> replacement = new AtomicReference<>();
    var executor = Executors.newSingleThreadExecutor();
    try {
      fixture()
          .observeNativeQueries(
              (sql, event) -> {
                String normalized = sql.replaceAll("\\s+", " ").trim().toUpperCase(Locale.ROOT);
                if (Thread.currentThread() == caller
                    && event.completed()
                    && event.operation().equals("getResultList")
                    && normalized.startsWith("SELECT")
                    && normalized.contains("SCHEDULER_BUSINESS_KEY_RESERVATION")
                    && !normalized.contains("FOR UPDATE")
                    && !normalized.contains("UPDLOCK")
                    && !normalized.contains("READPAST")
                    && fired.compareAndSet(false, true)) {
                  try {
                    executor
                        .submit(
                            () -> {
                              assertTrue(
                                  store.releaseBusinessKeyHold(
                                      claimed.getId(), claimed.getClaimSeq()));
                              var next = fixture().newPendingJob();
                              next.setBusinessKey(key);
                              store.create(next);
                              JobEntity nextClaim = store.claimNextBatch(1, node).get(0);
                              assertEquals(next.getId(), nextClaim.getId());
                              assertTrue(store.cancelRunningJob(nextClaim.getId(), heldUntil));
                              replacement.set(nextClaim.getId());
                            })
                        .get(30, TimeUnit.SECONDS);
                  } catch (Exception e) {
                    throw new IllegalStateException("could not replace the selected hold", e);
                  }
                }
              });
      int deleted =
          expiry
              ? store.deleteExpiredBusinessKeyHolds(Instant.now(), 50)
              : store.releaseBusinessKeyHolds(List.of(node));
      assertTrue(fired.get(), "the candidate read must trigger the replacement");
      assertEquals(0, deleted);
      assertEquals(List.of(key), remainingHoldKeys(key));
      assertEquals(
          new BusinessKeyHolder(replacement.get(), JobType.SINGLE, JobStatus.CANCELED),
          store.findBusinessKeyHolder(key).orElseThrow());
    } finally {
      fixture().observeNativeQueries((sql, event) -> {});
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
    }
  }

  private List<String> createHolds(String prefix, String node, Instant heldUntil) {
    var store = fixture().store();
    List<String> keys = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      String key = prefix + i;
      keys.add(key);
      var job = fixture().newPendingJob();
      job.setBusinessKey(key);
      store.create(job);
    }
    List<JobEntity> claimed = new ArrayList<>();
    for (int tries = 0; claimed.size() < keys.size() && tries < 20; tries++) {
      claimed.addAll(store.claimNextBatch(keys.size(), node));
    }
    assertEquals(keys.size(), claimed.size());
    for (JobEntity job : claimed) {
      assertTrue(store.cancelRunningJob(job.getId(), heldUntil));
    }
    return keys;
  }

  private void assertSweepSkipsLockedHold(String prefix, List<String> keys, Callable<Integer> sweep)
      throws Exception {
    String locked = keys.get(1);
    ExecutorService executor = Executors.newSingleThreadExecutor();
    Future<Integer> firstSweep = null;
    try {
      try (Connection lockHolder = fixture().openConnection()) {
        lockHolder.setAutoCommit(false);
        try {
          // SQL Server binds string parameters as NVARCHAR, which can stop a primary-key seek.
          try (Statement statement = lockHolder.createStatement()) {
            assertEquals(
                1,
                statement.executeUpdate(
                    "DELETE FROM scheduler_business_key_reservation WHERE business_key = '"
                        + locked
                        + "'"));
          }
          firstSweep = executor.submit(sweep);
          int deleted;
          try {
            deleted = firstSweep.get(10, TimeUnit.SECONDS);
          } catch (TimeoutException e) {
            throw new AssertionError(
                "the sweep waited on a hold that another transaction locked", e);
          }
          assertEquals(keys.size() - 1, deleted);
          assertEquals(List.of(locked), remainingHoldKeys(prefix));
        } finally {
          lockHolder.rollback();
        }
      }
      assertEquals(
          1,
          executor.submit(sweep).get(30, TimeUnit.SECONDS),
          "a later call deletes the hold once its lock is gone");
      assertEquals(List.of(), remainingHoldKeys(prefix));
    } finally {
      if (firstSweep != null && !firstSweep.isDone()) {
        try {
          firstSweep.get(60, TimeUnit.SECONDS);
        } catch (Exception ignored) {
          // Let a blocked store call finish after rollback before fixture cleanup.
        }
      }
      executor.shutdownNow();
    }
  }

  private List<String> remainingHoldKeys(String prefix) throws Exception {
    try (Connection connection = fixture().openConnection();
        PreparedStatement statement =
            connection.prepareStatement(
                """
                SELECT business_key FROM scheduler_business_key_reservation
                WHERE business_key LIKE ? ORDER BY business_key
                """)) {
      statement.setString(1, prefix + "%");
      List<String> keys = new ArrayList<>();
      try (ResultSet rows = statement.executeQuery()) {
        while (rows.next()) {
          keys.add(rows.getString(1));
        }
      }
      return keys;
    }
  }

  @Test
  void cancelRunningJobAndDirectTerminalWritesDoNotDeadlock() throws Exception {
    // Direct terminal writes lock the queue row first, as a running cancel does, so they cannot
    // deadlock with it, and exactly one of the two wins each job.
    var store = fixture().store();
    assertOneWinnerPerJobAgainstCancel(
        "terminal-race",
        (index, job, now) ->
            switch (index % 4) {
              case 0 -> store.markJobSucceeded(job.getId(), null, null, now, now, 0L, 0L);
              case 1 -> store.markJobSucceededMinimal(job.getId(), now, now, 0L, 0L);
              case 2 ->
                  store.markJobFailedTerminal(
                      job.getId(), "race failure", job.getAttempts(), job.getClaimSeq());
              default ->
                  store.compareAndSwapStatus(
                      job.getId(), JobStatus.RUNNING, JobStatus.FAILED, "race failure");
            });
  }

  @Test
  void cancelRunningJobAndCommitCompletionDoNotDeadlock() throws Exception {
    // The engine's completion locks the queue row first too, so it cannot deadlock with a cancel.
    var store = fixture().store();
    assertOneWinnerPerJobAgainstCancel(
        "completion-race",
        (index, job, now) ->
            store
                .commitCompletion(
                    new JobCompletionPlan(
                        job.getId(),
                        JobStatus.RUNNING,
                        JobStatus.SUCCEEDED,
                        null,
                        null,
                        null,
                        job.getAttempts(),
                        now,
                        now,
                        0L,
                        0L,
                        null,
                        null,
                        List.of(),
                        job.getClaimSeq(),
                        List.of()))
                .committed());
  }

  @Test
  void deleteAndCancelRunningJobKeepOneOutcome() throws Exception {
    // Delete and running cancel leave either no job or a canceled owner with its hold.
    var store = fixture().store();
    assertOneWinnerPerJobAgainstCancel(
        "delete-race",
        (index, job, now) -> {
          try {
            store.delete(job.getId());
            return true;
          } catch (IllegalStateException expected) {
            return false;
          }
        },
        (job, deleted, canceled) -> {
          assertTrue(deleted ^ canceled, "exactly one winner for " + job.getId());
          if (deleted) {
            assertTrue(store.findById(job.getId()).isEmpty());
          } else {
            assertEquals(JobStatus.CANCELED, store.findById(job.getId()).orElseThrow().getStatus());
            assertEquals(
                new BusinessKeyHolder(job.getId(), JobType.SINGLE, JobStatus.CANCELED),
                store.findBusinessKeyHolder(job.getBusinessKey()).orElseThrow());
          }
        });
  }

  @Test
  void rawCancelJobAndDirectTerminalWritesDoNotDeadlock() throws Exception {
    var store = fixture().store();
    assertOneWinnerPerJobAgainstCancel(
        "raw-terminal-race",
        (index, job, now) ->
            switch (index % 3) {
              case 0 -> store.markJobSucceeded(job.getId(), null, null, now, now, 0L, 0L);
              case 1 -> store.markJobSucceededMinimal(job.getId(), now, now, 0L, 0L);
              default ->
                  store.markJobFailedTerminal(
                      job.getId(), "race failure", job.getAttempts(), job.getClaimSeq());
            },
        job -> store.cancelJob(job.getId()),
        (job, won, canceled) ->
            assertTrue(won ^ canceled, "exactly one winner for " + job.getId()));
  }

  @Test
  void deleteAndRawCancelJobDoNotDeadlock() throws Exception {
    var store = fixture().store();
    assertOneWinnerPerJobAgainstCancel(
        "raw-delete-race",
        (index, job, now) -> {
          store.delete(job.getId());
          return true;
        },
        job -> store.cancelJob(job.getId()),
        (job, deleted, canceled) -> {
          assertTrue(deleted);
          assertTrue(store.findById(job.getId()).isEmpty());
        });
  }

  /** A terminal write that one thread races against a running cancel of the same job. */
  @FunctionalInterface
  private interface TerminalWrite {
    boolean write(int index, JobEntity job, Instant now);
  }

  private void assertOneWinnerPerJobAgainstCancel(String keyPrefix, TerminalWrite terminalWrite)
      throws Exception {
    assertOneWinnerPerJobAgainstCancel(
        keyPrefix,
        terminalWrite,
        (job, won, canceled) ->
            assertTrue(won ^ canceled, "exactly one winner for " + job.getId()));
  }

  private void assertOneWinnerPerJobAgainstCancel(
      String keyPrefix, TerminalWrite terminalWrite, OutcomeCheck check) throws Exception {
    assertOneWinnerPerJobAgainstCancel(
        keyPrefix,
        terminalWrite,
        job -> fixture().store().cancelRunningJob(job.getId(), Instant.now().plusSeconds(30)),
        check);
  }

  @FunctionalInterface
  private interface OutcomeCheck {
    void accept(JobEntity job, boolean writeWon, boolean cancelWon);
  }

  private void assertOneWinnerPerJobAgainstCancel(
      String keyPrefix,
      TerminalWrite terminalWrite,
      Predicate<JobEntity> cancelWrite,
      OutcomeCheck check)
      throws Exception {
    var store = fixture().store();
    String node = keyPrefix + "-node";
    var lockFailures = new ConcurrentHashMap<String, AtomicInteger>();
    fixture()
        .observeTransientFailures(
            (method, failure) -> {
              if (isLockFailure(failure)) {
                lockFailures
                    .computeIfAbsent(method, ignored -> new AtomicInteger())
                    .incrementAndGet();
              }
            });
    var executor = Executors.newFixedThreadPool(2);
    try {
      for (int round = 0; round < 60 && lockFailures.isEmpty(); round++) {
        List<UUID> ids = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
          var job = fixture().newPendingJob();
          job.setBusinessKey(keyPrefix + "-" + round + "-" + (char) ('a' + i));
          ids.add(store.create(job).getId());
        }
        List<JobEntity> claimed = new ArrayList<>();
        for (int tries = 0; claimed.size() < ids.size() && tries < 20; tries++) {
          claimed.addAll(store.claimNextBatch(ids.size(), node));
        }
        assertEquals(ids.size(), claimed.size());
        // Both threads walk the jobs in the same order, so they meet on the same row.
        List<JobEntity> jobs =
            ids.stream()
                .map(id -> claimed.stream().filter(j -> id.equals(j.getId())).findFirst())
                .map(Optional::orElseThrow)
                .toList();
        Instant now = Instant.now();
        var barrier = new CyclicBarrier(2);
        boolean[] terminalWins = new boolean[jobs.size()];
        boolean[] cancelWins = new boolean[jobs.size()];
        Future<?> terminal =
            executor.submit(
                () -> {
                  barrier.await();
                  for (int i = 0; i < jobs.size(); i++) {
                    terminalWins[i] = terminalWrite.write(i, jobs.get(i), now);
                  }
                  return null;
                });
        Future<?> cancel =
            executor.submit(
                () -> {
                  barrier.await();
                  for (int i = 0; i < jobs.size(); i++) {
                    cancelWins[i] = cancelWrite.test(jobs.get(i));
                  }
                  return null;
                });
        terminal.get(60, TimeUnit.SECONDS);
        cancel.get(60, TimeUnit.SECONDS);
        for (int i = 0; i < jobs.size(); i++) {
          check.accept(jobs.get(i), terminalWins[i], cancelWins[i]);
        }
      }
    } finally {
      executor.shutdownNow();
      fixture().observeTransientFailures((method, failure) -> {});
    }
    assertTrue(lockFailures.isEmpty(), keyPrefix + " lock failures: " + lockFailures);
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
