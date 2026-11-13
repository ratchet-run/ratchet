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
import static run.ratchet.store.spi.ResourcePermitStore.AcquireResult.ACQUIRED;
import static run.ratchet.store.spi.ResourcePermitStore.AcquireResult.AT_CAPACITY;
import static run.ratchet.store.spi.ResourcePermitStore.AcquireResult.SUPERSEDED;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobStatus;
import run.ratchet.api.exception.RatchetOptimisticLockException;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.spi.ResourcePermitStore;
import run.ratchet.store.spi.ResourcePermitStore.AcquireResult;
import run.ratchet.store.spi.ResourcePermitStore.WokenWaiter;
import run.ratchet.tck.store.JpaContainerFixture;

/**
 * Exercises a claim change after the permit claim check and before its write in the bundled SQL
 * stores. The attempt that passed the check can still acquire and holds capacity until its permit
 * is released, even though a newer claim is running by the time the write completes.
 *
 * <p>This is not a TCK contract. It pins the bundled stores' claim read, {@code SELECT status,
 * claim_seq FROM scheduler_job_queue}, followed by a separate permit write. A store that checks the
 * claim another way, or atomically with the write, conforms without this window.
 */
public abstract class AbstractSqlResourcePermitClaimRaceTest {

  protected abstract JpaContainerFixture fixture();

  @BeforeEach
  @AfterEach
  void clean() {
    fixture().cleanupStore();
  }

  @Test
  void staleClaimThatPassedTheCheckHoldsPermitUntilItReleases() {
    ResourcePermitStore permits = fixture().resourcePermitStore();
    permits.configureResource("res-race", 1, 60_000, "claim race");
    JobEntity first = claimNewJob("node-a");
    Thread testThread = Thread.currentThread();
    AtomicBoolean fired = new AtomicBoolean();
    AtomicReference<JobEntity> retry = new AtomicReference<>();
    ExecutorService executor = Executors.newSingleThreadExecutor();
    AcquireResult stale;
    try {
      fixture()
          .observeNativeQueries(
              (sql, event) -> {
                if (Thread.currentThread() != testThread
                    || !event.completed()
                    || !event.operation().equals("getResultList")
                    || !isClaimRead(sql)
                    || !fired.compareAndSet(false, true)) {
                  return;
                }
                try {
                  executor
                      .submit(
                          () -> {
                            assertTrue(
                                fixture()
                                    .store()
                                    .scheduleJobRetry(
                                        first.getId(),
                                        "timed out",
                                        Instant.now().minusSeconds(5),
                                        first.getAttempts() + 1,
                                        first.getClaimSeq()));
                            var claimed = fixture().store().claimNextBatch(1, "node-b");
                            assertEquals(1, claimed.size());
                            assertEquals(first.getId(), claimed.get(0).getId());
                            assertTrue(claimed.get(0).getClaimSeq() > first.getClaimSeq());
                            retry.set(claimed.get(0));
                          })
                      .get(30, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  throw new IllegalStateException("claim change interrupted", e);
                } catch (ExecutionException e) {
                  throw new IllegalStateException("claim change failed", e.getCause());
                } catch (TimeoutException e) {
                  throw new IllegalStateException("claim change timed out", e);
                }
              });
      stale = permits.tryAcquirePermit("res-race", first.getId(), first.getClaimSeq(), "node-a");
    } finally {
      fixture().observeNativeQueries((sql, event) -> {});
      executor.shutdownNow();
    }
    assertTrue(fired.get(), "the claim changed between the check and the write");
    assertEquals(ACQUIRED, stale);
    assertEquals(AT_CAPACITY, acquire(permits, retry.get(), "node-b"));
    JobEntity other = claimNewJob("node-c");
    assertEquals(AT_CAPACITY, acquire(permits, other, "node-c"));
    permits.releasePermit("res-race", first.getId(), first.getClaimSeq());
    permits.wakeWaiters("res-race");
    var reclaimed = fixture().store().claimNextBatch(1, "node-b").get(0);
    assertEquals(retry.get().getId(), reclaimed.getId());
    assertEquals(ACQUIRED, acquire(permits, reclaimed, "node-b"));
    assertEquals(SUPERSEDED, acquire(permits, other, "node-c"));
  }

  @Test
  void claimChangeAfterTheCheckParksNothing() throws Exception {
    var permits = fixture().resourcePermitStore();
    permits.configureResource("res-race", 1, 60_000, null);
    var holder = claimNewJob("holder");
    assertEquals(ACQUIRED, acquire(permits, holder, "holder"));
    var old = claimNewJob("old");
    var current = new AtomicReference<JobEntity>();
    var fired = new AtomicBoolean();
    var caller = Thread.currentThread();
    var executor = Executors.newSingleThreadExecutor();
    try {
      fixture()
          .observeNativeQueries(
              (sql, event) -> {
                if (Thread.currentThread() == caller
                    && event.completed()
                    && event.operation().equals("getResultList")
                    && isClaimRead(sql)
                    && fired.compareAndSet(false, true)) {
                  awaitFuture(
                      executor.submit(
                          () -> {
                            assertTrue(
                                fixture()
                                    .store()
                                    .scheduleJobRetry(
                                        old.getId(),
                                        "supersede",
                                        Instant.now().minusSeconds(5),
                                        old.getAttempts(),
                                        old.getClaimSeq()));
                            var claims = fixture().store().claimNextBatch(1, "new");
                            assertEquals(1, claims.size());
                            current.set(claims.get(0));
                          }));
                }
              });
      assertEquals(SUPERSEDED, acquire(permits, old, "old"));
    } finally {
      fixture().observeNativeQueries((sql, event) -> {});
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
    }
    assertTrue(fired.get());
    assertEquals(old.getId(), current.get().getId());
    assertTrue(current.get().getClaimSeq() > old.getClaimSeq());
    var job = fixture().store().findById(old.getId()).orElseThrow();
    assertEquals(JobStatus.RUNNING, job.getStatus());
    assertEquals(current.get().getClaimSeq(), job.getClaimSeq());
    assertTrue(permits.findResourcesWithWaiters(null, 10).isEmpty());
  }

  @Test
  void releaseDuringAParkStillWakesTheWaiter() throws Exception {
    var permits = fixture().resourcePermitStore();
    permits.configureResource("res-race", 1, 60_000, null);
    var holder = claimNewJob("holder");
    assertEquals(ACQUIRED, acquire(permits, holder, "holder"));
    var waiter = claimNewJob("waiter");
    var fired = new AtomicBoolean();
    var releaseStarted = new CountDownLatch(1);
    var waker = new AtomicReference<Future<List<WokenWaiter>>>();
    var caller = Thread.currentThread();
    var executor = Executors.newSingleThreadExecutor();
    try {
      fixture()
          .observeNativeQueries(
              (sql, event) -> {
                String normalized = sql.replaceAll("\\s+", " ").toUpperCase(Locale.ROOT);
                if (Thread.currentThread() != caller
                    && !event.completed()
                    && event.operation().equals("executeUpdate")
                    && normalized.contains("DELETE FROM SCHEDULER_RESOURCE_PERMIT")) {
                  releaseStarted.countDown();
                }
                // The acquire's first permit read, whatever form the store gives it.
                if (Thread.currentThread() == caller
                    && event.completed()
                    && !event.operation().equals("executeUpdate")
                    && normalized.trim().startsWith("SELECT")
                    && normalized.contains("FROM SCHEDULER_RESOURCE_PERMIT")
                    && fired.compareAndSet(false, true)) {
                  waker.set(
                      executor.submit(
                          () -> {
                            permits.releasePermit("res-race", holder.getId(), holder.getClaimSeq());
                            return permits.wakeWaiters("res-race");
                          }));
                  // MySQL's current read locks the holder's permit too, so release can wait
                  // for the park's commit before its separate wake takes the resource lock.
                  awaitLatch(releaseStarted);
                }
              });
      assertEquals(AT_CAPACITY, acquire(permits, waiter, "waiter"));
      assertTrue(fired.get());
      assertEquals(
          List.of(waiter.getId()),
          waker.get().get(30, TimeUnit.SECONDS).stream().map(w -> w.jobId()).toList());
      assertEquals(
          waiter.getScheduledTime().truncatedTo(ChronoUnit.MILLIS),
          fixture()
              .store()
              .findById(waiter.getId())
              .orElseThrow()
              .getScheduledTime()
              .truncatedTo(ChronoUnit.MILLIS));
    } finally {
      fixture().observeNativeQueries((sql, event) -> {});
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  @Test
  void wakeSweepAndWaiterRowsDoNotDeadlockWithTerminalWritesResetsAndBatchSweeps()
      throws Exception {
    var permits = fixture().resourcePermitStore();
    var batches = fixture().batchStore();
    var store = fixture().store();
    permits.configureResource("res-race", 2, 0, null);
    store.upsertHeartbeat("stress", Instant.now().plusSeconds(3600));
    var parent = fixture().newBatchParentJob();
    parent.setScheduledTime(Instant.now().plusSeconds(3600));
    parent = store.create(parent);
    UUID parentId = parent.getId();
    fixture().persistBatch(parentId, 60);
    var ids = new ArrayList<UUID>();
    for (int i = 0; i < 120; i++) {
      var job = fixture().newPendingJob();
      if (i < 60) {
        job.setDependsOn(parentId);
        job.setJobType(JobExecutionType.BATCH_CHILD);
      }
      ids.add(store.create(job).getId());
    }
    var failures = new ConcurrentLinkedQueue<Throwable>();
    var escapedLockFailures = new ConcurrentHashMap<String, AtomicInteger>();
    var lastLockFailure = new ThreadLocal<String>();
    var lockFailureSamples = new ConcurrentHashMap<String, String>();
    var waiterLockFailures = new ConcurrentHashMap<String, AtomicInteger>();
    var acquisitions = new AtomicInteger();
    var parks = new AtomicInteger();
    var woken = new AtomicInteger();
    var lockFailures = new ConcurrentHashMap<String, AtomicInteger>();
    var start = new CountDownLatch(1);
    long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
    Runnable worker =
        () -> {
          for (var job : store.claimNextBatch(3, "stress")) {
            var result = acquire(permits, job, "stress");
            if (result == AT_CAPACITY) parks.incrementAndGet();
            if (result != ACQUIRED) continue;
            acquisitions.incrementAndGet();
            try {
              switch (ThreadLocalRandom.current().nextInt(3)) {
                case 0 ->
                    store.markJobSucceeded(
                        job.getId(), null, null, Instant.now(), Instant.now(), 0L, 0L);
                case 1 ->
                    store.markJobFailedTerminal(
                        job.getId(), "stress terminal", job.getAttempts(), job.getClaimSeq());
                default ->
                    store.scheduleJobRetry(
                        job.getId(),
                        "stress retry",
                        Instant.now(),
                        job.getAttempts(),
                        job.getClaimSeq());
              }
            } finally {
              permits.releasePermit("res-race", job.getId(), job.getClaimSeq());
              woken.addAndGet(permits.wakeWaiters("res-race").size());
            }
          }
        };
    List<Runnable> roles =
        List.of(
            worker,
            worker,
            worker,
            worker,
            () -> {
              for (var job : store.claimNextBatch(1, "crashed")) {
                acquire(permits, job, "crashed");
              }
            },
            () -> store.cancelJob(ids.get(ThreadLocalRandom.current().nextInt(ids.size()))),
            () -> {
              store.resetOrphanJobsBefore(Instant.now().plusSeconds(60), 100_000, 100);
              // Recover only crashed claims; live workers have a heartbeat beyond the cutoff.
              permits.cleanupOrphanedPermits(List.of("crashed"));
              woken.addAndGet(permits.wakeWaiters("res-race").size());
            },
            () -> {
              if (store.transitionToPaused(parentId, JobStatus.PENDING)) {
                batches.pauseBatchChildren(parentId);
              }
              if (store.transitionFromPaused(parentId, JobStatus.PENDING)) {
                batches.resumeBatchChildren(parentId);
              }
            },
            () -> {
              for (String resource : permits.findResourcesWithWaiters(null, 10))
                woken.addAndGet(permits.wakeWaiters(resource).size());
              permits.deleteStaleWaiters(10);
            },
            () -> {
              var job = store.findById(ids.get(ThreadLocalRandom.current().nextInt(ids.size())));
              if (job.isPresent() && job.get().getStatus() == JobStatus.PENDING) {
                job.get().setScheduledTime(Instant.now().minusSeconds(1));
                store.save(job.get());
              }
            },
            () -> {
              try (var connection = fixture().openConnection();
                  var statement = connection.createStatement();
                  var rows =
                      statement.executeQuery(
                          """
                          SELECT COUNT(*) FROM scheduler_resource_permit
                          WHERE resource_name = 'res-race'
                          """)) {
                assertTrue(rows.next());
                assertTrue(rows.getInt(1) <= 2, "held permits exceeded the resource limit");
              } catch (SQLException e) {
                throw new IllegalStateException(e);
              }
            });
    var executor = Executors.newFixedThreadPool(roles.size());
    var futures = new ArrayList<Future<?>>();
    fixture()
        .observeTransientFailures(
            (method, failure) -> {
              if (isLockFailure(failure)) {
                var counts = isWaiterFailure(method, failure) ? waiterLockFailures : lockFailures;
                counts.computeIfAbsent(method, ignored -> new AtomicInteger()).incrementAndGet();
                lastLockFailure.set(method);
                lockFailureSamples.putIfAbsent(method, rootMessage(failure));
              }
            });
    try {
      for (Runnable role : roles) {
        futures.add(
            executor.submit(
                () -> {
                  awaitLatch(start);
                  for (int round = 0; round < 500 && System.nanoTime() < deadline; round++) {
                    try {
                      role.run();
                    } catch (Throwable failure) {
                      if (isLockFailure(failure)) {
                        String method = lastLockFailure.get();
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
      for (var future : futures) future.get(45, TimeUnit.SECONDS);
    } finally {
      executor.shutdownNow();
      try {
        assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
      } finally {
        fixture().observeTransientFailures((method, failure) -> {});
      }
    }
    System.out.println(
        "Resource race: acquisitions="
            + acquisitions.get()
            + ", parks="
            + parks.get()
            + ", woken waiters="
            + woken.get()
            + ", other lock failures="
            + lockFailures
            + ", escaped after retries="
            + escapedLockFailures);
    lockFailureSamples.forEach(
        (method, message) ->
            System.out.println("Resource race lock failure in " + method + ": " + message));
    // Deadlocks between existing statements (the batch sweeps against the claim, completion and
    // park; permit clean-up against an acquire's permit insert) are reported, not asserted.
    assertTrue(
        waiterLockFailures.isEmpty(),
        "wake-up, sweep or waiter-row lock failures: " + waiterLockFailures);
    if (!failures.isEmpty()) {
      var failure = new AssertionError("Unexpected stress failures: " + failures.size());
      failures.forEach(failure::addSuppressed);
      throw failure;
    }
    assertTrue(acquisitions.get() > 0, "workers must acquire permits");
    assertTrue(parks.get() > 0, "workers must exercise parking");
    assertTrue(woken.get() > 0, "wake must return waiters");
  }

  @Test
  void wakeSkipsAQueueRowThatAnotherTransactionHolds() throws Exception {
    var permits = fixture().resourcePermitStore();
    var store = fixture().store();
    permits.configureResource("res-skip", 2, 60_000, null);
    var holderA = claimNewJob("node-a");
    assertEquals(ACQUIRED, acquire(permits, holderA, "node-a", "res-skip"));
    var holderB = claimNewJob("node-a");
    assertEquals(ACQUIRED, acquire(permits, holderB, "node-a", "res-skip"));
    var first = claimNewJob("node-a");
    assertEquals(AT_CAPACITY, acquire(permits, first, "node-a", "res-skip"));
    var second = claimNewJob("node-a");
    assertEquals(AT_CAPACITY, acquire(permits, second, "node-a", "res-skip"));
    permits.releasePermit("res-skip", holderA.getId(), holderA.getClaimSeq());
    permits.releasePermit("res-skip", holderB.getId(), holderB.getClaimSeq());
    var held = new CountDownLatch(1);
    var release = new CountDownLatch(1);
    var executor = Executors.newFixedThreadPool(2);
    Future<?> holder =
        executor.submit(
            () -> {
              try {
                fixture()
                    .runInTransaction(
                        () -> {
                          assertTrue(store.cancelJob(first.getId()));
                          held.countDown();
                          awaitLatch(release);
                          throw new IllegalStateException("roll back");
                        });
                throw new AssertionError("holder transaction must roll back");
              } catch (IllegalStateException expected) {
                if (!"roll back".equals(expected.getMessage())) throw expected;
              }
            });
    try {
      awaitLatch(held);
      var result = executor.submit(() -> permits.wakeWaiters("res-skip")).get(10, TimeUnit.SECONDS);
      assertEquals(List.of(second.getId()), result.stream().map(WokenWaiter::jobId).toList());
    } finally {
      release.countDown();
      try {
        holder.get(30, TimeUnit.SECONDS);
      } finally {
        executor.shutdownNow();
        assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
      }
    }
    assertTrue(
        permits.wakeWaiters("res-skip").stream()
            .anyMatch(waiter -> waiter.jobId().equals(first.getId())));
  }

  private static boolean isLockFailure(Throwable failure) {
    for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
      if (cause instanceof SQLException sql) {
        if (sql.getErrorCode() == 1213
            || sql.getErrorCode() == 1205
            || sql.getErrorCode() == 1222
            || "40P01".equals(sql.getSQLState())) return true;
      }
      String message = cause.getMessage();
      if (message != null
          && (message.contains("ORA-00060")
              || message.toLowerCase(Locale.ROOT).contains("lock wait timeout"))) return true;
    }
    return false;
  }

  /** The innermost message, which for a deadlock carries the database's own detail. */
  private static String rootMessage(Throwable failure) {
    Throwable root = failure;
    while (root.getCause() != null) root = root.getCause();
    return String.valueOf(root.getMessage()).replace('\n', ' ');
  }

  /** A failure in the wake-up or the sweep, or in any statement on the waiter table. */
  private static boolean isWaiterFailure(String method, Throwable failure) {
    if (List.of("wakeWaiters", "findResourcesWithWaiters", "deleteStaleWaiters").contains(method)) {
      return true;
    }
    for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
      String message = cause.getMessage();
      if (message != null && message.toLowerCase(Locale.ROOT).contains("scheduler_resource_waiter"))
        return true;
    }
    return false;
  }

  private static boolean isClaimRead(String sql) {
    String normalized = sql.replaceAll("\\s+", " ").toUpperCase(Locale.ROOT);
    return normalized.contains("SELECT STATUS, CLAIM_SEQ FROM SCHEDULER_JOB_QUEUE")
        || normalized.contains("SELECT Q.STATUS, Q.CLAIM_SEQ, Q.PRIORITY, Q.SCHEDULED_TIME");
  }

  private static void awaitLatch(CountDownLatch latch) {
    try {
      assertTrue(latch.await(30, TimeUnit.SECONDS), "timed out waiting for race barrier");
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }

  private static void awaitFuture(Future<?> future) {
    try {
      future.get(30, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    } catch (ExecutionException | TimeoutException e) {
      throw new IllegalStateException("race participant failed", e);
    }
  }

  private JobEntity claimNewJob(String nodeId) {
    JobEntity pending = fixture().store().create(fixture().newPendingJob());
    var claimed = fixture().store().claimNextBatch(1, nodeId);
    assertEquals(1, claimed.size());
    assertEquals(pending.getId(), claimed.get(0).getId());
    return claimed.get(0);
  }

  private AcquireResult acquire(ResourcePermitStore permits, JobEntity job, String nodeId) {
    return acquire(permits, job, nodeId, "res-race");
  }

  private AcquireResult acquire(
      ResourcePermitStore permits, JobEntity job, String nodeId, String resource) {
    return permits.tryAcquirePermit(resource, job.getId(), job.getClaimSeq(), nodeId);
  }
}
