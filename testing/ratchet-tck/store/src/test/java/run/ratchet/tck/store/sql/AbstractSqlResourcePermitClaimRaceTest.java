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
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static run.ratchet.store.spi.ResourcePermitStore.AcquireResult.ACQUIRED;
import static run.ratchet.store.spi.ResourcePermitStore.AcquireResult.AT_CAPACITY;
import static run.ratchet.store.spi.ResourcePermitStore.AcquireResult.SUPERSEDED;
import static run.ratchet.tck.store.sql.SqlRaceFailures.isConnectionFailure;
import static run.ratchet.tck.store.sql.SqlRaceFailures.isLockFailure;
import static run.ratchet.tck.store.sql.SqlRaceFailures.rootMessage;

import java.sql.Connection;
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
import java.util.function.ToIntFunction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobStatus;
import run.ratchet.api.NodeTagFilter;
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

  protected boolean saveLocksTheQueueRowFirst() {
    return false;
  }

  /** Whether an acquire's resource lock also holds every other resource until it commits. */
  protected boolean resourceLockBlocksOtherResources() {
    return false;
  }

  @BeforeEach
  @AfterEach
  void clean() {
    fixture().cleanupStore();
  }

  /** Pins a park with no retry delay as due to a claim that reads the same database clock tick. */
  @Test
  void parkWithNoDelayIsDueToAClaimInTheSameClockTick() {
    var permits = fixture().resourcePermitStore();
    var store = fixture().store();
    permits.configureResource("res-tick", 1, 0, null);
    assertEquals(
        ACQUIRED, acquire(permits, claimNewJob("res-tick", "holder"), "holder", "res-tick"));
    var waiter = new AtomicReference<>(claimNewJob("res-tick", "node-a"));
    // SQL Server's clock moves in steps of a few milliseconds, so two statements in one
    // transaction often read the same value.
    for (int round = 1; round <= 200; round++) {
      String message = "round " + round + ": a job parked with no delay is due at once";
      fixture()
          .runInTransaction(
              () -> {
                assertEquals(AT_CAPACITY, acquire(permits, waiter.get(), "node-a", "res-tick"));
                var claims = store.claimNextBatch(1, "node-a");
                assertEquals(1, claims.size(), message);
                waiter.set(claims.get(0));
              });
    }
  }

  @Test
  void staleClaimThatPassedTheCheckHoldsPermitUntilItReleases() {
    ResourcePermitStore permits = fixture().resourcePermitStore();
    permits.configureResource("res-race", 1, 60_000, "claim race");
    JobEntity first = claimNewJob("res-race", "node-a");
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
    JobEntity other = claimNewJob("res-race", "node-c");
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
    var holder = claimNewJob("res-race", "holder");
    assertEquals(ACQUIRED, acquire(permits, holder, "holder"));
    var old = claimNewJob("res-race", "old");
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

  /** Pins preservation of a newer claim's waiter when a stale acquire succeeds elsewhere. */
  @Test
  void staleAcquireKeepsTheWaiterANewerClaimParkedOnAnotherResource() throws Exception {
    var permits = fixture().resourcePermitStore();
    var store = fixture().store();
    assumeFalse(
        resourceLockBlocksOtherResources(),
        "a park on res-b waits for the stale acquire to commit");
    permits.configureResource("res-a", 1, 0, null);
    permits.configureResource("res-b", 1, 60_000, null);
    var holderA = claimNewJob("res-a", "holder");
    assertEquals(ACQUIRED, acquire(permits, holderA, "holder", "res-a"));
    var holderB = claimNewJob("res-b", "holder");
    assertEquals(ACQUIRED, acquire(permits, holderB, "holder", "res-b"));
    var waiter = claimNewJob("res-a", "node-a");
    assertEquals(AT_CAPACITY, acquire(permits, waiter, "node-a", "res-a"));
    permits.releasePermit("res-a", holderA.getId(), holderA.getClaimSeq());
    var claims = store.claimNextBatch(1, "node-a");
    assertEquals(1, claims.size());
    assertEquals(waiter.getId(), claims.get(0).getId());
    var old = claims.get(0);
    var caller = Thread.currentThread();
    var fired = new AtomicBoolean();
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
                                store.scheduleJobRetry(
                                    old.getId(),
                                    "moved",
                                    Instant.now().minusSeconds(5),
                                    old.getAttempts(),
                                    old.getClaimSeq()));
                            var moved = store.findById(old.getId()).orElseThrow();
                            moved.setResourceName("res-b");
                            store.save(moved);
                            var newer = store.claimNextBatch(1, "node-b");
                            assertEquals(1, newer.size());
                            assertEquals(old.getId(), newer.get(0).getId());
                            assertTrue(newer.get(0).getClaimSeq() > old.getClaimSeq());
                            assertEquals(
                                AT_CAPACITY, acquire(permits, newer.get(0), "node-b", "res-b"));
                          }));
                }
              });
      assertEquals(ACQUIRED, acquire(permits, old, "node-a", "res-a"));
    } finally {
      fixture().observeNativeQueries((sql, event) -> {});
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
    }
    assertTrue(fired.get(), "the job moved and parked on res-b after the claim read");
    assertEquals(
        List.of("res-b"),
        permits.findResourcesWithWaiters(null, 10),
        "the stale acquire on res-a removed the waiter the newer claim wrote on res-b");
    var stored = store.findById(old.getId()).orElseThrow();
    assertEquals("res-b", stored.getResourceName());
    assertEquals(JobStatus.PENDING, stored.getStatus());
  }

  @Test
  void releaseDuringAParkStillWakesTheWaiter() throws Exception {
    var permits = fixture().resourcePermitStore();
    permits.configureResource("res-race", 1, 60_000, null);
    var holder = claimNewJob("res-race", "holder");
    assertEquals(ACQUIRED, acquire(permits, holder, "holder"));
    var waiter = claimNewJob("res-race", "waiter");
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

  /** Pins preservation of a user reschedule committed after wake reads the waiter line. */
  @Test
  void wakeDoesNotUndoARescheduleThatCommitsAfterItReadsTheLine() throws Exception {
    var permits = fixture().resourcePermitStore();
    var store = fixture().store();
    permits.configureResource("res-resched", 1, 60_000, null);
    var holder = claimNewJob("res-resched", "holder");
    assertEquals(ACQUIRED, acquire(permits, holder, "holder", "res-resched"));
    var waiter = claimNewJob("res-resched", "waiter");
    assertEquals(AT_CAPACITY, acquire(permits, waiter, "waiter", "res-resched"));
    permits.releasePermit("res-resched", holder.getId(), holder.getClaimSeq());
    var userTime = new AtomicReference<Instant>();
    var fired = new AtomicBoolean();
    var caller = Thread.currentThread();
    var executor = Executors.newSingleThreadExecutor();
    try {
      fixture()
          .observeNativeQueries(
              (sql, event) -> {
                String normalized = sql.replaceAll("\\s+", " ").toUpperCase(Locale.ROOT);
                if (Thread.currentThread() == caller
                    && event.completed()
                    && event.operation().equals("getResultList")
                    && normalized.contains(
                        "SELECT W.JOB_ID, Q.STATUS, W.PRIORITY, Q.EXECUTION_TARGET, W.WAIT_SINCE")
                    && fired.compareAndSet(false, true)) {
                  awaitFuture(
                      executor.submit(
                          () -> {
                            var parked = store.findById(waiter.getId()).orElseThrow();
                            userTime.set(
                                Instant.now().plusSeconds(3600).truncatedTo(ChronoUnit.MILLIS));
                            parked.setScheduledTime(userTime.get());
                            store.save(parked);
                          }));
                }
              });
      var result = permits.wakeWaiters("res-resched");
      assertTrue(fired.get(), "the user rescheduled after wake read the line");
      assertTrue(result.stream().noneMatch(w -> w.jobId().equals(waiter.getId())));
      assertEquals(
          userTime.get(),
          store
              .findById(waiter.getId())
              .orElseThrow()
              .getScheduledTime()
              .truncatedTo(ChronoUnit.MILLIS));
      assertTrue(permits.findResourcesWithWaiters(null, 10).isEmpty());
    } finally {
      fixture().observeNativeQueries((sql, event) -> {});
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  /** Pins parking without a waiter when a save moves the job after the claim read. */
  @Test
  void parkWritesNoWaiterRowWhenASaveMovesTheJobAfterTheClaimRead() throws Exception {
    var permits = fixture().resourcePermitStore();
    var store = fixture().store();
    permits.configureResource("res-a", 1, 0, null);
    permits.configureResource("res-b", 1, 0, null);
    var holder = claimNewJob("res-a", "holder");
    assertEquals(ACQUIRED, acquire(permits, holder, "holder", "res-a"));
    var waiter = claimNewJob("res-a", "node-a");
    assertEquals(AT_CAPACITY, acquire(permits, waiter, "node-a", "res-a"));
    var claims = store.claimNextBatch(1, "node-a");
    assertEquals(1, claims.size());
    assertEquals(waiter.getId(), claims.get(0).getId());
    var caller = Thread.currentThread();
    var fired = new AtomicBoolean();
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
                            var moved = store.findById(waiter.getId()).orElseThrow();
                            moved.setResourceName("res-b");
                            store.save(moved);
                          }));
                }
              });
      assertEquals(AT_CAPACITY, acquire(permits, claims.get(0), "node-a", "res-a"));
      assertTrue(fired.get());
      assertTrue(permits.findResourcesWithWaiters(null, 10).isEmpty());
      var stored = store.findById(waiter.getId()).orElseThrow();
      assertEquals("res-b", stored.getResourceName());
      assertEquals(JobStatus.PENDING, stored.getStatus());
    } finally {
      fixture().observeNativeQueries((sql, event) -> {});
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  /** Pins removal of a newly inserted waiter by a save moving the job during its park. */
  @Test
  void saveThatMovesTheJobWhileItParksRemovesTheNewWaiterRow() throws Exception {
    assumeFalse(saveLocksTheQueueRowFirst(), "save locks the queue row before handling waiters");
    var permits = fixture().resourcePermitStore();
    var store = fixture().store();
    permits.configureResource("res-a", 1, 60_000, null);
    permits.configureResource("res-b", 1, 60_000, null);
    var holder = claimNewJob("res-a", "holder");
    assertEquals(ACQUIRED, acquire(permits, holder, "holder", "res-a"));
    var waiter = claimNewJob("res-a", "node-a");
    var running = store.findById(waiter.getId()).orElseThrow();
    running.setResourceName("res-b");
    var caller = Thread.currentThread();
    var fired = new AtomicBoolean();
    var saveReachedTheJobRow = new CountDownLatch(1);
    var saveReachedBeforeCommit = new AtomicBoolean();
    var save = new AtomicReference<Future<?>>();
    var executor = Executors.newSingleThreadExecutor();
    try {
      fixture()
          .observeNativeQueries(
              (sql, event) -> {
                String normalized = sql.replaceAll("\\s+", " ").toUpperCase(Locale.ROOT);
                if (Thread.currentThread() != caller
                    && !event.completed()
                    && event.operation().equals("executeUpdate")
                    && normalized.trim().startsWith("UPDATE SCHEDULER_JOB SET")) {
                  saveReachedTheJobRow.countDown();
                }
                if (Thread.currentThread() == caller
                    && event.completed()
                    && event.operation().equals("executeUpdate")
                    && normalized.trim().startsWith("INSERT INTO SCHEDULER_RESOURCE_WAITER")
                    && fired.compareAndSet(false, true)) {
                  save.set(executor.submit(() -> store.save(running)));
                  try {
                    saveReachedBeforeCommit.set(saveReachedTheJobRow.await(5, TimeUnit.SECONDS));
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(e);
                  }
                }
              });
      assertEquals(AT_CAPACITY, acquire(permits, waiter, "node-a", "res-a"));
      assertTrue(fired.get());
      awaitFuture(save.get());
      assertTrue(
          permits.findResourcesWithWaiters(null, 10).isEmpty(),
          "save reached the job row before park committed: " + saveReachedBeforeCommit.get());
      assertTrue(saveReachedBeforeCommit.get(), "the save did not reach the job row mid-park");
      assertEquals("res-b", store.findById(waiter.getId()).orElseThrow().getResourceName());
    } finally {
      fixture().observeNativeQueries((sql, event) -> {});
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  /** Pins removal of a newer resource's waiter when a save writes back an older resource. */
  @Test
  void saveThatWritesBackAnOlderResourceRemovesAWaiterOnTheNewerOne() throws Exception {
    assumeFalse(saveLocksTheQueueRowFirst(), "save locks the job row before it reads the resource");
    var permits = fixture().resourcePermitStore();
    var store = fixture().store();
    permits.configureResource("res-a", 1, 60_000, null);
    permits.configureResource("res-b", 1, 60_000, null);
    var holderB = claimNewJob("res-b", "holder");
    assertEquals(ACQUIRED, acquire(permits, holderB, "holder", "res-b"));
    var job = fixture().newPendingJob();
    job.setResourceName("res-a");
    var created = store.create(job);
    var stale = store.findById(created.getId()).orElseThrow();
    var caller = Thread.currentThread();
    var fired = new AtomicBoolean();
    var executor = Executors.newSingleThreadExecutor();
    try {
      fixture()
          .observeNativeQueries(
              (sql, event) -> {
                String normalized = sql.replaceAll("\\s+", " ").toUpperCase(Locale.ROOT);
                if (Thread.currentThread() == caller
                    && event.completed()
                    && event.operation().equals("getResultList")
                    && normalized.contains("SELECT Q.STATUS, Q.SCHEDULED_TIME, Q.ATTEMPTS")
                    && fired.compareAndSet(false, true)) {
                  awaitFuture(
                      executor.submit(
                          () -> {
                            var moved = store.findById(created.getId()).orElseThrow();
                            moved.setResourceName("res-b");
                            store.save(moved);
                            var claims = store.claimNextBatch(1, "node-b");
                            assertEquals(1, claims.size());
                            assertEquals(created.getId(), claims.get(0).getId());
                            assertEquals(
                                AT_CAPACITY, acquire(permits, claims.get(0), "node-b", "res-b"));
                          }));
                }
              });
      store.save(stale);
    } finally {
      fixture().observeNativeQueries((sql, event) -> {});
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
    }
    assertTrue(fired.get(), "the job moved and parked on res-b after the save read it");
    assertEquals("res-a", store.findById(created.getId()).orElseThrow().getResourceName());
    assertTrue(
        permits.findResourcesWithWaiters(null, 10).isEmpty(),
        "a waiter on res-b stayed after the save moved the job back to res-a");
  }

  /** Pins skipping a waiter whose job moves after wake reads its old resource line. */
  @Test
  void wakeSkipsAWaiterWhoseJobMovesToAnotherResourceAfterTheLineIsRead() throws Exception {
    var permits = fixture().resourcePermitStore();
    var store = fixture().store();
    permits.configureResource("res-wake", 1, 60_000, null);
    var holder = claimNewJob("res-wake", "holder");
    assertEquals(ACQUIRED, acquire(permits, holder, "holder", "res-wake"));
    var waiter = claimNewJob("res-wake", "waiter");
    assertEquals(AT_CAPACITY, acquire(permits, waiter, "waiter", "res-wake"));
    var parkedTime =
        store
            .findById(waiter.getId())
            .orElseThrow()
            .getScheduledTime()
            .truncatedTo(ChronoUnit.MILLIS);
    permits.releasePermit("res-wake", holder.getId(), holder.getClaimSeq());
    var caller = Thread.currentThread();
    var fired = new AtomicBoolean();
    var executor = Executors.newSingleThreadExecutor();
    try {
      fixture()
          .observeNativeQueries(
              (sql, event) -> {
                String normalized = sql.replaceAll("\\s+", " ").toUpperCase(Locale.ROOT);
                if (Thread.currentThread() == caller
                    && event.completed()
                    && event.operation().equals("getResultList")
                    && normalized.contains(
                        "SELECT W.JOB_ID, Q.STATUS, W.PRIORITY, Q.EXECUTION_TARGET, W.WAIT_SINCE")
                    && fired.compareAndSet(false, true)) {
                  awaitFuture(
                      executor.submit(
                          () -> {
                            var moved = store.findById(waiter.getId()).orElseThrow();
                            moved.setResourceName("res-elsewhere");
                            store.save(moved);
                          }));
                }
              });
      var result = permits.wakeWaiters("res-wake");
      assertTrue(fired.get());
      assertTrue(result.stream().noneMatch(w -> w.jobId().equals(waiter.getId())));
      assertEquals(
          parkedTime,
          store
              .findById(waiter.getId())
              .orElseThrow()
              .getScheduledTime()
              .truncatedTo(ChronoUnit.MILLIS));
      assertTrue(permits.findResourcesWithWaiters(null, 10).isEmpty());
    } finally {
      fixture().observeNativeQueries((sql, event) -> {});
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  /** Pins waiter removal by a reschedule in a transaction with an earlier read view. */
  @Test
  void rescheduleInATransactionThatReadEarlierStillRemovesTheWaiter() throws Exception {
    rescheduleInATransactionThatReadEarlier(false);
  }

  /** Pins waiter removal by a reschedule after an earlier save in the same transaction. */
  @Test
  void rescheduleAfterAnEarlierSaveInTheSameTransactionStillRemovesTheWaiter() throws Exception {
    rescheduleInATransactionThatReadEarlier(true);
  }

  private void rescheduleInATransactionThatReadEarlier(boolean saveLastErrorFirst)
      throws Exception {
    var permits = fixture().resourcePermitStore();
    var store = fixture().store();
    permits.configureResource("res-view", 1, 60_000, null);
    var holder = claimNewJob("res-view", "holder");
    assertEquals(ACQUIRED, acquire(permits, holder, "holder", "res-view"));
    var waiter = claimNewJob("res-view", "node-a");
    var parked = new AtomicReference<JobEntity>();
    var userTime = Instant.now().plusSeconds(3600).truncatedTo(ChronoUnit.MILLIS);
    var executor = Executors.newSingleThreadExecutor();
    try {
      fixture()
          .runInTransaction(
              () -> {
                // The first plain read fixes MySQL's read view before the other thread parks.
                store.findById(holder.getId());
                awaitFuture(
                    executor.submit(
                        () -> {
                          assertEquals(AT_CAPACITY, acquire(permits, waiter, "node-a", "res-view"));
                          parked.set(store.findById(waiter.getId()).orElseThrow());
                        }));
                var entity = parked.get();
                if (saveLastErrorFirst) {
                  // First save writes the queue row; the second's plain read returns our own row.
                  entity.setLastError("checked by the operator");
                  store.save(entity);
                }
                entity.setScheduledTime(userTime);
                store.save(entity);
              });
      permits.releasePermit("res-view", holder.getId(), holder.getClaimSeq());
      var woken = permits.wakeWaiters("res-view");
      assertTrue(woken.stream().noneMatch(w -> w.jobId().equals(waiter.getId())));
      assertEquals(
          userTime,
          store
              .findById(waiter.getId())
              .orElseThrow()
              .getScheduledTime()
              .truncatedTo(ChronoUnit.MILLIS));
      assertTrue(permits.findResourcesWithWaiters(null, 10).isEmpty());
    } finally {
      fixture().observeNativeQueries((sql, event) -> {});
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  /** Pins preservation of a waiter renewed after the stale sweep reads its previous park. */
  @Test
  void staleSweepKeepsAWaiterThatParkedAgainAfterTheSweepReadIt() throws Exception {
    var permits = fixture().resourcePermitStore();
    var store = fixture().store();
    permits.configureResource("res-sweep", 1, 0, null);
    var holder = claimNewJob("res-sweep", "holder");
    assertEquals(ACQUIRED, acquire(permits, holder, "holder", "res-sweep"));
    var waiter = claimNewJob("res-sweep", "node-a");
    assertEquals(AT_CAPACITY, acquire(permits, waiter, "node-a", "res-sweep"));
    var reclaimed = store.claimNextBatch(1, "node-a");
    assertEquals(1, reclaimed.size());
    var failed = reclaimed.get(0);
    assertEquals(waiter.getId(), failed.getId());
    assertTrue(
        store.markJobFailedTerminal(
            failed.getId(), "boom", failed.getAttempts(), failed.getClaimSeq()));
    var fired = new AtomicBoolean();
    var caller = Thread.currentThread();
    var executor = Executors.newSingleThreadExecutor();
    try {
      fixture()
          .observeNativeQueries(
              (sql, event) -> {
                String normalized = sql.replaceAll("\\s+", " ").toUpperCase(Locale.ROOT);
                if (Thread.currentThread() == caller
                    && event.completed()
                    && event.operation().equals("getResultList")
                    && normalized.trim().startsWith("SELECT W.JOB_ID")
                    && normalized.contains(
                        "FROM SCHEDULER_RESOURCE_WAITER W LEFT JOIN SCHEDULER_JOB_QUEUE")
                    && fired.compareAndSet(false, true)) {
                  awaitFuture(
                      executor.submit(
                          () -> {
                            assertTrue(store.resetFailedToPending(waiter.getId()));
                            var claims = store.claimNextBatch(1, "node-a");
                            assertEquals(1, claims.size());
                            assertEquals(waiter.getId(), claims.get(0).getId());
                            assertEquals(
                                AT_CAPACITY,
                                acquire(permits, claims.get(0), "node-a", "res-sweep"));
                          }));
                }
              });
      int deleted = permits.deleteStaleWaiters(10);
      assertTrue(fired.get(), "the waiter parked again after the sweep read it");
      assertEquals(0, deleted);
      assertEquals(List.of("res-sweep"), permits.findResourcesWithWaiters(null, 10));
    } finally {
      fixture().observeNativeQueries((sql, event) -> {});
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  /** Pins a waiter kept in line when the stale sweep deletes its row while it parks again. */
  @Test
  void parkKeepsItsPlaceWhenTheSweepDeletesTheRowBeforeTheRenew() throws Exception {
    var permits = fixture().resourcePermitStore();
    var store = fixture().store();
    permits.configureResource("res-sweep", 1, 0, null);
    var holder = claimNewJob("res-sweep", "holder");
    assertEquals(ACQUIRED, acquire(permits, holder, "holder", "res-sweep"));
    var waiter = claimNewJob("res-sweep", "node-a");
    assertEquals(AT_CAPACITY, acquire(permits, waiter, "node-a", "res-sweep"));
    var reclaimed = store.claimNextBatch(1, "node-a");
    assertEquals(1, reclaimed.size());
    var failed = reclaimed.get(0);
    assertEquals(waiter.getId(), failed.getId());
    assertTrue(
        store.markJobFailedTerminal(
            failed.getId(), "boom", failed.getAttempts(), failed.getClaimSeq()));
    var sweepRead = new AtomicBoolean();
    var renewReached = new CountDownLatch(1);
    var sweepDone = new CountDownLatch(1);
    var deleteLandedFirst = new AtomicBoolean();
    var park = new AtomicReference<Future<AcquireResult>>();
    var caller = Thread.currentThread();
    var executor = Executors.newSingleThreadExecutor();
    try {
      fixture()
          .observeNativeQueries(
              (sql, event) -> {
                String normalized = sql.replaceAll("\\s+", " ").toUpperCase(Locale.ROOT);
                if (Thread.currentThread() == caller
                    && event.completed()
                    && event.operation().equals("getResultList")
                    && normalized.trim().startsWith("SELECT W.JOB_ID")
                    && normalized.contains(
                        "FROM SCHEDULER_RESOURCE_WAITER W LEFT JOIN SCHEDULER_JOB_QUEUE")
                    && sweepRead.compareAndSet(false, true)) {
                  park.set(
                      executor.submit(
                          () -> {
                            assertTrue(store.resetFailedToPending(waiter.getId()));
                            var claims = store.claimNextBatch(1, "node-a");
                            assertEquals(1, claims.size());
                            return acquire(permits, claims.get(0), "node-a", "res-sweep");
                          }));
                  awaitLatch(renewReached);
                }
                if (Thread.currentThread() != caller
                    && !event.completed()
                    && event.operation().equals("executeUpdate")
                    && normalized.trim().startsWith("UPDATE SCHEDULER_RESOURCE_WAITER")) {
                  renewReached.countDown();
                  // A store whose delete waits for the park's commit cannot finish the sweep here.
                  try {
                    deleteLandedFirst.set(sweepDone.await(5, TimeUnit.SECONDS));
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(e);
                  }
                }
              });
      permits.deleteStaleWaiters(10);
      sweepDone.countDown();
      assertTrue(sweepRead.get(), "the job parked again after the sweep read its row");
      assertEquals(AT_CAPACITY, park.get().get(30, TimeUnit.SECONDS));
      assertEquals(
          List.of("res-sweep"),
          permits.findResourcesWithWaiters(null, 10),
          "sweep delete landed before the renew: " + deleteLandedFirst.get());
      permits.releasePermit("res-sweep", holder.getId(), holder.getClaimSeq());
      assertEquals(
          List.of(waiter.getId()),
          permits.wakeWaiters("res-sweep").stream().map(WokenWaiter::jobId).toList());
      assertEquals(
          waiter.getScheduledTime().truncatedTo(ChronoUnit.MILLIS),
          store
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
    Duration softLimit = Duration.ofSeconds(15);
    int softRounds = 500;
    Duration hardCap = Duration.ofSeconds(60);
    var permits = fixture().resourcePermitStore();
    var batches = fixture().batchStore();
    var store = fixture().store();
    permits.configureResource("res-race", 2, 0, null);
    store.upsertHeartbeat("stress", Instant.now().plusSeconds(3600));
    // The reserved waiter is excluded until a wake returns it, then rejoins the run.
    // Holders run on the live "stress" node outside ids and the batch, so orphan recovery,
    // cancel and save leave them alone.
    var holders = List.of(claimNewJob("res-race", "stress"), claimNewJob("res-race", "stress"));
    for (var holder : holders) {
      assertEquals(ACQUIRED, acquire(permits, holder, "stress"));
    }
    var workerTags = new NodeTagFilter(List.of(), List.of("reserved"));
    var parent = fixture().newBatchParentJob();
    parent.setScheduledTime(Instant.now().plusSeconds(3600));
    parent = store.create(parent);
    UUID parentId = parent.getId();
    fixture().persistBatch(parentId, 60);
    var ids = new ArrayList<UUID>();
    for (int i = 0; i < 120; i++) {
      var job = fixture().newPendingJob();
      job.setResourceName("res-race");
      if (i < 60) {
        job.setDependsOn(parentId);
        job.setJobType(JobExecutionType.BATCH_CHILD);
      }
      ids.add(store.create(job).getId());
    }
    var reservedJob = fixture().newPendingJob();
    reservedJob.setResourceName("res-race");
    reservedJob.setTags(List.of("reserved"));
    // A new waiter's wait_since is its due time, so this puts it ahead of every worker job.
    reservedJob.setScheduledTime(Instant.now().minusSeconds(3600));
    var reserved = store.create(reservedJob);
    var reservedClaims =
        store.claimNextBatch(1, "reserved", new NodeTagFilter(List.of("reserved"), List.of()));
    assertEquals(1, reservedClaims.size());
    assertEquals(reserved.getId(), reservedClaims.get(0).getId());
    assertEquals(AT_CAPACITY, acquire(permits, reservedClaims.get(0), "reserved"));
    var reservedWoken = new AtomicBoolean();
    ToIntFunction<String> wake =
        resource -> {
          var result = permits.wakeWaiters(resource);
          if (result.stream().anyMatch(waiter -> waiter.jobId().equals(reserved.getId()))) {
            reservedWoken.set(true);
          }
          return result.size();
        };
    var connectionFailures = new ConcurrentLinkedQueue<Throwable>();
    var holdReleased = new AtomicBoolean();
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
    Runnable worker =
        () -> {
          for (var job :
              store.claimNextBatch(
                  3, "stress", reservedWoken.get() ? NodeTagFilter.NONE : workerTags)) {
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
              woken.addAndGet(wake.applyAsInt("res-race"));
            }
          }
        };
    Connection checkerConnection;
    try {
      checkerConnection = fixture().openConnection();
    } catch (SQLException e) {
      throw new AssertionError(
          "Test set-up failed: could not open the checker's JDBC connection", e);
    }
    // Each role thread uses at most one pooled store connection at a time, and the checker
    // reuses one raw connection for the whole run instead of opening one per round, so the run
    // itself uses at most roles.size() sessions at once, the same for every store. Idle pooled
    // connections and the database's own processes still count against the server's limit.
    List<Runnable> roles =
        List.of(
            worker,
            worker,
            worker,
            worker,
            () -> {
              if (holdReleased.get() || parks.get() == 0) return;
              for (var holder : holders) {
                permits.releasePermit("res-race", holder.getId(), holder.getClaimSeq());
              }
              holdReleased.set(true);
              woken.addAndGet(wake.applyAsInt("res-race"));
            },
            () -> {
              for (var job :
                  store.claimNextBatch(
                      1, "crashed", reservedWoken.get() ? NodeTagFilter.NONE : workerTags)) {
                acquire(permits, job, "crashed");
              }
            },
            () -> store.cancelJob(ids.get(ThreadLocalRandom.current().nextInt(ids.size()))),
            () -> {
              store.resetOrphanJobsBefore(Instant.now().plusSeconds(60), 100_000, 100);
              // Recover only crashed claims; live workers have a heartbeat beyond the cutoff.
              permits.cleanupOrphanedPermits(List.of("crashed"));
              woken.addAndGet(wake.applyAsInt("res-race"));
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
                woken.addAndGet(wake.applyAsInt(resource));
              permits.deleteStaleWaiters(10);
            },
            () -> {
              var job = store.findById(ids.get(ThreadLocalRandom.current().nextInt(ids.size())));
              if (job.isPresent() && job.get().getStatus() == JobStatus.PENDING) {
                if (ThreadLocalRandom.current().nextBoolean()) {
                  job.get().setScheduledTime(Instant.now().minusSeconds(1));
                } else {
                  // A resource change takes the job row that parks and wake-ups lock.
                  job.get()
                      .setResourceName(
                          "res-race".equals(job.get().getResourceName())
                              ? "res-moved"
                              : "res-race");
                }
                store.save(job.get());
              }
            },
            () -> {
              try (var statement = checkerConnection.createStatement();
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
    int sessionBound = roles.size();
    var executor = Executors.newFixedThreadPool(roles.size());
    var futures = new ArrayList<Future<?>>();
    fixture()
        .observeTransientFailures(
            (method, failure) -> {
              if (!isConnectionFailure(failure) && isLockFailure(failure)) {
                var counts = isWaiterFailure(method, failure) ? waiterLockFailures : lockFailures;
                counts.computeIfAbsent(method, ignored -> new AtomicInteger()).incrementAndGet();
                lastLockFailure.set(method);
                lockFailureSamples.putIfAbsent(method, rootMessage(failure));
              }
            });
    long runStart = System.nanoTime();
    long softDeadline = runStart + softLimit.toNanos();
    long hardDeadline = runStart + hardCap.toNanos();
    try {
      for (Runnable role : roles) {
        futures.add(
            executor.submit(
                () -> {
                  awaitLatch(start);
                  for (int round = 0; System.nanoTime() < hardDeadline; round++) {
                    boolean softLimitReached =
                        round >= softRounds || System.nanoTime() >= softDeadline;
                    if (softLimitReached
                        && acquisitions.get() > 0
                        && parks.get() > 0
                        && woken.get() > 0) break;
                    lastLockFailure.remove();
                    try {
                      role.run();
                    } catch (Throwable failure) {
                      if (isConnectionFailure(failure)) {
                        connectionFailures.add(failure);
                      } else if (isLockFailure(failure)) {
                        // A save does not translate a deadlock, so it escapes on its first attempt
                        // with no method name. Its statement text still shows a waiter-row failure.
                        String method = lastLockFailure.get();
                        String key = method == null ? "unknown" : method;
                        var counts =
                            isWaiterFailure(key, failure)
                                ? waiterLockFailures
                                : escapedLockFailures;
                        counts
                            .computeIfAbsent(key, ignored -> new AtomicInteger())
                            .incrementAndGet();
                        lockFailureSamples.putIfAbsent("escaped " + key, chainMessage(failure));
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
        checkerConnection.close();
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
    if (!connectionFailures.isEmpty()) {
      var failure =
          new AssertionError(
              "Test set-up failed: "
                  + connectionFailures.size()
                  + " database connection failures during the stress run. Its "
                  + sessionBound
                  + " threads hold at most one session each, but idle pooled connections and the"
                  + " database's own processes count against the same server limit. These are not"
                  + " store failures. Waiter lock failures seen: "
                  + waiterLockFailures);
      connectionFailures.stream().limit(5).forEach(failure::addSuppressed);
      throw failure;
    }
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
    if (acquisitions.get() == 0 || parks.get() == 0 || woken.get() == 0) {
      throw new AssertionError(
          "The stress run did not reach an acquire, a park and a wake-up within "
              + hardCap.toSeconds()
              + " s: acquisitions="
              + acquisitions.get()
              + ", parks="
              + parks.get()
              + ", woken="
              + woken.get()
              + ", hold released="
              + holdReleased.get()
              + ", other lock failures="
              + lockFailures
              + ", escaped after retries="
              + escapedLockFailures);
    }
    assertTrue(acquisitions.get() > 0, "workers must acquire permits");
    assertTrue(parks.get() > 0, "workers must exercise parking while the test held every permit");
    assertTrue(woken.get() > 0, "wake must return waiters");
  }

  @Test
  void wakeSkipsAQueueRowThatAnotherTransactionHolds() throws Exception {
    var permits = fixture().resourcePermitStore();
    var store = fixture().store();
    permits.configureResource("res-skip", 2, 60_000, null);
    var holderA = claimNewJob("res-skip", "node-a");
    assertEquals(ACQUIRED, acquire(permits, holderA, "node-a", "res-skip"));
    var holderB = claimNewJob("res-skip", "node-a");
    assertEquals(ACQUIRED, acquire(permits, holderB, "node-a", "res-skip"));
    var first = claimNewJob("res-skip", "node-a");
    assertEquals(AT_CAPACITY, acquire(permits, first, "node-a", "res-skip"));
    var second = claimNewJob("res-skip", "node-a");
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

  /** Every message in the cause chain, so an untranslated failure still names its statement. */
  private static String chainMessage(Throwable failure) {
    var messages = new ArrayList<String>();
    for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
      messages.add(cause.getClass().getSimpleName() + ": " + cause.getMessage());
    }
    return String.join(" <- ", messages).replace('\n', ' ');
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

  private JobEntity claimNewJob(String resource, String nodeId) {
    var job = fixture().newPendingJob();
    job.setResourceName(resource);
    JobEntity pending = fixture().store().create(job);
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
