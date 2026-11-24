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
package run.ratchet.ri.core.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.jupiter.api.Test;

class RetryDrainTest {
  private final AtomicLong clock = new AtomicLong();
  private final ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
  private final ScheduledExecutorService timer = mock(ScheduledExecutorService.class);
  private final AtomicReference<ScheduledExecutorService> current = new AtomicReference<>(executor);
  private final ArrayDeque<Task> tasks = new ArrayDeque<>();
  private final ArrayDeque<Task> timers = new ArrayDeque<>();
  private final RetryDrain.Refusal refusal = mock(RetryDrain.Refusal.class);
  private RetryDrain drain =
      new RetryDrain(
          2,
          "resource permit releases",
          current::get,
          () -> timer,
          clock::get,
          Duration.ofSeconds(1),
          Duration.ofSeconds(60),
          refusal);

  RetryDrainTest() {
    capture(executor, tasks);
    capture(timer, timers);
  }

  private record Task(Runnable run, long due, long delay, ScheduledFuture<?> future) {}

  private void capture(ScheduledExecutorService service, ArrayDeque<Task> target) {
    doAnswer(
            invocation -> {
              long delay = invocation.getArgument(1);
              ScheduledFuture<?> future = mock(ScheduledFuture.class);
              doAnswer(
                      cancel -> {
                        when(future.isCancelled()).thenReturn(true);
                        return true;
                      })
                  .when(future)
                  .cancel(false);
              target.add(new Task(invocation.getArgument(0), clock.get() + delay, delay, future));
              return future;
            })
        .when(service)
        .schedule(any(Runnable.class), anyLong(), eq(TimeUnit.NANOSECONDS));
  }

  private void add(Object key, long due, RetryDrain.Attempt attempt) {
    RetryDrain.Entry entry = drain.reserve(key, () -> {});
    assertNotNull(entry);
    assertTrue(drain.schedule(entry, due, attempt));
  }

  private Task next(ArrayDeque<Task> source) {
    Task task;
    do {
      task = source.remove();
    } while (task.future().isCancelled());
    return task;
  }

  private void runNext() {
    Task task = next(tasks);
    clock.set(Math.max(clock.get(), task.due()));
    task.run().run();
  }

  private RetryDrain realDrain(int parallelism, ScheduledExecutorService pool) {
    return new RetryDrain(
        parallelism,
        "resource permit releases",
        () -> pool,
        null,
        System::nanoTime,
        Duration.ofSeconds(1),
        Duration.ofSeconds(60),
        refusal);
  }

  private static void await(CountDownLatch latch) {
    try {
      assertTrue(latch.await(5, TimeUnit.SECONDS), "latch timed out");
    } catch (InterruptedException failure) {
      Thread.currentThread().interrupt();
      throw new AssertionError(failure);
    }
  }

  private static void awaitSoon(CountDownLatch latch) throws InterruptedException {
    assertTrue(latch.await(2, TimeUnit.SECONDS), "work did not finish while its peer was blocked");
  }

  private static void shutdown(ScheduledThreadPoolExecutor pool) throws InterruptedException {
    pool.shutdownNow();
    assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));
  }

  private static void submit(RetryDrain drain, Object key, long due, RetryDrain.Attempt attempt) {
    assertEquals(
        RetryDrain.Submission.ACCEPTED, drain.submit(key, due, entry -> attempt, () -> {}));
  }

  private static void assertReusable(RetryDrain drain, Object key) throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    RetryDrain.Entry entry;
    while ((entry = drain.reserve(key, () -> {})) == null && System.nanoTime() - deadline < 0) {
      Thread.sleep(1);
    }
    assertNotNull(entry, "key was not removed: " + key);
    drain.finish(entry);
  }

  private static void assertIdle(RetryDrain drain) throws Exception {
    Field lockField = RetryDrain.class.getDeclaredField("lock");
    Field runningField = RetryDrain.class.getDeclaredField("running");
    lockField.setAccessible(true);
    runningField.setAccessible(true);
    Object lock = lockField.get(drain);
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (true) {
      synchronized (lock) {
        if (runningField.getInt(drain) == 0) return;
      }
      assertTrue(System.nanoTime() - deadline < 0, "ticks did not finish");
      Thread.sleep(1);
    }
  }

  @Test
  void parallelismRejectsZero() {
    assertThrows(IllegalArgumentException.class, () -> realDrain(0, executor));
  }

  @Test
  void blockedAttemptDoesNotStallAnotherKeyWithTwoTicks() throws Exception {
    exerciseBlockedPeer(2);
  }

  @Test
  void oneTickKeepsAnotherKeyWaitingUntilBlockedAttemptReturns() throws Exception {
    exerciseBlockedPeer(1);
  }

  private void exerciseBlockedPeer(int parallelism) throws Exception {
    ScheduledThreadPoolExecutor pool = new ScheduledThreadPoolExecutor(2);
    RetryDrain retries = realDrain(parallelism, pool);
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    CountDownLatch peerDone = new CountDownLatch(1);
    try {
      submit(
          retries,
          "a",
          System.nanoTime(),
          () -> {
            entered.countDown();
            await(release);
            return null;
          });
      await(entered);
      submit(
          retries,
          "b",
          System.nanoTime(),
          () -> {
            peerDone.countDown();
            return null;
          });
      if (parallelism == 2) {
        awaitSoon(peerDone);
        assertEquals(1, release.getCount());
      } else assertFalse(peerDone.await(200, TimeUnit.MILLISECONDS));
      release.countDown();
      await(peerDone);
      assertReusable(retries, "a");
      assertReusable(retries, "b");
      assertIdle(retries);
    } finally {
      release.countDown();
      shutdown(pool);
    }
  }

  @Test
  void requeuedRetryRunsWhileTheSameTickBlocksOnItsNextEntry() throws Exception {
    ScheduledThreadPoolExecutor pool = new ScheduledThreadPoolExecutor(2);
    RetryDrain retries = realDrain(2, pool);
    CountDownLatch occupied = new CountDownLatch(2);
    CountDownLatch firstWorker = new CountDownLatch(1);
    CountDownLatch secondWorker = new CountDownLatch(1);
    CountDownLatch cEntered = new CountDownLatch(1);
    CountDownLatch releaseC = new CountDownLatch(1);
    CountDownLatch retried = new CountDownLatch(1);
    AtomicInteger tries = new AtomicInteger();
    AtomicReference<Thread> firstThread = new AtomicReference<>();
    AtomicReference<Thread> cThread = new AtomicReference<>();
    try {
      pool.execute(
          () -> {
            occupied.countDown();
            await(firstWorker);
          });
      pool.execute(
          () -> {
            occupied.countDown();
            await(secondWorker);
          });
      await(occupied);
      long now = System.nanoTime();
      submit(
          retries,
          "a",
          now,
          () -> {
            if (tries.incrementAndGet() == 1) {
              firstThread.set(Thread.currentThread());
              return System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(100);
            }
            retried.countDown();
            return null;
          });
      submit(
          retries,
          "c",
          now,
          () -> {
            cThread.set(Thread.currentThread());
            cEntered.countDown();
            await(releaseC);
            return null;
          });
      firstWorker.countDown();
      await(cEntered);
      assertSame(firstThread.get(), cThread.get());
      secondWorker.countDown();
      awaitSoon(retried);
      assertEquals(1, releaseC.getCount());
      releaseC.countDown();
      assertReusable(retries, "a");
      assertReusable(retries, "c");
    } finally {
      firstWorker.countDown();
      secondWorker.countDown();
      releaseC.countDown();
      shutdown(pool);
    }
  }

  @Test
  void concurrentAttemptsReachButNeverExceedParallelism() throws Exception {
    for (int parallelism : new int[] {1, 2}) {
      ScheduledThreadPoolExecutor pool = new ScheduledThreadPoolExecutor(2);
      RetryDrain retries = realDrain(parallelism, pool);
      CountDownLatch entered = new CountDownLatch(parallelism);
      CountDownLatch release = new CountDownLatch(1);
      CountDownLatch completed = new CountDownLatch(20);
      CountDownLatch exceeded = new CountDownLatch(1);
      AtomicInteger active = new AtomicInteger();
      AtomicInteger maximum = new AtomicInteger();
      try {
        for (int i = 0; i < 20; i++)
          submit(
              retries,
              i,
              System.nanoTime(),
              () -> {
                int count = active.incrementAndGet();
                maximum.accumulateAndGet(count, Math::max);
                if (count > parallelism) exceeded.countDown();
                entered.countDown();
                try {
                  await(release);
                  return null;
                } finally {
                  active.decrementAndGet();
                  completed.countDown();
                }
              });
        awaitSoon(entered);
        assertFalse(exceeded.await(200, TimeUnit.MILLISECONDS));
        assertEquals(parallelism, maximum.get());
        release.countDown();
        await(completed);
        assertIdle(retries);
        assertEquals(parallelism, maximum.get());
        assertEquals(1, exceeded.getCount());
      } finally {
        release.countDown();
        shutdown(pool);
      }
    }
  }

  @Test
  void singleDueEntryArmsExactlyOneTicketWithParallelismTwo() {
    RetryDrain retries =
        new RetryDrain(
            2,
            "resource permit releases",
            current::get,
            null,
            clock::get,
            Duration.ofSeconds(1),
            Duration.ofSeconds(60),
            refusal);
    submit(retries, "only", 0, () -> null);
    assertEquals(1, tasks.size());
    next(tasks).run().run();
    assertTrue(tasks.isEmpty());
    verify(executor, times(1)).schedule(any(Runnable.class), anyLong(), eq(TimeUnit.NANOSECONDS));
  }

  @Test
  void stoppingDropsARunningEntriesRequeuedRetryExactlyOnce() throws Exception {
    ScheduledThreadPoolExecutor pool = new ScheduledThreadPoolExecutor(2);
    pool.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    RetryDrain retries = realDrain(2, pool);
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    CountDownLatch dropped = new CountDownLatch(1);
    AtomicInteger drops = new AtomicInteger();
    Runnable queuedDropped = mock(Runnable.class);
    try {
      assertEquals(
          RetryDrain.Submission.ACCEPTED,
          retries.submit(
              "running",
              System.nanoTime(),
              entry ->
                  () -> {
                    entered.countDown();
                    await(release);
                    return System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
                  },
              () -> {
                drops.incrementAndGet();
                dropped.countDown();
              }));
      await(entered);
      assertEquals(
          RetryDrain.Submission.ACCEPTED,
          retries.submit(
              "queued",
              System.nanoTime() + TimeUnit.HOURS.toNanos(1),
              entry -> () -> null,
              queuedDropped));
      pool.shutdown();
      release.countDown();
      await(dropped);
      assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));
      assertEquals(1, drops.get());
      verify(queuedDropped).run();
      verifyNoMoreInteractions(queuedDropped);
      assertReusable(retries, "running");
      assertIdle(retries);
    } finally {
      release.countDown();
      shutdown(pool);
    }
  }

  @Test
  void errorInOneTickDoesNotStrandItsPeerOrTheRunningCount() throws Exception {
    ScheduledThreadPoolExecutor pool = new ScheduledThreadPoolExecutor(2);
    RetryDrain retries = realDrain(2, pool);
    CountDownLatch bothEntered = new CountDownLatch(2);
    CountDownLatch releasePeer = new CountDownLatch(1);
    CountDownLatch errorDropped = new CountDownLatch(1);
    CountDownLatch peerDone = new CountDownLatch(1);
    AtomicInteger drops = new AtomicInteger();
    try {
      assertEquals(
          RetryDrain.Submission.ACCEPTED,
          retries.submit(
              "error",
              System.nanoTime(),
              entry ->
                  () -> {
                    bothEntered.countDown();
                    await(bothEntered);
                    throw new AssertionError("store error");
                  },
              () -> {
                drops.incrementAndGet();
                errorDropped.countDown();
              }));
      submit(
          retries,
          "peer",
          System.nanoTime(),
          () -> {
            bothEntered.countDown();
            await(releasePeer);
            peerDone.countDown();
            return null;
          });
      awaitSoon(errorDropped);
      assertEquals(1, releasePeer.getCount());
      releasePeer.countDown();
      await(peerDone);
      assertReusable(retries, "error");
      assertReusable(retries, "peer");
      assertIdle(retries);
      CountDownLatch later = new CountDownLatch(1);
      submit(
          retries,
          "later",
          System.nanoTime(),
          () -> {
            later.countDown();
            return null;
          });
      await(later);
      assertReusable(retries, "later");
      assertIdle(retries);
      assertEquals(1, drops.get());
    } finally {
      releasePeer.countDown();
      shutdown(pool);
    }
  }

  @Test
  void queueingWarnsWhenBacklogExceedsThresholdWhileBothTicksBlock() throws Exception {
    exerciseQueueWarning(true);
  }

  @Test
  void schedulingWarnsWhenBacklogExceedsThresholdWhileBothTicksBlock() throws Exception {
    exerciseQueueWarning(false);
  }

  private void exerciseQueueWarning(boolean atomic) throws Exception {
    ScheduledThreadPoolExecutor pool = new ScheduledThreadPoolExecutor(2);
    RetryDrain retries = realDrain(2, pool);
    CountDownLatch entered = new CountDownLatch(2);
    CountDownLatch release = new CountDownLatch(1);
    try {
      for (int i = 0; i < 2; i++)
        submit(
            retries,
            i,
            System.nanoTime(),
            () -> {
              entered.countDown();
              await(release);
              return null;
            });
      awaitSoon(entered);
      try (LogCapture logs = new LogCapture()) {
        long due = System.nanoTime() + TimeUnit.HOURS.toNanos(1);
        for (int i = 2; i < 1002; i++) {
          if (atomic) submit(retries, i, due, () -> null);
          else {
            RetryDrain.Entry entry = retries.reserve(i, () -> {});
            assertNotNull(entry);
            assertTrue(retries.schedule(entry, due, () -> null));
          }
        }
        List<LogRecord> warnings = logs.atLevel(Level.WARNING);
        assertEquals(1, warnings.size());
        assertTrue(mentions(warnings.get(0), "1001"));
        assertTrue(mentions(warnings.get(0), "resource permit releases"));
      }
    } finally {
      release.countDown();
      shutdown(pool);
    }
  }

  @Test
  void bothBusySlotsDoNotArmAnotherTicketUntilOneFinishes() throws Exception {
    AtomicInteger schedules = new AtomicInteger();
    ScheduledThreadPoolExecutor pool =
        new ScheduledThreadPoolExecutor(2) {
          @Override
          public ScheduledFuture<?> schedule(Runnable task, long delay, TimeUnit unit) {
            schedules.incrementAndGet();
            return super.schedule(task, delay, unit);
          }
        };
    RetryDrain retries = realDrain(2, pool);
    CountDownLatch entered = new CountDownLatch(2);
    CountDownLatch first = new CountDownLatch(1);
    CountDownLatch second = new CountDownLatch(1);
    CountDownLatch earlierDone = new CountDownLatch(1);
    try {
      submit(
          retries,
          "first",
          System.nanoTime(),
          () -> {
            entered.countDown();
            await(first);
            return null;
          });
      submit(
          retries,
          "second",
          System.nanoTime(),
          () -> {
            entered.countDown();
            await(second);
            return null;
          });
      awaitSoon(entered);
      int before = schedules.get();
      submit(
          retries,
          "earlier",
          System.nanoTime() - TimeUnit.SECONDS.toNanos(1),
          () -> {
            earlierDone.countDown();
            return null;
          });
      assertEquals(before, schedules.get());
      first.countDown();
      awaitSoon(earlierDone);
      assertEquals(1, second.getCount());
    } finally {
      first.countDown();
      second.countDown();
      shutdown(pool);
    }
  }

  @Test
  void deadlinesRemainOrderedAndDueAcrossClockWrap() {
    long firstDue = Long.MAX_VALUE - 5;
    long secondDue = firstDue + 10;
    clock.set(firstDue - 5);
    List<String> order = new ArrayList<>();
    // Insert the wrapped deadline first, so the earlier entry must replace its ticket.
    add(
        "second",
        secondDue,
        () -> {
          order.add("second");
          return null;
        });
    Task later = tasks.peek();
    add(
        "first",
        firstDue,
        () -> {
          order.add("first");
          return null;
        });
    assertTrue(later.future().isCancelled());
    Task first = next(tasks);
    assertEquals(5, first.delay());
    clock.set(firstDue);
    first.run().run();
    assertEquals(List.of("first"), order);
    Task second = next(tasks);
    assertEquals(10, second.delay());
    clock.set(secondDue);
    second.run().run();
    assertEquals(List.of("first", "second"), order);
    assertTrue(tasks.isEmpty());
  }

  @Test
  void droppedCallbackErrorDoesNotSkipOtherCallbacksOrBreakReserve() throws Exception {
    Runnable firstDropped = mock(Runnable.class);
    Runnable secondDropped = mock(Runnable.class);
    Field lockField = RetryDrain.class.getDeclaredField("lock");
    lockField.setAccessible(true);
    Object lock = lockField.get(drain);
    AtomicBoolean firstUnderLock = new AtomicBoolean(true);
    AtomicBoolean secondUnderLock = new AtomicBoolean(true);
    doAnswer(
            invocation -> {
              firstUnderLock.set(Thread.holdsLock(lock));
              throw new AssertionError("first callback failed");
            })
        .when(firstDropped)
        .run();
    doAnswer(
            invocation -> {
              secondUnderLock.set(Thread.holdsLock(lock));
              return null;
            })
        .when(secondDropped)
        .run();
    RetryDrain.Entry first = drain.reserve("first", firstDropped);
    assertTrue(drain.schedule(first, 0, () -> null));
    RetryDrain.Entry second = drain.reserve("second", secondDropped);
    assertTrue(drain.schedule(second, 0, () -> null));
    when(executor.isShutdown()).thenReturn(true);
    doThrow(new RejectedExecutionException("stopping"))
        .when(executor)
        .schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
    try (LogCapture logs = new LogCapture()) {
      RetryDrain.Entry next = drain.reserve("next", () -> {});
      assertNotNull(next);
      assertFalse(firstUnderLock.get());
      assertFalse(secondUnderLock.get());
      verify(firstDropped).run();
      verify(secondDropped).run();
      verifyNoMoreInteractions(firstDropped, secondDropped);
      List<LogRecord> errors = logs.atLevel(Level.SEVERE);
      assertEquals(1, errors.size());
      assertTrue(mentions(errors.get(0), "resource permit releases"));
      assertNull(drain.reserve("next", () -> fail("duplicate callback")));
      drain.finish(next);
    }
  }

  @Test
  void droppedCallbackRuntimeFailureDoesNotBreakScheduleOrRearm() {
    Runnable dropped = mock(Runnable.class);
    doThrow(new IllegalStateException("callback failed")).when(dropped).run();
    RetryDrain.Entry first = drain.reserve("first", dropped);
    assertTrue(drain.schedule(first, 0, () -> TimeUnit.SECONDS.toNanos(1)));
    doThrow(new RejectedExecutionException("busy"))
        .when(executor)
        .schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
    doThrow(new RejectedExecutionException("timer busy"))
        .when(timer)
        .schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
    try (LogCapture logs = new LogCapture()) {
      runNext();
      verify(dropped).run();
      verifyNoMoreInteractions(dropped);
      assertEquals(1, logs.atLevel(Level.SEVERE).size());
      assertNotNull(drain.reserve("first", () -> {}));
    }
  }

  @Test
  void dueOrderThenInsertionOrderAndEarlierEntryRearms() {
    drain =
        new RetryDrain(
            1,
            "resource permit releases",
            current::get,
            () -> timer,
            clock::get,
            Duration.ofSeconds(1),
            Duration.ofSeconds(60),
            refusal);
    List<String> order = new ArrayList<>();
    add(
        "late",
        30,
        () -> {
          order.add("late");
          return null;
        });
    Task old = tasks.peek();
    add(
        "first",
        10,
        () -> {
          order.add("first");
          return null;
        });
    add(
        "second",
        10,
        () -> {
          order.add("second");
          return null;
        });
    assertTrue(old.future().isCancelled());
    old.run().run(); // Even a cancellation racing with dispatch cannot run the stale ticket.
    assertTrue(order.isEmpty());
    runNext();
    assertEquals(List.of("first", "second"), order);
    runNext();
    assertEquals(List.of("first", "second", "late"), order);
  }

  @Test
  void itemBudgetYieldsAtZeroDelay() {
    drain =
        new RetryDrain(
            1,
            "resource permit releases",
            current::get,
            () -> timer,
            clock::get,
            Duration.ofSeconds(1),
            Duration.ofSeconds(60),
            refusal);
    AtomicInteger calls = new AtomicInteger();
    for (int i = 0; i < 101; i++)
      add(
          i,
          0,
          () -> {
            calls.incrementAndGet();
            return null;
          });
    runNext();
    assertEquals(100, calls.get());
    assertEquals(0, tasks.peek().delay());
    runNext();
    assertEquals(101, calls.get());
  }

  @Test
  void timeBudgetYieldsAtZeroDelay() {
    drain =
        new RetryDrain(
            1,
            "resource permit releases",
            current::get,
            () -> timer,
            clock::get,
            Duration.ofSeconds(1),
            Duration.ofSeconds(60),
            refusal);
    AtomicInteger calls = new AtomicInteger();
    for (int i = 0; i < 4; i++)
      add(
          i,
          0,
          () -> {
            calls.incrementAndGet();
            clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(600));
            return null;
          });
    runNext();
    assertEquals(2, calls.get());
    assertEquals(0, tasks.peek().delay());
    runNext();
    assertEquals(4, calls.get());
  }

  @Test
  void onlyOneDrainRunsWhileNewEarlierWorkArrives() throws Exception {
    drain =
        new RetryDrain(
            1,
            "resource permit releases",
            current::get,
            () -> timer,
            clock::get,
            Duration.ofSeconds(1),
            Duration.ofSeconds(60),
            refusal);
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch finish = new CountDownLatch(1);
    AtomicInteger calls = new AtomicInteger();
    add(
        "running",
        0,
        () -> {
          entered.countDown();
          try {
            assertTrue(finish.await(5, TimeUnit.SECONDS));
          } catch (InterruptedException failure) {
            throw new AssertionError(failure);
          }
          return null;
        });
    Task task = tasks.remove();
    FutureTask<Void> work = new FutureTask<>(task.run(), null);
    Thread worker = new Thread(work);
    worker.start();
    try {
      assertTrue(entered.await(5, TimeUnit.SECONDS));
      add(
          "new",
          -1,
          () -> {
            calls.incrementAndGet();
            return null;
          });
      task.run().run();
      assertTrue(tasks.isEmpty());
      assertEquals(0, calls.get());
    } finally {
      finish.countDown();
      work.get(5, TimeUnit.SECONDS);
      worker.join(5000);
    }
    assertFalse(worker.isAlive());
    assertEquals(1, calls.get());
  }

  @Test
  void staleCompletionCannotRemoveANewerReservation() {
    RetryDrain.Entry first = drain.reserve("key", () -> {});
    AtomicInteger calls = new AtomicInteger();
    assertTrue(
        drain.schedule(
            first,
            0,
            () -> {
              drain.finish(first);
              add(
                  "key",
                  1,
                  () -> {
                    calls.incrementAndGet();
                    return null;
                  });
              return null;
            }));
    runNext();
    assertNull(drain.reserve("key", () -> {}));
    runNext();
    assertEquals(1, calls.get());
    assertNotNull(drain.reserve("key", () -> {}));
  }

  @Test
  void staleFinishDoesNotRemoveALaterReservationOfTheSameKey() {
    RetryDrain.Entry first = drain.reserve("key", () -> {});
    drain.finish(first);
    assertNotNull(drain.reserve("key", () -> {}));
    drain.finish(first);
    assertNull(drain.reserve("key", () -> {}));
  }

  @Test
  void errorDropsOnceOutsideLockLogsAndRearmsOtherWork() throws Exception {
    Runnable dropped = mock(Runnable.class);
    Runnable completedDropped = mock(Runnable.class);
    AssertionError failure = new AssertionError("store error");
    RetryDrain.Entry entry = drain.reserve("error", dropped);
    Object lock;
    Field lockField = RetryDrain.class.getDeclaredField("lock");
    lockField.setAccessible(true);
    lock = lockField.get(drain);
    AtomicBoolean droppedUnderLock = new AtomicBoolean(true);
    doAnswer(
            invocation -> {
              droppedUnderLock.set(Thread.holdsLock(lock));
              return null;
            })
        .when(dropped)
        .run();
    assertTrue(
        drain.schedule(
            entry,
            0,
            () -> {
              throw failure;
            }));
    RetryDrain.Entry other = drain.reserve("other", completedDropped);
    AtomicInteger calls = new AtomicInteger();
    assertTrue(
        drain.schedule(
            other,
            0,
            () -> {
              calls.incrementAndGet();
              return null;
            }));
    try (LogCapture logs = new LogCapture()) {
      assertSame(failure, assertThrows(AssertionError.class, this::runNext));
      assertFalse(droppedUnderLock.get());
      verify(dropped).run();
      verifyNoMoreInteractions(dropped);
      List<LogRecord> errors = logs.atLevel(Level.SEVERE);
      assertEquals(1, errors.size());
      assertTrue(mentions(errors.get(0), "resource permit releases"));
      assertSame(failure, errors.get(0).getThrown());
      assertNotNull(drain.reserve("error", () -> {}));
      runNext();
      assertEquals(1, calls.get());
      verifyNoInteractions(completedDropped);
      verifyNoMoreInteractions(dropped);
    }
  }

  @Test
  void aSecondStopAfterRestartLogsAgain() {
    when(executor.isShutdown()).thenReturn(true);
    doThrow(new RejectedExecutionException())
        .when(executor)
        .schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
    RetryDrain.Entry first = drain.reserve("first", () -> {});
    assertFalse(drain.schedule(first, 0, () -> null));
    RetryDrain.Entry duplicateStop = drain.reserve("same stop", () -> {});
    assertFalse(drain.schedule(duplicateStop, 0, () -> null));
    verify(refusal, times(1)).dropped(eq(1), eq(true), any());
    ScheduledExecutorService replacement = mock(ScheduledExecutorService.class);
    capture(replacement, tasks);
    current.set(replacement);
    add("after restart", 0, () -> null);
    runNext();
    when(replacement.isShutdown()).thenReturn(true);
    doThrow(new RejectedExecutionException())
        .when(replacement)
        .schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
    RetryDrain.Entry second = drain.reserve("second stop", () -> {});
    assertFalse(drain.schedule(second, 0, () -> null));
    verify(refusal, times(2)).dropped(eq(1), eq(true), any());
    verifyNoMoreInteractions(refusal);
  }

  @Test
  void executorReplacementRecoversDroppedDrainEvenForDuplicate() {
    AtomicInteger calls = new AtomicInteger();
    add(
        "pending",
        10,
        () -> {
          calls.incrementAndGet();
          return null;
        });
    ScheduledExecutorService replacement = mock(ScheduledExecutorService.class);
    capture(replacement, tasks);
    current.set(replacement);
    assertNull(drain.reserve("pending", () -> {}));
    runNext();
    assertEquals(1, calls.get());
  }

  @Test
  void shutdownNowWithoutCompletingFutureDoesNotStrandEntries() {
    ScheduledThreadPoolExecutor original = new ScheduledThreadPoolExecutor(1);
    current.set(original);
    AtomicInteger calls = new AtomicInteger();
    try {
      add(
          "pending",
          TimeUnit.HOURS.toNanos(1),
          () -> {
            calls.incrementAndGet();
            return null;
          });
      List<Runnable> abandoned = original.shutdownNow();
      assertEquals(1, abandoned.size());
      assertFalse(((ScheduledFuture<?>) abandoned.get(0)).isDone());
      current.set(executor);
      add(
          "later",
          TimeUnit.HOURS.toNanos(1),
          () -> {
            calls.incrementAndGet();
            return null;
          });
      runNext();
      assertEquals(2, calls.get());
    } finally {
      original.shutdownNow();
    }
  }

  @Test
  void stoppingDropsAllQueuedKeysAndLogsOnceWithCount() {
    drain =
        new RetryDrain(
            1,
            "resource permit releases",
            current::get,
            () -> timer,
            clock::get,
            Duration.ofSeconds(1),
            Duration.ofSeconds(60),
            refusal);
    AtomicInteger dropped = new AtomicInteger();
    for (int i = 0; i < 3; i++) {
      RetryDrain.Entry entry = drain.reserve(i, dropped::incrementAndGet);
      drain.schedule(entry, 0, () -> TimeUnit.SECONDS.toNanos(1));
    }
    doThrow(new RejectedExecutionException())
        .when(executor)
        .schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
    when(executor.isShutdown()).thenReturn(true);
    runNext();
    assertEquals(3, dropped.get());
    verify(refusal).dropped(eq(3), eq(true), any());
    RetryDrain.Entry entry = drain.reserve(0, () -> {});
    assertNotNull(entry);
    assertFalse(drain.schedule(entry, 0, () -> null));
    verifyNoMoreInteractions(refusal);
  }

  @Test
  void refusalBackoffRecoversAndResetsOnAcceptance() {
    doThrow(new RejectedExecutionException())
        .when(executor)
        .schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
    AtomicInteger calls = new AtomicInteger();
    add(
        "pending",
        0,
        () -> {
          calls.incrementAndGet();
          return null;
        });
    long[] seconds = {1, 2, 4, 8, 16, 32, 60, 60};
    for (long delay : seconds) {
      Task task = timers.remove();
      assertEquals(TimeUnit.SECONDS.toNanos(delay), task.delay());
      clock.set(task.due());
      task.run().run();
    }
    capture(executor, tasks);
    Task rearm = timers.remove();
    clock.set(rearm.due());
    rearm.run().run();
    runNext();
    assertEquals(1, calls.get());
    doThrow(new RejectedExecutionException())
        .when(executor)
        .schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
    add("next", clock.get(), () -> null);
    assertEquals(TimeUnit.SECONDS.toNanos(1), timers.peek().delay());
    verify(refusal).retrying(eq(TimeUnit.SECONDS.toNanos(1)), any());
  }

  @Test
  void refusalWarningsUseClockAndWarningInterval() {
    doThrow(new RejectedExecutionException())
        .when(executor)
        .schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
    add("pending", 0, () -> null);
    timers.remove().run().run();
    verify(refusal, times(1)).retrying(anyLong(), any());
    clock.set(ReleaseRetries.WARN_INTERVAL.toNanos());
    timers.remove().run().run();
    verify(refusal, times(2)).retrying(anyLong(), any());
    timers.remove().run().run();
    verify(refusal, times(2)).retrying(anyLong(), any());
  }

  @Test
  void duplicateDuringProviderFailureDoesNotAdvanceTimerBackoff() {
    current.set(null);
    add("pending", 0, () -> null);
    assertNull(drain.reserve("pending", () -> fail("duplicate callback")));
    assertNull(drain.reserve("pending", () -> fail("duplicate callback")));
    assertEquals(1, timers.size());
    assertEquals(TimeUnit.SECONDS.toNanos(1), timers.peek().delay());
  }

  @Test
  void refusalWithoutTimerDropsKeys() {
    RetryDrain noTimer =
        new RetryDrain(
            2,
            "business-key hold releases",
            current::get,
            null,
            clock::get,
            Duration.ofSeconds(5),
            Duration.ofSeconds(5),
            refusal);
    doThrow(new RejectedExecutionException())
        .when(executor)
        .schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
    Runnable dropped = mock(Runnable.class);
    RetryDrain.Entry entry = noTimer.reserve("key", dropped);
    assertFalse(noTimer.schedule(entry, 0, () -> null));
    verifyNoInteractions(dropped);
    assertNotNull(noTimer.reserve("key", () -> {}));
    verify(refusal).dropped(eq(1), eq(false), any());
  }

  @Test
  void backlogCountAndLagWarningsAreThrottled() {
    try (LogCapture logs = new LogCapture()) {
      for (int i = 0; i < 1001; i++) add(i, 0, () -> null);
      runNext();
      List<LogRecord> warnings = logs.atLevel(Level.WARNING);
      assertEquals(1, warnings.size());
      assertTrue(mentions(warnings.get(0), "resource permit releases"));
      assertTrue(mentions(warnings.get(0), "1001"));
      clock.set(TimeUnit.MINUTES.toNanos(2));
      runNext();
      assertEquals(1, logs.atLevel(Level.WARNING).size());
      clock.set(ReleaseRetries.WARN_INTERVAL.toNanos());
      runNext();
      warnings = logs.atLevel(Level.WARNING);
      assertEquals(2, warnings.size());
      assertTrue(mentions(warnings.get(1), "600"));
      assertTrue(mentions(warnings.get(1), "seconds past due"));
    }
  }

  private static boolean mentions(LogRecord record, String text) {
    return String.valueOf(record.getMessage()).contains(text)
        || Arrays.toString(record.getParameters()).contains(text);
  }

  private static final class LogCapture implements AutoCloseable {
    private final Logger logger = Logger.getLogger(RetryDrain.class.getName());
    private final Level previous = logger.getLevel();
    private final boolean previousParents = logger.getUseParentHandlers();
    private final List<LogRecord> records = new ArrayList<>();
    private final Handler handler =
        new Handler() {
          public void publish(LogRecord record) {
            records.add(record);
          }

          public void flush() {}

          public void close() {}
        };

    LogCapture() {
      logger.setLevel(Level.ALL);
      logger.setUseParentHandlers(false);
      logger.addHandler(handler);
    }

    List<LogRecord> atLevel(Level level) {
      return records.stream()
          .filter(record -> record.getLevel().intValue() == level.intValue())
          .toList();
    }

    public void close() {
      logger.removeHandler(handler);
      logger.setLevel(previous);
      logger.setUseParentHandlers(previousParents);
    }
  }
}
