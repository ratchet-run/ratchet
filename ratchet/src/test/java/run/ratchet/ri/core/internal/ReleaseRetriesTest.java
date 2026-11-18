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

import static org.jboss.logging.Logger.Level.WARN;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.jupiter.api.Test;
import run.ratchet.ri.core.internal.ReleaseRetries.Policy;

class ReleaseRetriesTest {

  @Test
  void policy_rejectsADelayThatIsNotPositive() {
    // A zero first delay would turn a failed handoff into a busy loop on the scheduler.
    assertThrows(
        IllegalArgumentException.class,
        () -> Policy.untilReleased(Duration.ZERO, Duration.ofMinutes(1)));
    assertThrows(IllegalArgumentException.class, () -> Policy.bounded(Duration.ofSeconds(-1), 3));
  }

  @Test
  void policy_rejectsAMaxDelayBelowTheFirstDelay() {
    assertThrows(
        IllegalArgumentException.class,
        () -> Policy.untilReleased(Duration.ofMinutes(1), Duration.ofSeconds(5)));
  }

  @Test
  void policy_rejectsARetryLimitBelowOne() {
    assertThrows(IllegalArgumentException.class, () -> Policy.bounded(Duration.ofSeconds(5), 0));
  }

  @Test
  void refusedRetriesBackOffAndWriteOnlyOnReleaseExecutor() {
    ScheduledExecutorService releases = mock(ScheduledExecutorService.class);
    ScheduledExecutorService timer = mock(ScheduledExecutorService.class);
    ArrayDeque<Runnable> resubmissions = new ArrayDeque<>();
    ArrayDeque<Runnable> writes = new ArrayDeque<>();
    List<Long> delays = new ArrayList<>();
    AtomicInteger refusals = new AtomicInteger(6);
    doAnswer(
            invocation -> {
              if (refusals.getAndDecrement() > 0) throw new RejectedExecutionException("busy");
              writes.add(invocation.getArgument(0));
              return null;
            })
        .when(releases)
        .schedule(any(Runnable.class), anyLong(), eq(TimeUnit.MILLISECONDS));
    doAnswer(
            invocation -> {
              resubmissions.add(invocation.getArgument(0));
              delays.add(invocation.getArgument(1));
              return null;
            })
        .when(timer)
        .schedule(any(Runnable.class), anyLong(), eq(TimeUnit.MILLISECONDS));
    Runnable write = mock(Runnable.class);
    ReleaseRetries retries =
        new ReleaseRetries(
            () -> releases,
            () -> timer,
            Policy.untilReleased(Duration.ofSeconds(5), Duration.ofMinutes(1)),
            () -> 0L);
    // The initial refused retry has zero delay because the caller is interrupted.
    try {
      Thread.currentThread().interrupt();
      assertFalse(retries.release(new ReleaseRetries.Release("write", "recovery", WARN, write)));
    } finally {
      Thread.interrupted();
    }
    while (!resubmissions.isEmpty()) {
      resubmissions.remove().run();
      verifyNoInteractions(write);
    }
    assertEquals(List.of(5_000L, 10_000L, 20_000L, 40_000L, 60_000L, 60_000L), delays);
    assertEquals(1, writes.size());
    writes.remove().run();
    verify(write).run();
  }

  @Test
  void acceptanceResetsRefusalBackoffBeforeNextFailedWrite() {
    ScheduledExecutorService releases = mock(ScheduledExecutorService.class);
    ScheduledExecutorService timer = mock(ScheduledExecutorService.class);
    ArrayDeque<Runnable> resubmissions = new ArrayDeque<>();
    ArrayDeque<Runnable> writes = new ArrayDeque<>();
    List<Long> delays = new ArrayList<>();
    AtomicInteger schedules = new AtomicInteger();
    doAnswer(
            invocation -> {
              // Refuse twice, accept, then refuse the next failed write's retry once.
              int number = schedules.incrementAndGet();
              if (number == 1 || number == 2 || number == 4)
                throw new RejectedExecutionException("busy");
              writes.add(invocation.getArgument(0));
              return null;
            })
        .when(releases)
        .schedule(any(Runnable.class), anyLong(), eq(TimeUnit.MILLISECONDS));
    doAnswer(
            invocation -> {
              resubmissions.add(invocation.getArgument(0));
              delays.add(invocation.getArgument(1));
              return null;
            })
        .when(timer)
        .schedule(any(Runnable.class), anyLong(), eq(TimeUnit.MILLISECONDS));
    AtomicInteger calls = new AtomicInteger();
    ReleaseRetries retries =
        new ReleaseRetries(
            () -> releases,
            () -> timer,
            Policy.untilReleased(Duration.ofSeconds(5), Duration.ofMinutes(1)),
            () -> 0L);
    try {
      Thread.currentThread().interrupt();
      retries.release(
          new ReleaseRetries.Release(
              "write",
              "recovery",
              WARN,
              () -> {
                if (calls.incrementAndGet() == 1) throw new IllegalStateException("store down");
              }));
    } finally {
      Thread.interrupted();
    }
    resubmissions.remove().run();
    resubmissions.remove().run();
    writes.remove().run();
    assertEquals(List.of(5_000L, 10_000L, 5_000L), delays);
    resubmissions.remove().run();
    writes.remove().run();
    assertEquals(2, calls.get());
    assertTrue(resubmissions.isEmpty());
    assertTrue(writes.isEmpty());
  }

  @Test
  void refusedTimerLogsOnceAndDoesNotLoop() {
    ScheduledExecutorService releases = mock(ScheduledExecutorService.class);
    ScheduledExecutorService timer = mock(ScheduledExecutorService.class);
    doThrow(new RejectedExecutionException("busy"))
        .when(releases)
        .schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
    doThrow(new RejectedExecutionException("timer busy"))
        .when(timer)
        .schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
    List<LogRecord> records = new ArrayList<>();
    Logger logger = Logger.getLogger(ReleaseRetries.class.getName());
    Level previous = logger.getLevel();
    Handler capture =
        new Handler() {
          public void publish(LogRecord record) {
            records.add(record);
          }

          public void flush() {}

          public void close() {}
        };
    logger.setLevel(Level.ALL);
    logger.addHandler(capture);
    try {
      handOffInterrupted(
          releases, timer, Policy.untilReleased(Duration.ofSeconds(5), Duration.ofMinutes(1)));
      assertEquals(1, records.size());
      assertTrue(
          records.get(0).getMessage().contains("Could not schedule write; left to recovery"));
    } finally {
      logger.removeHandler(capture);
      logger.setLevel(previous);
    }
    verify(releases).schedule(any(Runnable.class), eq(0L), eq(TimeUnit.MILLISECONDS));
    verify(timer).schedule(any(Runnable.class), eq(5_000L), eq(TimeUnit.MILLISECONDS));
  }

  @Test
  void stoppingRefusalDoesNotResubmit() {
    ScheduledExecutorService releases = mock(ScheduledExecutorService.class);
    ScheduledExecutorService timer = mock(ScheduledExecutorService.class);
    when(releases.isShutdown()).thenReturn(true);
    doThrow(new RejectedExecutionException("stopping"))
        .when(releases)
        .schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
    handOffInterrupted(
        releases, timer, Policy.untilReleased(Duration.ofSeconds(5), Duration.ofMinutes(1)));
    verifyNoInteractions(timer);
  }

  @Test
  void boundedPolicyDoesNotResubmitEvenWithTimer() {
    ScheduledExecutorService releases = mock(ScheduledExecutorService.class);
    ScheduledExecutorService timer = mock(ScheduledExecutorService.class);
    doThrow(new RejectedExecutionException("busy"))
        .when(releases)
        .schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
    handOffInterrupted(releases, timer, Policy.bounded(Duration.ofSeconds(5), 3));
    verifyNoInteractions(timer);
  }

  private void handOffInterrupted(
      ScheduledExecutorService releases, ScheduledExecutorService timer, Policy policy) {
    Runnable write = mock(Runnable.class);
    ReleaseRetries retries =
        new ReleaseRetries(() -> releases, () -> timer, policy, System::nanoTime);
    try {
      Thread.currentThread().interrupt();
      assertFalse(retries.release(new ReleaseRetries.Release("write", "recovery", WARN, write)));
    } finally {
      Thread.interrupted();
    }
    verifyNoInteractions(write);
  }
}
