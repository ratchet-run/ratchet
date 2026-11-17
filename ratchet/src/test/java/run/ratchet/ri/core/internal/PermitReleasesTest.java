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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import run.ratchet.api.exception.RatchetTransientStoreException;
import run.ratchet.ri.core.ResourcePermitService;
import run.ratchet.spi.AfterCommitRegistrar.Result;
import run.ratchet.store.spi.ResourcePermitStore;

@ExtendWith(MockitoExtension.class)
class PermitReleasesTest {
  private static final String RESOURCE = "gpu";
  private static final UUID JOB_ID = UUID.randomUUID();
  private static final long CLAIM_SEQ = 7L;
  // The documented cadence: the first retry after 5 seconds, then doubling up to 1 minute, with no
  // limit on the number of retries.
  private static final long FIRST_DELAY_MS = 5_000L;
  private static final long MAX_DELAY_MS = 60_000L;

  @Mock private ResourcePermitService permits;
  @Mock private ScheduledExecutorService scheduler;

  @AfterEach
  void clearInterrupt() {
    Thread.interrupted();
  }

  private PermitReleases releases() {
    return new PermitReleases(permits, () -> scheduler);
  }

  @Test
  void release_onFreeThread_releasesAtOnce() {
    // A free worker releases its claim's permit inline and schedules nothing.
    assertTrue(releases().release(RESOURCE, JOB_ID, CLAIM_SEQ));
    verify(permits).release(RESOURCE, JOB_ID, CLAIM_SEQ);
    verifyNoInteractions(scheduler);
  }

  @Test
  void release_onInterruptedThread_handsOffAtOnceAndKeepsTheInterrupt() {
    // A JDBC call on an interrupted thread can break the pooled connection, so the worker does not
    // call the store. The release goes to the scheduler with no delay and the interrupt stays set.
    Thread.currentThread().interrupt();
    assertFalse(releases().release(RESOURCE, JOB_ID, CLAIM_SEQ));
    verifyNoInteractions(permits);
    assertTrue(Thread.currentThread().isInterrupted());

    Runnable task = scheduled(1, 0L);
    Thread.interrupted();
    task.run();
    verify(permits).release(RESOURCE, JOB_ID, CLAIM_SEQ);
    verifyNoMoreInteractions(scheduler);
  }

  @Test
  void release_failure_retriesTheSameClaimAfterTheDelay() {
    // A failed release is retried on the scheduler for the same resource, job and claim, so a retry
    // can never remove a permit that a newer attempt of the job took.
    doThrow(new RatchetTransientStoreException("lock wait timeout"))
        .doNothing()
        .when(permits)
        .release(RESOURCE, JOB_ID, CLAIM_SEQ);

    assertFalse(assertDoesNotThrow(() -> releases().release(RESOURCE, JOB_ID, CLAIM_SEQ)));
    scheduled(1, FIRST_DELAY_MS).run();

    verify(permits, times(2)).release(RESOURCE, JOB_ID, CLAIM_SEQ);
    verifyNoMoreInteractions(permits);
    verifyNoMoreInteractions(scheduler);
  }

  @Test
  void retry_keepsRetryingUntilTheReleaseLands() {
    // Each failed retry schedules the next one; the first success ends the retries.
    doThrow(new RatchetTransientStoreException("first"))
        .doThrow(new IllegalStateException("second"))
        .doNothing()
        .when(permits)
        .release(RESOURCE, JOB_ID, CLAIM_SEQ);

    recordSchedules();

    releases().release(RESOURCE, JOB_ID, CLAIM_SEQ);
    runNext();
    runNext();

    verify(permits, times(3)).release(RESOURCE, JOB_ID, CLAIM_SEQ);
    assertEquals(List.of(FIRST_DELAY_MS, 2 * FIRST_DELAY_MS), delays);
    assertTrue(pending.isEmpty());
  }

  @Test
  void retry_backsOffFromFiveSecondsToOneMinute() {
    // Each failed retry doubles the wait, up to one minute.
    doThrow(new RatchetTransientStoreException("store unavailable"))
        .when(permits)
        .release(RESOURCE, JOB_ID, CLAIM_SEQ);
    recordSchedules();

    releases().release(RESOURCE, JOB_ID, CLAIM_SEQ);
    for (int i = 0; i < 6; i++) {
      runNext();
    }

    assertEquals(
        List.of(5_000L, 10_000L, 20_000L, 40_000L, MAX_DELAY_MS, MAX_DELAY_MS, MAX_DELAY_MS),
        delays);
  }

  @Test
  void retry_neverGivesUpWhileTheReleaseFails() {
    // A permit has no expiry, so a release that keeps failing is retried until it lands or the
    // node stops, long past the business-key hold's limit of 60 retries.
    doThrow(new RatchetTransientStoreException("store unavailable"))
        .when(permits)
        .release(RESOURCE, JOB_ID, CLAIM_SEQ);
    recordSchedules();

    releases().release(RESOURCE, JOB_ID, CLAIM_SEQ);
    int retries = 500;
    for (int i = 0; i < retries; i++) {
      runNext();
    }

    verify(permits, times(1 + retries)).release(RESOURCE, JOB_ID, CLAIM_SEQ);
    assertEquals(1, pending.size(), "the next retry must still be scheduled");
    assertEquals(MAX_DELAY_MS, delays.get(delays.size() - 1));
  }

  @Test
  void retry_afterAFailedHandoff_waitsTheFirstDelay() {
    // The handoff from an interrupted worker runs at once. When that try fails, the next one waits
    // the first delay rather than running again at once.
    doThrow(new RatchetTransientStoreException("first"))
        .doThrow(new RatchetTransientStoreException("second"))
        .doNothing()
        .when(permits)
        .release(RESOURCE, JOB_ID, CLAIM_SEQ);
    recordSchedules();
    Thread.currentThread().interrupt();

    releases().release(RESOURCE, JOB_ID, CLAIM_SEQ);
    Thread.interrupted();
    runNext();
    runNext();
    runNext();

    verify(permits, times(3)).release(RESOURCE, JOB_ID, CLAIM_SEQ);
    assertEquals(List.of(0L, FIRST_DELAY_MS, 2 * FIRST_DELAY_MS), delays);
    assertTrue(pending.isEmpty());
  }

  @Test
  void longOutage_warnsAtTheFirstFailureThenSparselyAndReportsTheSuccess() {
    // Over a long outage the release warns once at the first failure, then once per warning
    // interval of waiting, and reports at INFO when it finally lands. The tries in between log at
    // DEBUG only.
    String resource = "outage-" + UUID.randomUUID();
    AtomicBoolean storeBack = new AtomicBoolean();
    doAnswer(
            invocation -> {
              if (!storeBack.get()) throw new RatchetTransientStoreException("store unavailable");
              return null;
            })
        .when(permits)
        .release(resource, JOB_ID, CLAIM_SEQ);
    recordSchedules();
    long outageMillis =
        2 * ReleaseRetries.WARN_INTERVAL.toMillis() + Duration.ofMinutes(5).toMillis();

    try (LogCapture logs = LogCapture.start(ReleaseRetries.class)) {
      releases().release(resource, JOB_ID, CLAIM_SEQ);
      long waited = 0;
      while (!storeBack.get()) {
        assertNotNull(pending.peek(), "the retries stopped before the store came back");
        waited += pending.peek().delayMillis();
        storeBack.set(waited >= outageMillis);
        runNext();
      }

      List<LogRecord> records = logs.recordsMentioning(resource);
      assertEquals(3, count(records, Level.WARNING), "first failure plus one per interval");
      assertEquals(1, count(records, Level.INFO), "the release that lands");
      assertEquals(0, count(records, Level.SEVERE));
    }
    assertTrue(pending.isEmpty());
  }

  @Test
  void release_schedulerRejectsHandoff_doesNotThrowOrCallTheStore() {
    // A stopped scheduler rejects the handoff. The worker returns normally, keeps its interrupt
    // and still does not call the store.
    doThrow(new RejectedExecutionException("scheduler stopped"))
        .when(scheduler)
        .schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
    Thread.currentThread().interrupt();

    assertDoesNotThrow(() -> releases().release(RESOURCE, JOB_ID, CLAIM_SEQ));

    assertTrue(Thread.currentThread().isInterrupted());
    scheduled(1, 0L);
    verify(permits, never()).release(anyString(), any(UUID.class), anyLong());
  }

  @Test
  void release_schedulerRejectsRetry_doesNotThrow() {
    // A failed release whose retry the scheduler rejects returns normally.
    doThrow(new RatchetTransientStoreException("store unavailable"))
        .when(permits)
        .release(RESOURCE, JOB_ID, CLAIM_SEQ);
    doThrow(new RejectedExecutionException("scheduler stopped"))
        .when(scheduler)
        .schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));

    assertDoesNotThrow(() -> releases().release(RESOURCE, JOB_ID, CLAIM_SEQ));

    scheduled(1, FIRST_DELAY_MS);
    verify(permits).release(RESOURCE, JOB_ID, CLAIM_SEQ);
  }

  @Test
  void retry_thatLands_wakesTheResourceWaiters() {
    // The retry goes through the permit service, so a release that lands on retry also wakes the
    // jobs waiting for the resource.
    ResourcePermitStore store = mock(ResourcePermitStore.class);
    JobWakeupService wakeup = mock(JobWakeupService.class);
    DefaultResourcePermitService service =
        new DefaultResourcePermitService(store, wakeup, action -> Result.NO_ACTIVE_TRANSACTION);
    doThrow(new RatchetTransientStoreException("lock wait timeout"))
        .doNothing()
        .when(store)
        .releasePermit(RESOURCE, JOB_ID, CLAIM_SEQ);
    when(store.wakeWaiters(RESOURCE)).thenReturn(List.of());

    new PermitReleases(service, () -> scheduler).release(RESOURCE, JOB_ID, CLAIM_SEQ);
    verify(store, never()).wakeWaiters(anyString());
    scheduled(1, FIRST_DELAY_MS).run();

    InOrder order = inOrder(store);
    order.verify(store, times(2)).releasePermit(RESOURCE, JOB_ID, CLAIM_SEQ);
    order.verify(store).wakeWaiters(RESOURCE);
  }

  private record Scheduled(Runnable task, long delayMillis) {}

  private final Deque<Scheduled> pending = new ArrayDeque<>();
  private final List<Long> delays = new ArrayList<>();

  /** Records every scheduled try in order, so a test can run them one by one. */
  private void recordSchedules() {
    doAnswer(
            invocation -> {
              long delayMillis = invocation.getArgument(1);
              pending.add(new Scheduled(invocation.getArgument(0), delayMillis));
              delays.add(delayMillis);
              return null;
            })
        .when(scheduler)
        .schedule(any(Runnable.class), anyLong(), eq(TimeUnit.MILLISECONDS));
  }

  private void runNext() {
    Scheduled next = pending.poll();
    assertNotNull(next, "no retry is scheduled");
    next.task().run();
  }

  private static long count(List<LogRecord> records, Level level) {
    return records.stream().filter(record -> record.getLevel().equals(level)).count();
  }

  private static final class LogCapture implements AutoCloseable {
    private final Logger logger;
    private final Handler handler;
    private final Level originalLevel;
    private final boolean originalUseParentHandlers;
    private final List<LogRecord> records = new ArrayList<>();

    private LogCapture(Class<?> type) {
      logger = Logger.getLogger(type.getName());
      originalLevel = logger.getLevel();
      originalUseParentHandlers = logger.getUseParentHandlers();
      handler =
          new Handler() {
            @Override
            public void publish(LogRecord record) {
              records.add(record);
            }

            @Override
            public void flush() {}

            @Override
            public void close() {}
          };
      logger.setLevel(Level.ALL);
      logger.setUseParentHandlers(false);
      logger.addHandler(handler);
    }

    static LogCapture start(Class<?> type) {
      return new LogCapture(type);
    }

    /** Records whose message or parameters name the given text; other tests log here too. */
    List<LogRecord> recordsMentioning(String text) {
      return records.stream()
          .filter(
              record ->
                  String.valueOf(record.getMessage()).contains(text)
                      || Arrays.toString(record.getParameters()).contains(text))
          .toList();
    }

    @Override
    public void close() {
      logger.removeHandler(handler);
      logger.setLevel(originalLevel);
      logger.setUseParentHandlers(originalUseParentHandlers);
    }
  }

  private Runnable scheduled(int count, long delayMillis) {
    ArgumentCaptor<Runnable> task = ArgumentCaptor.forClass(Runnable.class);
    verify(scheduler, times(count))
        .schedule(task.capture(), eq(delayMillis), eq(TimeUnit.MILLISECONDS));
    return task.getAllValues().get(count - 1);
  }
}
