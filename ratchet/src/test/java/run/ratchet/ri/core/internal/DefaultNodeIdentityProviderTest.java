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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongUnaryOperator;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import run.ratchet.spi.ExecutorProvider;
import run.ratchet.store.spi.JobBulkStore;
import run.ratchet.store.spi.NodeStore;

@ExtendWith(MockitoExtension.class)
class DefaultNodeIdentityProviderTest {

  @Mock private NodeStore nodeStore;
  @Mock private JobBulkStore jobBulkStore;
  @Mock private ExecutorProvider executorProvider;
  @Mock private ScheduledExecutorService scheduledExecutor;
  @Mock private ScheduledFuture<Object> scheduledFuture;

  @Captor private ArgumentCaptor<Runnable> runnableCaptor;

  private DefaultNodeIdentityProvider provider;
  private TestHeartbeatCalculator heartbeatCalculator;
  private Clock clock;

  private static class TestHeartbeatCalculator extends DynamicHeartbeatCalculator {
    private final AtomicInteger calls = new AtomicInteger();
    private long intervalSeconds = 5L;

    void intervalSeconds(long intervalSeconds) {
      this.intervalSeconds = intervalSeconds;
    }

    int calls() {
      return calls.get();
    }

    @Override
    public long calculateHeartbeatInterval() {
      calls.incrementAndGet();
      return intervalSeconds;
    }
  }

  @BeforeEach
  void setUp() {
    heartbeatCalculator = new TestHeartbeatCalculator();
    clock = Clock.fixed(Instant.parse("2026-05-12T12:00:00Z"), ZoneOffset.UTC);
    when(executorProvider.getScheduledExecutor()).thenReturn(scheduledExecutor);
    doReturn(scheduledFuture)
        .when(scheduledExecutor)
        .schedule(runnableCaptor.capture(), anyLong(), eq(TimeUnit.SECONDS));

    when(nodeStore.getDatabaseTime()).thenReturn(clock.instant());

    provider = newProvider(false, "test-node", LongUnaryOperator.identity());
  }

  private DefaultNodeIdentityProvider newProvider(
      boolean dynamicHeartbeatEnabled, String explicitNodeId, LongUnaryOperator retryDelayJitter) {
    return new DefaultNodeIdentityProvider(
        nodeStore,
        jobBulkStore,
        heartbeatCalculator,
        executorProvider,
        5,
        30,
        dynamicHeartbeatEnabled,
        explicitNodeId,
        clock,
        retryDelayJitter);
  }

  @Test
  void getNodeId_returnsStableValueBeforeInitAndInitReusesIt() {
    assertEquals("test-node", provider.getNodeId());
    assertEquals("test-node", provider.getNodeId());

    provider.init();

    verify(nodeStore).upsertHeartbeat("test-node", clock.instant());
    verify(jobBulkStore).resetOrphanJobsForNode("test-node");
    verify(scheduledExecutor).schedule(any(Runnable.class), eq(5L), eq(TimeUnit.SECONDS));
  }

  @Test
  void shutdown_preventsScheduledHeartbeatFromTouchingStore() {
    provider.init();
    Runnable scheduledHeartbeat = runnableCaptor.getValue();

    clearInvocations(nodeStore, scheduledExecutor, scheduledFuture);

    provider.shutdown();
    assertFalse(provider.initialized.get(), "shutdown must lower the heartbeat guard");
    scheduledHeartbeat.run();

    verify(scheduledFuture).cancel(true);
    verify(nodeStore, never()).upsertHeartbeat(any(), any(Instant.class));
    verify(scheduledExecutor, never())
        .schedule(any(Runnable.class), anyLong(), eq(TimeUnit.SECONDS));
  }

  @Test
  void scheduledHeartbeatRunsAndReschedulesWhileInitialized() {
    provider.init();
    Runnable scheduledHeartbeat = runnableCaptor.getValue();

    clearInvocations(nodeStore, scheduledExecutor, scheduledFuture);

    scheduledHeartbeat.run();

    verify(nodeStore).upsertHeartbeat(eq("test-node"), any(Instant.class));
    verify(scheduledExecutor).schedule(any(Runnable.class), eq(5L), eq(TimeUnit.SECONDS));
  }

  @Test
  void initAndHeartbeat_useInjectedClockForTimestamps() {
    provider.init();
    Runnable scheduledHeartbeat = runnableCaptor.getValue();

    verify(nodeStore).upsertHeartbeat("test-node", clock.instant());

    clearInvocations(nodeStore);
    scheduledHeartbeat.run();

    verify(nodeStore).upsertHeartbeat("test-node", clock.instant());
  }

  @Test
  void shutdown_suppressesRescheduleFromInFlightHeartbeatFailure() throws Exception {
    provider.init();
    Runnable scheduledHeartbeat = runnableCaptor.getValue();

    CountDownLatch heartbeatEntered = new CountDownLatch(1);
    doAnswer(
            invocation -> {
              heartbeatEntered.countDown();
              // Spin until shutdown sets initialized=false before throwing, guaranteeing the
              // catch block observes the shutdown state without a release-before-signal race.
              long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
              while (provider.initialized.get() && System.nanoTime() < deadline) {
                Thread.onSpinWait();
              }
              throw new IllegalStateException("container stopping");
            })
        .when(nodeStore)
        .upsertHeartbeat(any(), any(Instant.class));

    clearInvocations(nodeStore, scheduledExecutor, scheduledFuture);

    Thread heartbeatThread = new Thread(scheduledHeartbeat, "heartbeat-test");
    heartbeatThread.start();
    assertTrue(heartbeatEntered.await(5, TimeUnit.SECONDS));

    Thread shutdownThread = new Thread(provider::shutdown, "heartbeat-shutdown");
    shutdownThread.start();

    shutdownThread.join(TimeUnit.SECONDS.toMillis(5));
    heartbeatThread.join(TimeUnit.SECONDS.toMillis(5));

    assertFalse(shutdownThread.isAlive(), "shutdown should finish");
    assertFalse(heartbeatThread.isAlive(), "heartbeat callback should finish");

    verify(scheduledFuture).cancel(true);
    verify(scheduledExecutor, never())
        .schedule(any(Runnable.class), anyLong(), eq(TimeUnit.SECONDS));
  }

  @Test
  void weldShutdownFailure_doesNotRetryHeartbeat() {
    provider.init();
    Runnable scheduledHeartbeat = runnableCaptor.getValue();

    clearInvocations(nodeStore, scheduledExecutor, scheduledFuture);
    doThrow(
            new IllegalStateException(
                "WELD-000229: Contextual reference is not valid after container shutdown"))
        .when(nodeStore)
        .upsertHeartbeat(any(), any(Instant.class));

    scheduledHeartbeat.run();

    verify(nodeStore).upsertHeartbeat(any(), any(Instant.class));
    verify(scheduledExecutor, never())
        .schedule(any(Runnable.class), anyLong(), eq(TimeUnit.SECONDS));
  }

  @Test
  void dynamicHeartbeatFailure_retriesFromDynamicInterval() {
    heartbeatCalculator.intervalSeconds(11L);
    provider = newProvider(true, "test-node", LongUnaryOperator.identity());

    provider.init();
    Runnable scheduledHeartbeat = runnableCaptor.getValue();

    verify(scheduledExecutor).schedule(any(Runnable.class), eq(11L), eq(TimeUnit.SECONDS));

    clearInvocations(nodeStore, scheduledExecutor, scheduledFuture);
    doThrow(new IllegalStateException("store unavailable"))
        .when(nodeStore)
        .upsertHeartbeat(any(), any(Instant.class));

    scheduledHeartbeat.run();

    verify(scheduledExecutor).schedule(any(Runnable.class), eq(22L), eq(TimeUnit.SECONDS));
  }

  @Test
  void heartbeatFailure_appliesRetryJitterAfterCapping() {
    provider = newProvider(false, "test-node", delay -> delay + 3);

    provider.init();
    Runnable scheduledHeartbeat = runnableCaptor.getValue();

    clearInvocations(nodeStore, scheduledExecutor, scheduledFuture);
    doThrow(new IllegalStateException("store unavailable"))
        .when(nodeStore)
        .upsertHeartbeat(any(), any(Instant.class));

    scheduledHeartbeat.run();

    verify(scheduledExecutor).schedule(any(Runnable.class), eq(13L), eq(TimeUnit.SECONDS));
  }

  @Test
  void hostnameFallbackLogsThrowable() {
    RuntimeException failure = new RuntimeException("executor unavailable");
    when(executorProvider.getJobExecutor()).thenThrow(failure);
    provider = newProvider(false, null, LongUnaryOperator.identity());

    LogCapture logs = LogCapture.start(DefaultNodeIdentityProvider.class);
    try {
      provider.init();
    } finally {
      logs.close();
    }

    UUID.fromString(provider.getNodeId());
    assertTrue(
        logs.records().stream()
            .anyMatch(
                record ->
                    record.getLevel().intValue() >= Level.WARNING.intValue()
                        && record.getThrown() == failure
                        && record.getMessage().contains("Hostname resolution error")),
        "Hostname fallback warning should include the original exception");
  }

  @Test
  void dynamicHeartbeatEnabledUsesCalculatorForInitialSchedule() {
    heartbeatCalculator.intervalSeconds(13L);
    provider = newProvider(true, "test-node", LongUnaryOperator.identity());

    clearInvocations(scheduledExecutor);

    provider.init();

    assertEquals(1, heartbeatCalculator.calls());
    verify(scheduledExecutor).schedule(any(Runnable.class), eq(13L), eq(TimeUnit.SECONDS));
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

    List<LogRecord> records() {
      return records;
    }

    @Override
    public void close() {
      logger.removeHandler(handler);
      logger.setLevel(originalLevel);
      logger.setUseParentHandlers(originalUseParentHandlers);
    }
  }
}
