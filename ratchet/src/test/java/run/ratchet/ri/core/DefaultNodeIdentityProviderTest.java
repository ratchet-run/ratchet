package run.ratchet.ri.core;

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

import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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
    when(executorProvider.getScheduledExecutor()).thenReturn(scheduledExecutor);
    doReturn(scheduledFuture)
        .when(scheduledExecutor)
        .schedule(runnableCaptor.capture(), anyLong(), eq(TimeUnit.SECONDS));

    when(nodeStore.getDatabaseTime()).thenReturn(Instant.now());

    provider =
        new DefaultNodeIdentityProvider(
            nodeStore,
            jobBulkStore,
            heartbeatCalculator,
            executorProvider,
            5,
            30,
            false,
            "test-node");
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
    provider =
        new DefaultNodeIdentityProvider(
            nodeStore,
            jobBulkStore,
            heartbeatCalculator,
            executorProvider,
            5,
            30,
            true,
            "test-node");

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
  void dynamicHeartbeatEnabledUsesCalculatorForInitialSchedule() {
    heartbeatCalculator.intervalSeconds(13L);
    provider =
        new DefaultNodeIdentityProvider(
            nodeStore,
            jobBulkStore,
            heartbeatCalculator,
            executorProvider,
            5,
            30,
            true,
            "test-node");

    clearInvocations(scheduledExecutor);

    provider.init();

    assertEquals(1, heartbeatCalculator.calls());
    verify(scheduledExecutor).schedule(any(Runnable.class), eq(13L), eq(TimeUnit.SECONDS));
  }
}
