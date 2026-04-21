package run.ratchet.ri.core;

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

import run.ratchet.spi.ExecutorProvider;
import run.ratchet.store.spi.JobBulkStore;
import run.ratchet.store.spi.NodeStore;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DefaultNodeIdentityProviderTest {

  @Mock private NodeStore nodeStore;
  @Mock private JobBulkStore jobBulkStore;
  @Mock private DynamicHeartbeatCalculator heartbeatCalculator;
  @Mock private ExecutorProvider executorProvider;
  @Mock private ScheduledExecutorService scheduledExecutor;
  @Mock private ScheduledFuture<Object> scheduledFuture;

  @Captor private ArgumentCaptor<Runnable> runnableCaptor;

  private DefaultNodeIdentityProvider provider;

  @BeforeEach
  void setUp() {
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
    scheduledHeartbeat.run();

    verify(scheduledFuture).cancel(true);
    verify(nodeStore, never()).upsertHeartbeat(any(), any(Instant.class));
    verify(scheduledExecutor, never())
        .schedule(any(Runnable.class), anyLong(), eq(TimeUnit.SECONDS));
  }

  @Test
  void shutdown_suppressesRescheduleFromInFlightHeartbeatFailure() throws Exception {
    provider.init();
    Runnable scheduledHeartbeat = runnableCaptor.getValue();

    CountDownLatch heartbeatEntered = new CountDownLatch(1);
    CountDownLatch releaseHeartbeat = new CountDownLatch(1);
    doAnswer(
            invocation -> {
              heartbeatEntered.countDown();
              assertTrue(releaseHeartbeat.await(5, TimeUnit.SECONDS));
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
    releaseHeartbeat.countDown();

    shutdownThread.join(TimeUnit.SECONDS.toMillis(5));
    heartbeatThread.join(TimeUnit.SECONDS.toMillis(5));

    assertTrue(!shutdownThread.isAlive(), "shutdown should finish");
    assertTrue(!heartbeatThread.isAlive(), "heartbeat callback should finish");

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
}
