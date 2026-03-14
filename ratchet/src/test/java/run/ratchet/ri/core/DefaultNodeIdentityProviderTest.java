package run.ratchet.ri.core;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import run.ratchet.spi.ExecutorProvider;
import run.ratchet.store.spi.NodeStore;
import java.time.Instant;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
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
  @Mock private DynamicHeartbeatCalculator heartbeatCalculator;
  @Mock private ExecutorProvider executorProvider;
  @Mock private ScheduledExecutorService scheduledExecutor;
  @Mock private ScheduledFuture<Object> scheduledFuture;

  @Captor private ArgumentCaptor<Runnable> runnableCaptor;

  private DefaultNodeIdentityProvider provider;
  private String previousNodeName;

  @BeforeEach
  void setUp() {
    previousNodeName = System.getProperty("jboss.node.name");
    System.setProperty("jboss.node.name", "test-node");

    when(executorProvider.getScheduledExecutor()).thenReturn(scheduledExecutor);
    doReturn(scheduledFuture)
        .when(scheduledExecutor)
        .schedule(runnableCaptor.capture(), anyLong(), eq(TimeUnit.SECONDS));

    provider =
        new DefaultNodeIdentityProvider(
            nodeStore, heartbeatCalculator, executorProvider, 5, 30, false);
  }

  @AfterEach
  void tearDown() {
    if (previousNodeName == null) {
      System.clearProperty("jboss.node.name");
    } else {
      System.setProperty("jboss.node.name", previousNodeName);
    }
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
}
