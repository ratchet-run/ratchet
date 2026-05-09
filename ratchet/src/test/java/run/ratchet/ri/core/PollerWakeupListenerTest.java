package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import run.ratchet.spi.ClusterCoordinator;
import run.ratchet.spi.MetricsCollector;

@ExtendWith(MockitoExtension.class)
class PollerWakeupListenerTest {

  @Mock private ClusterCoordinator clusterCoordinator;
  @Mock private PollerScheduler pollerScheduler;
  @Mock private MetricsCollector metricsCollector;

  @Test
  void init_registersListenerThatWakesPollerAndRecordsMetric() {
    AtomicReference<Runnable> listenerRef = new AtomicReference<>();
    doAnswer(
            invocation -> {
              listenerRef.set(invocation.getArgument(0));
              return null;
            })
        .when(clusterCoordinator)
        .registerWakeupListener(any());

    PollerWakeupListener listener =
        new PollerWakeupListener(clusterCoordinator, pollerScheduler, metricsCollector);

    listener.init();
    listenerRef.get().run();

    verify(metricsCollector).localWakeup("cluster_listener");
    verify(pollerScheduler).wakeup();
  }

  @Test
  void init_registrationFailureDoesNotPropagate() {
    doThrow(new RuntimeException("cluster unavailable"))
        .when(clusterCoordinator)
        .registerWakeupListener(any());
    PollerWakeupListener listener =
        new PollerWakeupListener(clusterCoordinator, pollerScheduler, metricsCollector);

    assertDoesNotThrow(listener::init);

    verify(pollerScheduler, never()).wakeup();
    verify(metricsCollector, never()).localWakeup(any());
  }

  @Test
  void registeredListener_wakeupFailureDoesNotPropagate() {
    AtomicReference<Runnable> listenerRef = new AtomicReference<>();
    doAnswer(
            invocation -> {
              listenerRef.set(invocation.getArgument(0));
              return null;
            })
        .when(clusterCoordinator)
        .registerWakeupListener(any());
    doThrow(new RuntimeException("scheduler stopped")).when(pollerScheduler).wakeup();
    PollerWakeupListener listener =
        new PollerWakeupListener(clusterCoordinator, pollerScheduler, metricsCollector);

    listener.init();
    assertNotNull(listenerRef.get());
    assertDoesNotThrow(() -> listenerRef.get().run());

    verify(metricsCollector).localWakeup("cluster_listener");
    verify(pollerScheduler).wakeup();
  }
}
