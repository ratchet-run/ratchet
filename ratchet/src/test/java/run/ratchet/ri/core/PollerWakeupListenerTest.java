package run.ratchet.ri.core;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;

import run.ratchet.spi.ClusterCoordinator;
import run.ratchet.spi.MetricsCollector;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

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
}
