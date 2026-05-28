package run.ratchet.ri.core;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import run.ratchet.api.JobPriority;
import run.ratchet.api.NodeIdentity;
import run.ratchet.spi.ClusterCoordinator;
import run.ratchet.spi.MetricsCollector;

/**
 * Registers with {@link ClusterCoordinator} to receive wakeup notifications and forwards them to
 * {@link PollerScheduler#wakeup()} for immediate polling. Notifications are an optimization; the
 * poller continues with adaptive polling if registration or delivery fails.
 *
 * <p>The listener intentionally ignores both the priority and source-node arguments: the local
 * poller wakes unconditionally on any cross-node hint. Per-source self-suppression lives in the
 * coordinator implementation, which is the only layer with full transport visibility.
 *
 * @see JobWakeupService
 */
@ApplicationScoped
public class PollerWakeupListener {

  private static final Logger log = Logger.getLogger(PollerWakeupListener.class);

  private final ClusterCoordinator clusterCoordinator;
  private final PollerScheduler pollerScheduler;
  private final MetricsCollector metricsCollector;

  protected PollerWakeupListener() {
    this.clusterCoordinator = null;
    this.pollerScheduler = null;
    this.metricsCollector = null;
  }

  @Inject
  public PollerWakeupListener(
      ClusterCoordinator clusterCoordinator,
      PollerScheduler pollerScheduler,
      MetricsCollector metricsCollector) {
    this.clusterCoordinator = clusterCoordinator;
    this.pollerScheduler = pollerScheduler;
    this.metricsCollector = metricsCollector;
  }

  public void init() {
    try {
      clusterCoordinator.registerWakeupListener(this::onWakeup);
      log.info("PollerWakeupListener registered with ClusterCoordinator");
    } catch (Exception e) {
      log.errorf(
          "Wakeup listener registration error — polling continues without push notifications: %s",
          e);
    }
  }

  private void onWakeup(JobPriority priority, NodeIdentity source) {
    // priority and source are intentionally ignored — the local poller wakes unconditionally
    // on any cross-node hint; per-source self-suppression is the coordinator's job.
    try {
      log.debug("Wakeup notification received");
      if (metricsCollector != null) {
        metricsCollector.localWakeup("cluster_listener");
      }
      pollerScheduler.wakeup();
    } catch (Exception e) {
      log.warn("Error processing wakeup notification", e);
    }
  }
}
