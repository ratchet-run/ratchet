package run.ratchet.ri.core;

import run.ratchet.spi.ClusterCoordinator;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

/**
 * Registers with {@link ClusterCoordinator} to receive wakeup notifications and forwards them to
 * {@link PollerScheduler#wakeup()} for immediate polling. Notifications are an optimization; the
 * poller continues with adaptive polling if registration or delivery fails.
 *
 * @see JobWakeupService
 */
@ApplicationScoped
public class PollerWakeupListener {

  private static final Logger log = Logger.getLogger(PollerWakeupListener.class);

  private final ClusterCoordinator clusterCoordinator;
  private final PollerScheduler pollerScheduler;

  // Required by CDI proxy
  protected PollerWakeupListener() {
    this.clusterCoordinator = null;
    this.pollerScheduler = null;
  }

  @Inject
  public PollerWakeupListener(
      ClusterCoordinator clusterCoordinator, PollerScheduler pollerScheduler) {
    this.clusterCoordinator = clusterCoordinator;
    this.pollerScheduler = pollerScheduler;
  }

  public void init() {
    try {
      clusterCoordinator.registerWakeupListener(this::onWakeup);
      log.info("PollerWakeupListener registered with ClusterCoordinator");
    } catch (Exception e) {
      log.errorf("Failed to register PollerWakeupListener - notifications disabled: %s", e);
    }
  }

  private void onWakeup() {
    try {
      log.debug("Wakeup notification received");
      pollerScheduler.wakeup();
    } catch (Exception e) {
      log.warnf("Error processing wakeup notification: %s", e.getMessage());
    }
  }
}
