package run.ratchet.ri.core;

import run.ratchet.spi.ClusterCoordinator;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

/**
 * Bridge that registers with the {@link ClusterCoordinator} to receive wakeup notifications and
 * forwards them to the {@link PollerScheduler} for immediate polling.
 *
 * <p>How It Works:
 *
 * <ol>
 *   <li>Job is created that needs immediate processing
 *   <li>{@link JobWakeupService} publishes notification via ClusterCoordinator
 *   <li>ClusterCoordinator invokes the registered wakeup listener on all cluster nodes
 *   <li>This listener calls {@link PollerScheduler#wakeup()} to trigger immediate polling
 *   <li>Poller claims and executes the job
 * </ol>
 *
 * <p>Fault Tolerance: If the notification system fails, the Poller continues with its normal
 * adaptive polling behavior. Notifications are an optimization, not a requirement.
 *
 * @see JobWakeupService
 * @see Poller
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

  /**
   * Registers this listener with the ClusterCoordinator to receive wakeup notifications.
   *
   * <p>If registration fails, the scheduler continues using its normal adaptive polling.
   */
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
      log.debug("Received wakeup notification, waking poller");
      pollerScheduler.wakeup();
    } catch (Exception e) {
      log.warnf("Error processing wakeup notification: %s", e.getMessage());
    }
  }
}
