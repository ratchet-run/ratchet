package run.ratchet.ri.core;

import run.ratchet.api.JobPriority;
import run.ratchet.spi.ClusterCoordinator;
import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.store.entity.JobExecutionType;
import jakarta.annotation.Resource;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Status;
import jakarta.transaction.Synchronization;
import jakarta.transaction.TransactionSynchronizationRegistry;
import java.time.Duration;
import org.jboss.logging.Logger;

/**
 * Publishes cluster-wide wakeup notifications when jobs requiring immediate processing are created.
 *
 * @see JobWakeupNotification
 * @see PollerWakeupListener
 */
@ApplicationScoped
public class JobWakeupService {

  private static final Logger log = Logger.getLogger(JobWakeupService.class);

  private final ClusterCoordinator clusterCoordinator;
  private final NodeIdentityProvider nodeIdProvider;

  @Resource private TransactionSynchronizationRegistry txRegistry;

  // Required by CDI proxy
  protected JobWakeupService() {
    this.clusterCoordinator = null;
    this.nodeIdProvider = null;
  }

  @Inject
  public JobWakeupService(
      ClusterCoordinator clusterCoordinator, NodeIdentityProvider nodeIdProvider) {
    this.clusterCoordinator = clusterCoordinator;
    this.nodeIdProvider = nodeIdProvider;
  }

  public void notify(JobPriority priority, boolean immediate) {
    if (immediate) {
      publishNotification(priority);
    }
  }

  public void notifyIfNeeded(JobExecutionType jobType, JobPriority priority, Duration delay) {
    if (shouldNotify(jobType, priority, delay)) {
      publishNotification(priority);
    }
  }

  /** Initializes the service. */
  public void init() {
    // no-op, wakeup listener handles registration
  }

  boolean shouldNotify(JobExecutionType jobType, JobPriority priority, Duration delay) {
    if (priority == JobPriority.CRITICAL) {
      return true;
    }

    if (jobType == JobExecutionType.SINGLE && (delay == null || delay.isZero())) {
      return true;
    }

    return jobType == JobExecutionType.BATCH_PARENT;
  }

  private void publishNotification(JobPriority priority) {
    if (registerAfterCommit(priority)) {
      return;
    }
    publishNotificationNow(priority);
  }

  private boolean registerAfterCommit(JobPriority priority) {
    if (txRegistry == null) {
      return false;
    }
    try {
      if (txRegistry.getTransactionStatus() != Status.STATUS_ACTIVE) {
        return false;
      }
      txRegistry.registerInterposedSynchronization(
          new Synchronization() {
            @Override
            public void beforeCompletion() {
              // no-op
            }

            @Override
            public void afterCompletion(int status) {
              if (status == Status.STATUS_COMMITTED) {
                publishNotificationNow(priority);
              }
            }
          });
      return true;
    } catch (Exception e) {
      log.warnf(
          "Failed to register after-commit wakeup publication; publishing immediately: %s",
          e.getMessage());
      return false;
    }
  }

  private void publishNotificationNow(JobPriority priority) {
    try {
      clusterCoordinator.notifyNewWork(priority);
      log.debugf("Published wakeup notification: priority=%s", priority);
    } catch (Exception e) {
      log.warnf("Failed to publish wakeup notification: %s", e.getMessage());
    }
  }
}
