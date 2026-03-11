package run.ratchet.ri.core;

import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;
import run.ratchet.spi.ClusterCoordinator;
import run.ratchet.spi.NodeIdentityProvider;
import jakarta.annotation.Resource;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Status;
import jakarta.transaction.Synchronization;
import jakarta.transaction.TransactionSynchronizationRegistry;
import java.time.Duration;
import java.util.logging.Logger;

/**
 * Service responsible for publishing job wakeup notifications across the cluster.
 *
 * <p>When jobs are created that require immediate processing (user-triggered actions, CRITICAL
 * priority jobs), this service publishes a notification via the {@link ClusterCoordinator}. All
 * cluster nodes receive the notification and wake their pollers to check for available work.
 *
 * <p>Notification Strategy:
 *
 * <ul>
 *   <li>User-triggered SINGLE jobs with no delay: Always notify
 *   <li>BATCH_PARENT jobs: Always notify (user initiated batch)
 *   <li>CRITICAL priority jobs: Always notify regardless of type
 *   <li>RECURRING, BATCH_CHILD, CHAIN_STEP, WORKFLOW_*: Do not notify
 * </ul>
 *
 * @see JobWakeupNotification
 * @see PollerWakeupListener
 */
@ApplicationScoped
public class JobWakeupService {

  private static final Logger log = Logger.getLogger(JobWakeupService.class.getName());

  private final ClusterCoordinator clusterCoordinator;
  private final NodeIdentityProvider nodeIdProvider;
  private final boolean enabled = true;

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

  /**
   * Publishes a wakeup notification with explicit immediate flag.
   *
   * @param priority the job priority
   * @param immediate true to mark as requiring immediate pickup
   */
  public void notify(JobPriority priority, boolean immediate) {
    if (!enabled) {
      return;
    }

    if (immediate) {
      publishNotification(priority);
    }
  }

  /**
   * Publishes a wakeup notification if the job requires immediate processing.
   *
   * @param jobType the type of job being created
   * @param priority the job priority
   * @param delay the scheduled delay (zero means immediate execution)
   */
  public void notifyIfNeeded(JobType jobType, JobPriority priority, Duration delay) {
    if (!enabled) {
      return;
    }

    if (shouldNotify(jobType, priority, delay)) {
      publishNotification(priority);
    }
  }

  /** Initializes the service. Logs initialization state for operational visibility. */
  public void init() {
    log.info("JobWakeupService initialized with ClusterCoordinator");
  }

  boolean shouldNotify(JobType jobType, JobPriority priority, Duration delay) {
    if (priority == JobPriority.CRITICAL) {
      return true;
    }

    if (jobType == JobType.SINGLE && (delay == null || delay.isZero())) {
      return true;
    }

    return jobType == JobType.BATCH_PARENT;
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
      log.warning(
          "Failed to register after-commit wakeup publication; publishing immediately: "
              + e.getMessage());
      return false;
    }
  }

  private void publishNotificationNow(JobPriority priority) {
    try {
      clusterCoordinator.notifyNewWork(priority);
      log.fine("Published wakeup notification: priority=" + priority);
    } catch (Exception e) {
      log.warning("Failed to publish wakeup notification: " + e.getMessage());
    }
  }
}
