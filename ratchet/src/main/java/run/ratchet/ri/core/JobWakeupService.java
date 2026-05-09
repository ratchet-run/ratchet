package run.ratchet.ri.core;

import jakarta.annotation.Resource;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.transaction.Status;
import jakarta.transaction.Synchronization;
import jakarta.transaction.TransactionSynchronizationRegistry;
import java.time.Duration;
import org.jboss.logging.Logger;
import run.ratchet.api.JobPriority;
import run.ratchet.spi.ClusterCoordinator;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.store.entity.JobExecutionType;

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
  private final Instance<PollerScheduler> pollerSchedulerInstance;
  private final MetricsCollector metricsCollector;

  @Resource private TransactionSynchronizationRegistry txRegistry;

  protected JobWakeupService() {
    this.clusterCoordinator = null;
    this.pollerSchedulerInstance = null;
    this.metricsCollector = null;
  }

  @Inject
  public JobWakeupService(
      ClusterCoordinator clusterCoordinator,
      Instance<PollerScheduler> pollerSchedulerInstance,
      MetricsCollector metricsCollector) {
    this.clusterCoordinator = clusterCoordinator;
    this.pollerSchedulerInstance = pollerSchedulerInstance;
    this.metricsCollector = metricsCollector;
  }

  JobWakeupService(
      ClusterCoordinator clusterCoordinator,
      Instance<PollerScheduler> pollerSchedulerInstance,
      MetricsCollector metricsCollector,
      TransactionSynchronizationRegistry txRegistry) {
    this(clusterCoordinator, pollerSchedulerInstance, metricsCollector);
    this.txRegistry = txRegistry;
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
    if (registerAfterCommit(() -> publishNotificationNow(priority))) {
      return;
    }
    publishNotificationNow(priority);
  }

  private boolean registerAfterCommit(Runnable action) {
    return registerAfterCommit(
        txRegistry, action, log, "After-commit wakeup registration error; firing now: %s");
  }

  static boolean registerAfterCommit(
      TransactionSynchronizationRegistry txRegistry,
      Runnable action,
      Logger log,
      String failureMessage) {
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
                action.run();
              }
            }
          });
      return true;
    } catch (Exception e) {
      log.warnf(failureMessage, e.getMessage());
      return false;
    }
  }

  private void publishNotificationNow(JobPriority priority) {
    wakeupLocalPoller();
    try {
      clusterCoordinator.notifyNewWork(priority);
      log.debugf("Published wakeup notification: priority=%s", priority);
    } catch (Exception e) {
      log.warnf("Wakeup notification error: %s", e.getMessage());
    }
  }

  private void wakeupLocalPoller() {
    if (pollerSchedulerInstance == null || !pollerSchedulerInstance.isResolvable()) {
      return;
    }

    try {
      if (metricsCollector != null) {
        metricsCollector.localWakeup("job_submit");
      }
      pollerSchedulerInstance.get().wakeup();
    } catch (Exception e) {
      log.warnf("Local wakeup error: %s", e.getMessage());
    }
  }
}
