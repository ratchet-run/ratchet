package run.ratchet.ri.core.internal;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.transaction.Status;
import jakarta.transaction.Synchronization;
import jakarta.transaction.TransactionSynchronizationRegistry;
import java.time.Duration;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import org.jboss.logging.Logger;
import run.ratchet.api.JobPriority;
import run.ratchet.api.NodeIdentity;
import run.ratchet.ri.core.PollerScheduler;
import run.ratchet.spi.ClusterCoordinator;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.spi.NodeIdentityProvider;
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
  private final NodeIdentityProvider nodeIdentityProvider;

  private volatile TransactionSynchronizationRegistry txRegistry;
  // NodeIdentity is stable per NodeIdentityProvider contract; cache lazily so each publish path
  // doesn't re-validate the provider string against NodeIdentity's pattern.
  private volatile NodeIdentity cachedNodeIdentity;

  protected JobWakeupService() {
    this.clusterCoordinator = null;
    this.pollerSchedulerInstance = null;
    this.metricsCollector = null;
    this.nodeIdentityProvider = null;
  }

  @Inject
  public JobWakeupService(
      ClusterCoordinator clusterCoordinator,
      Instance<PollerScheduler> pollerSchedulerInstance,
      MetricsCollector metricsCollector,
      NodeIdentityProvider nodeIdentityProvider) {
    this.clusterCoordinator = clusterCoordinator;
    this.pollerSchedulerInstance = pollerSchedulerInstance;
    this.metricsCollector = metricsCollector;
    this.nodeIdentityProvider = nodeIdentityProvider;
  }

  JobWakeupService(
      ClusterCoordinator clusterCoordinator,
      Instance<PollerScheduler> pollerSchedulerInstance,
      MetricsCollector metricsCollector,
      NodeIdentityProvider nodeIdentityProvider,
      TransactionSynchronizationRegistry txRegistry) {
    this(clusterCoordinator, pollerSchedulerInstance, metricsCollector, nodeIdentityProvider);
    this.txRegistry = txRegistry;
  }

  public void notify(JobPriority priority, boolean immediate, String executionTarget) {
    if (immediate) {
      publishNotification(priority, executionTarget);
    }
  }

  public void notifyIfNeeded(
      JobExecutionType jobType, JobPriority priority, Duration delay, String executionTarget) {
    if (shouldNotify(jobType, priority, delay)) {
      publishNotification(priority, executionTarget);
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

  private void publishNotification(JobPriority priority, String executionTarget) {
    if (registerAfterCommit(() -> publishNotificationNow(priority, executionTarget))) {
      return;
    }
    publishNotificationNow(priority, executionTarget);
  }

  private boolean registerAfterCommit(Runnable action) {
    return registerAfterCommit(
        resolveTxRegistry(), action, log, "After-commit wakeup registration error; firing now: %s");
  }

  private TransactionSynchronizationRegistry resolveTxRegistry() {
    TransactionSynchronizationRegistry reg = txRegistry;
    if (reg == null) {
      synchronized (this) {
        reg = txRegistry;
        if (reg == null) {
          reg = lookupTxRegistry(log);
          txRegistry = reg;
        }
      }
    }
    return reg;
  }

  public static TransactionSynchronizationRegistry lookupTxRegistry(Logger log) {
    try {
      return InitialContext.doLookup("java:comp/TransactionSynchronizationRegistry");
    } catch (NamingException e) {
      log.debugf(
          "TransactionSynchronizationRegistry lookup unavailable; using immediate fallback: %s",
          e.getMessage());
      return null;
    }
  }

  public static boolean registerAfterCommit(
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
      log.warnf(e, failureMessage, e.getMessage());
      return false;
    }
  }

  private void publishNotificationNow(JobPriority priority, String executionTarget) {
    wakeupLocalPoller();
    try {
      clusterCoordinator.notifyNewWork(priority, resolveNodeIdentity(), executionTarget);
      log.debugf(
          "Published wakeup notification: priority=%s, target=%s", priority, executionTarget);
    } catch (Exception e) {
      log.warnf(e, "Wakeup notification error: %s", e.getMessage());
    }
  }

  private NodeIdentity resolveNodeIdentity() {
    NodeIdentity local = cachedNodeIdentity;
    if (local == null) {
      synchronized (this) {
        local = cachedNodeIdentity;
        if (local == null) {
          local = new NodeIdentity(nodeIdentityProvider.getNodeId());
          cachedNodeIdentity = local;
        }
      }
    }
    return local;
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
      log.warnf(e, "Local wakeup error: %s", e.getMessage());
    }
  }
}
