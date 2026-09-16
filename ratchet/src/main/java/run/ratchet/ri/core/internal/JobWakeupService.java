/*
 * Copyright 2026 Ratchet Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package run.ratchet.ri.core.internal;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.transaction.Status;
import jakarta.transaction.Synchronization;
import jakarta.transaction.TransactionSynchronizationRegistry;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
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
  private static final String AFTER_COMMIT_ACTIONS_KEY =
      JobWakeupService.class.getName() + ".afterCommitActions";

  private static final Logger log = Logger.getLogger(JobWakeupService.class);

  /**
   * Outcome of attempting to defer an action until the current transaction commits.
   *
   * <p>Callers may run the action immediately only for {@link #NO_ACTIVE_TRANSACTION}. Both {@link
   * #REGISTERED} and {@link #ACTIVE_TRANSACTION_REGISTRATION_FAILED} require the caller to suppress
   * immediate execution: the former already owns the action, while the latter cannot safely know
   * whether the transaction will commit.
   */
  public enum AfterCommitRegistrationResult {
    /** No transaction registry is available, or the registry reports no current transaction. */
    NO_ACTIVE_TRANSACTION,

    /** The action was registered and will run only if the transaction commits. */
    REGISTERED,

    /** A transaction exists, but its state prevented registration or registration itself failed. */
    ACTIVE_TRANSACTION_REGISTRATION_FAILED
  }

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
    AfterCommitRegistrationResult result =
        registerAfterCommit(() -> publishNotificationNow(priority, executionTarget));
    if (result == AfterCommitRegistrationResult.NO_ACTIVE_TRANSACTION) {
      publishNotificationNow(priority, executionTarget);
    }
  }

  private AfterCommitRegistrationResult registerAfterCommit(Runnable action) {
    return registerAfterCommit(
        resolveTxRegistry(),
        action,
        log,
        "After-commit wakeup registration failed; wakeup suppressed: %s");
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
    return lookupTxRegistry(
        log,
        () -> {
          var registry =
              jakarta
                  .enterprise
                  .inject
                  .spi
                  .CDI
                  .current()
                  .select(TransactionSynchronizationRegistry.class);
          return registry.isResolvable() ? registry.get() : null;
        });
  }

  static TransactionSynchronizationRegistry lookupTxRegistry(
      Logger log, Supplier<TransactionSynchronizationRegistry> cdiLookup) {
    try {
      return InitialContext.doLookup("java:comp/TransactionSynchronizationRegistry");
    } catch (NamingException unavailable) {
      // Standalone CDI runtimes such as Quarkus expose the registry as a bean without JNDI.
      // Treating that as no transaction would publish events and run followups before commit.
      try {
        return cdiLookup.get();
      } catch (IllegalStateException unavailableCdi) {
        log.debugf(
            "TransactionSynchronizationRegistry unavailable through JNDI and CDI: %s",
            unavailableCdi.getMessage());
        return null;
      }
    }
  }

  /**
   * Registers an action for post-commit execution without allowing an uncertain transaction state
   * to leak the action early.
   *
   * <p>A missing registry and {@link Status#STATUS_NO_TRANSACTION} produce {@link
   * AfterCommitRegistrationResult#NO_ACTIVE_TRANSACTION}, allowing the caller to execute the action
   * immediately. An active transaction produces {@link AfterCommitRegistrationResult#REGISTERED}
   * when synchronization registration succeeds. Any other transaction state, status lookup failure,
   * or synchronization registration failure produces {@link
   * AfterCommitRegistrationResult#ACTIVE_TRANSACTION_REGISTRATION_FAILED}; callers must log and
   * suppress immediate execution because the transaction outcome is unknown.
   */
  public static AfterCommitRegistrationResult registerAfterCommit(
      TransactionSynchronizationRegistry txRegistry,
      Runnable action,
      Logger log,
      String failureMessage) {
    if (txRegistry == null) {
      return AfterCommitRegistrationResult.NO_ACTIVE_TRANSACTION;
    }

    try {
      int transactionStatus = txRegistry.getTransactionStatus();
      if (transactionStatus == Status.STATUS_NO_TRANSACTION) {
        return AfterCommitRegistrationResult.NO_ACTIVE_TRANSACTION;
      }
      if (transactionStatus != Status.STATUS_ACTIVE) {
        log.warnf(
            failureMessage,
            "transaction status " + transactionStatus + " does not allow registration");
        return AfterCommitRegistrationResult.ACTIVE_TRANSACTION_REGISTRATION_FAILED;
      }
      // JTA does not promise FIFO delivery between separate synchronizations. Keep all Ratchet
      // actions in one transaction-scoped queue so terminal events precede dependent events and
      // parent followups. The registry discards this resource with the transaction.
      synchronized (txRegistry) {
        @SuppressWarnings("unchecked")
        List<Runnable> existing = (List<Runnable>) txRegistry.getResource(AFTER_COMMIT_ACTIONS_KEY);
        List<Runnable> actions = existing;
        if (actions == null) {
          actions = new ArrayList<>();
          List<Runnable> registeredActions = actions;
          txRegistry.registerInterposedSynchronization(
              new Synchronization() {
                @Override
                public void beforeCompletion() {}

                @Override
                public void afterCompletion(int status) {
                  List<Runnable> ready;
                  synchronized (txRegistry) {
                    ready =
                        status == Status.STATUS_COMMITTED
                            ? List.copyOf(registeredActions)
                            : List.of();
                    registeredActions.clear();
                  }
                  for (Runnable callback : ready) {
                    try {
                      callback.run();
                    } catch (RuntimeException failure) {
                      log.warn("After-commit action failed; continuing remaining actions", failure);
                    }
                  }
                }
              });
          // If resource storage fails, the registered callback retains an empty queue and cannot
          // execute an action whose registration was reported as failed.
          txRegistry.putResource(AFTER_COMMIT_ACTIONS_KEY, actions);
        }
        actions.add(action);
      }
      return AfterCommitRegistrationResult.REGISTERED;
    } catch (Exception e) {
      log.warnf(e, failureMessage, e.getMessage());
      return AfterCommitRegistrationResult.ACTIVE_TRANSACTION_REGISTRATION_FAILED;
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
