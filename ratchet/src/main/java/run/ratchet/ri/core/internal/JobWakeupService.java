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
import java.time.Duration;
import java.util.Objects;
import java.util.function.Supplier;
import org.jboss.logging.Logger;
import run.ratchet.api.JobPriority;
import run.ratchet.api.NodeIdentity;
import run.ratchet.ri.core.PollerScheduler;
import run.ratchet.spi.AfterCommitRegistrar;
import run.ratchet.spi.AfterCommitRegistrar.Outcome;
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
  private final Supplier<PollerScheduler> pollerSchedulerSupplier;
  private final MetricsCollector metricsCollector;
  private final NodeIdentityProvider nodeIdentityProvider;
  private final AfterCommitRegistrar afterCommitRegistrar;

  // NodeIdentity is stable per NodeIdentityProvider contract; cache lazily so each publish path
  // doesn't re-validate the provider string against NodeIdentity's pattern.
  private volatile NodeIdentity cachedNodeIdentity;

  protected JobWakeupService() {
    this.clusterCoordinator = null;
    this.pollerSchedulerSupplier = null;
    this.metricsCollector = null;
    this.nodeIdentityProvider = null;
    this.afterCommitRegistrar = null;
  }

  @Inject
  public JobWakeupService(
      ClusterCoordinator clusterCoordinator,
      Instance<PollerScheduler> pollerSchedulerInstance,
      MetricsCollector metricsCollector,
      NodeIdentityProvider nodeIdentityProvider,
      AfterCommitRegistrar afterCommitRegistrar) {
    this(
        clusterCoordinator,
        () ->
            pollerSchedulerInstance != null && pollerSchedulerInstance.isResolvable()
                ? pollerSchedulerInstance.get()
                : null,
        metricsCollector,
        nodeIdentityProvider,
        afterCommitRegistrar);
  }

  /** Creates the service with portable, lazy poller-scheduler resolution. */
  public JobWakeupService(
      ClusterCoordinator clusterCoordinator,
      Supplier<PollerScheduler> pollerSchedulerSupplier,
      MetricsCollector metricsCollector,
      NodeIdentityProvider nodeIdentityProvider,
      AfterCommitRegistrar afterCommitRegistrar) {
    this.clusterCoordinator = clusterCoordinator;
    this.pollerSchedulerSupplier =
        Objects.requireNonNull(pollerSchedulerSupplier, "pollerSchedulerSupplier must not be null");
    this.metricsCollector = metricsCollector;
    this.nodeIdentityProvider = nodeIdentityProvider;
    this.afterCommitRegistrar = afterCommitRegistrar;
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
    Outcome result = registerAfterCommit(() -> publishNotificationNow(priority, executionTarget));
    if (result == Outcome.NO_ACTIVE_TRANSACTION) {
      publishNotificationNow(priority, executionTarget);
    }
  }

  private Outcome registerAfterCommit(Runnable action) {
    return afterCommitRegistrar.registerAfterCommit(
        action, "After-commit wakeup registration failed; wakeup suppressed: %s");
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
    if (pollerSchedulerSupplier == null) {
      return;
    }

    try {
      PollerScheduler pollerScheduler = pollerSchedulerSupplier.get();
      if (pollerScheduler == null) {
        return;
      }
      if (metricsCollector != null) {
        metricsCollector.localWakeup("job_submit");
      }
      pollerScheduler.wakeup();
    } catch (Exception e) {
      log.warnf(e, "Local wakeup error: %s", e.getMessage());
    }
  }
}
