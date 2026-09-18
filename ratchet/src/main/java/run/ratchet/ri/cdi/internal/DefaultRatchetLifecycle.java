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
package run.ratchet.ri.cdi.internal;

import jakarta.annotation.PreDestroy;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.Initialized;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import run.ratchet.api.RatchetOptions;
import run.ratchet.ri.cdi.CdiRuntimeContextInstallation;
import run.ratchet.ri.cdi.RatchetLifecycle;
import run.ratchet.ri.cdi.RatchetRuntimeStart;
import run.ratchet.ri.core.DrainController;
import run.ratchet.ri.core.JobArchivingService;
import run.ratchet.ri.core.JobExecutorService;
import run.ratchet.ri.core.RatchetRuntime;
import run.ratchet.ri.core.RecurringScheduler;
import run.ratchet.ri.core.internal.BatchRecoveryTimer;
import run.ratchet.ri.core.internal.DeadLetterService;
import run.ratchet.ri.core.internal.JobExecutionCoordinator;
import run.ratchet.ri.core.internal.LogPurgeTimer;
import run.ratchet.ri.core.internal.OrphanRecoveryTimer;
import run.ratchet.ri.core.internal.Poller;
import run.ratchet.ri.core.internal.PollerWakeupListener;
import run.ratchet.spi.ClusterCoordinator;
import run.ratchet.spi.ExecutorProvider;
import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.spi.SchedulerLifecycleHook;

/** CDI lifecycle adapter for the shared scheduler runtime. */
@ApplicationScoped
public class DefaultRatchetLifecycle implements RatchetLifecycle {
  private static final Logger log = Logger.getLogger(DefaultRatchetLifecycle.class);
  private final RatchetRuntime runtime;
  @Inject CdiRuntimeContextInstallation runtimeInstallation;
  @Inject JobExecutorService jobExecutor;

  protected DefaultRatchetLifecycle() {
    this.runtime = null;
  }

  public DefaultRatchetLifecycle(
      Poller poller,
      RecurringScheduler recurringScheduler,
      OrphanRecoveryTimer orphanRecoveryTimer,
      BatchRecoveryTimer batchRecoveryTimer,
      DeadLetterService deadLetterService,
      JobArchivingService jobArchivingService,
      LogPurgeTimer logPurgeTimer,
      PollerWakeupListener pollerWakeupListener,
      ExecutorProvider executorProvider,
      NodeIdentityProvider nodeIdentityProvider,
      DrainController drainController,
      RatchetOptions options,
      JobExecutionCoordinator jobExecutionCoordinator,
      ClusterCoordinator clusterCoordinator) {
    this(
        poller,
        recurringScheduler,
        orphanRecoveryTimer,
        batchRecoveryTimer,
        deadLetterService,
        jobArchivingService,
        logPurgeTimer,
        pollerWakeupListener,
        executorProvider,
        nodeIdentityProvider,
        drainController,
        options,
        jobExecutionCoordinator,
        clusterCoordinator,
        null);
  }

  @Inject
  public DefaultRatchetLifecycle(
      Poller poller,
      RecurringScheduler recurringScheduler,
      OrphanRecoveryTimer orphanRecoveryTimer,
      BatchRecoveryTimer batchRecoveryTimer,
      DeadLetterService deadLetterService,
      JobArchivingService jobArchivingService,
      LogPurgeTimer logPurgeTimer,
      PollerWakeupListener pollerWakeupListener,
      ExecutorProvider executorProvider,
      NodeIdentityProvider nodeIdentityProvider,
      DrainController drainController,
      RatchetOptions options,
      JobExecutionCoordinator jobExecutionCoordinator,
      ClusterCoordinator clusterCoordinator,
      Instance<SchedulerLifecycleHook> lifecycleHooks) {
    this.runtime =
        new RatchetRuntime(
            poller,
            recurringScheduler,
            orphanRecoveryTimer,
            batchRecoveryTimer,
            deadLetterService,
            jobArchivingService,
            logPurgeTimer,
            pollerWakeupListener,
            executorProvider,
            nodeIdentityProvider,
            drainController,
            options,
            jobExecutionCoordinator,
            clusterCoordinator,
            lifecycleHooks == null ? null : () -> lifecycleHooks.stream().toList(),
            hook -> {
              if (lifecycleHooks != null) lifecycleHooks.destroy(hook);
            },
            () -> {});
  }

  void onStartup(
      @Observes
          @Priority(RatchetRuntimeStart.PRIORITY_LIFECYCLE_START)
          @Initialized(ApplicationScoped.class) Object init) {
    // Build-time-CDI runtimes (e.g. Quarkus/ArC) fire @Initialized(ApplicationScoped.class) during
    // STATIC_INIT, before the JPA persistence unit exists. They set
    // -Dratchet.lifecycle.defer-auto-start=true and drive start() from a later, post-persistence
    // event (RatchetRuntimeStart) instead.
    if (RatchetRuntimeStart.logIfDeferred(
        log,
        "Ratchet start deferred pending RatchetRuntimeStart event; if this runtime never fires"
            + " that event, the engine will never start")) {
      return;
    }
    start();
  }

  void onRuntimeStart(
      @Observes @Priority(RatchetRuntimeStart.PRIORITY_LIFECYCLE_START) RatchetRuntimeStart event) {
    start();
  }

  public void start() {
    if (runtimeInstallation != null) runtimeInstallation.bindRuntime(this::onShutdown, jobExecutor);
    runtime.start();
  }

  @Override
  @PreDestroy
  public void onShutdown() {
    runtime.onShutdown();
  }
}
