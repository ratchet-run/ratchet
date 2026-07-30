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
import java.util.ArrayList;
import java.util.List;
import org.jboss.logging.Logger;
import run.ratchet.api.RatchetOptions;
import run.ratchet.ri.cdi.EncryptionInstaller;
import run.ratchet.ri.cdi.PayloadMaskingPolicyInstaller;
import run.ratchet.ri.cdi.RatchetLifecycle;
import run.ratchet.ri.cdi.RatchetProducer;
import run.ratchet.ri.cdi.RatchetRuntimeStart;
import run.ratchet.ri.cdi.RecurringJobProcessor;
import run.ratchet.ri.core.DrainController;
import run.ratchet.ri.core.JobArchivingService;
import run.ratchet.ri.core.RecurringScheduler;
import run.ratchet.ri.core.internal.BatchRecoveryTimer;
import run.ratchet.ri.core.internal.DeadLetterService;
import run.ratchet.ri.core.internal.DefaultRatchetRuntime;
import run.ratchet.ri.core.internal.JobExecutionCoordinator;
import run.ratchet.ri.core.internal.LogPurgeTimer;
import run.ratchet.ri.core.internal.OrphanRecoveryTimer;
import run.ratchet.ri.core.internal.Poller;
import run.ratchet.ri.core.internal.PollerWakeupListener;
import run.ratchet.ri.core.internal.RecurringRegistration;
import run.ratchet.ri.core.internal.RuntimeInstallation;
import run.ratchet.ri.runtime.RatchetRuntime;
import run.ratchet.spi.ClusterCoordinator;
import run.ratchet.spi.ExecutorProvider;
import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.spi.PayloadSerializer;
import run.ratchet.spi.SchedulerLifecycleHook;

/**
 * CDI lifecycle observer that initializes and shuts down the Ratchet job scheduler subsystem.
 *
 * @apiNote Internal RI implementation. Startup and shutdown observe CDI events automatically;
 *     applications must not reference this class directly. Public visibility is retained only to
 *     support a negative-control integration test that injects the lifecycle bean and reflectively
 *     invokes {@code onShutdown()} to verify {@code @Dependent} hook destruction. Not part of the
 *     supported API surface.
 */
@ApplicationScoped
public class DefaultRatchetLifecycle implements RatchetLifecycle {

  private static final Logger log = Logger.getLogger(DefaultRatchetLifecycle.class);

  private final Poller poller;
  private final RecurringScheduler recurringScheduler;
  private final OrphanRecoveryTimer orphanRecoveryTimer;
  private final BatchRecoveryTimer batchRecoveryTimer;
  private final DeadLetterService deadLetterService;
  private final JobArchivingService jobArchivingService;
  private final LogPurgeTimer logPurgeTimer;
  private final PollerWakeupListener pollerWakeupListener;
  private final ExecutorProvider executorProvider;
  private final NodeIdentityProvider nodeIdentityProvider;
  private final DrainController drainController;
  private final RatchetOptions options;
  private final JobExecutionCoordinator jobExecutionCoordinator;
  private final ClusterCoordinator clusterCoordinator;
  private final Instance<SchedulerLifecycleHook> lifecycleHooks;
  private final EncryptionInstaller encryptionInstaller;
  private final RatchetProducer ratchetProducer;
  private final PayloadMaskingPolicyInstaller payloadMaskingPolicyInstaller;
  private final RecurringJobProcessor recurringJobProcessor;
  private final Instance<PayloadSerializer> payloadSerializers;
  private volatile List<SchedulerLifecycleHook> resolvedHooks;
  private volatile RatchetRuntime runtime;
  private volatile boolean shutdownComplete;

  protected DefaultRatchetLifecycle() {
    this.poller = null;
    this.recurringScheduler = null;
    this.orphanRecoveryTimer = null;
    this.batchRecoveryTimer = null;
    this.deadLetterService = null;
    this.jobArchivingService = null;
    this.logPurgeTimer = null;
    this.pollerWakeupListener = null;
    this.executorProvider = null;
    this.nodeIdentityProvider = null;
    this.drainController = null;
    this.options = null;
    this.jobExecutionCoordinator = null;
    this.clusterCoordinator = null;
    this.lifecycleHooks = null;
    this.encryptionInstaller = null;
    this.ratchetProducer = null;
    this.payloadMaskingPolicyInstaller = null;
    this.recurringJobProcessor = null;
    this.payloadSerializers = null;
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
        null,
        null,
        null,
        null,
        null,
        null);
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
      ClusterCoordinator clusterCoordinator,
      Instance<SchedulerLifecycleHook> lifecycleHooks) {
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
        lifecycleHooks,
        null,
        null,
        null,
        null,
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
      Instance<SchedulerLifecycleHook> lifecycleHooks,
      EncryptionInstaller encryptionInstaller,
      RatchetProducer ratchetProducer,
      PayloadMaskingPolicyInstaller payloadMaskingPolicyInstaller,
      RecurringJobProcessor recurringJobProcessor,
      Instance<PayloadSerializer> payloadSerializers) {
    this.poller = poller;
    this.recurringScheduler = recurringScheduler;
    this.orphanRecoveryTimer = orphanRecoveryTimer;
    this.batchRecoveryTimer = batchRecoveryTimer;
    this.deadLetterService = deadLetterService;
    this.jobArchivingService = jobArchivingService;
    this.logPurgeTimer = logPurgeTimer;
    this.pollerWakeupListener = pollerWakeupListener;
    this.executorProvider = executorProvider;
    this.nodeIdentityProvider = nodeIdentityProvider;
    this.drainController = drainController;
    this.options = options;
    this.jobExecutionCoordinator = jobExecutionCoordinator;
    this.clusterCoordinator = clusterCoordinator;
    this.lifecycleHooks = lifecycleHooks;
    this.encryptionInstaller = encryptionInstaller;
    this.ratchetProducer = ratchetProducer;
    this.payloadMaskingPolicyInstaller = payloadMaskingPolicyInstaller;
    this.recurringJobProcessor = recurringJobProcessor;
    this.payloadSerializers = payloadSerializers;
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
    start(recurringRegistrationForApplicationStart());
  }

  void onRuntimeStart(
      @Observes @Priority(RatchetRuntimeStart.PRIORITY_LIFECYCLE_START) RatchetRuntimeStart event) {
    start(recurringRegistrationForRuntimeStart());
  }

  public void start() {
    start(recurringRegistrationForApplicationStart());
  }

  private void start(RecurringRegistration recurringRegistration) {
    RatchetRuntime delegate;
    synchronized (this) {
      delegate = runtime;
      if (delegate == null) {
        delegate = createRuntime(recurringRegistration);
        runtime = delegate;
      }
    }
    delegate.start();
  }

  @Override
  @PreDestroy
  public void onShutdown() {
    RatchetRuntime delegate;
    synchronized (this) {
      if (shutdownComplete) {
        return;
      }
      shutdownComplete = true;
      delegate = runtime;
    }

    try {
      if (delegate != null) {
        delegate.stop();
      }
    } finally {
      destroyHooks();
    }
  }

  private synchronized List<SchedulerLifecycleHook> hooks() {
    if (lifecycleHooks == null) {
      return List.of();
    }
    if (resolvedHooks == null) {
      List<SchedulerLifecycleHook> resolved = new ArrayList<>();
      lifecycleHooks.forEach(resolved::add);
      resolvedHooks = List.copyOf(resolved);
    }
    return resolvedHooks;
  }

  private RatchetRuntime createRuntime(RecurringRegistration recurringRegistration) {
    List<RuntimeInstallation> installations = new ArrayList<>(3);
    if (encryptionInstaller != null) {
      installations.add(encryptionInstaller.runtimeInstallation());
    }
    if (ratchetProducer != null && payloadSerializers != null) {
      installations.add(ratchetProducer.payloadSerializerInstallation(payloadSerializers));
    }
    if (payloadMaskingPolicyInstaller != null) {
      installations.add(payloadMaskingPolicyInstaller.runtimeInstallation());
    }

    return new DefaultRatchetRuntime(
        poller,
        recurringScheduler,
        orphanRecoveryTimer,
        batchRecoveryTimer,
        deadLetterService,
        jobArchivingService,
        logPurgeTimer,
        jobExecutionCoordinator,
        pollerWakeupListener,
        drainController,
        clusterCoordinator,
        nodeIdentityProvider,
        options,
        hooks(),
        executorProvider == null ? null : executorProvider::getScheduledExecutor,
        recurringRegistration,
        installations);
  }

  private RecurringRegistration recurringRegistrationForApplicationStart() {
    if (recurringJobProcessor == null) {
      return () -> {};
    }
    return recurringRegistration(
        recurringJobProcessor::registerFromApplicationStart,
        recurringJobProcessor::cancelRegistration);
  }

  private RecurringRegistration recurringRegistrationForRuntimeStart() {
    if (recurringJobProcessor == null) {
      return () -> {};
    }
    return recurringRegistration(
        recurringJobProcessor::registerFromRuntimeStart, recurringJobProcessor::cancelRegistration);
  }

  private static RecurringRegistration recurringRegistration(
      Runnable registration, Runnable cancellation) {
    return new RecurringRegistration() {
      @Override
      public void register() {
        registration.run();
      }

      @Override
      public void cancel() {
        cancellation.run();
      }
    };
  }

  /**
   * Releases each resolved lifecycle hook back to the CDI container so that {@code @Dependent}
   * scoped hooks have their {@code @PreDestroy} methods invoked. For non-dependent scopes, {@link
   * Instance#destroy(Object)} is effectively a no-op (the container manages the lifecycle).
   */
  private void destroyHooks() {
    if (lifecycleHooks == null || resolvedHooks == null) {
      return;
    }
    for (SchedulerLifecycleHook hook : resolvedHooks) {
      try {
        lifecycleHooks.destroy(hook);
      } catch (Exception e) {
        log.warnf(e, "Failed to destroy scheduler lifecycle hook: %s", e.getMessage());
      }
    }
  }
}
