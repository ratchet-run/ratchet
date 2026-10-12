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
package run.ratchet.micronaut;

import static run.ratchet.micronaut.RatchetAutoConfiguration.virtualThreadsEnabled;

import io.micronaut.context.BeanContext;
import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.annotation.Secondary;
import io.micronaut.context.env.Environment;
import io.micronaut.context.event.ApplicationEventPublisher;
import jakarta.inject.Singleton;
import jakarta.transaction.Transactional;
import java.time.Clock;
import java.util.Objects;
import run.ratchet.api.RatchetOptions;
import run.ratchet.micronaut.internal.RatchetTransactionBoundary;
import run.ratchet.micronaut.internal.tx.MicronautDefaultJobArchivingService;
import run.ratchet.micronaut.internal.tx.MicronautDefaultRecurringAnnotationMaintenanceService;
import run.ratchet.ri.cdi.MicronautStandaloneExecutorProvider;
import run.ratchet.ri.cdi.RecurringMethodInvoker;
import run.ratchet.ri.core.BatchService;
import run.ratchet.ri.core.DrainController;
import run.ratchet.ri.core.JobArchivingService;
import run.ratchet.ri.core.JobExecutorService;
import run.ratchet.ri.core.JobStateManager;
import run.ratchet.ri.core.JobSubmissionService;
import run.ratchet.ri.core.RatchetRuntime;
import run.ratchet.ri.core.RecurringScheduler;
import run.ratchet.ri.core.ResourcePermitService;
import run.ratchet.ri.core.RetryBufferDrainer;
import run.ratchet.ri.core.RetryBufferManager;
import run.ratchet.ri.core.StoreBackedStartupCoordinator;
import run.ratchet.ri.core.internal.BatchRecoveryTimer;
import run.ratchet.ri.core.internal.DeadLetterService;
import run.ratchet.ri.core.internal.DefaultDrainController;
import run.ratchet.ri.core.internal.DefaultNodeIdentityProvider;
import run.ratchet.ri.core.internal.DynamicHeartbeatCalculator;
import run.ratchet.ri.core.internal.ExecutionObserver;
import run.ratchet.ri.core.internal.InternalEventPublisher;
import run.ratchet.ri.core.internal.JobExecutionCoordinator;
import run.ratchet.ri.core.internal.JobPayloadInvoker;
import run.ratchet.ri.core.internal.LifecycleCallbackInvoker;
import run.ratchet.ri.core.internal.LogPurgeTimer;
import run.ratchet.ri.core.internal.OrphanRecoveryTimer;
import run.ratchet.ri.core.internal.Poller;
import run.ratchet.ri.core.internal.PollerWakeupListener;
import run.ratchet.ri.core.internal.PoolRegistry;
import run.ratchet.ri.core.internal.PostExecutionHandler;
import run.ratchet.ri.core.internal.PreExecutionValidator;
import run.ratchet.ri.core.internal.RecurringAnnotationMaintenanceService;
import run.ratchet.ri.core.internal.RecurringRegistrationState;
import run.ratchet.ri.core.internal.SingletonLeaseService;
import run.ratchet.spi.ClusterCoordinator;
import run.ratchet.spi.ExecutionTuningProvider;
import run.ratchet.spi.ExecutorProvider;
import run.ratchet.spi.InvocationSubmissionService;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.spi.PayloadSerializer;
import run.ratchet.spi.StartupCoordinator;
import run.ratchet.store.converter.RuntimeContextInstallation;
import run.ratchet.store.spi.ArchiveStore;
import run.ratchet.store.spi.JobAuditStore;
import run.ratchet.store.spi.JobStore;
import run.ratchet.store.spi.LockStore;

/** Internal runtime wiring imported by the engine auto-configuration. */
@Factory
@Requires(beans = JobStore.class)
@Requires(property = "ratchet.enabled", notEquals = "false")
class RatchetRuntimeConfiguration {
  @Singleton
  @Requires(missingBeans = RetryBufferManager.class)
  RetryBufferManager retryBufferManager(
      JobStateManager jobStateManager,
      PostExecutionHandler lifecycleFacade,
      JobStore store,
      PreExecutionValidator validationFacade,
      JobPayloadInvoker payloadInvoker,
      PayloadSerializer payloadSerializer,
      ExecutionObserver observabilityFacade,
      Clock clock) {
    return new RetryBufferManager(
        jobStateManager,
        lifecycleFacade,
        store,
        new LifecycleCallbackInvoker(
            validationFacade, payloadInvoker, payloadSerializer, observabilityFacade, clock));
  }

  @Singleton
  @Requires(missingBeans = RetryBufferDrainer.class)
  RetryBufferDrainer retryBufferDrainer(
      ExecutorProvider executorProvider,
      RetryBufferManager retryBufferManager,
      JobSubmissionService jobSubmissionService,
      PoolRegistry poolRegistry,
      DrainController drainController,
      RatchetOptions options) {
    return new RetryBufferDrainer(
        executorProvider,
        retryBufferManager,
        jobSubmissionService,
        poolRegistry,
        drainController,
        options);
  }

  @Singleton
  @Requires(missingBeans = StartupCoordinator.class)
  StartupCoordinator storeBackedStartupCoordinator(
      JobStore store, NodeIdentityProvider nodeIdentityProvider) {
    return new StoreBackedStartupCoordinator(
        store.capability(LockStore.class).orElse(null), nodeIdentityProvider);
  }

  @Singleton
  @Secondary
  @RatchetTransactionBoundary
  @Transactional
  MicronautDefaultJobArchivingService defaultJobArchivingService(
      JobStore store,
      SingletonLeaseService singletonLeaseService,
      ExecutorProvider executorProvider,
      Clock clock) {
    return new MicronautDefaultJobArchivingService(
        store.capability(ArchiveStore.class).orElse(null),
        singletonLeaseService,
        executorProvider,
        clock);
  }

  @Singleton
  @Requires(missingBeans = LogPurgeTimer.class)
  LogPurgeTimer logPurgeTimer(
      JobStore store,
      SingletonLeaseService singletonLeaseService,
      ExecutorProvider executorProvider,
      Clock clock) {
    return new LogPurgeTimer(
        store.capability(JobAuditStore.class).orElse(null),
        singletonLeaseService,
        executorProvider,
        clock);
  }

  @Singleton
  @Requires(missingBeans = RecurringRegistrationState.class)
  RecurringRegistrationState recurringRegistrationState(RatchetOptions options, Clock clock) {
    return new RecurringRegistrationState(options, clock);
  }

  @Singleton
  @Requires(missingBeans = SingletonLeaseService.class)
  SingletonLeaseService singletonLeaseService(
      JobStore store, NodeIdentityProvider nodeIdentityProvider) {
    return new SingletonLeaseService(
        store.capability(LockStore.class).orElse(null), nodeIdentityProvider);
  }

  @Singleton
  @Requires(missingBeans = DrainController.class)
  DrainController defaultDrainController() {
    return new DefaultDrainController();
  }

  @Singleton
  @Requires(missingBeans = BatchRecoveryTimer.class)
  BatchRecoveryTimer batchRecoveryTimer(
      RatchetOptions options,
      BatchService batchService,
      SingletonLeaseService singletonLeaseService) {
    return new BatchRecoveryTimer(batchService, singletonLeaseService, options);
  }

  @Singleton
  @Bean(preDestroy = "shutdown")
  @Requires(missingBeans = ExecutorProvider.class)
  // Keep the concrete product for shutdown; applications override its ExecutorProvider SPI.
  MicronautStandaloneExecutorProvider ratchetExecutorProvider() {
    return new MicronautStandaloneExecutorProvider();
  }

  @Singleton
  @Requires(missingBeans = InternalEventPublisher.class)
  InternalEventPublisher internalEventPublisher(ApplicationEventPublisher<Object> publisher) {
    InternalEventPublisher events = new InternalEventPublisher();
    events.addListener(publisher::publishEvent);
    return events;
  }

  @Singleton
  @Requires(missingBeans = PoolRegistry.class)
  PoolRegistry poolRegistry(
      RatchetOptions options,
      Environment environment,
      ExecutorProvider executorProvider,
      MetricsCollector metricsCollector,
      ExecutionTuningProvider tuning) {
    return PoolRegistry.create(
        options, executorProvider, metricsCollector, tuning, virtualThreadsEnabled(environment));
  }

  @Singleton
  @Requires(missingBeans = DynamicHeartbeatCalculator.class)
  DynamicHeartbeatCalculator dynamicHeartbeatCalculator(
      JobStore store, RatchetOptions options, Clock clock) {
    return new DynamicHeartbeatCalculator(
        store,
        options.node().heartbeatIntervalSeconds(),
        options.polling().minDelayMs(),
        options.polling().maxDelayMs(),
        clock);
  }

  @Singleton
  @Requires(missingBeans = NodeIdentityProvider.class)
  NodeIdentityProvider nodeIdentityProvider(
      JobStore store,
      DynamicHeartbeatCalculator heartbeat,
      ExecutorProvider executor,
      RatchetOptions options,
      Clock clock) {
    return new DefaultNodeIdentityProvider(
        store,
        store,
        heartbeat,
        executor,
        options.node().heartbeatIntervalSeconds(),
        options.node().orphanGraceSeconds(),
        options.node().dynamicHeartbeatEnabled(),
        options.node().explicitNodeId().orElse(null),
        clock);
  }

  @Singleton
  @Requires(missingBeans = OrphanRecoveryTimer.class)
  OrphanRecoveryTimer orphanRecoveryTimer(
      JobStore store,
      ResourcePermitService permits,
      SingletonLeaseService leases,
      RatchetOptions options,
      Clock clock) {
    return new OrphanRecoveryTimer(
        store,
        store,
        permits,
        leases,
        options.node().orphanGraceSeconds(),
        options.node().orphanRecoveryLeaseTtlSeconds(),
        clock);
  }

  @Singleton
  @Requires(missingBeans = RatchetRuntime.class)
  RatchetRuntime ratchetRuntime(
      Poller poller,
      RecurringScheduler recurring,
      OrphanRecoveryTimer orphanRecovery,
      BatchRecoveryTimer batchRecovery,
      DeadLetterService deadLetter,
      JobArchivingService archiving,
      LogPurgeTimer logPurge,
      PollerWakeupListener wakeup,
      ExecutorProvider executor,
      NodeIdentityProvider node,
      DrainController drain,
      RatchetOptions options,
      JobExecutionCoordinator coordinator,
      ClusterCoordinator cluster,
      MicronautRecurringDiscovery discovery,
      BeanContext beanFactory,
      RatchetProperties properties,
      RuntimeContextInstallation installation,
      JobExecutorService jobExecutor) {
    installation.releaseWhen(jobExecutor.onIdle(installation::releaseIfRequested));
    MicronautLifecycleHooks hooks = new MicronautLifecycleHooks(beanFactory);
    RatchetRuntime runtime =
        new RatchetRuntime(
            poller,
            recurring,
            orphanRecovery,
            batchRecovery,
            deadLetter,
            archiving,
            logPurge,
            wakeup,
            executor,
            node,
            drain,
            options,
            coordinator,
            cluster,
            hooks::acquire,
            hooks::release,
            () -> {
              Objects.requireNonNull(
                  executor.getScheduledExecutor(), "Ratchet scheduled executor must not be null");
              discovery.register();
            });
    runtime.setShutdownTimeout(properties.getShutdownTimeout());
    return runtime;
  }

  @Singleton
  @Secondary
  @RatchetTransactionBoundary
  MicronautDefaultRecurringAnnotationMaintenanceService recurringAnnotationMaintenanceService(
      JobStore store) {
    return new MicronautDefaultRecurringAnnotationMaintenanceService(store);
  }

  @Singleton
  @Requires(missingBeans = MicronautRecurringDiscovery.class)
  MicronautRecurringDiscovery micronautRecurringDiscovery(
      BeanContext factory,
      InvocationSubmissionService submissions,
      JobStore store,
      RecurringAnnotationMaintenanceService maintenance,
      RecurringMethodInvoker invoker,
      StartupCoordinator startup,
      RecurringRegistrationState registration,
      RatchetOptions options,
      Clock clock) {
    return new MicronautRecurringDiscovery(
        factory, submissions, store, maintenance, invoker, startup, registration, options, clock);
  }

  @Singleton
  @Requires(missingBeans = MicronautRatchetLifecycle.class)
  MicronautRatchetLifecycle micronautRatchetLifecycle(
      RatchetRuntime runtime, RuntimeContextInstallation installation) {
    return new MicronautRatchetLifecycle(runtime, installation);
  }
}
