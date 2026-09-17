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
package run.ratchet.spring.boot.autoconfigure;

import static run.ratchet.spring.boot.autoconfigure.RatchetAutoConfiguration.virtualThreadsEnabled;

import java.time.Clock;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import run.ratchet.api.ExecutorTargets;
import run.ratchet.api.RatchetOptions;
import run.ratchet.ri.cdi.RecurringMethodInvoker;
import run.ratchet.ri.cdi.StandaloneExecutorProvider;
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
import run.ratchet.ri.core.internal.DefaultJobArchivingService;
import run.ratchet.ri.core.internal.DefaultNodeIdentityProvider;
import run.ratchet.ri.core.internal.DefaultRecurringAnnotationMaintenanceService;
import run.ratchet.ri.core.internal.DynamicHeartbeatCalculator;
import run.ratchet.ri.core.internal.InternalEventPublisher;
import run.ratchet.ri.core.internal.JobExecutionCoordinator;
import run.ratchet.ri.core.internal.LogPurgeTimer;
import run.ratchet.ri.core.internal.OrphanRecoveryTimer;
import run.ratchet.ri.core.internal.Poller;
import run.ratchet.ri.core.internal.PollerWakeupListener;
import run.ratchet.ri.core.internal.PoolRegistry;
import run.ratchet.ri.core.internal.PostExecutionHandler;
import run.ratchet.ri.core.internal.RecurringAnnotationMaintenanceService;
import run.ratchet.ri.core.internal.RecurringRegistrationState;
import run.ratchet.ri.core.internal.SingletonLeaseService;
import run.ratchet.ri.core.internal.ThreadPoolManager;
import run.ratchet.spi.ClusterCoordinator;
import run.ratchet.spi.ExecutionTuningProvider;
import run.ratchet.spi.ExecutorProvider;
import run.ratchet.spi.InvocationSubmissionService;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.spi.StartupCoordinator;
import run.ratchet.store.converter.RuntimeContextInstallation;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.spi.ArchiveStore;
import run.ratchet.store.spi.JobAuditStore;
import run.ratchet.store.spi.JobStore;
import run.ratchet.store.spi.LockStore;

/** Internal runtime wiring imported by the engine auto-configuration. */
@Configuration(proxyBeanMethods = false)
class RatchetRuntimeConfiguration {
  @Bean(destroyMethod = "")
  @ConditionalOnMissingBean
  RetryBufferManager retryBufferManager(
      JobStateManager jobStateManager, PostExecutionHandler lifecycleFacade) {
    return new RetryBufferManager(jobStateManager, lifecycleFacade);
  }

  @Bean(destroyMethod = "")
  @ConditionalOnMissingBean
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

  @Bean(destroyMethod = "")
  @ConditionalOnMissingBean(StartupCoordinator.class)
  StoreBackedStartupCoordinator storeBackedStartupCoordinator(
      JobStore store, NodeIdentityProvider nodeIdentityProvider) {
    return new StoreBackedStartupCoordinator(
        store.capability(LockStore.class).orElse(null), nodeIdentityProvider);
  }

  @Bean(destroyMethod = "")
  @ConditionalOnMissingBean(JobArchivingService.class)
  DefaultJobArchivingService defaultJobArchivingService(
      JobStore store,
      SingletonLeaseService singletonLeaseService,
      ExecutorProvider executorProvider,
      Clock clock) {
    return new DefaultJobArchivingService(
        store.capability(ArchiveStore.class).orElse(null),
        singletonLeaseService,
        executorProvider,
        clock);
  }

  @Bean(destroyMethod = "")
  @ConditionalOnMissingBean
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

  @Bean(destroyMethod = "")
  @ConditionalOnMissingBean
  RecurringRegistrationState recurringRegistrationState(RatchetOptions options, Clock clock) {
    return new RecurringRegistrationState(options, clock);
  }

  @Bean(destroyMethod = "")
  @ConditionalOnMissingBean
  SingletonLeaseService singletonLeaseService(
      JobStore store, NodeIdentityProvider nodeIdentityProvider) {
    return new SingletonLeaseService(
        store.capability(LockStore.class).orElse(null), nodeIdentityProvider);
  }

  @Bean(destroyMethod = "")
  @ConditionalOnMissingBean(DrainController.class)
  DefaultDrainController defaultDrainController() {
    return new DefaultDrainController();
  }

  @Bean(destroyMethod = "")
  @ConditionalOnMissingBean
  BatchRecoveryTimer batchRecoveryTimer(
      BatchService batchService, SingletonLeaseService singletonLeaseService) {
    return new BatchRecoveryTimer(batchService, singletonLeaseService);
  }

  @Bean(destroyMethod = "shutdown")
  @ConditionalOnMissingBean(ExecutorProvider.class)
  StandaloneExecutorProvider ratchetExecutorProvider() {
    return new StandaloneExecutorProvider();
  }

  @Bean
  @ConditionalOnMissingBean
  InternalEventPublisher internalEventPublisher(ApplicationEventPublisher publisher) {
    InternalEventPublisher events = new InternalEventPublisher();
    events.addListener(publisher::publishEvent);
    return events;
  }

  @Bean
  @ConditionalOnMissingBean
  PoolRegistry poolRegistry(
      RatchetOptions options,
      Environment environment,
      ExecutorProvider executorProvider,
      MetricsCollector metricsCollector,
      ExecutionTuningProvider tuning) {
    Map<String, ThreadPoolManager> pools = new LinkedHashMap<>();
    Map<JobExecutionType, Integer> platform = new EnumMap<>(JobExecutionType.class);
    for (JobExecutionType type : JobExecutionType.values()) {
      int fallback =
          switch (type) {
            case SINGLE -> 20;
            case RECURRING -> 5;
            case BATCH_CHILD -> 30;
            case BATCH_PARENT -> 2;
            case CHAIN_STEP, WORKFLOW_BRANCH, WORKFLOW_JOIN -> 10;
          };
      platform.put(
          type,
          tuning.maxConcurrency(
              type.name(), options.execution().maxConcurrency(type.name(), fallback)));
    }
    pools.put(
        ExecutorTargets.PLATFORM,
        new ThreadPoolManager(
            ExecutorTargets.PLATFORM,
            executorProvider,
            metricsCollector,
            ThreadPoolManager.AccountingMode.SEMAPHORE,
            platform));
    if (virtualThreadsEnabled(environment)) {
      Map<JobExecutionType, Integer> virtual = new EnumMap<>(JobExecutionType.class);
      for (JobExecutionType type : JobExecutionType.values())
        virtual.put(type, tuning.virtualThreadLimit(type.name(), 1000));
      pools.put(
          ExecutorTargets.VIRTUAL,
          new ThreadPoolManager(
              ExecutorTargets.VIRTUAL,
              executorProvider,
              metricsCollector,
              options.execution().virtualCounterAccounting()
                  ? ThreadPoolManager.AccountingMode.COUNTER
                  : ThreadPoolManager.AccountingMode.SEMAPHORE,
              virtual));
    }
    return new PoolRegistry(pools);
  }

  @Bean
  @ConditionalOnMissingBean
  DynamicHeartbeatCalculator dynamicHeartbeatCalculator(
      JobStore store, RatchetOptions options, Clock clock) {
    return new DynamicHeartbeatCalculator(
        store,
        options.node().heartbeatIntervalSeconds(),
        options.polling().minDelayMs(),
        options.polling().maxDelayMs(),
        clock);
  }

  @Bean(destroyMethod = "")
  @ConditionalOnMissingBean(NodeIdentityProvider.class)
  DefaultNodeIdentityProvider nodeIdentityProvider(
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

  @Bean
  @ConditionalOnMissingBean
  OrphanRecoveryTimer orphanRecoveryTimer(
      JobStore store,
      ResourcePermitService permits,
      SingletonLeaseService leases,
      RatchetOptions options,
      Clock clock) {
    return new OrphanRecoveryTimer(
        store, store, permits, leases, options.node().orphanGraceSeconds(), clock);
  }

  @Bean(destroyMethod = "")
  @ConditionalOnMissingBean
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
      SpringRecurringDiscovery discovery,
      ConfigurableListableBeanFactory beanFactory,
      RatchetProperties properties,
      RuntimeContextInstallation installation,
      JobExecutorService jobExecutor) {
    installation.releaseWhen(jobExecutor.onIdle(installation::releaseIfRequested));
    SpringLifecycleHooks hooks = new SpringLifecycleHooks(beanFactory);
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

  @Bean
  @ConditionalOnMissingBean(RecurringAnnotationMaintenanceService.class)
  DefaultRecurringAnnotationMaintenanceService recurringAnnotationMaintenanceService(
      JobStore store) {
    return new DefaultRecurringAnnotationMaintenanceService(store);
  }

  @Bean
  SpringRecurringDiscovery springRecurringDiscovery(
      ConfigurableListableBeanFactory factory,
      InvocationSubmissionService submissions,
      JobStore store,
      RecurringAnnotationMaintenanceService maintenance,
      RecurringMethodInvoker invoker,
      StartupCoordinator startup,
      RecurringRegistrationState registration,
      RatchetOptions options,
      Clock clock) {
    return new SpringRecurringDiscovery(
        factory, submissions, store, maintenance, invoker, startup, registration, options, clock);
  }

  @Bean
  SpringRatchetLifecycle springRatchetLifecycle(
      RatchetRuntime runtime, RuntimeContextInstallation installation) {
    return new SpringRatchetLifecycle(runtime, installation);
  }
}
