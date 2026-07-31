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
package run.ratchet.ri.runtime;

import jakarta.enterprise.inject.Instance;
import jakarta.transaction.Transactional;
import java.time.Clock;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.api.JobSubmitter;
import run.ratchet.api.RatchetOptions;
import run.ratchet.ri.cdi.CdiBeanResolver;
import run.ratchet.ri.cdi.RecurringMethodInvoker;
import run.ratchet.ri.core.BatchRecoveryService;
import run.ratchet.ri.core.BatchService;
import run.ratchet.ri.core.BatchSubmitter;
import run.ratchet.ri.core.DefaultClusterQueryService;
import run.ratchet.ri.core.DefaultInvocationSubmissionService;
import run.ratchet.ri.core.DefaultJobCreationService;
import run.ratchet.ri.core.DefaultJobLoggerFactory;
import run.ratchet.ri.core.DefaultJobQueryService;
import run.ratchet.ri.core.DefaultJobSchedulerService;
import run.ratchet.ri.core.DefaultResultPersistenceStrategy;
import run.ratchet.ri.core.DrainController;
import run.ratchet.ri.core.JobArchivingService;
import run.ratchet.ri.core.JobCascadeService;
import run.ratchet.ri.core.JobExecutorService;
import run.ratchet.ri.core.JobStateManager;
import run.ratchet.ri.core.JobSubmissionService;
import run.ratchet.ri.core.JobTypeRateLimiter;
import run.ratchet.ri.core.PollerScheduler;
import run.ratchet.ri.core.RecurringJobExecutor;
import run.ratchet.ri.core.RecurringJobSubmitter;
import run.ratchet.ri.core.RecurringScheduler;
import run.ratchet.ri.core.ResourcePermitService;
import run.ratchet.ri.core.RetryBufferDrainer;
import run.ratchet.ri.core.RetryBufferManager;
import run.ratchet.ri.core.StoreBackedStartupCoordinator;
import run.ratchet.ri.core.StreamingBatchSubmitter;
import run.ratchet.ri.core.SubmissionFailureHandler;
import run.ratchet.ri.core.SubmissionGateChecker;
import run.ratchet.ri.core.WorkflowConditionEvaluator;
import run.ratchet.ri.core.internal.BatchRecoveryTimer;
import run.ratchet.ri.core.internal.ChainScheduler;
import run.ratchet.ri.core.internal.DeadLetterService;
import run.ratchet.ri.core.internal.DefaultDrainController;
import run.ratchet.ri.core.internal.DefaultJobArchivingService;
import run.ratchet.ri.core.internal.DefaultJobExecutorService;
import run.ratchet.ri.core.internal.DefaultNodeIdentityProvider;
import run.ratchet.ri.core.internal.DefaultPollerScheduler;
import run.ratchet.ri.core.internal.DefaultRatchetRuntime;
import run.ratchet.ri.core.internal.DefaultRecurringScheduler;
import run.ratchet.ri.core.internal.DefaultResourcePermitService;
import run.ratchet.ri.core.internal.DoNotRetryPolicy;
import run.ratchet.ri.core.internal.DynamicHeartbeatCalculator;
import run.ratchet.ri.core.internal.ExecutionObserver;
import run.ratchet.ri.core.internal.ExecutionTargetRouter;
import run.ratchet.ri.core.internal.InternalEventPublisher;
import run.ratchet.ri.core.internal.JakartaAfterCommitRegistrar;
import run.ratchet.ri.core.internal.JobExecutionCoordinator;
import run.ratchet.ri.core.internal.JobPayloadInvoker;
import run.ratchet.ri.core.internal.JobSuccessFinalizer;
import run.ratchet.ri.core.internal.JobTimeoutHandler;
import run.ratchet.ri.core.internal.JobWakeupService;
import run.ratchet.ri.core.internal.LogPurgeTimer;
import run.ratchet.ri.core.internal.OrphanRecoveryTimer;
import run.ratchet.ri.core.internal.Poller;
import run.ratchet.ri.core.internal.PollerCycleExecutor;
import run.ratchet.ri.core.internal.PollerWakeupListener;
import run.ratchet.ri.core.internal.PoolRegistry;
import run.ratchet.ri.core.internal.PostExecutionHandler;
import run.ratchet.ri.core.internal.PreExecutionValidator;
import run.ratchet.ri.core.internal.RecurringAnnotationMaintenanceService;
import run.ratchet.ri.core.internal.RecurringMethodRegistrar;
import run.ratchet.ri.core.internal.RecurringRegistration;
import run.ratchet.ri.core.internal.RecurringRegistrationState;
import run.ratchet.ri.core.internal.SingletonLeaseService;
import run.ratchet.ri.core.internal.WorkflowScheduler;
import run.ratchet.ri.security.CallerPrincipalProvider;
import run.ratchet.ri.security.JobPayloadInputValidator;
import run.ratchet.ri.security.JobSecurityValidator;
import run.ratchet.spi.AfterCommitRegistrar;
import run.ratchet.spi.BeanResolver;
import run.ratchet.spi.CircuitBreakerConfigProvider;
import run.ratchet.spi.CircuitBreakerManager;
import run.ratchet.spi.ClassPolicy;
import run.ratchet.spi.ClusterCoordinator;
import run.ratchet.spi.ErrorSanitizer;
import run.ratchet.spi.ExecutionTuningProvider;
import run.ratchet.spi.ExecutorProvider;
import run.ratchet.spi.JobAuthorizationPolicy;
import run.ratchet.spi.JobInvocationResolver;
import run.ratchet.spi.JobLoggerFactory;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.spi.NodeTagAffinityProvider;
import run.ratchet.spi.PayloadSerializer;
import run.ratchet.spi.PollingStrategyProvider;
import run.ratchet.spi.PreExecutionArgResolver;
import run.ratchet.spi.ResilienceStrategy;
import run.ratchet.spi.ResultPersistenceStrategy;
import run.ratchet.spi.RetryPolicy;
import run.ratchet.spi.StartupCoordinator;
import run.ratchet.spi.TracingCollector;
import run.ratchet.store.spi.ArchiveStore;
import run.ratchet.store.spi.BatchStore;
import run.ratchet.store.spi.JobAnalyticsStore;
import run.ratchet.store.spi.JobAuditStore;
import run.ratchet.store.spi.JobBatchStatusStore;
import run.ratchet.store.spi.JobBulkStore;
import run.ratchet.store.spi.JobClaimStore;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.JobExtensionStore;
import run.ratchet.store.spi.JobPauseStore;
import run.ratchet.store.spi.JobQueryStore;
import run.ratchet.store.spi.JobRetryStore;
import run.ratchet.store.spi.JobStore;
import run.ratchet.store.spi.JobTerminalStore;
import run.ratchet.store.spi.LockStore;
import run.ratchet.store.spi.NodeStore;
import run.ratchet.store.spi.RecurringJobStore;
import run.ratchet.store.spi.ResourcePermitStore;
import run.ratchet.store.spi.SignalStore;
import run.ratchet.store.spi.TagStore;
import run.ratchet.store.spi.WorkflowConditionStore;

/**
 * Catalog of the complete Ratchet runtime graph eligible for container-managed construction.
 *
 * <p>Each descriptor pins one constructor made only of portable bean references, option objects,
 * generic lists, or lazy suppliers. Optional store capabilities use nullable plain references;
 * containers must resolve an absent optional capability as {@code null}. The two Jakarta adapter
 * entries retain their container-specific constructors so non-CDI integrations can filter and
 * replace them with native adapters.
 */
public final class RatchetRuntimeComponentCatalog {

  private static final List<RatchetComponentDescriptor> COMPONENTS =
      List.of(
          component(JakartaAfterCommitRegistrar.class),
          component(CdiBeanResolver.class, Instance.class),
          component(RecurringMethodInvoker.class, BeanResolver.class, ClassPolicy.class),
          component(InternalEventPublisher.class, Consumer.class),
          component(CallerPrincipalProvider.class, List.class),
          component(JobPayloadInputValidator.class, RatchetOptions.class),
          component(JobSecurityValidator.class, ClassPolicy.class),
          component(DoNotRetryPolicy.class),
          component(
              PreExecutionValidator.class, JobSecurityValidator.class, DoNotRetryPolicy.class),
          component(DefaultDrainController.class),
          component(JobTypeRateLimiter.class, RatchetOptions.class),
          component(
              PoolRegistry.class,
              RatchetOptions.class,
              ExecutorProvider.class,
              MetricsCollector.class,
              ExecutionTuningProvider.class),
          component(
              DynamicHeartbeatCalculator.class,
              JobCrudStore.class,
              RatchetOptions.class,
              Clock.class),
          component(
              DefaultNodeIdentityProvider.class,
              NodeStore.class,
              JobBulkStore.class,
              DynamicHeartbeatCalculator.class,
              ExecutorProvider.class,
              RatchetOptions.class,
              Clock.class),
          component(SingletonLeaseService.class, LockStore.class, NodeIdentityProvider.class),
          component(
              StoreBackedStartupCoordinator.class, LockStore.class, NodeIdentityProvider.class),
          component(RecurringRegistrationState.class, RatchetOptions.class, Clock.class),
          component(JobPayloadInvoker.class, BeanResolver.class, ClassPolicy.class),
          component(
              ExecutionObserver.class,
              MetricsCollector.class,
              TracingCollector.class,
              InternalEventPublisher.class,
              JobAuditStore.class,
              ExecutorProvider.class),
          component(JobSuccessFinalizer.class, JobStore.class, ExecutionObserver.class),
          component(DefaultJobLoggerFactory.class, InternalEventPublisher.class, Clock.class),
          component(
              DefaultResultPersistenceStrategy.class,
              RatchetOptions.class,
              PayloadSerializer.class,
              JobCrudStore.class),
          component(
              ExecutionTargetRouter.class,
              PoolRegistry.class,
              ExecutionTuningProvider.class,
              MetricsCollector.class),
          component(
              WorkflowConditionEvaluator.class,
              BatchStore.class,
              BeanResolver.class,
              ClassPolicy.class,
              PayloadSerializer.class),
          component(
              ChainScheduler.class,
              JobCrudStore.class,
              JobTerminalStore.class,
              Clock.class,
              InternalEventPublisher.class,
              AfterCommitRegistrar.class),
          component(
              WorkflowScheduler.class,
              JobCrudStore.class,
              JobTerminalStore.class,
              WorkflowConditionStore.class,
              WorkflowConditionEvaluator.class,
              Clock.class,
              InternalEventPublisher.class,
              AfterCommitRegistrar.class),
          component(
              RecurringJobExecutor.class,
              JobBulkStore.class,
              RecurringJobStore.class,
              RecurringRegistrationState.class,
              NodeTagAffinityProvider.class,
              Clock.class),
          component(
              DefaultRecurringScheduler.class,
              ExecutorProvider.class,
              RecurringJobStore.class,
              SingletonLeaseService.class,
              NodeIdentityProvider.class,
              RecurringJobExecutor.class,
              PollerScheduler.class,
              Clock.class),
          component(
              JobWakeupService.class,
              ClusterCoordinator.class,
              Supplier.class,
              MetricsCollector.class,
              NodeIdentityProvider.class,
              AfterCommitRegistrar.class),
          component(
              DefaultResourcePermitService.class, ResourcePermitStore.class, PollerScheduler.class),
          component(BatchRecoveryService.class, BatchStore.class),
          component(
              DeadLetterService.class,
              ExecutorProvider.class,
              JobBulkStore.class,
              JobTerminalStore.class,
              SingletonLeaseService.class,
              InternalEventPublisher.class,
              ErrorSanitizer.class,
              Clock.class,
              AfterCommitRegistrar.class),
          component(
              BatchService.class,
              BatchStore.class,
              JobCrudStore.class,
              JobBatchStatusStore.class,
              JobTerminalStore.class,
              MetricsCollector.class,
              InternalEventPublisher.class,
              DeadLetterService.class,
              WorkflowScheduler.class,
              ClassPolicy.class,
              BeanResolver.class,
              Clock.class,
              BatchRecoveryService.class,
              AfterCommitRegistrar.class),
          component(BatchRecoveryTimer.class, BatchService.class, SingletonLeaseService.class),
          component(
              DefaultJobArchivingService.class,
              ArchiveStore.class,
              SingletonLeaseService.class,
              ExecutorProvider.class,
              Clock.class),
          component(
              LogPurgeTimer.class,
              JobAuditStore.class,
              SingletonLeaseService.class,
              ExecutorProvider.class,
              Clock.class),
          component(
              PostExecutionHandler.class,
              BatchService.class,
              WorkflowScheduler.class,
              DeadLetterService.class,
              PollerScheduler.class),
          component(JobStateManager.class, JobBatchStatusStore.class, NodeIdentityProvider.class),
          component(RetryBufferManager.class, JobStateManager.class, PostExecutionHandler.class),
          component(
              SubmissionGateChecker.class,
              DrainController.class,
              JobTypeRateLimiter.class,
              PoolRegistry.class,
              ExecutionTargetRouter.class),
          component(
              SubmissionFailureHandler.class,
              JobStateManager.class,
              RetryBufferManager.class,
              PoolRegistry.class,
              PollerScheduler.class,
              MetricsCollector.class),
          component(
              DefaultJobExecutorService.class,
              PoolRegistry.class,
              JobTimeoutHandler.class,
              ExecutorProvider.class,
              JobStore.class,
              ResourcePermitService.class,
              PostExecutionHandler.class,
              NodeIdentityProvider.class,
              ExecutionObserver.class,
              PreExecutionValidator.class,
              JobPayloadInvoker.class,
              JobSuccessFinalizer.class,
              RetryPolicy.class,
              ResilienceStrategy.class,
              ErrorSanitizer.class,
              PollerScheduler.class,
              JobLoggerFactory.class,
              ResultPersistenceStrategy.class,
              JobAuthorizationPolicy.class,
              PayloadSerializer.class,
              Clock.class,
              PreExecutionArgResolver.class),
          component(
              JobSubmissionService.class,
              SubmissionGateChecker.class,
              JobExecutorService.class,
              SubmissionFailureHandler.class),
          component(
              RetryBufferDrainer.class,
              ExecutorProvider.class,
              RetryBufferManager.class,
              JobSubmissionService.class,
              PoolRegistry.class,
              DrainController.class,
              RatchetOptions.class),
          component(
              JobExecutionCoordinator.class,
              JobSubmissionService.class,
              JobStateManager.class,
              RetryBufferDrainer.class,
              JobExecutorService.class),
          component(
              JobTimeoutHandler.class,
              JobCrudStore.class,
              JobRetryStore.class,
              JobBatchStatusStore.class,
              PostExecutionHandler.class,
              Clock.class,
              InternalEventPublisher.class,
              SignalStore.class,
              MetricsCollector.class,
              AfterCommitRegistrar.class,
              SingletonLeaseService.class,
              ErrorSanitizer.class,
              RatchetOptions.class),
          component(
              Poller.class,
              JobClaimStore.class,
              JobExecutionCoordinator.class,
              NodeIdentityProvider.class,
              PoolRegistry.class,
              DrainController.class,
              PollerScheduler.class,
              RatchetOptions.class,
              MetricsCollector.class,
              CircuitBreakerManager.class,
              CircuitBreakerConfigProvider.class,
              PollingStrategyProvider.class,
              NodeTagAffinityProvider.class,
              JobTimeoutHandler.class),
          component(PollerCycleExecutor.class, Supplier.class),
          component(
              DefaultPollerScheduler.class, ExecutorProvider.class, PollerCycleExecutor.class),
          component(
              PollerWakeupListener.class,
              ClusterCoordinator.class,
              PollerScheduler.class,
              MetricsCollector.class),
          component(
              OrphanRecoveryTimer.class,
              JobBulkStore.class,
              NodeStore.class,
              ResourcePermitService.class,
              SingletonLeaseService.class,
              RatchetOptions.class,
              Clock.class),
          component(
              JobCascadeService.class,
              JobCrudStore.class,
              JobPauseStore.class,
              InternalEventPublisher.class,
              Clock.class,
              AfterCommitRegistrar.class),
          component(
              DefaultJobCreationService.class,
              JobBatchStatusStore.class,
              JobTerminalStore.class,
              JobCrudStore.class,
              JobBulkStore.class,
              BatchStore.class,
              TagStore.class,
              WorkflowConditionStore.class,
              JobStore.class,
              SignalStore.class,
              ResourcePermitStore.class,
              JobWakeupService.class,
              RecurringScheduler.class,
              JobInvocationResolver.class,
              JobPayloadInputValidator.class,
              CallerPrincipalProvider.class,
              TracingCollector.class,
              JobAuthorizationPolicy.class,
              ClassPolicy.class,
              InternalEventPublisher.class,
              MetricsCollector.class,
              Clock.class,
              RatchetOptions.class,
              AfterCommitRegistrar.class),
          component(
              DefaultInvocationSubmissionService.class,
              JobSubmitter.class,
              BatchSubmitter.class,
              StreamingBatchSubmitter.class,
              JobInvocationResolver.class),
          component(
              DefaultJobSchedulerService.class,
              InternalEventPublisher.class,
              JobBatchStatusStore.class,
              JobPauseStore.class,
              JobRetryStore.class,
              JobTerminalStore.class,
              JobCrudStore.class,
              BatchStore.class,
              TagStore.class,
              WorkflowConditionStore.class,
              RecurringJobStore.class,
              JobWakeupService.class,
              RecurringScheduler.class,
              JobInvocationResolver.class,
              JobSubmitter.class,
              BatchSubmitter.class,
              StreamingBatchSubmitter.class,
              RecurringJobSubmitter.class,
              CallerPrincipalProvider.class,
              JobAuthorizationPolicy.class,
              SignalStore.class,
              PayloadSerializer.class,
              MetricsCollector.class,
              Clock.class,
              RatchetOptions.class,
              AfterCommitRegistrar.class),
          component(
              DefaultJobQueryService.class,
              JobQueryStore.class,
              JobCrudStore.class,
              JobAnalyticsStore.class,
              JobAuditStore.class,
              RecurringJobStore.class,
              JobAuthorizationPolicy.class,
              CallerPrincipalProvider.class,
              Clock.class,
              RatchetOptions.class,
              JobExtensionStore.class),
          component(
              DefaultClusterQueryService.class,
              NodeStore.class,
              NodeIdentityProvider.class,
              RatchetOptions.class,
              Clock.class),
          component(
              RecurringMethodRegistrar.class,
              JobSchedulerService.class,
              RecurringAnnotationMaintenanceService.class,
              RecurringMethodDiscovery.class,
              RecurringMethodInvoker.class,
              StartupCoordinator.class,
              RecurringRegistrationState.class,
              RatchetOptions.class,
              ExecutorProvider.class,
              JobStore.class,
              Clock.class),
          component(
              DefaultRatchetRuntime.class,
              Poller.class,
              RecurringScheduler.class,
              OrphanRecoveryTimer.class,
              BatchRecoveryTimer.class,
              DeadLetterService.class,
              JobArchivingService.class,
              LogPurgeTimer.class,
              JobExecutionCoordinator.class,
              PollerWakeupListener.class,
              DrainController.class,
              ClusterCoordinator.class,
              NodeIdentityProvider.class,
              RatchetOptions.class,
              List.class,
              Supplier.class,
              RecurringRegistration.class,
              List.class));

  private RatchetRuntimeComponentCatalog() {}

  /** Returns the deterministic, immutable component catalog. */
  public static List<RatchetComponentDescriptor> components() {
    return COMPONENTS;
  }

  private static RatchetComponentDescriptor component(
      Class<?> componentType, Class<?>... constructorParameterTypes) {
    boolean transactional =
        componentType.isAnnotationPresent(Transactional.class)
            || Arrays.stream(componentType.getDeclaredMethods())
                .anyMatch(method -> method.isAnnotationPresent(Transactional.class));
    return new RatchetComponentDescriptor(
        componentType, List.of(constructorParameterTypes), true, transactional);
  }
}
