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

import io.micronaut.context.BeanProvider;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.annotation.Secondary;
import jakarta.inject.Singleton;
import jakarta.transaction.Transactional;
import jakarta.transaction.Transactional.TxType;
import java.time.Clock;
import run.ratchet.api.RatchetOptions;
import run.ratchet.micronaut.internal.RatchetTransactionBoundary;
import run.ratchet.micronaut.internal.tx.MicronautBatchService;
import run.ratchet.micronaut.internal.tx.MicronautDeadLetterService;
import run.ratchet.micronaut.internal.tx.MicronautJobStateManager;
import run.ratchet.micronaut.internal.tx.MicronautPollerCycleExecutor;
import run.ratchet.micronaut.internal.tx.MicronautPostExecutionHandler;
import run.ratchet.micronaut.internal.tx.MicronautRecurringJobExecutor;
import run.ratchet.micronaut.internal.tx.MicronautWorkflowScheduler;
import run.ratchet.ri.cdi.RecurringMethodInvoker;
import run.ratchet.ri.core.BatchCompletionTransaction;
import run.ratchet.ri.core.BatchService;
import run.ratchet.ri.core.DefaultJobLoggerFactory;
import run.ratchet.ri.core.DefaultResultPersistenceStrategy;
import run.ratchet.ri.core.DrainController;
import run.ratchet.ri.core.JobExecutorService;
import run.ratchet.ri.core.JobStateManager;
import run.ratchet.ri.core.JobSubmissionService;
import run.ratchet.ri.core.PollerScheduler;
import run.ratchet.ri.core.RecurringJobExecutor;
import run.ratchet.ri.core.RecurringScheduler;
import run.ratchet.ri.core.ResourcePermitService;
import run.ratchet.ri.core.RetryBufferDrainer;
import run.ratchet.ri.core.WorkflowConditionEvaluator;
import run.ratchet.ri.core.internal.DeadLetterService;
import run.ratchet.ri.core.internal.DefaultJobExecutorService;
import run.ratchet.ri.core.internal.DefaultPollerScheduler;
import run.ratchet.ri.core.internal.DefaultRecurringScheduler;
import run.ratchet.ri.core.internal.DefaultResourcePermitService;
import run.ratchet.ri.core.internal.DoNotRetryPolicy;
import run.ratchet.ri.core.internal.ExecutionObserver;
import run.ratchet.ri.core.internal.ExecutionTargetRouter;
import run.ratchet.ri.core.internal.InternalEventPublisher;
import run.ratchet.ri.core.internal.JobExecutionCoordinator;
import run.ratchet.ri.core.internal.JobPayloadInvoker;
import run.ratchet.ri.core.internal.JobSuccessFinalizer;
import run.ratchet.ri.core.internal.JobTimeoutHandler;
import run.ratchet.ri.core.internal.JobWakeupService;
import run.ratchet.ri.core.internal.LifecycleCallbackInvoker;
import run.ratchet.ri.core.internal.Poller;
import run.ratchet.ri.core.internal.PollerCycleExecutor;
import run.ratchet.ri.core.internal.PollerWakeupListener;
import run.ratchet.ri.core.internal.PoolRegistry;
import run.ratchet.ri.core.internal.PostExecutionHandler;
import run.ratchet.ri.core.internal.PreExecutionValidator;
import run.ratchet.ri.core.internal.RecurringRegistrationState;
import run.ratchet.ri.core.internal.SingletonLeaseService;
import run.ratchet.ri.core.internal.WorkflowScheduler;
import run.ratchet.ri.resilience.CircuitBreakerRegistry;
import run.ratchet.ri.security.JobSecurityValidator;
import run.ratchet.spi.AfterCommitRegistrar;
import run.ratchet.spi.BeanResolver;
import run.ratchet.spi.CircuitBreakerConfigProvider;
import run.ratchet.spi.ClassPolicy;
import run.ratchet.spi.ClusterCoordinator;
import run.ratchet.spi.ErrorSanitizer;
import run.ratchet.spi.ExecutionTuningProvider;
import run.ratchet.spi.ExecutorProvider;
import run.ratchet.spi.JobAuthorizationPolicy;
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
import run.ratchet.spi.TracingCollector;
import run.ratchet.store.spi.BatchStore;
import run.ratchet.store.spi.JobAuditStore;
import run.ratchet.store.spi.JobStore;
import run.ratchet.store.spi.RecurringJobStore;
import run.ratchet.store.spi.ResourcePermitStore;
import run.ratchet.store.spi.SignalStore;
import run.ratchet.store.spi.WorkflowConditionStore;

/**
 * Internal execution wiring imported by the engine auto-configuration.
 *
 * <p>Transaction carriers stay secondary: missingBeans sees their own proxy definitions and
 * recurses.
 */
@Factory
@Requires(beans = JobStore.class)
@Requires(property = "ratchet.enabled", notEquals = "false")
class RatchetExecutionConfiguration {
  @Singleton
  @Requires(missingBeans = ResultPersistenceStrategy.class)
  ResultPersistenceStrategy defaultResultPersistenceStrategy(
      JobStore store, RatchetOptions options, PayloadSerializer payloadSerializer) {
    return new DefaultResultPersistenceStrategy(options, payloadSerializer, store);
  }

  @Singleton
  @Secondary
  @RatchetTransactionBoundary
  @Transactional
  MicronautJobStateManager jobStateManager(
      JobStore store, NodeIdentityProvider nodeIdentityProvider) {
    return new MicronautJobStateManager(store, nodeIdentityProvider);
  }

  @Singleton
  @Secondary
  @RatchetTransactionBoundary
  @Transactional
  MicronautRecurringJobExecutor recurringJobExecutor(
      JobStore store,
      RecurringRegistrationState registrationState,
      NodeTagAffinityProvider tagAffinityProvider,
      Clock clock) {
    return new MicronautRecurringJobExecutor(
        store,
        store.capability(RecurringJobStore.class).orElse(null),
        registrationState,
        tagAffinityProvider,
        clock);
  }

  @Singleton
  @Requires(missingBeans = JobLoggerFactory.class)
  JobLoggerFactory defaultJobLoggerFactory(InternalEventPublisher eventPublisher, Clock clock) {
    return new DefaultJobLoggerFactory(eventPublisher, clock);
  }

  @Singleton
  @Requires(missingBeans = ResourcePermitService.class)
  ResourcePermitService defaultResourcePermitService(
      JobStore store, PollerScheduler pollerScheduler) {
    return new DefaultResourcePermitService(
        store.capability(ResourcePermitStore.class).orElse(null), pollerScheduler);
  }

  @Singleton
  @Requires(missingBeans = RecurringScheduler.class)
  RecurringScheduler defaultRecurringScheduler(
      JobStore store,
      ExecutorProvider executorProvider,
      SingletonLeaseService singletonLeaseService,
      NodeIdentityProvider nodeIdentityProvider,
      RecurringJobExecutor recurringJobExecutor,
      PollerScheduler pollerScheduler,
      Clock clock) {
    return new DefaultRecurringScheduler(
        executorProvider,
        store.capability(RecurringJobStore.class).orElse(null),
        singletonLeaseService,
        nodeIdentityProvider,
        recurringJobExecutor,
        pollerScheduler,
        clock);
  }

  @Singleton
  @Requires(missingBeans = JobSuccessFinalizer.class)
  JobSuccessFinalizer jobSuccessFinalizer(
      PostExecutionHandler lifecycle, ExecutionObserver observer) {
    return new JobSuccessFinalizer(lifecycle, observer);
  }

  @Singleton
  @Requires(missingBeans = PollerWakeupListener.class)
  PollerWakeupListener pollerWakeupListener(
      ClusterCoordinator clusterCoordinator,
      PollerScheduler pollerScheduler,
      MetricsCollector metricsCollector) {
    return new PollerWakeupListener(clusterCoordinator, pollerScheduler, metricsCollector);
  }

  @Singleton
  @Requires(missingBeans = JobPayloadInvoker.class)
  JobPayloadInvoker jobPayloadInvoker(BeanResolver beanResolver, ClassPolicy classPolicy) {
    return new JobPayloadInvoker(beanResolver, classPolicy);
  }

  @Singleton
  @Secondary
  @RatchetTransactionBoundary
  @Transactional
  MicronautDeadLetterService deadLetterService(
      JobStore store,
      ExecutorProvider executorProvider,
      SingletonLeaseService singletonLeaseService,
      InternalEventPublisher eventPublisher,
      ErrorSanitizer errorSanitizer,
      Clock clock,
      AfterCommitRegistrar afterCommitRegistrar) {
    return new MicronautDeadLetterService(
        executorProvider,
        store,
        store,
        singletonLeaseService,
        eventPublisher,
        errorSanitizer,
        clock,
        afterCommitRegistrar);
  }

  @Singleton
  @Requires(missingBeans = PollerScheduler.class)
  PollerScheduler defaultPollerScheduler(
      ExecutorProvider executorProvider, PollerCycleExecutor pollerCycleExecutor) {
    return new DefaultPollerScheduler(executorProvider, pollerCycleExecutor);
  }

  @Singleton
  @Secondary
  @RatchetTransactionBoundary
  @Transactional(value = TxType.REQUIRES_NEW, rollbackOn = Exception.class)
  MicronautPostExecutionHandler postExecutionHandler(
      JobStore store,
      BatchService batchService,
      WorkflowScheduler workflowScheduler,
      DeadLetterService deadLetterService,
      PollerScheduler pollerScheduler,
      AfterCommitRegistrar afterCommitRegistrar) {
    return new MicronautPostExecutionHandler(
        batchService,
        workflowScheduler,
        deadLetterService,
        pollerScheduler,
        store,
        afterCommitRegistrar);
  }

  @Singleton
  @Secondary
  @RatchetTransactionBoundary
  @Transactional
  // WorkflowScheduler also exposes its transactional ChainScheduler superclass, as in Spring.
  MicronautWorkflowScheduler workflowScheduler(
      JobStore store,
      WorkflowConditionEvaluator conditionEvaluator,
      Clock clock,
      InternalEventPublisher eventPublisher,
      AfterCommitRegistrar afterCommitRegistrar) {
    return new MicronautWorkflowScheduler(
        store,
        store,
        store.capability(WorkflowConditionStore.class).orElse(null),
        conditionEvaluator,
        clock,
        eventPublisher,
        afterCommitRegistrar);
  }

  @Singleton
  @Requires(missingBeans = JobExecutionCoordinator.class)
  JobExecutionCoordinator jobExecutionCoordinator(
      JobSubmissionService jobSubmissionService,
      JobStateManager jobStateManager,
      RetryBufferDrainer retryBufferDrainer,
      JobExecutorService jobExecutorService) {
    return new JobExecutionCoordinator(
        jobSubmissionService, jobStateManager, retryBufferDrainer, jobExecutorService);
  }

  @Singleton
  @Requires(missingBeans = ExecutionTargetRouter.class)
  ExecutionTargetRouter executionTargetRouter(
      PoolRegistry poolRegistry,
      ExecutionTuningProvider executionTuningProvider,
      MetricsCollector metricsCollector) {
    return new ExecutionTargetRouter(poolRegistry, executionTuningProvider, metricsCollector);
  }

  @Singleton
  @Requires(missingBeans = PreExecutionValidator.class)
  PreExecutionValidator preExecutionValidator(
      JobSecurityValidator securityValidator, DoNotRetryPolicy doNotRetryPolicy) {
    return new PreExecutionValidator(securityValidator, doNotRetryPolicy);
  }

  @Singleton
  @Secondary
  @RatchetTransactionBoundary
  @Transactional(TxType.NOT_SUPPORTED)
  MicronautPollerCycleExecutor pollerCycleExecutor(BeanProvider<Poller> poller) {
    return new MicronautPollerCycleExecutor(poller::get);
  }

  @Singleton
  @Requires(missingBeans = RecurringMethodInvoker.class)
  RecurringMethodInvoker recurringMethodInvoker(
      BeanResolver beanResolver, ClassPolicy classPolicy) {
    return new RecurringMethodInvoker(beanResolver, classPolicy);
  }

  @Singleton
  @Requires(missingBeans = JobExecutorService.class)
  JobExecutorService defaultJobExecutorService(
      JobStore store,
      BeanProvider<PreExecutionArgResolver> argResolver,
      PoolRegistry poolRegistry,
      JobTimeoutHandler timeoutHandler,
      ExecutorProvider executorProvider,
      ResourcePermitService resourcePermitService,
      PostExecutionHandler postExecutionHandler,
      NodeIdentityProvider nodeIdProvider,
      ExecutionObserver executionObserver,
      PreExecutionValidator preExecutionValidator,
      JobPayloadInvoker payloadInvoker,
      JobSuccessFinalizer successFinalizer,
      RetryPolicy retryPolicy,
      ResilienceStrategy resilienceStrategy,
      ErrorSanitizer errorSanitizer,
      PollerScheduler pollerScheduler,
      JobLoggerFactory jobLoggerFactory,
      ResultPersistenceStrategy resultPersistenceStrategy,
      JobAuthorizationPolicy authorizationPolicy,
      PayloadSerializer payloadSerializer,
      Clock clock) {
    return new DefaultJobExecutorService(
        argResolver.orElse(null),
        poolRegistry,
        timeoutHandler,
        executorProvider,
        store,
        resourcePermitService,
        postExecutionHandler,
        nodeIdProvider,
        executionObserver,
        preExecutionValidator,
        payloadInvoker,
        successFinalizer,
        retryPolicy,
        resilienceStrategy,
        errorSanitizer,
        pollerScheduler,
        jobLoggerFactory,
        resultPersistenceStrategy,
        authorizationPolicy,
        payloadSerializer,
        clock);
  }

  @Singleton
  @Requires(missingBeans = Poller.class)
  Poller poller(
      JobStore store,
      JobExecutionCoordinator coordinator,
      NodeIdentityProvider node,
      PoolRegistry pools,
      DrainController drain,
      PollerScheduler scheduler,
      RatchetOptions options,
      MetricsCollector metrics,
      CircuitBreakerRegistry circuitBreaker,
      CircuitBreakerConfigProvider breakerConfig,
      PollingStrategyProvider polling,
      NodeTagAffinityProvider affinity,
      JobTimeoutHandler timeout) {
    return new Poller(
        store,
        coordinator,
        node,
        pools,
        drain,
        scheduler,
        options,
        metrics,
        circuitBreaker,
        breakerConfig.isEnabled(),
        polling,
        affinity,
        options.polling().batchSize(),
        timeout);
  }

  @Singleton
  @Requires(missingBeans = ExecutionObserver.class)
  ExecutionObserver executionObserver(
      MetricsCollector metrics,
      TracingCollector tracing,
      InternalEventPublisher publisher,
      JobStore store,
      ExecutorProvider executor) {
    return new ExecutionObserver(
        metrics,
        tracing,
        publisher,
        store.capability(JobAuditStore.class).orElse(null),
        executor,
        null);
  }

  @Singleton
  @Requires(missingBeans = JobWakeupService.class)
  JobWakeupService jobWakeupService(
      ClusterCoordinator cluster,
      BeanProvider<PollerScheduler> poller,
      MetricsCollector metrics,
      NodeIdentityProvider node,
      AfterCommitRegistrar registrar) {
    return new JobWakeupService(cluster, poller::get, metrics, node, registrar);
  }

  @Singleton
  @Secondary
  @RatchetTransactionBoundary
  BatchCompletionTransaction batchCompletionTransaction() {
    return new BatchCompletionTransaction();
  }

  @Singleton
  @Secondary
  @RatchetTransactionBoundary
  @Transactional
  MicronautBatchService batchService(
      JobStore store,
      MetricsCollector metrics,
      InternalEventPublisher events,
      DeadLetterService deadLetter,
      WorkflowScheduler workflow,
      ClassPolicy policy,
      BeanResolver resolver,
      Clock clock,
      AfterCommitRegistrar registrar,
      BatchCompletionTransaction completionTransaction,
      PayloadSerializer payloadSerializer) {
    return new MicronautBatchService(
        store.capability(BatchStore.class).orElse(null),
        store,
        store,
        store,
        metrics,
        events,
        deadLetter,
        workflow,
        policy,
        resolver,
        clock,
        registrar,
        completionTransaction,
        payloadSerializer);
  }

  @Singleton
  @Requires(missingBeans = JobTimeoutHandler.class)
  JobTimeoutHandler jobTimeoutHandler(
      JobStore store,
      PostExecutionHandler lifecycle,
      RatchetOptions options,
      Clock clock,
      InternalEventPublisher events,
      MetricsCollector metrics,
      AfterCommitRegistrar registrar,
      SingletonLeaseService leases,
      ErrorSanitizer sanitizer,
      PreExecutionValidator validationFacade,
      JobPayloadInvoker payloadInvoker,
      PayloadSerializer payloadSerializer,
      ExecutionObserver observabilityFacade) {
    return new JobTimeoutHandler(
        registrar,
        store,
        store,
        store,
        lifecycle,
        options.timeout().softTimeoutPercent(),
        options.timeout().defaultSlaSeconds(),
        clock,
        events,
        store.capability(SignalStore.class).orElse(null),
        metrics,
        options.timeout().signalTimeoutBatchSize(),
        leases,
        sanitizer,
        new LifecycleCallbackInvoker(
            validationFacade, payloadInvoker, payloadSerializer, observabilityFacade, clock));
  }
}
