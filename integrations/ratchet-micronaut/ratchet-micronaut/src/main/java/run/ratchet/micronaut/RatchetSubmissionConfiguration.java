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

import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.annotation.Secondary;
import jakarta.inject.Singleton;
import jakarta.transaction.Transactional;
import java.time.Clock;
import run.ratchet.api.ClusterQueryService;
import run.ratchet.api.JobQueryService;
import run.ratchet.api.RatchetOptions;
import run.ratchet.micronaut.internal.RatchetTransactionBoundary;
import run.ratchet.micronaut.internal.tx.MicronautDefaultJobCreationService;
import run.ratchet.micronaut.internal.tx.MicronautDefaultJobSchedulerService;
import run.ratchet.micronaut.internal.tx.MicronautJobCascadeService;
import run.ratchet.ri.core.DefaultClusterQueryService;
import run.ratchet.ri.core.DefaultInvocationSubmissionService;
import run.ratchet.ri.core.DefaultJobCreationService;
import run.ratchet.ri.core.DefaultJobQueryService;
import run.ratchet.ri.core.DrainController;
import run.ratchet.ri.core.JobExecutorService;
import run.ratchet.ri.core.JobStateManager;
import run.ratchet.ri.core.JobSubmissionService;
import run.ratchet.ri.core.JobTypeRateLimiter;
import run.ratchet.ri.core.PollerScheduler;
import run.ratchet.ri.core.RecurringScheduler;
import run.ratchet.ri.core.RetryBufferManager;
import run.ratchet.ri.core.SubmissionFailureHandler;
import run.ratchet.ri.core.SubmissionGateChecker;
import run.ratchet.ri.core.WorkflowConditionEvaluator;
import run.ratchet.ri.core.internal.ExecutionTargetRouter;
import run.ratchet.ri.core.internal.InternalEventPublisher;
import run.ratchet.ri.core.internal.JobWakeupService;
import run.ratchet.ri.core.internal.PoolRegistry;
import run.ratchet.ri.security.CallerPrincipalProvider;
import run.ratchet.ri.security.JobPayloadInputValidator;
import run.ratchet.spi.AfterCommitRegistrar;
import run.ratchet.spi.BeanResolver;
import run.ratchet.spi.ClassPolicy;
import run.ratchet.spi.ErrorSanitizer;
import run.ratchet.spi.InvocationSubmissionService;
import run.ratchet.spi.JobAuthorizationPolicy;
import run.ratchet.spi.JobInvocationResolver;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.spi.PayloadSerializer;
import run.ratchet.spi.TracingCollector;
import run.ratchet.store.spi.BatchStore;
import run.ratchet.store.spi.JobAnalyticsStore;
import run.ratchet.store.spi.JobAuditStore;
import run.ratchet.store.spi.JobPropertyStore;
import run.ratchet.store.spi.JobQueryStore;
import run.ratchet.store.spi.JobStore;
import run.ratchet.store.spi.RecurringJobStore;
import run.ratchet.store.spi.ResourcePermitStore;
import run.ratchet.store.spi.SignalStore;
import run.ratchet.store.spi.WorkflowConditionStore;

/** Internal submission wiring imported by the engine auto-configuration. */
@Factory
@Requires(beans = JobStore.class)
@Requires(property = "ratchet.enabled", notEquals = "false")
class RatchetSubmissionConfiguration {
  @Singleton
  @Secondary
  @RatchetTransactionBoundary
  @Transactional
  MicronautJobCascadeService jobCascadeService(
      JobStore store,
      InternalEventPublisher eventPublisher,
      Clock clock,
      AfterCommitRegistrar afterCommitRegistrar) {
    return new MicronautJobCascadeService(
        store, store, eventPublisher, clock, afterCommitRegistrar);
  }

  @Singleton
  @Requires(missingBeans = WorkflowConditionEvaluator.class)
  WorkflowConditionEvaluator workflowConditionEvaluator(
      JobStore store,
      BeanResolver beanResolver,
      ClassPolicy classPolicy,
      PayloadSerializer payloadSerializer) {
    return new WorkflowConditionEvaluator(
        store.capability(BatchStore.class).orElse(null),
        beanResolver,
        classPolicy,
        payloadSerializer);
  }

  @Singleton
  @Requires(missingBeans = JobQueryService.class)
  JobQueryService defaultJobQueryService(
      JobStore store,
      JobAuthorizationPolicy authPolicy,
      CallerPrincipalProvider principalProvider,
      Clock clock,
      RatchetOptions options) {
    return new DefaultJobQueryService(
        store.capability(JobQueryStore.class).orElse(null),
        store,
        store.capability(JobAnalyticsStore.class).orElse(null),
        store.capability(JobAuditStore.class).orElse(null),
        store.capability(RecurringJobStore.class).orElse(null),
        authPolicy,
        principalProvider,
        clock,
        options,
        store.capability(JobPropertyStore.class).orElse(null));
  }

  @Singleton
  @Requires(missingBeans = JobSubmissionService.class)
  JobSubmissionService jobSubmissionService(
      SubmissionGateChecker gateChecker,
      JobExecutorService executorService,
      SubmissionFailureHandler failureHandler) {
    return new JobSubmissionService(gateChecker, executorService, failureHandler);
  }

  @Singleton
  @Requires(missingBeans = SubmissionFailureHandler.class)
  SubmissionFailureHandler submissionFailureHandler(
      JobStateManager jobStateManager,
      RetryBufferManager retryBufferManager,
      PoolRegistry poolRegistry,
      PollerScheduler pollerScheduler,
      MetricsCollector metricsCollector) {
    return new SubmissionFailureHandler(
        jobStateManager, retryBufferManager, poolRegistry, pollerScheduler, metricsCollector);
  }

  @Singleton
  @Requires(missingBeans = InvocationSubmissionService.class)
  InvocationSubmissionService defaultInvocationSubmissionService(
      DefaultJobCreationService jobCreationService, JobInvocationResolver jobInvocationResolver) {
    return new DefaultInvocationSubmissionService(jobCreationService, jobInvocationResolver);
  }

  @Singleton
  @Requires(missingBeans = ClusterQueryService.class)
  ClusterQueryService defaultClusterQueryService(
      JobStore store,
      NodeIdentityProvider nodeIdentityProvider,
      RatchetOptions options,
      Clock clock) {
    return new DefaultClusterQueryService(store, nodeIdentityProvider, options, clock);
  }

  @Singleton
  @Requires(missingBeans = SubmissionGateChecker.class)
  SubmissionGateChecker submissionGateChecker(
      DrainController drainController,
      JobTypeRateLimiter rateLimiter,
      PoolRegistry poolRegistry,
      ExecutionTargetRouter router) {
    return new SubmissionGateChecker(drainController, rateLimiter, poolRegistry, router);
  }

  @Singleton
  @Secondary
  @RatchetTransactionBoundary
  MicronautDefaultJobCreationService defaultJobCreationService(
      JobStore store,
      JobWakeupService wakeup,
      RecurringScheduler recurring,
      JobInvocationResolver invocations,
      JobPayloadInputValidator validation,
      CallerPrincipalProvider principal,
      TracingCollector tracing,
      JobAuthorizationPolicy authorization,
      ClassPolicy policy,
      InternalEventPublisher events,
      MetricsCollector metrics,
      Clock clock,
      RatchetOptions options,
      AfterCommitRegistrar registrar,
      ErrorSanitizer errorSanitizer) {
    return new MicronautDefaultJobCreationService(
        store,
        store,
        store,
        store,
        store.capability(BatchStore.class).orElse(null),
        store,
        store.capability(WorkflowConditionStore.class).orElse(null),
        store.capability(RecurringJobStore.class).orElse(null),
        wakeup,
        recurring,
        invocations,
        validation,
        principal,
        tracing,
        authorization,
        policy,
        events,
        metrics,
        clock,
        store.capability(SignalStore.class).isPresent(),
        store.capability(ResourcePermitStore.class).isPresent(),
        options.callerPrincipalResolver(),
        registrar,
        errorSanitizer);
  }

  @Singleton
  @Secondary
  @RatchetTransactionBoundary
  MicronautDefaultJobSchedulerService defaultJobSchedulerService(
      JobStore store,
      InternalEventPublisher events,
      JobWakeupService wakeup,
      RecurringScheduler recurring,
      JobInvocationResolver invocations,
      DefaultJobCreationService creation,
      CallerPrincipalProvider principal,
      JobAuthorizationPolicy authorization,
      PayloadSerializer serializer,
      MetricsCollector metrics,
      Clock clock,
      RatchetOptions options,
      AfterCommitRegistrar registrar) {
    return new MicronautDefaultJobSchedulerService(
        events,
        store,
        store,
        store,
        store,
        store,
        store.capability(BatchStore.class).orElse(null),
        store,
        store.capability(WorkflowConditionStore.class).orElse(null),
        store.capability(RecurringJobStore.class).orElse(null),
        wakeup,
        recurring,
        invocations,
        creation,
        principal,
        authorization,
        store.capability(SignalStore.class).orElse(null),
        serializer,
        metrics,
        clock,
        options,
        registrar);
  }
}
