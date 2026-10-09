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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.micronaut.context.ApplicationContext;
import jakarta.inject.Singleton;
import java.lang.reflect.Method;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;
import run.ratchet.api.ClusterQueryService;
import run.ratchet.api.ExecutorTargets;
import run.ratchet.api.JobBuilder;
import run.ratchet.api.JobQueryService;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.api.RatchetOptions;
import run.ratchet.micronaut.internal.CircuitBreakerProtectedInterceptor;
import run.ratchet.micronaut.internal.NoTransactionOperations;
import run.ratchet.micronaut.internal.RatchetTransactionBoundary;
import run.ratchet.ri.cdi.RecurringMethodInvoker;
import run.ratchet.ri.core.BatchCompletionTransaction;
import run.ratchet.ri.core.BatchService;
import run.ratchet.ri.core.DefaultJobCreationService;
import run.ratchet.ri.core.DrainController;
import run.ratchet.ri.core.JobArchivingService;
import run.ratchet.ri.core.JobCascadeService;
import run.ratchet.ri.core.JobExecutorService;
import run.ratchet.ri.core.JobStateManager;
import run.ratchet.ri.core.JobSubmissionService;
import run.ratchet.ri.core.JobTypeRateLimiter;
import run.ratchet.ri.core.PollerScheduler;
import run.ratchet.ri.core.RatchetRuntime;
import run.ratchet.ri.core.RecurringJobExecutor;
import run.ratchet.ri.core.RecurringScheduler;
import run.ratchet.ri.core.ResourcePermitService;
import run.ratchet.ri.core.RetryBufferDrainer;
import run.ratchet.ri.core.RetryBufferManager;
import run.ratchet.ri.core.SubmissionFailureHandler;
import run.ratchet.ri.core.SubmissionGateChecker;
import run.ratchet.ri.core.WorkflowConditionEvaluator;
import run.ratchet.ri.core.internal.BatchRecoveryTimer;
import run.ratchet.ri.core.internal.DeadLetterService;
import run.ratchet.ri.core.internal.DoNotRetryPolicy;
import run.ratchet.ri.core.internal.DynamicHeartbeatCalculator;
import run.ratchet.ri.core.internal.ExecutionObserver;
import run.ratchet.ri.core.internal.ExecutionTargetRouter;
import run.ratchet.ri.core.internal.InternalEventPublisher;
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
import run.ratchet.ri.core.internal.RecurringRegistrationState;
import run.ratchet.ri.core.internal.SingletonLeaseService;
import run.ratchet.ri.core.internal.WorkflowScheduler;
import run.ratchet.ri.resilience.CircuitBreakerRegistry;
import run.ratchet.ri.security.CallerPrincipalProvider;
import run.ratchet.ri.security.JobPayloadInputValidator;
import run.ratchet.ri.security.JobSecurityValidator;
import run.ratchet.ri.security.PackagePrefixClassPolicy;
import run.ratchet.spi.AfterCommitRegistrar;
import run.ratchet.spi.BeanResolver;
import run.ratchet.spi.CircuitBreakerConfigProvider;
import run.ratchet.spi.ClassPolicy;
import run.ratchet.spi.ClusterCoordinator;
import run.ratchet.spi.ErrorSanitizer;
import run.ratchet.spi.ExecutionTuningProvider;
import run.ratchet.spi.ExecutorProvider;
import run.ratchet.spi.InvocationSubmissionService;
import run.ratchet.spi.JobAuthorizationPolicy;
import run.ratchet.spi.JobInvocationResolver;
import run.ratchet.spi.JobLoggerFactory;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.spi.NodeTagAffinityProvider;
import run.ratchet.spi.PayloadSerializer;
import run.ratchet.spi.PollingStrategyProvider;
import run.ratchet.spi.ResilienceStrategy;
import run.ratchet.spi.ResultPersistenceStrategy;
import run.ratchet.spi.RetryPolicy;
import run.ratchet.spi.StartupCoordinator;
import run.ratchet.spi.TracingCollector;
import run.ratchet.store.spi.JobStore;

class RatchetEngineWiringTest {
  @Test
  void schemaMigrationDefaultsAndExplicitOverrides() {
    for (Map<String, Object> values :
        List.<Map<String, Object>>of(
            Map.of(),
            Map.of("ratchet.schema.auto-migrate", false),
            Map.of("ratchet.schema.auto-migrate", true))) {
      try (var context = TestContexts.engine(values)) {
        context.start();
        assertEquals(
            values.getOrDefault("ratchet.schema.auto-migrate", true),
            context.getBean(RatchetOptions.class).schema().autoMigrate());
      }
    }
  }

  @Test
  void mandatoryOnlyStoreResolvesFullGraphWithoutTransactionManager() {
    try (var context = TestContexts.engine(Map.of())) {
      context.start();
      assertNotNull(context.getBean(JobSchedulerService.class));
      assertNotNull(context.getBean(MicronautRecurringDiscovery.class));
      assertNotNull(context.getBean(RecurringAnnotationMaintenanceService.class));
      assertFalse(context.getBean(PoolRegistry.class).hasPool(ExecutorTargets.VIRTUAL));
      var operations = context.getBean(NoTransactionOperations.class);
      assertEquals(Optional.empty(), operations.findTransactionStatus());
      // A real proxied, class-level NOT_SUPPORTED bean must be callable without a tx manager.
      assertEquals(
          0,
          context
              .getBean(RecurringAnnotationMaintenanceService.class)
              .cancelOrphanedRecurringAnnotationJobs(Set.of(), Instant.now()));
    }
  }

  @Test
  void customSchedulerDoesNotRemoveInternalMaintenance() {
    var scheduler = mock(JobSchedulerService.class);
    try (var context = TestContexts.engine(Map.of())) {
      context.registerSingleton(JobSchedulerService.class, scheduler);
      context.start();
      assertSame(scheduler, context.getBean(JobSchedulerService.class));
      assertNotNull(context.getBean(RecurringAnnotationMaintenanceService.class));
      assertNotNull(context.getBean(MicronautRecurringDiscovery.class));
    }
  }

  @Test
  void schedulerInjectsUserCoreCreationServiceInsteadOfTheCarrier() {
    var creation = mock(DefaultJobCreationService.class);
    try (var context = TestContexts.engine(Map.of())) {
      context.registerSingleton(DefaultJobCreationService.class, creation);
      context.start();
      context.getBean(JobSchedulerService.class).enqueue(() -> {}).submit();
      verify(creation).submit(any(JobBuilder.class));
    }
  }

  @Test
  void disabledRatchetDoesNotCreateEngine() {
    try (var context = TestContexts.core(Map.of("ratchet.enabled", false))) {
      context.registerSingleton(JobStore.class, mock(JobStore.class));
      context.start();
      assertFalse(context.containsBean(JobSchedulerService.class));
      assertFalse(context.containsBean(RatchetOptions.class));
      assertNotNull(context.getBean(CircuitBreakerProtectedInterceptor.class));
    }
  }

  @Test
  void noStoreHasOnlyFrameworkAdapters() {
    try (var context = TestContexts.core(Map.of())) {
      context.start();
      assertNotNull(context.getBean(RatchetOptions.class));
      assertFalse(context.containsBean(JobSchedulerService.class));
      assertNotNull(context.getBean(CircuitBreakerProtectedInterceptor.class));
    }
  }

  @Test
  void customExecutorRemainsLazy() {
    var executor = mock(ExecutorProvider.class);
    try (var context = TestContexts.engine(Map.of())) {
      context.registerSingleton(ExecutorProvider.class, executor);
      context.start();
      context.getBean(PoolRegistry.class);
      assertSame(executor, context.getBean(ExecutorProvider.class));
      verifyNoInteractions(executor);
    }
  }

  @Test
  void virtualSettingRespectsRuntimeAndExplicitDefault() {
    try (var context =
        TestContexts.engine(
            Map.of(
                "micronaut.executors.default.virtual",
                true,
                "ratchet.worker.default-threading-mode",
                "platform"))) {
      context.start();
      assertEquals(
          Runtime.version().feature() >= 21,
          context.getBean(PoolRegistry.class).hasPool(ExecutorTargets.VIRTUAL));
    }
  }

  @Test
  void userSingletonsAreTheOnlyBeansEvenWithEmptyAllowedPackages() {
    var policy = mock(ClassPolicy.class);
    var executor = mock(ExecutorProvider.class);
    var serializer = mock(PayloadSerializer.class);
    var resolver = mock(BeanResolver.class);
    var metrics = mock(MetricsCollector.class);
    var service = mock(JobSubmissionService.class);
    try (var context = TestContexts.engine(Map.of("ratchet.allowed-packages", ""))) {
      context.registerSingleton(ClassPolicy.class, policy);
      context.registerSingleton(ExecutorProvider.class, executor);
      context.registerSingleton(PayloadSerializer.class, serializer);
      context.registerSingleton(BeanResolver.class, resolver);
      context.registerSingleton(MetricsCollector.class, metrics);
      context.registerSingleton(JobSubmissionService.class, service);
      context.start();
      // Enumerating ClassPolicy would build the throwing default if it were still enabled.
      assertOnlyBean(context, ClassPolicy.class, policy);
      assertOnlyBean(context, ExecutorProvider.class, executor);
      assertOnlyBean(context, PayloadSerializer.class, serializer);
      assertOnlyBean(context, BeanResolver.class, resolver);
      assertOnlyBean(context, MetricsCollector.class, metrics);
      assertOnlyBean(context, JobSubmissionService.class, service);
      assertFalse(context.containsBean(PackagePrefixClassPolicy.class));
    }
  }

  private static <T> void assertOnlyBean(ApplicationContext context, Class<T> type, T singleton) {
    var beans = context.getBeansOfType(type);
    assertEquals(1, beans.size(), type.getName());
    assertSame(singleton, beans.iterator().next(), type.getName());
    var provided = context.getProvider(type).stream().toList();
    assertEquals(1, provided.size(), type.getName());
    assertSame(singleton, provided.get(0), type.getName());
  }

  // Mirrors the Spring override contract, including SPI types rather than carrier subclasses.
  private static final Map<String, Class<?>> OVERRIDE_TYPES =
      Map.ofEntries(
          Map.entry("ratchetOptions", RatchetOptions.class),
          Map.entry("ratchetClock", Clock.class),
          Map.entry("ratchetMetricsCollector", MetricsCollector.class),
          Map.entry("ratchetTracingCollector", TracingCollector.class),
          Map.entry("ratchetClusterCoordinator", ClusterCoordinator.class),
          Map.entry("ratchetPayloadSerializer", PayloadSerializer.class),
          Map.entry("ratchetAfterCommitRegistrar", AfterCommitRegistrar.class),
          Map.entry("ratchetBeanResolver", BeanResolver.class),
          Map.entry("ratchetClassPolicy", ClassPolicy.class),
          Map.entry("defaultResultPersistenceStrategy", ResultPersistenceStrategy.class),
          Map.entry("jobStateManager", JobStateManager.class),
          Map.entry("recurringJobExecutor", RecurringJobExecutor.class),
          Map.entry("defaultJobLoggerFactory", JobLoggerFactory.class),
          Map.entry("defaultResourcePermitService", ResourcePermitService.class),
          Map.entry("defaultRecurringScheduler", RecurringScheduler.class),
          Map.entry("jobSuccessFinalizer", JobSuccessFinalizer.class),
          Map.entry("pollerWakeupListener", PollerWakeupListener.class),
          Map.entry("jobPayloadInvoker", JobPayloadInvoker.class),
          Map.entry("deadLetterService", DeadLetterService.class),
          Map.entry("defaultPollerScheduler", PollerScheduler.class),
          Map.entry("postExecutionHandler", PostExecutionHandler.class),
          Map.entry("workflowScheduler", WorkflowScheduler.class),
          Map.entry("jobExecutionCoordinator", JobExecutionCoordinator.class),
          Map.entry("executionTargetRouter", ExecutionTargetRouter.class),
          Map.entry("preExecutionValidator", PreExecutionValidator.class),
          Map.entry("pollerCycleExecutor", PollerCycleExecutor.class),
          Map.entry("recurringMethodInvoker", RecurringMethodInvoker.class),
          Map.entry("defaultJobExecutorService", JobExecutorService.class),
          Map.entry("poller", Poller.class),
          Map.entry("executionObserver", ExecutionObserver.class),
          Map.entry("jobWakeupService", JobWakeupService.class),
          Map.entry("batchCompletionTransaction", BatchCompletionTransaction.class),
          Map.entry("batchService", BatchService.class),
          Map.entry("jobTimeoutHandler", JobTimeoutHandler.class),
          Map.entry("defaultRetryPolicy", RetryPolicy.class),
          Map.entry("defaultCircuitBreakerConfigProvider", CircuitBreakerConfigProvider.class),
          Map.entry("circuitBreakerRegistry", CircuitBreakerRegistry.class),
          Map.entry("defaultJobInvocationResolver", JobInvocationResolver.class),
          Map.entry("permitAllJobAuthorizationPolicy", JobAuthorizationPolicy.class),
          Map.entry("jobPayloadInputValidator", JobPayloadInputValidator.class),
          Map.entry("jobSecurityValidator", JobSecurityValidator.class),
          Map.entry("defaultPollingStrategyProvider", PollingStrategyProvider.class),
          Map.entry("defaultExecutionTuningProvider", ExecutionTuningProvider.class),
          Map.entry("jobTypeRateLimiter", JobTypeRateLimiter.class),
          Map.entry("doNotRetryPolicy", DoNotRetryPolicy.class),
          Map.entry("callerPrincipalProvider", CallerPrincipalProvider.class),
          Map.entry("errorSanitizer", ErrorSanitizer.class),
          Map.entry("resilienceStrategy", ResilienceStrategy.class),
          Map.entry("nodeTagAffinityProvider", NodeTagAffinityProvider.class),
          Map.entry("jobCascadeService", JobCascadeService.class),
          Map.entry("workflowConditionEvaluator", WorkflowConditionEvaluator.class),
          Map.entry("defaultJobQueryService", JobQueryService.class),
          Map.entry("jobSubmissionService", JobSubmissionService.class),
          Map.entry("submissionFailureHandler", SubmissionFailureHandler.class),
          Map.entry("defaultInvocationSubmissionService", InvocationSubmissionService.class),
          Map.entry("defaultClusterQueryService", ClusterQueryService.class),
          Map.entry("submissionGateChecker", SubmissionGateChecker.class),
          Map.entry("defaultJobCreationService", DefaultJobCreationService.class),
          Map.entry("defaultJobSchedulerService", JobSchedulerService.class),
          Map.entry("retryBufferManager", RetryBufferManager.class),
          Map.entry("retryBufferDrainer", RetryBufferDrainer.class),
          Map.entry("storeBackedStartupCoordinator", StartupCoordinator.class),
          Map.entry("defaultJobArchivingService", JobArchivingService.class),
          Map.entry("logPurgeTimer", LogPurgeTimer.class),
          Map.entry("recurringRegistrationState", RecurringRegistrationState.class),
          Map.entry("singletonLeaseService", SingletonLeaseService.class),
          Map.entry("defaultDrainController", DrainController.class),
          Map.entry("batchRecoveryTimer", BatchRecoveryTimer.class),
          Map.entry("ratchetExecutorProvider", ExecutorProvider.class),
          Map.entry("internalEventPublisher", InternalEventPublisher.class),
          Map.entry("poolRegistry", PoolRegistry.class),
          Map.entry("dynamicHeartbeatCalculator", DynamicHeartbeatCalculator.class),
          Map.entry("nodeIdentityProvider", NodeIdentityProvider.class),
          Map.entry("orphanRecoveryTimer", OrphanRecoveryTimer.class),
          Map.entry("ratchetRuntime", RatchetRuntime.class),
          Map.entry(
              "recurringAnnotationMaintenanceService",
              RecurringAnnotationMaintenanceService.class));

  @Test
  void everyDefaultAcceptsAUserBean() {
    for (Class<?> factory :
        new Class<?>[] {
          RatchetAutoConfiguration.class,
          RatchetPolicyConfiguration.class,
          RatchetSubmissionConfiguration.class,
          RatchetExecutionConfiguration.class,
          RatchetRuntimeConfiguration.class
        }) {
      for (Method method : factory.getDeclaredMethods()) {
        if (!method.isAnnotationPresent(Singleton.class)
            || !OVERRIDE_TYPES.containsKey(method.getName())) continue;
        Class<?> type = OVERRIDE_TYPES.get(method.getName());
        assertNotNull(type, method.toString());
        assertTrue(type.isAssignableFrom(method.getReturnType()), method.toString());
        assertOverride(type, !method.isAnnotationPresent(RatchetTransactionBoundary.class));
      }
    }
  }

  private <T> void assertOverride(Class<T> type, boolean onlyBean) {
    T replacement =
        type == Clock.class
            ? type.cast(Clock.systemUTC())
            : type == RatchetOptions.class ? type.cast(RatchetOptions.defaults()) : mock(type);
    try (ApplicationContext context = TestContexts.core(Map.of(), replacement)) {
      context.registerSingleton(JobStore.class, mock(JobStore.class));
      if (type != RatchetRuntime.class)
        context.registerSingleton(RatchetRuntime.class, mock(RatchetRuntime.class));
      context.start();
      assertSame(replacement, context.getBean(type), type.getName());
      assertSame(replacement, context.getProvider(type).get(), type.getName());
      if (onlyBean) assertOnlyBean(context, type, replacement);
    }
  }
}
