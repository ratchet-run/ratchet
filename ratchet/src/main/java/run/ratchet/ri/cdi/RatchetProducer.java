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
package run.ratchet.ri.cdi;

import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.Dependent;
import jakarta.enterprise.context.Initialized;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Default;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import jakarta.enterprise.inject.spi.DeploymentException;
import jakarta.inject.Inject;
import jakarta.transaction.TransactionSynchronizationRegistry;
import java.time.Clock;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.jboss.logging.Logger;
import run.ratchet.api.ExecutorTargets;
import run.ratchet.api.RatchetOptions;
import run.ratchet.ri.core.DrainController;
import run.ratchet.ri.core.PollerScheduler;
import run.ratchet.ri.core.ResourcePermitService;
import run.ratchet.ri.core.internal.DefaultNodeIdentityProvider;
import run.ratchet.ri.core.internal.DefaultNodeTagAffinityProvider;
import run.ratchet.ri.core.internal.DynamicHeartbeatCalculator;
import run.ratchet.ri.core.internal.ExecutionObserver;
import run.ratchet.ri.core.internal.InternalEventPublisher;
import run.ratchet.ri.core.internal.JobExecutionCoordinator;
import run.ratchet.ri.core.internal.JobTimeoutHandler;
import run.ratchet.ri.core.internal.JobWakeupService;
import run.ratchet.ri.core.internal.OrphanRecoveryTimer;
import run.ratchet.ri.core.internal.Poller;
import run.ratchet.ri.core.internal.PoolRegistry;
import run.ratchet.ri.core.internal.PostExecutionHandler;
import run.ratchet.ri.core.internal.SingletonLeaseService;
import run.ratchet.ri.core.internal.ThreadPoolManager;
import run.ratchet.ri.core.internal.WorkflowScheduler;
import run.ratchet.ri.resilience.CircuitBreakerRegistry;
import run.ratchet.ri.resilience.DefaultResilienceStrategy;
import run.ratchet.ri.security.DefaultErrorSanitizer;
import run.ratchet.ri.security.PackagePrefixClassPolicy;
import run.ratchet.spi.CircuitBreakerConfigProvider;
import run.ratchet.spi.ClassPolicy;
import run.ratchet.spi.ErrorSanitizer;
import run.ratchet.spi.ExecutionTuningProvider;
import run.ratchet.spi.ExecutorProvider;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.spi.NodeTagAffinityProvider;
import run.ratchet.spi.PayloadSerializer;
import run.ratchet.spi.PollingStrategyProvider;
import run.ratchet.spi.ResilienceStrategy;
import run.ratchet.spi.TracingCollector;
import run.ratchet.store.converter.PayloadSerializerHolder;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.spi.JobAuditStore;
import run.ratchet.store.spi.JobBatchStatusStore;
import run.ratchet.store.spi.JobBulkStore;
import run.ratchet.store.spi.JobClaimStore;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.JobRetryStore;
import run.ratchet.store.spi.NodeStore;
import run.ratchet.store.spi.SignalStore;

/**
 * CDI producer for Ratchet beans that require configuration values mixed with injectable
 * dependencies. Configuration is read from the injected {@link RatchetOptions} bean.
 *
 * <p>Beans with purely injectable constructors are annotated directly with
 * {@code @ApplicationScoped} and {@code @Inject}. This producer handles the remaining beans whose
 * constructors require primitive configuration parameters.
 */
@ApplicationScoped
public class RatchetProducer {

  private static final Logger log = Logger.getLogger(RatchetProducer.class);

  /** Per-type default limit for the virtual pool when no explicit limit is configured. */
  private static final int DEFAULT_VIRTUAL_LIMIT = 1000;

  private final ExecutorProvider executorProvider;
  private final MetricsCollector metricsCollector;
  private final TracingCollector tracingCollector;
  private final JobCrudStore jobCrudStore;
  private final JobRetryStore jobRetryStore;
  private final JobBatchStatusStore jobBatchStatusStore;
  private final PostExecutionHandler postExecutionHandler;
  private final NodeStore nodeStore;
  private final RatchetOptions options;
  private final ExecutionTuningProvider executionTuningProvider;
  private final PollingStrategyProvider pollingStrategyProvider;
  private final CircuitBreakerConfigProvider circuitBreakerConfigProvider;
  private volatile Instance.Handle<PayloadSerializer> dependentPayloadSerializerHandle;

  private volatile TransactionSynchronizationRegistry txRegistry;

  protected RatchetProducer() {
    this.executorProvider = null;
    this.metricsCollector = null;
    this.tracingCollector = null;
    this.jobCrudStore = null;
    this.jobRetryStore = null;
    this.jobBatchStatusStore = null;
    this.postExecutionHandler = null;
    this.nodeStore = null;
    this.options = null;
    this.executionTuningProvider = null;
    this.pollingStrategyProvider = null;
    this.circuitBreakerConfigProvider = null;
  }

  @Inject
  public RatchetProducer(
      ExecutorProvider executorProvider,
      MetricsCollector metricsCollector,
      TracingCollector tracingCollector,
      JobCrudStore jobCrudStore,
      JobRetryStore jobRetryStore,
      JobBatchStatusStore jobBatchStatusStore,
      PostExecutionHandler postExecutionHandler,
      NodeStore nodeStore,
      RatchetOptions options,
      ExecutionTuningProvider executionTuningProvider,
      PollingStrategyProvider pollingStrategyProvider,
      CircuitBreakerConfigProvider circuitBreakerConfigProvider) {
    this.executorProvider = executorProvider;
    this.metricsCollector = metricsCollector;
    this.tracingCollector = tracingCollector;
    this.jobCrudStore = jobCrudStore;
    this.jobRetryStore = jobRetryStore;
    this.jobBatchStatusStore = jobBatchStatusStore;
    this.postExecutionHandler = postExecutionHandler;
    this.nodeStore = nodeStore;
    this.options = options;
    this.executionTuningProvider = executionTuningProvider;
    this.pollingStrategyProvider = pollingStrategyProvider;
    this.circuitBreakerConfigProvider = circuitBreakerConfigProvider;
  }

  @Produces
  @ApplicationScoped
  public PoolRegistry poolRegistry() {
    Map<String, ThreadPoolManager> pools = new LinkedHashMap<>();

    Map<JobExecutionType, Integer> platformLimits = new EnumMap<>(JobExecutionType.class);
    for (JobExecutionType type : JobExecutionType.values()) {
      platformLimits.put(
          type, executionTuningProvider.maxConcurrency(type.name(), configuredConcurrency(type)));
    }
    pools.put(
        ExecutorTargets.PLATFORM,
        new ThreadPoolManager(
            ExecutorTargets.PLATFORM,
            executorProvider,
            metricsCollector,
            ThreadPoolManager.AccountingMode.SEMAPHORE,
            platformLimits));

    if (options.execution().hasVirtualExecutor()) {
      Map<JobExecutionType, Integer> virtualLimits = new EnumMap<>(JobExecutionType.class);
      for (JobExecutionType type : JobExecutionType.values()) {
        virtualLimits.put(
            type, executionTuningProvider.virtualThreadLimit(type.name(), DEFAULT_VIRTUAL_LIMIT));
      }
      ThreadPoolManager.AccountingMode accountingMode =
          options.execution().virtualCounterAccounting()
              ? ThreadPoolManager.AccountingMode.COUNTER
              : ThreadPoolManager.AccountingMode.SEMAPHORE;
      pools.put(
          ExecutorTargets.VIRTUAL,
          new ThreadPoolManager(
              ExecutorTargets.VIRTUAL,
              executorProvider,
              metricsCollector,
              accountingMode,
              virtualLimits));
    }

    return new PoolRegistry(pools);
  }

  @Produces
  @ApplicationScoped
  public JobTimeoutHandler jobTimeoutHandler(
      Clock clock,
      InternalEventPublisher eventPublisher,
      WorkflowScheduler workflowScheduler,
      SignalStore signalStore) {
    int softTimeoutPercent = options.timeout().softTimeoutPercent();
    long defaultTimeoutSeconds = options.timeout().defaultSlaSeconds();
    int signalTimeoutBatchSize = options.timeout().signalTimeoutBatchSize();

    return new JobTimeoutHandler(
        jobCrudStore,
        jobRetryStore,
        jobBatchStatusStore,
        postExecutionHandler,
        softTimeoutPercent,
        defaultTimeoutSeconds,
        clock,
        eventPublisher,
        workflowScheduler,
        signalStore,
        metricsCollector,
        signalTimeoutBatchSize,
        resolveTxRegistry());
  }

  private TransactionSynchronizationRegistry resolveTxRegistry() {
    TransactionSynchronizationRegistry reg = txRegistry;
    if (reg == null) {
      synchronized (this) {
        reg = txRegistry;
        if (reg == null) {
          reg = JobWakeupService.lookupTxRegistry(log);
          txRegistry = reg;
        }
      }
    }
    return reg;
  }

  @Produces
  @ApplicationScoped
  public DynamicHeartbeatCalculator dynamicHeartbeatCalculator() {
    long baseHeartbeatIntervalSeconds = options.node().heartbeatIntervalSeconds();
    long pollerMinDelayMs = options.polling().minDelayMs();
    long pollerMaxDelayMs = options.polling().maxDelayMs();

    return new DynamicHeartbeatCalculator(
        jobCrudStore, baseHeartbeatIntervalSeconds, pollerMinDelayMs, pollerMaxDelayMs);
  }

  @Produces
  @ApplicationScoped
  public NodeIdentityProvider nodeIdentityProvider(
      DynamicHeartbeatCalculator heartbeatCalculator, JobBulkStore jobBulkStore, Clock clock) {
    long heartbeatIntervalSeconds = options.node().heartbeatIntervalSeconds();
    long orphanGraceSeconds = options.node().orphanGraceSeconds();
    boolean dynamicHeartbeatEnabled = options.node().dynamicHeartbeatEnabled();

    DefaultNodeIdentityProvider provider =
        new DefaultNodeIdentityProvider(
            nodeStore,
            jobBulkStore,
            heartbeatCalculator,
            executorProvider,
            heartbeatIntervalSeconds,
            orphanGraceSeconds,
            dynamicHeartbeatEnabled,
            options.node().explicitNodeId().orElse(null),
            clock);
    provider.init();
    return provider;
  }

  @Produces
  @ApplicationScoped
  public Poller poller(
      JobClaimStore jobClaimStore,
      JobExecutionCoordinator jobExecutionCoordinator,
      NodeIdentityProvider nodeIdProvider,
      PoolRegistry poolRegistry,
      DrainController drainController,
      PollerScheduler pollerScheduler,
      CircuitBreakerRegistry circuitBreakerRegistry,
      NodeTagAffinityProvider tagAffinityProvider,
      JobTimeoutHandler timeoutHandler) {
    int batchSize = options.polling().batchSize();
    return new Poller(
        jobClaimStore,
        jobExecutionCoordinator,
        nodeIdProvider,
        poolRegistry,
        drainController,
        pollerScheduler,
        options,
        metricsCollector,
        circuitBreakerRegistry,
        circuitBreakerConfigProvider.isEnabled(),
        pollingStrategyProvider,
        tagAffinityProvider,
        batchSize,
        timeoutHandler);
  }

  @Produces
  @ApplicationScoped
  public ExecutionObserver executionObserver(
      InternalEventPublisher eventPublisher, JobAuditStore executionStore) {
    // delayedJobReadyCallback is null by default; can be wired later if needed
    return new ExecutionObserver(
        metricsCollector, tracingCollector, eventPublisher, executionStore, executorProvider, null);
  }

  @Produces
  @ApplicationScoped
  public OrphanRecoveryTimer orphanRecoveryTimer(
      JobBulkStore jobBulkStore,
      ResourcePermitService resourcePermitService,
      SingletonLeaseService singletonLeaseService) {
    long orphanGraceSeconds = options.node().orphanGraceSeconds();
    return new OrphanRecoveryTimer(
        jobBulkStore, nodeStore, resourcePermitService, singletonLeaseService, orphanGraceSeconds);
  }

  /**
   * Produces the default {@link ClassPolicy} bean. Users override by providing their own
   * {@code @Alternative @Priority(APPLICATION) ClassPolicy} bean.
   *
   * <p><b>Fail-fast:</b> if no allowlist is configured, this producer throws {@link
   * DeploymentException} at container startup. The default {@link PackagePrefixClassPolicy} with an
   * empty allowlist is a deny-all configuration that would prevent any job from running. Set {@link
   * RatchetOptions.SecurityBuilder#allowEmptyClassPolicy} only for demos, fixtures, or tests that
   * deliberately want a deny-all policy.
   */
  @Produces
  @Default
  @ApplicationScoped
  public ClassPolicy classPolicy() {
    PackagePrefixClassPolicy policy = new PackagePrefixClassPolicy();
    if (policy.getAllowedPackages().isEmpty()) {
      if (!options.security().allowEmptyClassPolicy()) {
        String message =
            "ClassPolicy allowedPackages is empty — refusing to start. "
                + "Provide an @Alternative @Priority(APPLICATION) ClassPolicy bean with your "
                + "application's package prefixes, or set "
                + "RatchetOptions.security(...allowEmptyClassPolicy(true)) ONLY for demos/tests.";
        log.error(message);
        throw new DeploymentException(message);
      }
      log.errorf(
          "ClassPolicy allowedPackages is empty and RatchetOptions allows it. ALL job targets will be rejected.");
    }
    return policy;
  }

  /**
   * Produces the default {@link ErrorSanitizer} bean that strips PII and credentials from exception
   * messages before persistence. Users can override by providing their own
   * {@code @Alternative @Priority(APPLICATION) ErrorSanitizer} bean.
   */
  @Produces
  @Default
  @ApplicationScoped
  public ErrorSanitizer errorSanitizer() {
    return new DefaultErrorSanitizer(options.security().redactEmails());
  }

  /**
   * Produces the default {@link ResilienceStrategy} bean backed by the built-in circuit breaker.
   * Users can override by providing their own {@code @Alternative @Priority(APPLICATION)
   * ResilienceStrategy} bean (e.g., backed by Resilience4j or MicroProfile Fault Tolerance).
   */
  @Produces
  @Default
  @ApplicationScoped
  public ResilienceStrategy resilienceStrategy(CircuitBreakerRegistry circuitBreakerRegistry) {
    return new DefaultResilienceStrategy(circuitBreakerRegistry, circuitBreakerConfigProvider);
  }

  /**
   * Produces the default {@link Clock} bean used by scheduling code that computes job {@code
   * scheduledTime}. Tests override with a deterministic clock by providing an
   * {@code @Alternative @Priority(APPLICATION) Clock} bean (e.g., {@code SteppingTestClock} from
   * {@code ratchet-tck-api}).
   */
  @Produces
  @Default
  @ApplicationScoped
  public Clock systemClock() {
    return Clock.systemUTC();
  }

  /**
   * Produces the default {@link NodeTagAffinityProvider} bean. Users can override by providing
   * their own {@code @Alternative @Priority(APPLICATION) NodeTagAffinityProvider} bean for
   * runtime-dynamic tag affinity (e.g., based on hardware availability).
   */
  @Produces
  @Default
  @ApplicationScoped
  public NodeTagAffinityProvider nodeTagAffinityProvider() {
    return new DefaultNodeTagAffinityProvider(options);
  }

  /**
   * Wires the framework-resolved {@link PayloadSerializer} into {@link PayloadSerializerHolder} at
   * application startup so JPA {@link jakarta.persistence.AttributeConverter} instances (which are
   * instantiated by the persistence provider, not CDI) can route JSON I/O through the SPI. If a
   * user has installed an {@code @Alternative PayloadSerializer}, this observer picks it up
   * automatically via CDI resolution.
   */
  void registerPayloadSerializer(
      @Observes @Initialized(ApplicationScoped.class) Object init,
      Instance<PayloadSerializer> payloadSerializers) {
    if (payloadSerializers.isResolvable()) {
      destroyDependentPayloadSerializer();
      Instance.Handle<PayloadSerializer> handle = payloadSerializers.getHandle();
      PayloadSerializerHolder.set(handle.get());
      if (handle.getBean().getScope().equals(Dependent.class)) {
        dependentPayloadSerializerHandle = handle;
      }
    } else {
      log.warn(
          "No PayloadSerializer bean resolvable at startup; JPA converters will use fallback JSON-B.");
    }
  }

  @PreDestroy
  void unregisterPayloadSerializer() {
    PayloadSerializerHolder.set(null);
    destroyDependentPayloadSerializer();
  }

  private void destroyDependentPayloadSerializer() {
    Instance.Handle<PayloadSerializer> handle = dependentPayloadSerializerHandle;
    dependentPayloadSerializerHandle = null;
    if (handle != null) {
      try {
        handle.destroy();
      } catch (RuntimeException e) {
        log.warnf(e, "PayloadSerializer destruction failed during Ratchet shutdown");
      }
    }
  }

  private int configuredConcurrency(JobExecutionType type) {
    return switch (type) {
      case SINGLE -> options.execution().maxConcurrency("SINGLE", 20);
      case RECURRING -> options.execution().maxConcurrency("RECURRING", 5);
      case BATCH_CHILD -> options.execution().maxConcurrency("BATCH_CHILD", 30);
      case BATCH_PARENT -> options.execution().maxConcurrency("BATCH_PARENT", 2);
      case CHAIN_STEP -> options.execution().maxConcurrency("CHAIN_STEP", 10);
      case DLQ_ALERT -> options.execution().maxConcurrency("DLQ_ALERT", 2);
      case WORKFLOW_BRANCH -> options.execution().maxConcurrency("WORKFLOW_BRANCH", 10);
      case WORKFLOW_JOIN -> options.execution().maxConcurrency("WORKFLOW_JOIN", 10);
    };
  }
}
