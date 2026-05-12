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
import java.time.Clock;
import java.util.EnumMap;
import java.util.Map;
import org.jboss.logging.Logger;
import run.ratchet.api.RatchetOptions;
import run.ratchet.ri.core.DefaultNodeIdentityProvider;
import run.ratchet.ri.core.DefaultNodeTagAffinityProvider;
import run.ratchet.ri.core.DrainController;
import run.ratchet.ri.core.DynamicHeartbeatCalculator;
import run.ratchet.ri.core.ExecutionObserver;
import run.ratchet.ri.core.InternalEventPublisher;
import run.ratchet.ri.core.JobExecutionCoordinator;
import run.ratchet.ri.core.JobTimeoutHandler;
import run.ratchet.ri.core.OrphanRecoveryTimer;
import run.ratchet.ri.core.Poller;
import run.ratchet.ri.core.PollerScheduler;
import run.ratchet.ri.core.PostExecutionHandler;
import run.ratchet.ri.core.PreExecutionValidator;
import run.ratchet.ri.core.ResourcePermitService;
import run.ratchet.ri.core.SingletonLeaseService;
import run.ratchet.ri.core.ThreadPoolManager;
import run.ratchet.ri.core.WorkflowScheduler;
import run.ratchet.ri.resilience.CircuitBreakerRegistry;
import run.ratchet.ri.resilience.DefaultResilienceStrategy;
import run.ratchet.ri.security.DefaultErrorSanitizer;
import run.ratchet.ri.security.JobSecurityValidator;
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
import run.ratchet.store.spi.ExecutionStore;
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
  public ThreadPoolManager threadPoolManager() {
    boolean useVirtualThreads = executionTuningProvider.useVirtualThreads();

    Map<JobExecutionType, Integer> maxConcurrencyMap = new EnumMap<>(JobExecutionType.class);
    for (JobExecutionType type : JobExecutionType.values()) {
      maxConcurrencyMap.put(
          type, executionTuningProvider.maxConcurrency(type.name(), configuredConcurrency(type)));
    }

    return new ThreadPoolManager(
        executorProvider,
        metricsCollector,
        useVirtualThreads,
        maxConcurrencyMap,
        executionTuningProvider);
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
        signalTimeoutBatchSize);
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
      ThreadPoolManager threadPoolManager,
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
        threadPoolManager,
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
      InternalEventPublisher eventPublisher, ExecutionStore executionStore) {
    // delayedJobReadyCallback is null by default; can be wired later if needed
    return new ExecutionObserver(
        metricsCollector, tracingCollector, eventPublisher, executionStore, executorProvider, null);
  }

  @Produces
  @ApplicationScoped
  public PreExecutionValidator.SecurityValidator securityValidator(
      JobSecurityValidator jobSecurityValidator) {
    return jobSecurityValidator::validate;
  }

  @Produces
  @ApplicationScoped
  public PreExecutionValidator.DoNotRetryPolicy doNotRetryPolicy(
      run.ratchet.ri.core.DoNotRetryPolicy policy) {
    return policy::shouldNotRetry;
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
  public void registerPayloadSerializer(
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
