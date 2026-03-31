package run.ratchet.ri.cdi;

import run.ratchet.ri.core.DefaultNodeIdentityProvider;
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
import run.ratchet.ri.core.ThreadPoolManager;
import run.ratchet.ri.resilience.CircuitBreakerRegistry;
import run.ratchet.ri.resilience.DefaultResilienceStrategy;
import run.ratchet.ri.security.DefaultErrorSanitizer;
import run.ratchet.ri.security.JobSecurityValidator;
import run.ratchet.ri.security.PackagePrefixClassPolicy;
import run.ratchet.ri.util.RatchetConfiguration;
import run.ratchet.spi.ClassPolicy;
import run.ratchet.spi.ErrorSanitizer;
import run.ratchet.spi.ExecutorProvider;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.spi.ResilienceStrategy;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.spi.ExecutionStore;
import run.ratchet.store.spi.JobBulkStore;
import run.ratchet.store.spi.JobClaimStore;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.JobStatusStore;
import run.ratchet.store.spi.NodeStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Default;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import java.util.EnumMap;
import java.util.Map;
import java.util.logging.Logger;

/**
 * CDI producer for Ratchet beans that require configuration values mixed with injectable
 * dependencies. Configuration is read from the injected {@link RatchetConfiguration} bean.
 *
 * <p>Beans with purely injectable constructors are annotated directly with
 * {@code @ApplicationScoped} and {@code @Inject}. This producer handles the remaining beans whose
 * constructors require primitive configuration parameters.
 */
@ApplicationScoped
public class RatchetProducer {

  private static final Logger log = Logger.getLogger(RatchetProducer.class.getName());

  private final ExecutorProvider executorProvider;
  private final MetricsCollector metricsCollector;
  private final JobCrudStore jobCrudStore;
  private final JobStatusStore jobStatusStore;
  private final PostExecutionHandler postExecutionHandler;
  private final NodeStore nodeStore;
  private final RatchetConfiguration config;

  protected RatchetProducer() {
    this.executorProvider = null;
    this.metricsCollector = null;
    this.jobCrudStore = null;
    this.jobStatusStore = null;
    this.postExecutionHandler = null;
    this.nodeStore = null;
    this.config = null;
  }

  @Inject
  public RatchetProducer(
      ExecutorProvider executorProvider,
      MetricsCollector metricsCollector,
      JobCrudStore jobCrudStore,
      JobStatusStore jobStatusStore,
      PostExecutionHandler postExecutionHandler,
      NodeStore nodeStore,
      RatchetConfiguration config) {
    this.executorProvider = executorProvider;
    this.metricsCollector = metricsCollector;
    this.jobCrudStore = jobCrudStore;
    this.jobStatusStore = jobStatusStore;
    this.postExecutionHandler = postExecutionHandler;
    this.nodeStore = nodeStore;
    this.config = config;
  }

  @Produces
  @ApplicationScoped
  public ThreadPoolManager threadPoolManager() {
    boolean useVirtualThreads = config.isWorkerUseVirtualThreads();

    Map<JobExecutionType, Integer> maxConcurrencyMap = new EnumMap<>(JobExecutionType.class);
    maxConcurrencyMap.put(JobExecutionType.SINGLE, config.getThreadPoolSizeSingle());
    maxConcurrencyMap.put(JobExecutionType.RECURRING, config.getThreadPoolSizeRecurring());
    maxConcurrencyMap.put(JobExecutionType.BATCH_CHILD, config.getThreadPoolSizeBatchChild());
    maxConcurrencyMap.put(JobExecutionType.BATCH_PARENT, config.getThreadPoolSizeBatchParent());
    maxConcurrencyMap.put(JobExecutionType.CHAIN_STEP, config.getThreadPoolSizeChain());
    maxConcurrencyMap.put(JobExecutionType.WORKFLOW_BRANCH, config.getThreadPoolSizeDefault());

    return new ThreadPoolManager(
        executorProvider, metricsCollector, useVirtualThreads, maxConcurrencyMap, config);
  }

  @Produces
  @ApplicationScoped
  public JobTimeoutHandler jobTimeoutHandler() {
    int softTimeoutPercent = config.getSoftTimeoutPercent();
    long defaultTimeoutSeconds = config.getWorkerDefaultSLA();

    return new JobTimeoutHandler(
        jobCrudStore,
        jobStatusStore,
        postExecutionHandler,
        softTimeoutPercent,
        defaultTimeoutSeconds);
  }

  @Produces
  @ApplicationScoped
  public DynamicHeartbeatCalculator dynamicHeartbeatCalculator() {
    long baseHeartbeatIntervalSeconds = config.getNodeHeartbeatIntervalSeconds();
    long pollerMinDelayMs = config.getPollerMinDelayMs();
    long pollerMaxDelayMs = config.getPollerMaxDelayMs();

    return new DynamicHeartbeatCalculator(
        jobCrudStore, baseHeartbeatIntervalSeconds, pollerMinDelayMs, pollerMaxDelayMs);
  }

  @Produces
  @ApplicationScoped
  public NodeIdentityProvider nodeIdentityProvider(
      DynamicHeartbeatCalculator heartbeatCalculator, JobBulkStore jobBulkStore) {
    long heartbeatIntervalSeconds = config.getNodeHeartbeatIntervalSeconds();
    long orphanGraceSeconds = config.getNodeOrphanGraceSeconds();
    boolean dynamicHeartbeatEnabled = config.isDynamicHeartbeatEnabled();

    DefaultNodeIdentityProvider provider =
        new DefaultNodeIdentityProvider(
            nodeStore,
            jobBulkStore,
            heartbeatCalculator,
            executorProvider,
            heartbeatIntervalSeconds,
            orphanGraceSeconds,
            dynamicHeartbeatEnabled);
    provider.init();
    return provider;
  }

  @Produces
  @ApplicationScoped
  public ExecutionObserver executionObserver(
      InternalEventPublisher eventPublisher, ExecutionStore executionStore) {
    // delayedJobReadyCallback is null by default; can be wired later if needed
    return new ExecutionObserver(
        metricsCollector, eventPublisher, executionStore, executorProvider, null);
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
  public Poller poller(
      JobClaimStore jobClaimStore,
      JobExecutionCoordinator jobExecutionCoordinator,
      NodeIdentityProvider nodeIdProvider,
      ThreadPoolManager threadPoolManager,
      DrainController drainController,
      PollerScheduler pollerScheduler) {
    int batchSize = config.getPollerBatchSize();
    return new Poller(
        jobClaimStore,
        jobExecutionCoordinator,
        nodeIdProvider,
        threadPoolManager,
        drainController,
        pollerScheduler,
        config,
        batchSize);
  }

  @Produces
  @ApplicationScoped
  public OrphanRecoveryTimer orphanRecoveryTimer(
      JobBulkStore jobBulkStore, ResourcePermitService resourcePermitService) {
    long orphanGraceSeconds = config.getNodeOrphanGraceSeconds();
    return new OrphanRecoveryTimer(
        jobBulkStore, nodeStore, resourcePermitService, orphanGraceSeconds);
  }

  /**
   * Produces the default {@link ClassPolicy} bean. Users can override by providing their own
   * {@code @Alternative @Priority(APPLICATION) ClassPolicy} bean.
   */
  @Produces
  @Default
  @ApplicationScoped
  public ClassPolicy classPolicy() {
    PackagePrefixClassPolicy policy = new PackagePrefixClassPolicy();
    if (policy.getAllowedPackages().isEmpty()) {
      log.severe(
          "ClassPolicy allowedPackages is empty — all job targets will be rejected. "
              + "Provide an @Alternative ClassPolicy bean with your application's package prefixes.");
    }
    return policy;
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
    return new DefaultResilienceStrategy(circuitBreakerRegistry);
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
    return new DefaultErrorSanitizer();
  }
}
