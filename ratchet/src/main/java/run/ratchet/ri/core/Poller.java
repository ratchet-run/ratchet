package run.ratchet.ri.core;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.jboss.logging.Logger;
import run.ratchet.api.CircuitBreakerProfile;
import run.ratchet.api.NodeTagFilter;
import run.ratchet.api.RatchetOptions;
import run.ratchet.api.exception.RatchetTransientStoreException;
import run.ratchet.ri.resilience.CircuitBreaker;
import run.ratchet.ri.resilience.CircuitBreakerRegistry;
import run.ratchet.ri.resilience.ServiceUnavailableException;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.spi.NodeTagAffinityProvider;
import run.ratchet.spi.PollingConfig;
import run.ratchet.spi.PollingDelayStrategy;
import run.ratchet.spi.PollingStrategyProvider;
import run.ratchet.store.dto.JobClaimDto;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.spi.JobClaimStore;

/**
 * Claims pending jobs from the store in batches and submits them to {@link
 * JobExecutionCoordinator}. Polling frequency adapts based on job availability and thread-pool
 * utilization; see {@link PollingStrategy} for details.
 */
public class Poller {

  private static final Logger log = Logger.getLogger(Poller.class);
  private static final String CLAIM_BREAKER_NAME = "store.claim";
  private static final JobExecutionType[] POLLER_EXECUTABLE_TYPES = {
    JobExecutionType.SINGLE,
    JobExecutionType.BATCH_CHILD,
    JobExecutionType.CHAIN_STEP,
    JobExecutionType.WORKFLOW_BRANCH
  };

  private final AtomicBoolean started = new AtomicBoolean();
  private final AtomicBoolean running = new AtomicBoolean();

  private final JobTimeoutHandler timeoutHandler;
  private final JobClaimStore jobClaimStore;
  private final JobExecutionCoordinator jobExecutionCoordinator;
  private final NodeIdentityProvider nodeIdProvider;
  private final ThreadPoolManager threadPoolManager;
  private final DrainController drainController;
  private final PollerScheduler pollerScheduler;
  private final RatchetOptions options;
  private final MetricsCollector metricsCollector;
  private final PollingStrategyProvider pollingStrategyProvider;
  private final NodeTagAffinityProvider tagAffinityProvider;
  private final CircuitBreaker claimCircuitBreaker;
  private final boolean claimCircuitBreakerEnabled;
  private final int batchSize;
  private final int claimHeadroomFactor;

  @SuppressWarnings("java:S3077")
  private volatile PollingDelayStrategy strategy;

  protected Poller() {
    this.timeoutHandler = null;
    this.jobClaimStore = null;
    this.jobExecutionCoordinator = null;
    this.nodeIdProvider = null;
    this.threadPoolManager = null;
    this.drainController = null;
    this.pollerScheduler = null;
    this.options = null;
    this.metricsCollector = null;
    this.pollingStrategyProvider = null;
    this.tagAffinityProvider = null;
    this.claimCircuitBreaker = null;
    this.claimCircuitBreakerEnabled = false;
    this.batchSize = 0;
    this.claimHeadroomFactor = 0;
  }

  public Poller(
      JobClaimStore jobClaimStore,
      JobExecutionCoordinator jobExecutionCoordinator,
      NodeIdentityProvider nodeIdProvider,
      ThreadPoolManager threadPoolManager,
      DrainController drainController,
      PollerScheduler pollerScheduler,
      RatchetOptions options,
      MetricsCollector metricsCollector,
      CircuitBreakerRegistry circuitBreakerRegistry,
      boolean claimCircuitBreakerEnabled,
      PollingStrategyProvider pollingStrategyProvider,
      NodeTagAffinityProvider tagAffinityProvider,
      int batchSize,
      JobTimeoutHandler timeoutHandler) {
    this.timeoutHandler = timeoutHandler;
    this.jobClaimStore = jobClaimStore;
    this.jobExecutionCoordinator = jobExecutionCoordinator;
    this.nodeIdProvider = nodeIdProvider;
    this.threadPoolManager = threadPoolManager;
    this.drainController = drainController;
    this.pollerScheduler = pollerScheduler;
    this.options = options;
    this.metricsCollector = metricsCollector;
    this.claimCircuitBreakerEnabled = claimCircuitBreakerEnabled;
    this.claimCircuitBreaker =
        claimCircuitBreakerEnabled && circuitBreakerRegistry != null
            ? circuitBreakerRegistry.getBreaker(
                CLAIM_BREAKER_NAME, CircuitBreakerProfile.CLAIM_PATH)
            : null;
    this.pollingStrategyProvider = pollingStrategyProvider;
    this.tagAffinityProvider = tagAffinityProvider;
    this.batchSize = batchSize;
    this.claimHeadroomFactor = Math.max(0, options.polling().claimHeadroomFactor());
  }

  public PollingStrategy.PollingStats getPollingStats() {
    PollingDelayStrategy local = strategy;
    return local instanceof PollingStrategy pollingStrategy ? pollingStrategy.getStats() : null;
  }

  /** Must be called after database migrations complete. */
  public void init() {
    if (!started.compareAndSet(false, true)) {
      log.warn("Poller already initialized; skipping re-init");
      return;
    }

    this.strategy =
        pollingStrategyProvider.create(
            new PollingConfig(
                options.polling().burstDelayMs(),
                options.polling().minDelayMs(),
                options.polling().maxDelayMs(),
                options.polling().deepIdleDelayMs(),
                options.polling().deepIdleThresholdMs(),
                options.polling().idleThreshold(),
                batchSize));

    pollerScheduler.start();
    publishClaimBreakerState();

    log.infof("Poller initialized (batch=%s)", batchSize);
  }

  public void onWakeup() {
    if (strategy != null) {
      boolean wasInDeepIdle = strategy.isInDeepIdle();
      strategy.onWakeup();
      if (wasInDeepIdle) {
        log.info("Wakeup received - exited deep idle mode");
      } else {
        log.debug("Wakeup received - reset to minimum delay");
      }
    }
  }

  public void stop() {
    started.set(false);
    pollerScheduler.stop();
    log.info("Poller marked as stopped");
  }

  @SuppressWarnings("java:S1181")
  public long tick() {
    if (!started.get()) {
      return strategy != null ? strategy.getCurrentDelay() : 1000;
    }

    if (!running.compareAndSet(false, true)) {
      log.warn("tick() already running, skipping overlapping call");
      PollingDelayStrategy local = strategy;
      return local != null ? local.getCurrentDelay() : 1000;
    }

    try {
      return pollOnce();
    } catch (Throwable t) {
      log.error("Poller tick failed", t);
      return 5000;
    } finally {
      running.set(false);
    }
  }

  private void handleJobsFound(List<JobClaimDto> claims, int jobCount) {
    claims.forEach(jobExecutionCoordinator::submit);
    log.infov("Claimed {0} job(s) for execution", jobCount);
  }

  private long pollOnce() {
    if (drainController.isDraining()) {
      log.debug("Poller skipping due to drain mode");
      return strategy.getCurrentDelay();
    }

    long pollStartTime = System.currentTimeMillis();
    if (!hasAvailableCapacity()) {
      updateSystemLoadFactor();
      return strategy.recordPollResult(0, pollStartTime);
    }

    List<JobClaimDto> claims;
    try {
      claims = claimJobsWithCircuitBreaker();
    } catch (RatchetTransientStoreException e) {
      publishClaimBreakerState();
      return handleTransientClaimFailure(pollStartTime, e);
    } catch (ServiceUnavailableException e) {
      publishClaimBreakerState();
      return handleOpenCircuit(pollStartTime, e);
    }
    publishClaimBreakerState();

    int jobCount = claims.size();
    if (jobCount > 0) {
      handleJobsFound(claims, jobCount);
    }

    updateSystemLoadFactor();

    if (timeoutHandler != null) {
      try {
        timeoutHandler.scanSignalTimeouts();
      } catch (Exception e) {
        log.warnf("Signal timeout scan failed: %s", e.getMessage());
      }
    }

    long nextDelay = strategy.recordPollResult(jobCount, pollStartTime);

    log.debugf("Poll completed: claimed %d job(s), next delay %d ms", (long) jobCount, nextDelay);

    return nextDelay;
  }

  private boolean hasAvailableCapacity() {
    for (JobExecutionType jobType : POLLER_EXECUTABLE_TYPES) {
      if (threadPoolManager.getAvailableCapacity(jobType) > 0) {
        return true;
      }
    }
    return false;
  }

  private List<JobClaimDto> claimJobsByTypeBudget() {
    NodeTagFilter tagFilter =
        tagAffinityProvider != null ? tagAffinityProvider.tagFilter() : NodeTagFilter.NONE;
    List<JobClaimDto> claims = new ArrayList<>();
    String nodeId = nodeIdProvider.getNodeId();
    for (JobExecutionType jobType : POLLER_EXECUTABLE_TYPES) {
      int availableCapacity = threadPoolManager.getAvailableCapacity(jobType);
      if (availableCapacity <= 0) {
        continue;
      }
      int claimLimit = computeClaimLimit(availableCapacity);
      try {
        List<JobClaimDto> claimed =
            jobClaimStore.claimNextBatchOptimized(jobType, claimLimit, nodeId, tagFilter);
        if (metricsCollector != null && !claimed.isEmpty()) {
          metricsCollector.jobsClaimed(jobType.name(), claimed.size());
        }
        claims.addAll(claimed);
      } catch (RatchetTransientStoreException e) {
        if (metricsCollector != null) {
          metricsCollector.claimTransientFailure(jobType.name());
        }
        throw e;
      }
    }
    return claims;
  }

  private List<JobClaimDto> claimJobsWithCircuitBreaker() {
    if (!claimCircuitBreakerEnabled || claimCircuitBreaker == null) {
      return claimJobsByTypeBudget();
    }
    try {
      return claimCircuitBreaker.execute(this::claimJobsByTypeBudget);
    } catch (Exception e) {
      if (e instanceof RatchetTransientStoreException transientStoreException) {
        throw transientStoreException;
      }
      if (e instanceof ServiceUnavailableException serviceUnavailableException) {
        throw serviceUnavailableException;
      }
      if (e instanceof RuntimeException runtimeException) {
        throw runtimeException;
      }
      throw new RuntimeException("Unexpected checked exception while claiming jobs", e);
    }
  }

  private long handleTransientClaimFailure(long pollStartTime, RatchetTransientStoreException e) {
    updateSystemLoadFactor();
    log.warnf("Transient claim store failure: %s", e.getMessage());
    long baseDelay = strategy.recordPollResult(0, pollStartTime);
    return Math.min(options.polling().maxDelayMs(), Math.max(baseDelay, 1L) * 2L);
  }

  private long handleOpenCircuit(long pollStartTime, ServiceUnavailableException e) {
    updateSystemLoadFactor();
    log.warnf("Claim path circuit breaker open: %s", e.getMessage());
    long baseDelay = strategy.recordPollResult(0, pollStartTime);
    long breakerDelay =
        claimCircuitBreaker != null ? claimCircuitBreaker.getWaitDurationMs() : baseDelay;
    return Math.min(options.polling().maxDelayMs(), Math.max(baseDelay, breakerDelay));
  }

  private int computeClaimLimit(int availableCapacity) {
    int immediateLimit = Math.min(batchSize, availableCapacity);
    if (claimHeadroomFactor <= 0 || immediateLimit >= batchSize) {
      return immediateLimit;
    }
    int reserve = Math.min(batchSize - immediateLimit, availableCapacity * claimHeadroomFactor);
    return immediateLimit + reserve;
  }

  private void updateSystemLoadFactor() {
    double totalUtilization = 0;
    int poolCount = 0;

    var healthMap = threadPoolManager.getThreadPoolHealth();
    for (var health : healthMap.values()) {
      if (!health.isVirtual()) {
        totalUtilization += health.getUtilizationPercent();
        poolCount++;
      }
    }

    if (poolCount > 0) {
      double avgUtilization = totalUtilization / poolCount;
      strategy.updateSystemLoadFactor(avgUtilization);
    }
  }

  private void publishClaimBreakerState() {
    if (metricsCollector != null && claimCircuitBreakerEnabled && claimCircuitBreaker != null) {
      metricsCollector.pollerBreakerState(
          CLAIM_BREAKER_NAME, claimCircuitBreaker.getState().name());
    }
  }
}
