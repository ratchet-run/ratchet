package run.ratchet.ri.core;

import run.ratchet.api.exception.RatchetTransientStoreException;
import run.ratchet.ri.util.RatchetConfiguration;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.spi.PollingConfig;
import run.ratchet.spi.PollingDelayStrategy;
import run.ratchet.spi.PollingStrategyProvider;
import run.ratchet.store.dto.JobClaimDto;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.spi.JobClaimStore;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.jboss.logging.Logger;

/**
 * Claims pending jobs from the store in batches and submits them to {@link
 * JobExecutionCoordinator}. Polling frequency adapts based on job availability and thread-pool
 * utilization; see {@link PollingStrategy} for details.
 */
public class Poller {

  private static final Logger log = Logger.getLogger(Poller.class);
  private static final JobExecutionType[] POLLER_EXECUTABLE_TYPES = {
    JobExecutionType.SINGLE,
    JobExecutionType.BATCH_CHILD,
    JobExecutionType.CHAIN_STEP,
    JobExecutionType.WORKFLOW_BRANCH
  };

  private final AtomicBoolean started = new AtomicBoolean();
  private final AtomicBoolean running = new AtomicBoolean();

  private final JobClaimStore jobClaimStore;
  private final JobExecutionCoordinator jobExecutionCoordinator;
  private final NodeIdentityProvider nodeIdProvider;
  private final ThreadPoolManager threadPoolManager;
  private final DrainController drainController;
  private final PollerScheduler pollerScheduler;
  private final RatchetConfiguration config;
  private final MetricsCollector metricsCollector;
  private final PollingStrategyProvider pollingStrategyProvider;
  private final int batchSize;

  @SuppressWarnings("java:S3077")
  private volatile PollingDelayStrategy strategy;

  protected Poller() {
    this.jobClaimStore = null;
    this.jobExecutionCoordinator = null;
    this.nodeIdProvider = null;
    this.threadPoolManager = null;
    this.drainController = null;
    this.pollerScheduler = null;
    this.config = null;
    this.metricsCollector = null;
    this.pollingStrategyProvider = null;
    this.batchSize = 0;
  }

  public Poller(
      JobClaimStore jobClaimStore,
      JobExecutionCoordinator jobExecutionCoordinator,
      NodeIdentityProvider nodeIdProvider,
      ThreadPoolManager threadPoolManager,
      DrainController drainController,
      PollerScheduler pollerScheduler,
      RatchetConfiguration config,
      MetricsCollector metricsCollector,
      PollingStrategyProvider pollingStrategyProvider,
      int batchSize) {
    this.jobClaimStore = jobClaimStore;
    this.jobExecutionCoordinator = jobExecutionCoordinator;
    this.nodeIdProvider = nodeIdProvider;
    this.threadPoolManager = threadPoolManager;
    this.drainController = drainController;
    this.pollerScheduler = pollerScheduler;
    this.config = config;
    this.metricsCollector = metricsCollector;
    this.pollingStrategyProvider = pollingStrategyProvider;
    this.batchSize = batchSize;
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
                config.getPollerBurstDelayMs(),
                config.getPollerMinDelayMs(),
                config.getPollerMaxDelayMs(),
                config.getPollerDeepIdleDelayMs(),
                config.getPollerDeepIdleThresholdMs(),
                config.getPollerIdleThreshold(),
                batchSize));

    pollerScheduler.start();

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
      claims = claimJobsByTypeBudget();
    } catch (RatchetTransientStoreException e) {
      return handleTransientClaimFailure(pollStartTime, e);
    }

    int jobCount = claims.size();
    if (jobCount > 0) {
      handleJobsFound(claims, jobCount);
    }

    updateSystemLoadFactor();

    long nextDelay = strategy.recordPollResult(jobCount, pollStartTime);

    log.debugf("Poll completed: claimed %s job(s), next delay %s ms", jobCount, nextDelay);

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
    List<JobClaimDto> claims = new ArrayList<>();
    String nodeId = nodeIdProvider.getNodeId();
    for (JobExecutionType jobType : POLLER_EXECUTABLE_TYPES) {
      int availableCapacity = threadPoolManager.getAvailableCapacity(jobType);
      if (availableCapacity <= 0) {
        continue;
      }
      int claimLimit = Math.min(batchSize, availableCapacity);
      try {
        List<JobClaimDto> claimed = jobClaimStore.claimNextBatchOptimized(jobType, claimLimit, nodeId);
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

  private long handleTransientClaimFailure(long pollStartTime, RatchetTransientStoreException e) {
    updateSystemLoadFactor();
    log.warnf("Transient claim store failure: %s", e.getMessage());
    long baseDelay = strategy.recordPollResult(0, pollStartTime);
    return Math.min(config.getPollerMaxDelayMs(), Math.max(baseDelay, 1L) * 2L);
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
}
