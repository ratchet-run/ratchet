package run.ratchet.ri.core;

import run.ratchet.ri.util.RatchetConfiguration;
import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.store.dto.JobClaimDto;
import run.ratchet.store.spi.JobClaimStore;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.jboss.logging.Logger;

/**
 * High-performance job polling engine that claims pending jobs from the database for execution by
 * the job execution coordinator.
 *
 * <p>This service implements sophisticated adaptive algorithms to optimize polling frequency based
 * on system load and job availability patterns. The algorithm logic is delegated to {@link
 * PollingStrategy} for testability.
 *
 * <p>The Poller is the heart of the job scheduler's pull-based architecture:
 *
 * <ul>
 *   <li><b>Pull Model:</b> Workers pull jobs when ready, enabling natural backpressure
 *   <li><b>Batch Processing:</b> Claims multiple jobs per poll to reduce database round trips
 *   <li><b>Adaptive Timing:</b> Dynamically adjusts polling frequency based on job availability
 *   <li><b>Self-Healing:</b> Automatically recovers from crashes
 * </ul>
 *
 * @see PollingStrategy for the adaptive algorithm implementation
 * @see PollerScheduler for scheduling infrastructure
 */
public class Poller {

  private static final Logger log = Logger.getLogger(Poller.class);

  private final AtomicBoolean started = new AtomicBoolean();
  private final AtomicBoolean running = new AtomicBoolean();

  private final JobClaimStore jobClaimStore;
  private final JobExecutionCoordinator jobExecutionCoordinator;
  private final NodeIdentityProvider nodeIdProvider;
  private final ThreadPoolManager threadPoolManager;
  private final DrainController drainController;
  private final PollerScheduler pollerScheduler;
  private final RatchetConfiguration config;
  private final int batchSize;

  @SuppressWarnings("java:S3077")
  private volatile PollingStrategy strategy;

  // Required by CDI proxy
  protected Poller() {
    this.jobClaimStore = null;
    this.jobExecutionCoordinator = null;
    this.nodeIdProvider = null;
    this.threadPoolManager = null;
    this.drainController = null;
    this.pollerScheduler = null;
    this.config = null;
    this.batchSize = 0;
  }

  /**
   * Creates a new Poller.
   *
   * @param jobClaimStore store for atomic batch claiming operations
   * @param jobExecutionCoordinator coordinator for job execution dispatch
   * @param nodeIdProvider provides the unique node identifier
   * @param threadPoolManager manages thread pools and utilization metrics
   * @param drainController controls drain mode during graceful shutdown
   * @param pollerScheduler handles scheduling of poll cycles
   * @param config Ratchet configuration for poller tuning parameters
   * @param batchSize maximum number of jobs to claim per poll cycle
   */
  public Poller(
      JobClaimStore jobClaimStore,
      JobExecutionCoordinator jobExecutionCoordinator,
      NodeIdentityProvider nodeIdProvider,
      ThreadPoolManager threadPoolManager,
      DrainController drainController,
      PollerScheduler pollerScheduler,
      RatchetConfiguration config,
      int batchSize) {
    this.jobClaimStore = jobClaimStore;
    this.jobExecutionCoordinator = jobExecutionCoordinator;
    this.nodeIdProvider = nodeIdProvider;
    this.threadPoolManager = threadPoolManager;
    this.drainController = drainController;
    this.pollerScheduler = pollerScheduler;
    this.config = config;
    this.batchSize = batchSize;
  }

  /**
   * Gets current polling statistics for monitoring and debugging.
   *
   * @return PollingStats containing current delay, idle count, load metrics
   */
  public PollingStrategy.PollingStats getPollingStats() {
    return strategy != null ? strategy.getStats() : null;
  }

  /**
   * Initializes the poller and starts the scheduling loop.
   *
   * <p>This method must be called explicitly after database migrations have completed.
   */
  public void init() {
    if (!started.compareAndSet(false, true)) {
      log.warn("Poller already initialized; skipping re-init");
      return;
    }

    this.strategy =
        new PollingStrategy(
            config.getPollerBurstDelayMs(),
            config.getPollerMinDelayMs(),
            config.getPollerMaxDelayMs(),
            config.getPollerDeepIdleDelayMs(),
            config.getPollerDeepIdleThresholdMs(),
            config.getPollerIdleThreshold(),
            batchSize);

    pollerScheduler.start();

    log.infof("Poller initialized (batch=%s)", batchSize);
  }

  /**
   * Called when a wakeup signal is received (e.g., from cluster notification). Resets the strategy
   * to minimum delay and exits deep idle mode.
   */
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

  /**
   * Marks the poller as stopped and stops the underlying scheduler. Called during graceful
   * shutdown.
   */
  public void stop() {
    started.set(false);
    pollerScheduler.stop();
    log.info("Poller marked as stopped");
  }

  /**
   * Executes a single poll cycle: claims jobs from the database and submits them to the job
   * execution coordinator.
   *
   * @return the recommended delay in milliseconds before the next poll
   */
  @SuppressWarnings("java:S1181")
  public long tick() {
    if (!started.get()) {
      return strategy != null ? strategy.getCurrentDelay() : 1000;
    }

    if (!running.compareAndSet(false, true)) {
      log.warn("tick() already running, skipping overlapping call");
      PollingStrategy local = strategy;
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
    List<JobClaimDto> claims =
        jobClaimStore.claimNextBatchOptimized(batchSize, nodeIdProvider.getNodeId());
    int jobCount = claims.size();

    if (jobCount > 0) {
      handleJobsFound(claims, jobCount);
    }

    updateSystemLoadFactor();

    long nextDelay = strategy.recordPollResult(jobCount, pollStartTime);

    log.debugf("Poll completed: claimed %s job(s), next delay %s ms", jobCount, nextDelay);

    return nextDelay;
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
