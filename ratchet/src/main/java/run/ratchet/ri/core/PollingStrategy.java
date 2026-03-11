package run.ratchet.ri.core;

import jakarta.enterprise.context.ApplicationScoped;

/**
 * Pure computation class implementing the adaptive polling delay algorithm.
 *
 * <p>This class encapsulates all the logic for determining optimal polling intervals based on job
 * availability patterns, system load, and idle detection. It has no CDI dependencies, making it
 * easily unit testable.
 *
 * <p>The adaptive algorithm considers multiple factors:
 *
 * <ul>
 *   <li><b>Rolling Window:</b> Tracks job counts over last 10 polls to identify trends
 *   <li><b>Full Batch Detection:</b> Aggressive polling when consecutive full batches indicate high
 *       job availability
 *   <li><b>Load Factor:</b> Adjusts polling based on thread pool utilization
 *   <li><b>Idle Detection:</b> Progressive backoff when no jobs found
 *   <li><b>Deep Idle:</b> Extended delay after prolonged inactivity
 *   <li><b>Burst Mode:</b> Aggressive polling after wakeup notifications
 * </ul>
 *
 * @see Poller
 */
@ApplicationScoped
public class PollingStrategy {

  private static final long ABSOLUTE_MIN_DELAY_MS = 50;
  private static final long AGGRESSIVE_BACKOFF_THRESHOLD_MS = 30_000;
  private static final int ROLLING_WINDOW_SIZE = 10;

  private static final long DEFAULT_BURST_DELAY_MS = 500;
  private static final long DEFAULT_MIN_DELAY_MS = 2000;
  private static final long DEFAULT_MAX_DELAY_MS = 30_000;
  private static final long DEFAULT_DEEP_IDLE_DELAY_MS = 60_000;
  private static final long DEFAULT_DEEP_IDLE_THRESHOLD_MS = 300_000;
  private static final int DEFAULT_IDLE_THRESHOLD = 5;
  private static final int DEFAULT_BATCH_SIZE = 50;

  private final long burstDelayMs;
  private final long minDelayMs;
  private final long maxDelayMs;
  private final long deepIdleDelayMs;
  private final long deepIdleThresholdMs;
  private final int idleThreshold;
  private final int batchSize;

  private final long[] recentJobCounts = new long[ROLLING_WINDOW_SIZE];
  private int recentJobCountIndex = 0;
  private long lastJobFoundTime;
  private int idleCount = 0;
  private int consecutiveFullBatches = 0;
  private double systemLoadFactor = 1.0;
  private boolean inDeepIdle = false;
  private boolean inBurstMode = false;
  private long currentDelayMs;

  /** Creates a PollingStrategy with default configuration. */
  public PollingStrategy() {
    this(
        DEFAULT_BURST_DELAY_MS,
        DEFAULT_MIN_DELAY_MS,
        DEFAULT_MAX_DELAY_MS,
        DEFAULT_DEEP_IDLE_DELAY_MS,
        DEFAULT_DEEP_IDLE_THRESHOLD_MS,
        DEFAULT_IDLE_THRESHOLD,
        DEFAULT_BATCH_SIZE);
  }

  /**
   * Creates a PollingStrategy with explicit configuration (for testing).
   *
   * @param burstDelayMs minimum delay used during burst mode (after wakeup)
   * @param minDelayMs minimum delay for normal steady-state polling
   * @param maxDelayMs maximum delay when backing off
   * @param deepIdleDelayMs delay used in deep idle mode
   * @param deepIdleThresholdMs time since last job before entering deep idle
   * @param idleThreshold number of empty polls before backing off
   * @param batchSize expected batch size for full-batch detection
   */
  public PollingStrategy(
      long burstDelayMs,
      long minDelayMs,
      long maxDelayMs,
      long deepIdleDelayMs,
      long deepIdleThresholdMs,
      int idleThreshold,
      int batchSize) {
    this.burstDelayMs = burstDelayMs;
    this.minDelayMs = minDelayMs;
    this.maxDelayMs = maxDelayMs;
    this.deepIdleDelayMs = deepIdleDelayMs;
    this.deepIdleThresholdMs = deepIdleThresholdMs;
    this.idleThreshold = idleThreshold;
    this.batchSize = batchSize;
    this.currentDelayMs = minDelayMs;
    this.lastJobFoundTime = System.currentTimeMillis();
  }

  /**
   * Returns the current polling delay without modifying state.
   *
   * @return current delay in milliseconds
   */
  public long getCurrentDelay() {
    return currentDelayMs;
  }

  /**
   * Returns current polling statistics for monitoring.
   *
   * @return immutable statistics record
   */
  public PollingStats getStats() {
    double avgRecentJobs = 0;
    for (long count : recentJobCounts) {
      avgRecentJobs += count;
    }
    avgRecentJobs /= ROLLING_WINDOW_SIZE;

    return new PollingStats(
        currentDelayMs,
        idleCount,
        consecutiveFullBatches,
        systemLoadFactor,
        avgRecentJobs,
        System.currentTimeMillis() - lastJobFoundTime,
        inDeepIdle,
        inBurstMode);
  }

  /** Called when the poller receives a wakeup signal. Enters burst mode with aggressive polling. */
  public void onWakeup() {
    lastJobFoundTime = System.currentTimeMillis();
    inDeepIdle = false;
    inBurstMode = true;
    currentDelayMs = burstDelayMs;
  }

  /**
   * Records the result of a poll and calculates the next delay.
   *
   * @param jobCount number of jobs found in the poll
   * @param pollStartTime timestamp when the poll started
   * @return the recommended delay before the next poll in milliseconds
   */
  public long recordPollResult(int jobCount, long pollStartTime) {
    recentJobCounts[recentJobCountIndex] = jobCount;
    recentJobCountIndex = (recentJobCountIndex + 1) % ROLLING_WINDOW_SIZE;

    if (jobCount == 0) {
      handleNoJobsFound(pollStartTime);
    } else {
      handleJobsFound(jobCount, pollStartTime);
    }

    return currentDelayMs;
  }

  /**
   * Updates the system load factor based on thread pool utilization.
   *
   * @param avgUtilization average utilization percentage (0-100) across thread pools
   */
  public void updateSystemLoadFactor(double avgUtilization) {
    this.systemLoadFactor = 0.5 + (avgUtilization / 100.0) * 1.5;
  }

  /**
   * Returns whether the strategy is in deep idle mode.
   *
   * @return true if in deep idle mode
   */
  public boolean isInDeepIdle() {
    return inDeepIdle;
  }

  /**
   * Returns whether the strategy is in burst mode.
   *
   * @return true if in burst mode
   */
  public boolean isInBurstMode() {
    return inBurstMode;
  }

  private void calculateAdaptiveDelay() {
    double avgRecentJobs = 0;
    for (long count : recentJobCounts) {
      avgRecentJobs += count;
    }
    avgRecentJobs /= ROLLING_WINDOW_SIZE;

    long baseDelay = calculateBaseDelay(avgRecentJobs);

    currentDelayMs = Math.max(ABSOLUTE_MIN_DELAY_MS, Math.min(baseDelay, maxDelayMs));
  }

  private long calculateBaseDelay(double avgRecentJobs) {
    long effectiveMinDelay = inBurstMode ? burstDelayMs : minDelayMs;
    long baseDelay = effectiveMinDelay;

    if (avgRecentJobs > batchSize * 0.8) {
      baseDelay = Math.max(effectiveMinDelay / 2, 100);
    } else if (avgRecentJobs < batchSize * 0.2 && !inBurstMode) {
      baseDelay = effectiveMinDelay * 2;
    }

    if (consecutiveFullBatches >= 3) {
      baseDelay = Math.max(effectiveMinDelay / 4, ABSOLUTE_MIN_DELAY_MS);
    } else if (consecutiveFullBatches >= 1) {
      baseDelay = Math.max(baseDelay / 2, 100);
    }

    baseDelay = (long) (baseDelay / systemLoadFactor);
    return baseDelay;
  }

  private void handleJobsFound(int jobCount, long pollStartTime) {
    lastJobFoundTime = pollStartTime;
    idleCount = 0;
    inDeepIdle = false;

    if (jobCount == batchSize) {
      consecutiveFullBatches++;
    } else {
      consecutiveFullBatches = 0;
    }

    calculateAdaptiveDelay();
  }

  @SuppressWarnings("java:S125")
  private void handleNoJobsFound(long pollStartTime) {
    idleCount++;
    consecutiveFullBatches = 0;
    long timeSinceLastJob = pollStartTime - lastJobFoundTime;

    if (inBurstMode && idleCount >= idleThreshold) {
      inBurstMode = false;
      currentDelayMs = minDelayMs;
      idleCount = 0;
      return;
    }

    if (timeSinceLastJob >= deepIdleThresholdMs) {
      if (!inDeepIdle) {
        inDeepIdle = true;
      }
      inBurstMode = false;
      currentDelayMs = deepIdleDelayMs;
      idleCount = 0;
      return;
    }

    if (timeSinceLastJob > AGGRESSIVE_BACKOFF_THRESHOLD_MS) {
      if (idleCount >= idleThreshold / 2) {
        inBurstMode = false;
        currentDelayMs = Math.min(currentDelayMs * 3, maxDelayMs);
        idleCount = 0;
      }
    } else if (!inBurstMode && idleCount >= idleThreshold) {
      currentDelayMs = Math.min(currentDelayMs * 2, maxDelayMs);
      idleCount = 0;
    }
  }

  /**
   * Immutable snapshot of current polling statistics.
   *
   * @param currentDelayMs current delay between polls in milliseconds
   * @param currentIdleCount number of consecutive empty polls
   * @param consecutiveFullBatches number of consecutive polls returning full batches
   * @param systemLoadFactor load multiplier based on thread pool utilization (0.5-2.0)
   * @param avgRecentJobs average job count from the rolling window
   * @param timeSinceLastJobMs milliseconds since the last poll that found jobs
   * @param inDeepIdle whether the strategy is in deep idle mode
   * @param inBurstMode whether the strategy is in burst mode after a wakeup
   */
  public record PollingStats(
      long currentDelayMs,
      int currentIdleCount,
      int consecutiveFullBatches,
      double systemLoadFactor,
      double avgRecentJobs,
      long timeSinceLastJobMs,
      boolean inDeepIdle,
      boolean inBurstMode) {

    /**
     * Returns a human-readable description of the current load state.
     *
     * @return one of "HIGH", "BURST", "DEEP_IDLE", "IDLE", or "NORMAL"
     */
    public String getLoadDescription() {
      if (isHighLoad()) {
        return "HIGH";
      }
      if (inBurstMode) {
        return "BURST";
      }
      if (inDeepIdle) {
        return "DEEP_IDLE";
      }
      if (isIdle()) {
        return "IDLE";
      }
      return "NORMAL";
    }

    public boolean isHighLoad() {
      return consecutiveFullBatches >= 2 || avgRecentJobs > 40;
    }

    public boolean isIdle() {
      return timeSinceLastJobMs > 10000;
    }
  }
}
