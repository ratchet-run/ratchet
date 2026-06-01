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
package run.ratchet.ri.core.internal;

import java.util.function.LongSupplier;
import run.ratchet.spi.PollingDelayStrategy;

/**
 * Stateful, CDI-free computation of adaptive polling delays. Tracks a rolling window of job counts,
 * thread-pool load, idle depth, and burst mode to determine the next poll interval.
 *
 * @see Poller
 */
public class PollingStrategy implements PollingDelayStrategy {

  private static final long ABSOLUTE_MIN_DELAY_MS = 50;
  private static final long AGGRESSIVE_BACKOFF_THRESHOLD_MS = 30_000;
  private static final int ROLLING_WINDOW_SIZE = 10;
  private static final double HIGH_LOAD_THRESHOLD = 0.8;
  private static final double LOW_LOAD_THRESHOLD = 0.2;
  private static final long FLOOR_DELAY_MS = 100;
  private static final int CONSECUTIVE_FULL_BATCH_THRESHOLD = 3;

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
  private final LongSupplier clockMillis;

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

  public PollingStrategy(
      long burstDelayMs,
      long minDelayMs,
      long maxDelayMs,
      long deepIdleDelayMs,
      long deepIdleThresholdMs,
      int idleThreshold,
      int batchSize) {
    this(
        burstDelayMs,
        minDelayMs,
        maxDelayMs,
        deepIdleDelayMs,
        deepIdleThresholdMs,
        idleThreshold,
        batchSize,
        System::currentTimeMillis);
  }

  public PollingStrategy(
      long burstDelayMs,
      long minDelayMs,
      long maxDelayMs,
      long deepIdleDelayMs,
      long deepIdleThresholdMs,
      int idleThreshold,
      int batchSize,
      LongSupplier clockMillis) {
    this.burstDelayMs = burstDelayMs;
    this.minDelayMs = minDelayMs;
    this.maxDelayMs = maxDelayMs;
    this.deepIdleDelayMs = deepIdleDelayMs;
    this.deepIdleThresholdMs = deepIdleThresholdMs;
    this.idleThreshold = idleThreshold;
    this.batchSize = batchSize;
    this.clockMillis = clockMillis;
    this.currentDelayMs = minDelayMs;
    this.lastJobFoundTime = clockMillis.getAsLong();
  }

  public synchronized long getCurrentDelay() {
    return currentDelayMs;
  }

  public synchronized PollingStats getStats() {
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
        clockMillis.getAsLong() - lastJobFoundTime,
        inDeepIdle,
        inBurstMode,
        deepIdleThresholdMs);
  }

  public synchronized void onWakeup() {
    lastJobFoundTime = clockMillis.getAsLong();
    inDeepIdle = false;
    inBurstMode = true;
    currentDelayMs = burstDelayMs;
  }

  public synchronized long recordPollResult(int jobCount, long pollStartTime) {
    recentJobCounts[recentJobCountIndex] = jobCount;
    recentJobCountIndex = (recentJobCountIndex + 1) % ROLLING_WINDOW_SIZE;

    if (jobCount == 0) {
      handleNoJobsFound(pollStartTime);
    } else {
      handleJobsFound(jobCount, pollStartTime);
    }

    return currentDelayMs;
  }

  public synchronized void updateSystemLoadFactor(double avgUtilization) {
    this.systemLoadFactor = 0.5 + (avgUtilization / 100.0) * 1.5;
  }

  public synchronized boolean isInDeepIdle() {
    return inDeepIdle;
  }

  public synchronized boolean isInBurstMode() {
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

    if (avgRecentJobs > batchSize * HIGH_LOAD_THRESHOLD) {
      baseDelay = Math.max(effectiveMinDelay / 2, FLOOR_DELAY_MS);
    } else if (avgRecentJobs < batchSize * LOW_LOAD_THRESHOLD && !inBurstMode) {
      baseDelay = effectiveMinDelay * 2;
    }

    if (consecutiveFullBatches >= CONSECUTIVE_FULL_BATCH_THRESHOLD) {
      baseDelay = Math.max(effectiveMinDelay / 4, ABSOLUTE_MIN_DELAY_MS);
    } else if (consecutiveFullBatches >= 1) {
      baseDelay = Math.max(baseDelay / 2, FLOOR_DELAY_MS);
    }

    baseDelay = (long) (baseDelay / systemLoadFactor);
    return baseDelay;
  }

  private void handleJobsFound(int jobCount, long pollStartTime) {
    lastJobFoundTime = pollStartTime;
    idleCount = 0;
    inDeepIdle = false;

    if (jobCount >= batchSize) {
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

  public record PollingStats(
      long currentDelayMs,
      int currentIdleCount,
      int consecutiveFullBatches,
      double systemLoadFactor,
      double avgRecentJobs,
      long timeSinceLastJobMs,
      boolean inDeepIdle,
      boolean inBurstMode,
      long idleThresholdMs) {

    /**
     * @return one of {@code "HIGH"}, {@code "BURST"}, {@code "DEEP_IDLE"}, {@code "IDLE"}, {@code
     *     "NORMAL"}
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
      return timeSinceLastJobMs > idleThresholdMs;
    }
  }
}
