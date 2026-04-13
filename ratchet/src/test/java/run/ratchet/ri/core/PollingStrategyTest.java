package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PollingStrategyTest {

  // Short config values for fast, deterministic tests
  private static final long BURST_DELAY = 100;
  private static final long MIN_DELAY = 500;
  private static final long MAX_DELAY = 5000;
  private static final long DEEP_IDLE_DELAY = 8000;
  private static final long DEEP_IDLE_THRESHOLD = 2000;
  private static final int IDLE_THRESHOLD = 3;
  private static final int BATCH_SIZE = 10;

  private PollingStrategy strategy;

  @BeforeEach
  void setUp() {
    strategy =
        new PollingStrategy(
            BURST_DELAY,
            MIN_DELAY,
            MAX_DELAY,
            DEEP_IDLE_DELAY,
            DEEP_IDLE_THRESHOLD,
            IDLE_THRESHOLD,
            BATCH_SIZE);
  }

  @Test
  void initialDelay_equalsMinDelay() {
    assertEquals(MIN_DELAY, strategy.getCurrentDelay());
  }

  @Test
  void initialState_notInDeepIdleOrBurst() {
    assertFalse(strategy.isInDeepIdle());
    assertFalse(strategy.isInBurstMode());
  }

  @Test
  void onWakeup_entersBurstMode() {
    strategy.onWakeup();
    assertTrue(strategy.isInBurstMode());
    assertEquals(BURST_DELAY, strategy.getCurrentDelay());
  }

  @Test
  void onWakeup_exitsDeepIdle() {
    // Force deep idle: pollStartTime must be far enough ahead of lastJobFoundTime (set at
    // construction)
    long futureTime = System.currentTimeMillis() + DEEP_IDLE_THRESHOLD + 1;
    strategy.recordPollResult(0, futureTime);
    assertTrue(strategy.isInDeepIdle());

    strategy.onWakeup();
    assertFalse(strategy.isInDeepIdle());
    assertTrue(strategy.isInBurstMode());
  }

  @Test
  void burstMode_exitsAfterIdleThresholdEmptyPolls() {
    strategy.onWakeup();
    assertTrue(strategy.isInBurstMode());

    long now = System.currentTimeMillis();
    // Send enough empty polls to exit burst mode
    for (int i = 0; i < IDLE_THRESHOLD; i++) {
      strategy.recordPollResult(0, now);
    }
    assertFalse(strategy.isInBurstMode());
    assertEquals(MIN_DELAY, strategy.getCurrentDelay());
  }

  @Test
  void deepIdle_enteredAfterThresholdElapsed() {
    // pollStartTime must exceed lastJobFoundTime by deepIdleThreshold
    long futureTime = System.currentTimeMillis() + DEEP_IDLE_THRESHOLD + 1;
    strategy.recordPollResult(0, futureTime);

    assertTrue(strategy.isInDeepIdle());
    assertEquals(DEEP_IDLE_DELAY, strategy.getCurrentDelay());
  }

  @Test
  void deepIdle_exitedOnJobsFound() {
    long futureTime = System.currentTimeMillis() + DEEP_IDLE_THRESHOLD + 1;
    strategy.recordPollResult(0, futureTime);
    assertTrue(strategy.isInDeepIdle());

    strategy.recordPollResult(5, futureTime + 1);
    assertFalse(strategy.isInDeepIdle());
  }

  @Test
  void jobsFound_resetsIdleCount() {
    long now = System.currentTimeMillis();
    // Accumulate some idle polls (but not enough to trigger backoff)
    strategy.recordPollResult(0, now);
    strategy.recordPollResult(0, now);

    // Finding jobs should reset idle state
    strategy.recordPollResult(5, now);

    PollingStrategy.PollingStats stats = strategy.getStats();
    assertEquals(0, stats.currentIdleCount());
  }

  @Test
  void consecutiveFullBatches_reducesDelay() {
    long now = System.currentTimeMillis();
    // Send several full batches (jobCount == batchSize)
    for (int i = 0; i < 4; i++) {
      strategy.recordPollResult(BATCH_SIZE, now);
    }

    long delay = strategy.getCurrentDelay();
    // After 4 consecutive full batches (>= threshold of 3), delay should be aggressive
    assertTrue(
        delay < MIN_DELAY,
        "Delay should be less than minDelay after consecutive full batches, got " + delay);
  }

  @Test
  void nonFullBatch_resetsConsecutiveFullBatchCounter() {
    long now = System.currentTimeMillis();
    strategy.recordPollResult(BATCH_SIZE, now);
    strategy.recordPollResult(BATCH_SIZE, now);
    strategy.recordPollResult(BATCH_SIZE, now);

    // Partial batch resets the counter
    strategy.recordPollResult(3, now);

    PollingStrategy.PollingStats stats = strategy.getStats();
    assertEquals(0, stats.consecutiveFullBatches());
  }

  @Test
  void highLoadFactor_reducesDelay() {
    long now = System.currentTimeMillis();
    // Fill rolling window with high-load jobs (> batchSize * 0.8 = 8)
    for (int i = 0; i < 10; i++) {
      strategy.recordPollResult(9, now);
    }
    long highLoadDelay = strategy.getCurrentDelay();

    // Reset and test with low load
    PollingStrategy lowStrategy =
        new PollingStrategy(
            BURST_DELAY,
            MIN_DELAY,
            MAX_DELAY,
            DEEP_IDLE_DELAY,
            DEEP_IDLE_THRESHOLD,
            IDLE_THRESHOLD,
            BATCH_SIZE);
    for (int i = 0; i < 10; i++) {
      lowStrategy.recordPollResult(1, now);
    }
    long lowLoadDelay = lowStrategy.getCurrentDelay();

    assertTrue(
        highLoadDelay < lowLoadDelay,
        "High load delay ("
            + highLoadDelay
            + ") should be less than low load delay ("
            + lowLoadDelay
            + ")");
  }

  @Test
  void updateSystemLoadFactor_affectsDelay() {
    long now = System.currentTimeMillis();

    // High utilization → higher load factor → shorter delay (baseDelay / loadFactor)
    strategy.updateSystemLoadFactor(100.0);
    strategy.recordPollResult(5, now);
    long highUtilDelay = strategy.getCurrentDelay();

    PollingStrategy lowUtilStrategy =
        new PollingStrategy(
            BURST_DELAY,
            MIN_DELAY,
            MAX_DELAY,
            DEEP_IDLE_DELAY,
            DEEP_IDLE_THRESHOLD,
            IDLE_THRESHOLD,
            BATCH_SIZE);
    lowUtilStrategy.updateSystemLoadFactor(0.0);
    lowUtilStrategy.recordPollResult(5, now);
    long lowUtilDelay = lowUtilStrategy.getCurrentDelay();

    assertTrue(
        highUtilDelay < lowUtilDelay,
        "High utilization delay ("
            + highUtilDelay
            + ") should be less than low utilization delay ("
            + lowUtilDelay
            + ")");
  }

  @Test
  void delay_neverBelowAbsoluteMinimum() {
    strategy.onWakeup();
    // Even in burst mode with full batches, should not go below 50ms
    long now = System.currentTimeMillis();
    strategy.updateSystemLoadFactor(100.0);
    for (int i = 0; i < 5; i++) {
      strategy.recordPollResult(BATCH_SIZE, now);
    }
    assertTrue(
        strategy.getCurrentDelay() >= 50, "Delay must never go below absolute minimum of 50ms");
  }

  @Test
  void delay_neverExceedsMaxDelay() {
    long now = System.currentTimeMillis();
    // Many empty polls with aggressive backoff
    strategy.updateSystemLoadFactor(0.0);
    for (int i = 0; i < 50; i++) {
      strategy.recordPollResult(0, now);
    }
    assertTrue(
        strategy.getCurrentDelay() <= MAX_DELAY,
        "Delay must not exceed maxDelayMs, got " + strategy.getCurrentDelay());
  }

  @Test
  void defaultConstructor_usesReasonableDefaults() {
    PollingStrategy defaultStrategy = new PollingStrategy();
    assertEquals(2000L, defaultStrategy.getCurrentDelay());
    assertFalse(defaultStrategy.isInDeepIdle());
    assertFalse(defaultStrategy.isInBurstMode());
  }

  @Test
  void getStats_returnsConsistentSnapshot() {
    PollingStrategy.PollingStats stats = strategy.getStats();
    assertEquals(MIN_DELAY, stats.currentDelayMs());
    assertEquals(0, stats.currentIdleCount());
    assertEquals(0, stats.consecutiveFullBatches());
    assertFalse(stats.inDeepIdle());
    assertFalse(stats.inBurstMode());
  }
}
