package run.ratchet.spi;

import run.ratchet.api.Incubating;

/**
 * Initial settings used to create an adaptive polling delay strategy.
 *
 * @param burstDelayMs short delay used immediately after wakeups
 * @param minDelayMs minimum steady-state poll delay
 * @param maxDelayMs maximum backoff delay before deep idle
 * @param deepIdleDelayMs delay used after the poller has been idle long enough
 * @param deepIdleThresholdMs idle time required before entering deep idle
 * @param idleThreshold number of idle polls before increasing delay
 * @param batchSize target claim batch size used to interpret full vs partial polls
 * @apiNote Ratchet validates this configuration before constructing its built-in strategy: delays
 *     must be non-negative, {@code minDelayMs <= maxDelayMs <= deepIdleDelayMs}, {@code batchSize}
 *     must be positive, and {@code idleThreshold} must be non-negative.
 */
@Incubating
public record PollingConfig(
    long burstDelayMs,
    long minDelayMs,
    long maxDelayMs,
    long deepIdleDelayMs,
    long deepIdleThresholdMs,
    int idleThreshold,
    int batchSize) {

  public PollingConfig {
    if (burstDelayMs < 0) {
      throw new IllegalArgumentException("burstDelayMs must be non-negative");
    }
    if (minDelayMs < 0) {
      throw new IllegalArgumentException("minDelayMs must be non-negative");
    }
    if (maxDelayMs < 0) {
      throw new IllegalArgumentException("maxDelayMs must be non-negative");
    }
    if (deepIdleDelayMs < 0) {
      throw new IllegalArgumentException("deepIdleDelayMs must be non-negative");
    }
    if (deepIdleThresholdMs < 0) {
      throw new IllegalArgumentException("deepIdleThresholdMs must be non-negative");
    }
    if (minDelayMs > maxDelayMs) {
      throw new IllegalArgumentException("minDelayMs must be less than or equal to maxDelayMs");
    }
    if (maxDelayMs > deepIdleDelayMs) {
      throw new IllegalArgumentException(
          "maxDelayMs must be less than or equal to deepIdleDelayMs");
    }
    if (idleThreshold < 0) {
      throw new IllegalArgumentException("idleThreshold must be non-negative");
    }
    if (batchSize <= 0) {
      throw new IllegalArgumentException("batchSize must be positive");
    }
  }
}
