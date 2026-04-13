package run.ratchet.ri.core;

import run.ratchet.api.BackoffPolicy;

/**
 * Utility class that calculates the next back-off delay for a retry attempt.
 *
 * <p>This handler implements multiple backoff strategies to control the timing of retry attempts
 * after job failures. The delay calculation considers:
 *
 * <ul>
 *   <li>The configured {@link BackoffPolicy} (NONE, FIXED, or EXPONENTIAL)
 *   <li>A base delay in milliseconds that serves as the foundation for calculations
 *   <li>The current attempt number to enable progressive delay increases
 * </ul>
 *
 * <p><b>Important:</b> The {@code attempts} parameter is the <em>next</em> attempt number
 * (1-based). For example, if a job has already failed once, the next call comes with {@code
 * attempts==2}.
 *
 * <p>Backoff strategies:
 *
 * <ul>
 *   <li><b>NONE:</b> No delay between retries (immediate retry)
 *   <li><b>FIXED:</b> Constant delay equal to the base delay for every attempt
 *   <li><b>EXPONENTIAL:</b> Delay doubles with each attempt: baseMs * 2^(attempts-1)
 * </ul>
 *
 * <p>Thread Safety: This class is stateless and thread-safe.
 *
 * @see BackoffPolicy for available backoff strategies
 */
public final class BackoffPolicyHandler {

  /**
   * Maximum delay cap for exponential backoff (24 hours in milliseconds). Prevents overflow and
   * unreasonably long delays for high attempt counts.
   */
  private static final long MAX_EXPONENTIAL_DELAY_MS = 86_400_000L;

  /**
   * Maximum exponent to use in exponential backoff calculation. 2^20 = 1,048,576 which, multiplied
   * by a reasonable base (e.g., 1000ms), gives ~17 minutes before the cap kicks in.
   */
  private static final int MAX_EXPONENT = 20;

  private BackoffPolicyHandler() {
    /* util */
  }

  /**
   * Computes the delay for the next retry attempt based on the specified backoff policy.
   *
   * @param policy the {@link BackoffPolicy} strategy (NONE, FIXED, or EXPONENTIAL)
   * @param baseMs the base delay in milliseconds
   * @param attempts the next attempt number (1-based); e.g. 2 means one prior failure
   * @return the computed delay in milliseconds
   */
  public static long computeDelay(BackoffPolicy policy, int baseMs, int attempts) {
    return switch (policy) {
      case NONE -> 0L;
      case FIXED -> baseMs;
      case EXPONENTIAL -> {
        // Cap the exponent to prevent overflow with large attempt numbers
        int cappedExponent = Math.min(attempts - 1, MAX_EXPONENT);
        long multiplier =
            1L << cappedExponent; // 2^cappedExponent using bit shift (no floating point)
        // Guard against long overflow: if baseMs * multiplier would overflow, use the cap directly
        long exponentialDelay =
            (multiplier > 0 && baseMs <= MAX_EXPONENTIAL_DELAY_MS / multiplier)
                ? baseMs * multiplier
                : MAX_EXPONENTIAL_DELAY_MS;
        // Cap the total delay at MAX_EXPONENTIAL_DELAY_MS (24 hours)
        yield Math.min(exponentialDelay, MAX_EXPONENTIAL_DELAY_MS);
      }
    };
  }
}
