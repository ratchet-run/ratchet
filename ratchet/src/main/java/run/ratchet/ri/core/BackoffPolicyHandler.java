package run.ratchet.ri.core;

import run.ratchet.api.BackoffPolicy;

/**
 * Computes retry back-off delays. {@code attempts} is the <em>next</em> attempt number (1-based).
 * EXPONENTIAL delay = {@code baseMs * 2^(attempts-1)}, capped at 24 hours.
 *
 * @see BackoffPolicy
 */
public final class BackoffPolicyHandler {

  /** 24 hours in milliseconds — upper bound for exponential delay. */
  private static final long MAX_EXPONENTIAL_DELAY_MS = 86_400_000L;

  /** 2^20 * 1000ms ≈ 17 min; caps the exponent to prevent long overflow. */
  private static final int MAX_EXPONENT = 20;

  private BackoffPolicyHandler() {
    /* util */
  }

  public static long computeDelay(BackoffPolicy policy, int baseMs, int attempts) {
    return switch (policy) {
      case NONE -> 0L;
      case FIXED -> baseMs;
      case EXPONENTIAL -> {
        int cappedExponent = Math.min(attempts - 1, MAX_EXPONENT);
        long multiplier =
            1L << cappedExponent; // 2^cappedExponent using bit shift (no floating point)
        long exponentialDelay =
            (multiplier > 0 && baseMs <= MAX_EXPONENTIAL_DELAY_MS / multiplier)
                ? baseMs * multiplier
                : MAX_EXPONENTIAL_DELAY_MS;
        yield Math.min(exponentialDelay, MAX_EXPONENTIAL_DELAY_MS);
      }
    };
  }
}
