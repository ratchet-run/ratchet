package run.ratchet.api;

import java.time.Duration;
import java.util.Objects;

/**
 * Immutable configuration for job execution: priority, retry behavior, backoff strategy, and
 * timeout. Use {@link #defaults()} and the {@code with*} methods to build custom configurations.
 *
 * @param maxRetries maximum number of retry attempts (0 = no retries)
 * @param timeoutSec maximum execution time in seconds (0 = no timeout)
 * @see JobBuilder
 * @see BackoffPolicy
 */
public record JobOptions(
    JobPriority priority,
    int maxRetries,
    BackoffPolicy backoffPolicy,
    Duration backoffParam,
    int timeoutSec) {

  public JobOptions {
    priority = Objects.requireNonNull(priority, "priority must not be null");
    backoffPolicy = Objects.requireNonNull(backoffPolicy, "backoffPolicy must not be null");
    backoffParam = Objects.requireNonNull(backoffParam, "backoffParam must not be null");
    if (maxRetries < 0) {
      throw new IllegalArgumentException("maxRetries must be >= 0");
    }
    if (timeoutSec < 0) {
      throw new IllegalArgumentException("timeoutSec must be >= 0");
    }
  }

  /** Returns a JobOptions with NORMAL priority, no retries, no backoff, and no timeout. */
  public static JobOptions defaults() {
    return new JobOptions(JobPriority.NORMAL, 0, BackoffPolicy.NONE, Duration.ZERO, 0);
  }

  /** Returns a copy with the specified backoff policy and base delay. */
  public JobOptions withBackoff(BackoffPolicy bp, Duration param) {
    return new JobOptions(priority, maxRetries, bp, param, timeoutSec);
  }

  /** Returns a copy with the specified maximum retry count (must be &gt;= 0). */
  public JobOptions withMaxRetries(int r) {
    return new JobOptions(priority, r, backoffPolicy, backoffParam, timeoutSec);
  }

  /** Returns a copy with the specified execution priority. */
  public JobOptions withPriority(JobPriority p) {
    return new JobOptions(p, maxRetries, backoffPolicy, backoffParam, timeoutSec);
  }

  /**
   * Returns a copy with the specified execution timeout. Use {@link Duration#ZERO} to disable
   * timeout enforcement. The timeout is stored in whole seconds and must fit in a signed 32-bit
   * integer.
   *
   * @throws NullPointerException if {@code t} is null
   * @throws IllegalArgumentException if {@code t} is negative or exceeds {@link Integer#MAX_VALUE}
   *     seconds
   */
  public JobOptions withTimeout(Duration t) {
    long timeoutSeconds = Objects.requireNonNull(t, "timeout must not be null").toSeconds();
    if (timeoutSeconds < 0) {
      throw new IllegalArgumentException("timeout must be >= 0 seconds");
    }
    if (timeoutSeconds > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("timeout must be <= " + Integer.MAX_VALUE + " seconds");
    }
    return new JobOptions(priority, maxRetries, backoffPolicy, backoffParam, (int) timeoutSeconds);
  }
}
