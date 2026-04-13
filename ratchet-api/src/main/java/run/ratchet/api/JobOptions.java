package run.ratchet.api;

import java.time.Duration;

/**
 * Immutable configuration for job execution: priority, retry behavior, backoff strategy, and
 * timeout. Use {@link #defaults()} and the {@code with*} methods to build custom configurations.
 *
 * @param priority the job execution priority
 * @param maxRetries maximum number of retry attempts (0 = no retries)
 * @param backoffPolicy the retry delay strategy
 * @param backoffParam the base duration for backoff calculations
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
   * timeout enforcement.
   */
  public JobOptions withTimeout(Duration t) {
    return new JobOptions(priority, maxRetries, backoffPolicy, backoffParam, (int) t.toSeconds());
  }
}
