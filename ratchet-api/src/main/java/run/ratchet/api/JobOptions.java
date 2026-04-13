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

  /**
   * Creates a JobOptions instance with default settings.
   *
   * <p>Default configuration provides a basic setup suitable for most jobs: NORMAL priority, no
   * retries, no backoff, and no timeout.
   *
   * @return a new JobOptions instance with default values
   */
  public static JobOptions defaults() {
    return new JobOptions(JobPriority.NORMAL, 0, BackoffPolicy.NONE, Duration.ZERO, 0);
  }

  /**
   * Returns a new JobOptions with the specified backoff configuration.
   *
   * <p>The backoff policy determines how delays are calculated between retry attempts. This is only
   * relevant when maxRetries is greater than 0.
   *
   * @param bp the backoff policy to use for retry delays
   * @param param the base duration for backoff calculations
   * @return a new JobOptions instance with updated backoff settings
   */
  public JobOptions withBackoff(BackoffPolicy bp, Duration param) {
    return new JobOptions(priority, maxRetries, bp, param, timeoutSec);
  }

  /**
   * Returns a new JobOptions with the specified maximum retry count.
   *
   * <p>Sets the maximum number of times a job will be retried after failure. Each retry follows the
   * configured backoff policy.
   *
   * @param r the maximum number of retry attempts (must be >= 0)
   * @return a new JobOptions instance with updated retry limit
   */
  public JobOptions withMaxRetries(int r) {
    return new JobOptions(priority, r, backoffPolicy, backoffParam, timeoutSec);
  }

  /**
   * Returns a new JobOptions with the specified execution priority.
   *
   * <p>Priority affects the order in which jobs are selected for execution when multiple jobs are
   * queued.
   *
   * @param p the job priority level
   * @return a new JobOptions instance with updated priority
   */
  public JobOptions withPriority(JobPriority p) {
    return new JobOptions(p, maxRetries, backoffPolicy, backoffParam, timeoutSec);
  }

  /**
   * Returns a new JobOptions with the specified execution timeout.
   *
   * <p>Jobs that exceed the timeout duration will be forcibly terminated and marked as failed. Use
   * Duration.ZERO or a timeout of 0 seconds to disable timeout enforcement.
   *
   * @param t the maximum execution duration
   * @return a new JobOptions instance with updated timeout
   */
  public JobOptions withTimeout(Duration t) {
    return new JobOptions(priority, maxRetries, backoffPolicy, backoffParam, (int) t.toSeconds());
  }
}
