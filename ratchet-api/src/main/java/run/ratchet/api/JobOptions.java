package run.ratchet.api;

import java.time.Duration;

/**
 * Immutable configuration record for job execution behavior and policies.
 *
 * <p>JobOptions encapsulates all configurable aspects of job execution, including scheduling
 * priority, retry behavior, timeout settings, and backoff strategies. The record design ensures
 * immutability and thread-safety while providing a fluent API for creating customized
 * configurations.
 *
 * <h2>Configuration Properties:</h2>
 *
 * <dl>
 *   <dt><b>priority</b>
 *   <dd>Determines execution order when multiple jobs are queued. Higher priority jobs execute
 *       before lower priority ones.
 *   <dt><b>maxRetries</b>
 *   <dd>Maximum number of retry attempts after failure. Set to 0 for no retries.
 *   <dt><b>backoffPolicy</b>
 *   <dd>Strategy for calculating delay between retry attempts:
 *       <ul>
 *         <li>NONE - No delay between retries
 *         <li>FIXED - Constant delay specified by backoffParam
 *         <li>EXPONENTIAL - Exponentially increasing delay
 *       </ul>
 *   <dt><b>backoffParam</b>
 *   <dd>Base duration for backoff calculations. For FIXED policy, this is the constant delay. For
 *       EXPONENTIAL, this is the initial delay.
 *   <dt><b>timeoutSec</b>
 *   <dd>Maximum execution time in seconds. Jobs exceeding this limit are forcibly terminated. Set
 *       to 0 for no timeout.
 * </dl>
 *
 * <h2>Usage Example:</h2>
 *
 * <pre>{@code
 * // Create custom job options
 * JobOptions options = JobOptions.defaults()
 *     .withPriority(JobPriority.HIGH)
 *     .withMaxRetries(3)
 *     .withBackoff(BackoffPolicy.EXPONENTIAL, Duration.ofSeconds(5))
 *     .withTimeout(Duration.ofMinutes(10));
 *
 * // Use in job builder
 * schedulerService.enqueue(() -> processData())
 *     .withPriority(JobPriority.HIGH)
 *     .withMaxRetries(3)
 *     .submit();
 * }</pre>
 *
 * <h2>Default Configuration:</h2>
 *
 * <ul>
 *   <li>Priority: NORMAL
 *   <li>Max Retries: 0 (no retries)
 *   <li>Backoff Policy: NONE
 *   <li>Backoff Parameter: 0 seconds
 *   <li>Timeout: 0 (no timeout)
 * </ul>
 *
 * @param priority the job execution priority
 * @param maxRetries maximum number of retry attempts (0 = no retries)
 * @param backoffPolicy the retry delay strategy
 * @param backoffParam the base duration for backoff calculations
 * @param timeoutSec maximum execution time in seconds (0 = no timeout)
 * @see JobBuilder
 * @see BackoffPolicy
 * @see JobPriority
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
