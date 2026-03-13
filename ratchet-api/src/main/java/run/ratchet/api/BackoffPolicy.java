package run.ratchet.api;

/**
 * Specifies the retry backoff strategy applied between job execution attempts.
 *
 * <p>The backoff policy determines how delays between retries are handled when a job execution
 * fails. This ensures robustness by allowing appropriate cooldown periods, based on the nature of
 * the failure and the configured policy.
 *
 * <h2>Available Policies:</h2>
 *
 * <ul>
 *   <li><b>NONE:</b> No delay between retries. Best for non-recoverable errors.
 *   <li><b>FIXED:</b> A constant delay between retries. Useful for predictable retry intervals.
 *   <li><b>EXPONENTIAL:</b> Delays grow exponentially. Ideal for minimizing burden on stressed
 *       systems.
 * </ul>
 *
 * <h2>Behavior:</h2>
 *
 * <ul>
 *   <li>The policy is configured at the time of job creation.
 *   <li>Each policy relies on the `backoffParamMs` to compute actual delays.
 *   <li>Policies apply until a job completes successfully or reaches its retry limit.
 * </ul>
 *
 * <h2>Considerations:</h2>
 *
 * <ul>
 *   <li>Choose a policy that aligns with system capabilities and failure characteristics.
 *   <li>EXAMPLES: - Use {@code NONE} for immediate retries of non-transient errors. - Use {@code
 *       FIXED} when a predictable retry schedule is needed. - Use {@code EXPONENTIAL} to gradually
 *       reduce retry frequency for transient issues.
 * </ul>
 *
 * <h2>Interaction with Overall Job Scheduling:</h2>
 *
 * <ul>
 *   <li>A retry limit or end condition may override the backoff delay.
 *   <li>This policy does not affect job concurrency or task dependencies.
 *   <li>The delay is enforced per job execution attempt within the scheduler's lifecycle.
 * </ul>
 */
public enum BackoffPolicy {

  /**
   * No delay between retry attempts.
   *
   * <p>Jobs are retried immediately after failure. This is suitable for failures that are unlikely
   * to be caused by temporary conditions, such as data validation errors or business logic
   * failures.
   */
  NONE,

  /**
   * Fixed delay between retry attempts.
   *
   * <p>A constant delay is applied between each retry, as specified by the job's backoffParamMs
   * value. This is suitable for rate-limited services or when a predictable retry pattern is
   * desired.
   *
   * <p>Example: If backoffParamMs = 5000, retries occur at:
   *
   * <ul>
   *   <li>Attempt 1: immediate
   *   <li>Attempt 2: after 5 seconds
   *   <li>Attempt 3: after 5 seconds
   *   <li>Attempt 4: after 5 seconds
   * </ul>
   */
  FIXED,

  /**
   * Exponentially increasing delay between retry attempts.
   *
   * <p>The delay doubles with each retry attempt, starting from the backoffParamMs value. This
   * helps prevent overwhelming systems under stress and gives progressively more time for recovery.
   *
   * <p>Example: If backoffParamMs = 1000, retries occur at:
   *
   * <ul>
   *   <li>Attempt 1: immediate
   *   <li>Attempt 2: after 1 second
   *   <li>Attempt 3: after 2 seconds
   *   <li>Attempt 4: after 4 seconds
   *   <li>Attempt 5: after 8 seconds
   * </ul>
   *
   * <p>The delay is capped at a reasonable maximum (typically 5 minutes) to prevent excessive wait
   * times.
   */
  EXPONENTIAL
}
