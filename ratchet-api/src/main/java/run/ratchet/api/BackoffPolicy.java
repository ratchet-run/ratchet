package run.ratchet.api;

/**
 * Retry backoff strategy applied between job execution attempts. Each policy uses the job's {@code
 * backoffParamMs} to compute actual delays.
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
