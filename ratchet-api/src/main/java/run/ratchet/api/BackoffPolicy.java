package run.ratchet.api;

/**
 * Retry backoff strategy applied between job execution attempts. Each policy uses the job's {@code
 * backoffParamMs} to compute actual delays.
 *
 * @since 0.1
 */
public enum BackoffPolicy {

  /** Retries immediately with no delay. */
  NONE,

  /**
   * Constant delay between retries equal to {@code backoffParamMs}.
   *
   * <p>Example (backoffParamMs = 5000): immediate, +5 s, +5 s, +5 s, …
   */
  FIXED,

  /**
   * Delay doubles with each attempt, starting from {@code backoffParamMs}, capped at 24 hours.
   *
   * <p>Example (backoffParamMs = 1000): immediate, +1 s, +2 s, +4 s, +8 s, …
   */
  EXPONENTIAL
}
