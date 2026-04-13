package run.ratchet.spi;

import run.ratchet.api.Incubating;
import java.time.Duration;

/** Controls whether and when a failed job should be retried. */
@Incubating
public interface RetryPolicy {

  /**
   * Returns whether another attempt should be made.
   *
   * @param attempt 1-based attempt number
   * @param cause the failure from the previous attempt
   */
  boolean shouldRetry(int attempt, Throwable cause);

  /**
   * Returns the delay before the next retry.
   *
   * @param attempt 1-based attempt number
   */
  Duration getDelay(int attempt);
}
