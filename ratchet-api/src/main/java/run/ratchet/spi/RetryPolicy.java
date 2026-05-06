package run.ratchet.spi;

import java.time.Duration;
import run.ratchet.api.Incubating;

/** Controls whether and when a failed job should be retried. */
@Incubating
public interface RetryPolicy {

  /**
   * @param attempt 1-based attempt number
   */
  boolean shouldRetry(int attempt, Throwable cause);

  Duration getDelay(int attempt);
}
