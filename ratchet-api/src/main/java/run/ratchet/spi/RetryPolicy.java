package run.ratchet.spi;

import java.time.Duration;
import run.ratchet.api.Incubating;

/** Controls whether and when a failed job should be retried. */
@Incubating
public interface RetryPolicy {

  /**
   * Returns whether a failed job attempt should be retried.
   *
   * @param attempt 1-based attempt number
   * @param cause failure that ended the attempt; never {@code null}
   * @return {@code true} to schedule another attempt, {@code false} to move toward terminal failure
   */
  boolean shouldRetry(int attempt, Throwable cause);

  /**
   * Returns the retry delay for an attempt.
   *
   * @param attempt 1-based attempt number being scheduled
   * @return non-null delay. {@link Duration#ZERO} means retry immediately or fall back to the job's
   *     configured backoff policy, depending on the caller.
   * @apiNote Returning {@code null} violates the SPI contract and may fail the retry path with a
   *     {@link NullPointerException}.
   */
  Duration getDelay(int attempt);
}
