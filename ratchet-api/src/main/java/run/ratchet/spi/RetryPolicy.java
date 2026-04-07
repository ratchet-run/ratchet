package run.ratchet.spi;

import run.ratchet.api.Incubating;
import java.time.Duration;

/**
 * Represents a policy for determining retry behavior in the context of failure recovery. This
 * interface provides methods for evaluating whether a retry attempt should be made and for
 * calculating the delay before the next retry attempt.
 *
 * <p>Marked {@link Incubating} — the retry contract may evolve.
 */
@Incubating
public interface RetryPolicy {

  /**
   * Evaluates whether a retry attempt should be made based on the current attempt number and the
   * cause of the previous failure.
   *
   * @param attempt the current retry attempt number, starting from 1
   * @param cause the throwable that caused the previous failure; may provide context for retry
   *     decisions
   * @return {@code true} if another retry attempt should be made; {@code false} otherwise
   */
  boolean shouldRetry(int attempt, Throwable cause);

  /**
   * Calculates the delay duration before the next retry attempt based on the current retry attempt
   * number.
   *
   * @param attempt the current retry attempt number, starting from 1
   * @return the delay duration before the next retry attempt
   */
  Duration getDelay(int attempt);
}
