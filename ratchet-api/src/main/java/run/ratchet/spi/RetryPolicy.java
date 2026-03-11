package run.ratchet.spi;

import java.time.Duration;

/** Determines retry behavior when a job fails. */
public interface RetryPolicy {

  boolean shouldRetry(int attempt, Throwable cause);

  Duration getDelay(int attempt);
}
