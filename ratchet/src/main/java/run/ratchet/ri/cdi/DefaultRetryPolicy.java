package run.ratchet.ri.cdi;

import run.ratchet.spi.RetryPolicy;
import jakarta.enterprise.context.ApplicationScoped;
import java.time.Duration;

/**
 * Default {@link RetryPolicy} that defers all retry decisions to the job's configured max-retries
 * and backoff policy. Always returns {@code true}/{@link Duration#ZERO}; override with an
 * {@code @Alternative @Priority(APPLICATION) RetryPolicy} bean for custom logic.
 */
@ApplicationScoped
public class DefaultRetryPolicy implements RetryPolicy {

  @Override
  public boolean shouldRetry(int attempt, Throwable cause) {
    return true;
  }

  @Override
  public Duration getDelay(int attempt) {
    return Duration.ZERO;
  }
}
