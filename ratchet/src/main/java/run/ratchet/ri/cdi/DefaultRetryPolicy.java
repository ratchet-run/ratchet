package run.ratchet.ri.cdi;

import run.ratchet.spi.RetryPolicy;
import jakarta.enterprise.context.ApplicationScoped;
import java.time.Duration;

/**
 * Default {@link RetryPolicy} that defers all retry decisions to the job's configured max-retries
 * and backoff policy.
 *
 * <p>This implementation always returns {@code true} for {@link #shouldRetry} and {@link
 * Duration#ZERO} for {@link #getDelay}, meaning the job's own configuration (maxRetries,
 * backoffPolicy, backoffParamMs) controls retry behavior entirely.
 *
 * <p>Users can override by providing their own {@code @ApplicationScoped RetryPolicy} bean to
 * implement custom retry logic (e.g., circuit-breaker-aware retries, exception-type-based
 * strategies, or rate-limited retries).
 */
@ApplicationScoped
public class DefaultRetryPolicy implements RetryPolicy {

  @Override
  public boolean shouldRetry(int attempt, Throwable cause) {
    // Defer to job-level maxRetries configuration
    return true;
  }

  @Override
  public Duration getDelay(int attempt) {
    // Defer to job-level backoff policy configuration
    return Duration.ZERO;
  }
}
