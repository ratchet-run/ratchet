package run.ratchet.testsuite.app;

import run.ratchet.spi.RetryPolicy;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Custom {@link RetryPolicy} for testing SPI overridability.
 *
 * <p>Vetoes all retry attempts after the first failure, regardless of the job's configured
 * maxRetries. This verifies that the custom policy takes precedence over the default retry logic.
 */
@Alternative
@Priority(1)
@ApplicationScoped
public class VetoRetryPolicy implements RetryPolicy {

  private static final AtomicInteger SHOULD_RETRY_COUNT = new AtomicInteger(0);

  @Override
  public boolean shouldRetry(int attempt, Throwable cause) {
    SHOULD_RETRY_COUNT.incrementAndGet();
    // Veto all retries — job should fail immediately after first attempt
    return false;
  }

  @Override
  public Duration getDelay(int attempt) {
    return Duration.ZERO;
  }

  public static int getShouldRetryCount() {
    return SHOULD_RETRY_COUNT.get();
  }

  public static void resetCounts() {
    SHOULD_RETRY_COUNT.set(0);
  }
}
