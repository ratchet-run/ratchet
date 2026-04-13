package run.ratchet.testsuite.app;

import run.ratchet.spi.RetryPolicy;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

/** Custom {@link RetryPolicy} for testing SPI overridability. */
@Alternative
@Priority(1)
@ApplicationScoped
public class VetoRetryPolicy implements RetryPolicy {

  private static final AtomicInteger SHOULD_RETRY_COUNT = new AtomicInteger(0);

  @Override
  public boolean shouldRetry(int attempt, Throwable cause) {
    SHOULD_RETRY_COUNT.incrementAndGet();
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
