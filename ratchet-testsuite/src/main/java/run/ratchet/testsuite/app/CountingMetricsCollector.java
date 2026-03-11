package run.ratchet.testsuite.app;

import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;
import run.ratchet.spi.MetricsCollector;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Custom {@link MetricsCollector} for testing SPI overridability.
 *
 * <p>Counts invocations of each callback so tests can verify the custom collector was selected over
 * the default {@code NoOpMetricsCollector} and receives metrics during job execution.
 */
@Alternative
@Priority(1)
@ApplicationScoped
public class CountingMetricsCollector implements MetricsCollector {

  private static final AtomicInteger STARTED_COUNT = new AtomicInteger(0);
  private static final AtomicInteger COMPLETED_COUNT = new AtomicInteger(0);
  private static final AtomicInteger FAILED_COUNT = new AtomicInteger(0);

  @Override
  public void jobStarted(long jobId, JobType type, JobPriority priority) {
    STARTED_COUNT.incrementAndGet();
  }

  @Override
  public void jobCompleted(long jobId, JobType type, long executionTimeMs) {
    COMPLETED_COUNT.incrementAndGet();
  }

  @Override
  public void jobFailed(long jobId, JobType type, Throwable cause, int attempt) {
    FAILED_COUNT.incrementAndGet();
  }

  public static int getStartedCount() {
    return STARTED_COUNT.get();
  }

  public static int getCompletedCount() {
    return COMPLETED_COUNT.get();
  }

  public static int getFailedCount() {
    return FAILED_COUNT.get();
  }

  public static void resetCounts() {
    STARTED_COUNT.set(0);
    COMPLETED_COUNT.set(0);
    FAILED_COUNT.set(0);
  }
}
