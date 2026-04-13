package run.ratchet.testsuite.app;

import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;
import run.ratchet.spi.MetricsCollector;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import java.time.Instant;
import java.util.Arrays;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Performance-focused {@link MetricsCollector} that records timing data for throughput and latency
 * analysis.
 *
 * <p>Uses {@code @Priority(2)} to win over {@link CountingMetricsCollector} ({@code @Priority(1)})
 * when both are deployed in the same archive.
 */
@Alternative
@Priority(2)
@ApplicationScoped
public class PerformanceMetricsCollector implements MetricsCollector {

  private static final ConcurrentLinkedQueue<Long> EXECUTION_TIMES = new ConcurrentLinkedQueue<>();
  private static final AtomicLong STARTED_COUNT = new AtomicLong(0);
  private static final AtomicLong COMPLETED_COUNT = new AtomicLong(0);
  private static final AtomicLong FAILED_COUNT = new AtomicLong(0);
  private static volatile Instant firstStart;
  private static volatile Instant lastCompletion;

  @Override
  public void jobStarted(long jobId, JobType type, JobPriority priority) {
    STARTED_COUNT.incrementAndGet();
    if (firstStart == null) {
      firstStart = Instant.now();
    }
  }

  @Override
  public void jobCompleted(long jobId, JobType type, long executionTimeMs) {
    COMPLETED_COUNT.incrementAndGet();
    EXECUTION_TIMES.add(executionTimeMs);
    lastCompletion = Instant.now();
  }

  @Override
  public void jobFailed(long jobId, JobType type, Throwable cause, int attempt) {
    FAILED_COUNT.incrementAndGet();
  }

  public static PerformanceSnapshot snapshot() {
    long[] times = EXECUTION_TIMES.stream().mapToLong(Long::longValue).toArray();
    Arrays.sort(times);

    long completed = COMPLETED_COUNT.get();
    double throughput = 0.0;

    if (firstStart != null && lastCompletion != null && completed > 0) {
      long elapsedMs = lastCompletion.toEpochMilli() - firstStart.toEpochMilli();
      if (elapsedMs > 0) {
        throughput = (completed * 1000.0) / elapsedMs;
      }
    }

    return new PerformanceSnapshot(
        completed,
        throughput,
        percentile(times, 0.50),
        percentile(times, 0.95),
        percentile(times, 0.99),
        STARTED_COUNT.get(),
        FAILED_COUNT.get());
  }

  public static void reset() {
    EXECUTION_TIMES.clear();
    STARTED_COUNT.set(0);
    COMPLETED_COUNT.set(0);
    FAILED_COUNT.set(0);
    firstStart = null;
    lastCompletion = null;
  }

  private static long percentile(long[] sorted, double p) {
    if (sorted.length == 0) {
      return 0;
    }
    int index = (int) Math.ceil(p * sorted.length) - 1;
    return sorted[Math.max(0, index)];
  }

  public record PerformanceSnapshot(
      long completedCount,
      double throughputJobsPerSec,
      long p50Ms,
      long p95Ms,
      long p99Ms,
      long startedCount,
      long failedCount) {}
}
