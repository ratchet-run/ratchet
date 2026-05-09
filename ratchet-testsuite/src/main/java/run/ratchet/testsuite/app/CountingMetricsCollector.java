package run.ratchet.testsuite.app;

import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;

@Alternative
@Priority(1)
@ApplicationScoped
public class CountingMetricsCollector extends TestMetricsCollectorAdapter {

  private static final AtomicInteger STARTED_COUNT = new AtomicInteger(0);
  private static final AtomicInteger COMPLETED_COUNT = new AtomicInteger(0);
  private static final AtomicInteger FAILED_COUNT = new AtomicInteger(0);
  private static final ConcurrentLinkedQueue<StartedMetric> STARTED = new ConcurrentLinkedQueue<>();
  private static final ConcurrentLinkedQueue<CompletedMetric> COMPLETED =
      new ConcurrentLinkedQueue<>();
  private static final ConcurrentLinkedQueue<FailedMetric> FAILED = new ConcurrentLinkedQueue<>();

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
    STARTED.clear();
    COMPLETED.clear();
    FAILED.clear();
  }

  public static List<StartedMetric> startedEvents() {
    return List.copyOf(STARTED);
  }

  public static List<CompletedMetric> completedEvents() {
    return List.copyOf(COMPLETED);
  }

  public static List<FailedMetric> failedEvents() {
    return List.copyOf(FAILED);
  }

  @Override
  public void jobStarted(UUID jobId, JobType type, JobPriority priority) {
    STARTED_COUNT.incrementAndGet();
    STARTED.add(new StartedMetric(jobId, type, priority));
  }

  @Override
  public void jobCompleted(UUID jobId, JobType type, long executionTimeMs) {
    COMPLETED_COUNT.incrementAndGet();
    COMPLETED.add(new CompletedMetric(jobId, type, executionTimeMs));
  }

  @Override
  public void jobFailed(UUID jobId, JobType type, Throwable cause, int attempt) {
    FAILED_COUNT.incrementAndGet();
    FAILED.add(
        new FailedMetric(jobId, type, attempt, cause == null ? null : cause.getClass().getName()));
  }

  public record StartedMetric(UUID jobId, JobType type, JobPriority priority) {}

  public record CompletedMetric(UUID jobId, JobType type, long executionTimeMs) {}

  public record FailedMetric(UUID jobId, JobType type, int attempt, String causeType) {}
}
