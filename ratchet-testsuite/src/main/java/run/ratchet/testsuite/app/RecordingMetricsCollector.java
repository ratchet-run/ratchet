package run.ratchet.testsuite.app;

import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;
import run.ratchet.spi.MetricsCollector;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

/** Metrics collector that records callback payloads for integration assertions. */
@Alternative
@Priority(3)
@ApplicationScoped
public class RecordingMetricsCollector implements MetricsCollector {

  private static final ConcurrentLinkedQueue<StartedMetric> STARTED = new ConcurrentLinkedQueue<>();
  private static final ConcurrentLinkedQueue<CompletedMetric> COMPLETED =
      new ConcurrentLinkedQueue<>();
  private static final ConcurrentLinkedQueue<FailedMetric> FAILED = new ConcurrentLinkedQueue<>();

  @Override
  public void jobStarted(long jobId, JobType type, JobPriority priority) {
    STARTED.add(new StartedMetric(jobId, type, priority));
  }

  @Override
  public void jobCompleted(long jobId, JobType type, long executionTimeMs) {
    COMPLETED.add(new CompletedMetric(jobId, type, executionTimeMs));
  }

  @Override
  public void jobFailed(long jobId, JobType type, Throwable cause, int attempt) {
    FAILED.add(
        new FailedMetric(jobId, type, attempt, cause == null ? null : cause.getClass().getName()));
  }

  @Override
  public void successFinalizationRetried(long jobId, JobType type) {}

  @Override
  public void successFinalizationMinimal(long jobId, JobType type) {}

  @Override
  public void successFinalizationStuck(long jobId, JobType type) {}

  @Override
  public void claimTransientFailure(String executionType) {}

  @Override
  public void jobsClaimed(String executionType, int claimedCount) {}

  @Override
  public void gateRejected(String executionType, String gateStatus) {}

  @Override
  public void localWakeup(String source) {}

  @Override
  public void clusterWakeupPublished(String transport, String outcome) {}

  @Override
  public void clusterWakeupReceived(String transport, String outcome) {}

  public static List<StartedMetric> startedEvents() {
    return List.copyOf(STARTED);
  }

  public static List<CompletedMetric> completedEvents() {
    return List.copyOf(COMPLETED);
  }

  public static List<FailedMetric> failedEvents() {
    return List.copyOf(FAILED);
  }

  public static void reset() {
    STARTED.clear();
    COMPLETED.clear();
    FAILED.clear();
  }

  public record StartedMetric(long jobId, JobType type, JobPriority priority) {}

  public record CompletedMetric(long jobId, JobType type, long executionTimeMs) {}

  public record FailedMetric(long jobId, JobType type, int attempt, String causeType) {}
}
