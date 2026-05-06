package run.ratchet.testsuite.app;

import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;
import run.ratchet.spi.MetricsCollector;

/** Metrics collector that records callback payloads for integration assertions. */
@Alternative
@Priority(3)
@ApplicationScoped
public class RecordingMetricsCollector implements MetricsCollector {

  private static final ConcurrentLinkedQueue<StartedMetric> STARTED = new ConcurrentLinkedQueue<>();
  private static final ConcurrentLinkedQueue<CompletedMetric> COMPLETED =
      new ConcurrentLinkedQueue<>();
  private static final ConcurrentLinkedQueue<FailedMetric> FAILED = new ConcurrentLinkedQueue<>();

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

  @Override
  public void jobStarted(UUID jobId, JobType type, JobPriority priority) {
    STARTED.add(new StartedMetric(jobId, type, priority));
  }

  @Override
  public void jobCompleted(UUID jobId, JobType type, long executionTimeMs) {
    COMPLETED.add(new CompletedMetric(jobId, type, executionTimeMs));
  }

  @Override
  public void jobFailed(UUID jobId, JobType type, Throwable cause, int attempt) {
    FAILED.add(
        new FailedMetric(jobId, type, attempt, cause == null ? null : cause.getClass().getName()));
  }

  @Override
  public void successFinalizationRetried(UUID jobId, JobType type) {}

  @Override
  public void successFinalizationMinimal(UUID jobId, JobType type) {}

  @Override
  public void successFinalizationStuck(UUID jobId, JobType type) {}

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

  public record StartedMetric(UUID jobId, JobType type, JobPriority priority) {}

  public record CompletedMetric(UUID jobId, JobType type, long executionTimeMs) {}

  public record FailedMetric(UUID jobId, JobType type, int attempt, String causeType) {}
}
