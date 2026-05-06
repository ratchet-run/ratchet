package run.ratchet.testsuite.app;

import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;
import run.ratchet.spi.MetricsCollector;

@Alternative
@Priority(1)
@ApplicationScoped
public class CountingMetricsCollector implements MetricsCollector {

  private static final AtomicInteger STARTED_COUNT = new AtomicInteger(0);
  private static final AtomicInteger COMPLETED_COUNT = new AtomicInteger(0);
  private static final AtomicInteger FAILED_COUNT = new AtomicInteger(0);

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

  @Override
  public void jobStarted(UUID jobId, JobType type, JobPriority priority) {
    STARTED_COUNT.incrementAndGet();
  }

  @Override
  public void jobCompleted(UUID jobId, JobType type, long executionTimeMs) {
    COMPLETED_COUNT.incrementAndGet();
  }

  @Override
  public void jobFailed(UUID jobId, JobType type, Throwable cause, int attempt) {
    FAILED_COUNT.incrementAndGet();
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
}
