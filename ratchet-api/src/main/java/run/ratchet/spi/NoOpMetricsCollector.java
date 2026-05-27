package run.ratchet.spi;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Default;
import java.util.UUID;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;

/** Default no-op {@link MetricsCollector} for deployments without a monitoring integration. */
@ApplicationScoped
@Default
public class NoOpMetricsCollector implements MetricsCollector {

  @Override
  public void jobStarted(UUID jobId, JobType type, JobPriority priority) {}

  @Override
  public void jobCompleted(UUID jobId, JobType type, long executionTimeMs) {}

  @Override
  public void jobFailed(UUID jobId, JobType type, Throwable cause, int attempt) {}

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
