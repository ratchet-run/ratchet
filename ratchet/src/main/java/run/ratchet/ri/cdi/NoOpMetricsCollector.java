package run.ratchet.ri.cdi;

import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;
import run.ratchet.spi.MetricsCollector;
import jakarta.enterprise.context.ApplicationScoped;

/** Default no-op {@link MetricsCollector} for deployments without a monitoring integration. */
@ApplicationScoped
public class NoOpMetricsCollector implements MetricsCollector {

  @Override
  public void jobStarted(long jobId, JobType type, JobPriority priority) {}

  @Override
  public void jobCompleted(long jobId, JobType type, long executionTimeMs) {}

  @Override
  public void jobFailed(long jobId, JobType type, Throwable cause, int attempt) {}

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
}
