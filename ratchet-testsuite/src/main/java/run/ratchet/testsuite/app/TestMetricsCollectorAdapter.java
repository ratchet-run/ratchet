package run.ratchet.testsuite.app;

import java.util.UUID;
import run.ratchet.api.JobType;
import run.ratchet.spi.MetricsCollector;

/** No-op adapter for tests that only assert a subset of {@link MetricsCollector} callbacks. */
public abstract class TestMetricsCollectorAdapter implements MetricsCollector {

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
