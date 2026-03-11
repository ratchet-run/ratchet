package run.ratchet.ri.cdi;

import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;
import run.ratchet.spi.MetricsCollector;
import jakarta.enterprise.context.ApplicationScoped;

/**
 * Default no-op {@link MetricsCollector} for deployments without a monitoring integration.
 *
 * <p>Users can override by providing their own {@code @ApplicationScoped MetricsCollector} bean.
 */
@ApplicationScoped
public class NoOpMetricsCollector implements MetricsCollector {

  @Override
  public void jobStarted(long jobId, JobType type, JobPriority priority) {}

  @Override
  public void jobCompleted(long jobId, JobType type, long executionTimeMs) {}

  @Override
  public void jobFailed(long jobId, JobType type, Throwable cause, int attempt) {}
}
