package run.ratchet.ri.cdi;

import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;
import run.ratchet.spi.MetricsCollector;
import jakarta.enterprise.context.ApplicationScoped;

/**
 * Default no-op {@link MetricsCollector} for deployments without a monitoring integration.
 *
 * <p>All methods are intentionally empty — this bean exists solely to satisfy the {@code
 * MetricsCollector} injection point when no monitoring integration is configured.
 *
 * <p>Users can override by providing their own {@code @Alternative @Priority(APPLICATION)
 * MetricsCollector} bean (e.g., backed by MicroProfile Metrics, Micrometer, or a custom dashboard
 * integration). See {@code CustomSerializationStrategyIT} in the test suite for the CDI override
 * pattern.
 */
@ApplicationScoped
public class NoOpMetricsCollector implements MetricsCollector {

  @Override
  public void jobStarted(long jobId, JobType type, JobPriority priority) {
    // No-op: no metrics to collect
  }

  @Override
  public void jobCompleted(long jobId, JobType type, long executionTimeMs) {
    // No-op: no metrics to collect
  }

  @Override
  public void jobFailed(long jobId, JobType type, Throwable cause, int attempt) {
    // No-op: no metrics to collect
  }
}
