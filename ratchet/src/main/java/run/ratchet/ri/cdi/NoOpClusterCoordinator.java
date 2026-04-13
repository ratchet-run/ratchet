package run.ratchet.ri.cdi;

import run.ratchet.api.JobPriority;
import run.ratchet.spi.ClusterCoordinator;
import jakarta.enterprise.context.ApplicationScoped;

/**
 * Default no-op {@link ClusterCoordinator} for single-node deployments.
 *
 * <p>In a single-node environment, there is no need for cluster-wide job wakeup notifications. This
 * implementation silently discards all publish calls and ignores listener registrations.
 *
 * <p>Users can override by providing their own {@code @Alternative @Priority(APPLICATION)
 * ClusterCoordinator} bean (e.g., backed by JGroups, JMS topics, or a Redis pub/sub channel for
 * multi-node deployments). See {@code CustomSerializationStrategyIT} in the test suite for the CDI
 * override pattern.
 */
@ApplicationScoped
public class NoOpClusterCoordinator implements ClusterCoordinator {

  @Override
  public void notifyNewWork(JobPriority priority) {}

  @Override
  public void registerWakeupListener(Runnable listener) {}
}
