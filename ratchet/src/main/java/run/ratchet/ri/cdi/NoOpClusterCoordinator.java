package run.ratchet.ri.cdi;

import run.ratchet.api.JobPriority;
import run.ratchet.spi.ClusterCoordinator;
import jakarta.enterprise.context.ApplicationScoped;

/**
 * Default no-op {@link ClusterCoordinator} for single-node deployments.
 *
 * <p>In a single-node environment, there is no need for cluster-wide job wakeup notifications. This
 * implementation silently discards all publish calls and ignores listener registrations.
 */
@ApplicationScoped
public class NoOpClusterCoordinator implements ClusterCoordinator {

  @Override
  public void notifyNewWork(JobPriority priority) {
    // No-op: single node, no cluster to notify
  }

  @Override
  public void registerWakeupListener(Runnable listener) {
    // No-op: single node, no remote notifications to receive
  }
}
