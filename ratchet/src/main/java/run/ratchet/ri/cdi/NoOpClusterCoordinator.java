package run.ratchet.ri.cdi;

import run.ratchet.api.JobPriority;
import run.ratchet.spi.ClusterCoordinator;
import jakarta.enterprise.context.ApplicationScoped;

/** Default no-op {@link ClusterCoordinator} for deployments that do not need cross-node wakeups. */
@ApplicationScoped
public class NoOpClusterCoordinator implements ClusterCoordinator {

  @Override
  public void notifyNewWork(JobPriority priority) {}

  @Override
  public void registerWakeupListener(Runnable listener) {}
}
