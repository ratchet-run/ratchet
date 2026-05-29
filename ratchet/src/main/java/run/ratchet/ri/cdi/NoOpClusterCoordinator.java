package run.ratchet.ri.cdi;

import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.function.Consumer;
import org.jboss.logging.Logger;
import run.ratchet.api.JobPriority;
import run.ratchet.api.NodeIdentity;
import run.ratchet.spi.ClusterCoordinator;
import run.ratchet.spi.JobWakeupHint;

/** Default no-op {@link ClusterCoordinator} for deployments that do not need cross-node wakeups. */
@ApplicationScoped
public class NoOpClusterCoordinator implements ClusterCoordinator {

  private static final Logger log = Logger.getLogger(NoOpClusterCoordinator.class);

  /**
   * One INFO line at startup so an operator can confirm cluster coordination is opt-out — without
   * this, a misconfigured deployment that should have had a coordinator silently uses NoOp and
   * loses cross-node wakeups with no visible signal.
   */
  @PostConstruct
  void announce() {
    log.info(
        "Ratchet cluster coordination: NoOp (no cross-node wakeups). Add a ratchet-coordinator-*"
            + " module to enable push-based wakeups.");
  }

  @Override
  public void notifyNewWork(JobPriority priority, NodeIdentity source, String executionTarget) {}

  @Override
  public void registerWakeupListener(Consumer<JobWakeupHint> listener) {}

  @Override
  public void close() {}
}
