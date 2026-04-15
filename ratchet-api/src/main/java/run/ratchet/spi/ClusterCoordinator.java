package run.ratchet.spi;

import run.ratchet.api.Incubating;
import run.ratchet.api.JobPriority;

/**
 * Coordinates job scheduling across cluster nodes.
 *
 * <p>This interface is marked {@link Incubating} — the cluster coordination contract may evolve as
 * high-availability support matures. It is intentionally limited to cross-node wakeup
 * notifications; destructive startup coordination uses {@link StartupCoordinator}.
 */
@Incubating
public interface ClusterCoordinator {

  /** Signals that new work at the given priority is available. */
  void notifyNewWork(JobPriority priority);

  /** Registers a listener to receive wakeup notifications from the cluster coordinator. */
  void registerWakeupListener(Runnable listener);
}
