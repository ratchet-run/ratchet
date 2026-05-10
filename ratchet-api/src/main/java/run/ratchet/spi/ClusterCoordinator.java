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

  /**
   * Signals that new work is available somewhere in the cluster.
   *
   * @param priority priority of the newly available work; never {@code null}
   */
  void notifyNewWork(JobPriority priority);

  /**
   * Registers a local listener for cross-node wakeup notifications.
   *
   * <p>Implementations may invoke listeners from coordinator-owned threads, transport callback
   * threads, or scheduler threads. The listener must be fast, non-blocking, and thread-safe.
   * Registering the same listener more than once may produce duplicate callbacks.
   *
   * @param listener callback to invoke when another node reports available work; never {@code null}
   */
  void registerWakeupListener(Runnable listener);
}
