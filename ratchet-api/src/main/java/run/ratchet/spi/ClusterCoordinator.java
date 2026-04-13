package run.ratchet.spi;

import run.ratchet.api.Incubating;
import run.ratchet.api.JobPriority;

/**
 * Coordinates job scheduling across cluster nodes.
 *
 * <p>This interface is marked {@link Incubating} — the cluster coordination contract may evolve as
 * high-availability support matures.
 */
@Incubating
public interface ClusterCoordinator {

  /** Signals that new work at the given priority is available. */
  void notifyNewWork(JobPriority priority);

  /** Registers a listener to receive wakeup notifications from the cluster coordinator. */
  void registerWakeupListener(Runnable listener);

  /**
   * Indicates whether this node is currently the cluster leader.
   *
   * <p>Used to gate one-time, destructive startup actions (for example, cancelling orphaned
   * recurring jobs) to a single node in a multi-node deployment. The single-node default returns
   * {@code true} — every node is its own leader. Multi-node implementations must return an honest
   * value based on a real leader-election mechanism (database lock, distributed lease, etc.).
   *
   * @return {@code true} if this node should execute leader-only work; {@code false} otherwise
   */
  default boolean isLeader() {
    return true;
  }

  /**
   * Executes the given task if and only if this node is currently the cluster leader.
   *
   * <p>Convenience wrapper around {@link #isLeader()}. Implementations may override to provide
   * richer semantics (e.g., hold a lease for the duration of the task, or fail-fast if the lease is
   * lost mid-execution).
   */
  default void runAsLeader(Runnable task) {
    if (isLeader()) {
      task.run();
    }
  }
}
