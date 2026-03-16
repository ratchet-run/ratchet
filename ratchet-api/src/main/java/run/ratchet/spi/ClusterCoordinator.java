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

  /**
   * Notifies the cluster coordinator of the availability of new work with the specified priority
   * level. This method can be used to trigger scheduling or wakeup mechanisms based on the new
   * job's priority.
   *
   * @param priority the priority level of the new work; higher ordinal values represent higher
   *     priority levels
   */
  void notifyNewWork(JobPriority priority);

  /**
   * Registers a listener to receive wakeup notifications from the cluster coordinator.
   *
   * @param listener the {@code Runnable} to be executed when a wakeup notification is triggered
   */
  void registerWakeupListener(Runnable listener);
}
