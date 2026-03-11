package run.ratchet.spi;

import run.ratchet.api.JobPriority;

/** Coordinates job scheduling across cluster nodes. */
public interface ClusterCoordinator {

  void notifyNewWork(JobPriority priority);

  void registerWakeupListener(Runnable listener);
}
