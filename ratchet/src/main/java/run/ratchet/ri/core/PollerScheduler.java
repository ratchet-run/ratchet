package run.ratchet.ri.core;

/**
 * SPI for the job-poller scheduler — controls poll-cycle lifecycle and wakeup signals. Default
 * implementation: {@link run.ratchet.ri.core.internal.DefaultPollerScheduler}.
 *
 * @apiNote Framework SPI consumed by ri.core collaborators (Poller, PollerWakeupListener,
 *     ResourcePermitService, PostExecutionHandler, etc.) and by ratchet-testsuite integration
 *     tests. Applications must not implement this interface.
 */
public interface PollerScheduler {

  void start();

  void stop();

  /**
   * Wakes up the poller to immediately check for available jobs. Called when a job notification is
   * received from the cluster, indicating that new work is available.
   */
  void wakeup();
}
