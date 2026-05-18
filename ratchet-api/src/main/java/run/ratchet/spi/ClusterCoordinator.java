package run.ratchet.spi;

import run.ratchet.api.Incubating;
import run.ratchet.api.JobPriority;

/**
 * Coordinates job scheduling across cluster nodes.
 *
 * <p>This interface is marked {@link Incubating} — the cluster coordination contract may evolve as
 * high-availability support matures. It is intentionally limited to cross-node wakeup
 * notifications; destructive startup coordination uses {@link StartupCoordinator}.
 *
 * @since 0.1
 */
@Incubating
public interface ClusterCoordinator {

  /**
   * Signals that new work is available somewhere in the cluster.
   *
   * <p>Wakeups are best-effort hints. Implementations should tolerate transport failures without
   * throwing to the caller; the local poll loop remains the source of truth.
   *
   * @param priority priority of the newly available work; never {@code null}
   */
  void notifyNewWork(JobPriority priority);

  /**
   * Registers a local listener for cross-node wakeup notifications.
   *
   * <p>Implementations may invoke listeners from coordinator-owned threads, transport callback
   * threads, or scheduler threads. The listener must be fast, non-blocking, and thread-safe.
   * Implementations must isolate listener failures so one throwing listener does not prevent other
   * listeners or future wakeups. Registering the same listener more than once may produce duplicate
   * callbacks.
   *
   * <p>The SPI has no unregister method. Callers that need shutdown behavior should register a
   * wrapper that checks local lifecycle state before dispatching.
   *
   * @param listener callback to invoke when another node reports available work; never {@code null}
   */
  void registerWakeupListener(Runnable listener);
}
