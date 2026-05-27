package run.ratchet.spi;

import java.util.function.BiConsumer;
import run.ratchet.api.Incubating;
import run.ratchet.api.JobPriority;
import run.ratchet.api.NodeIdentity;

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
public interface ClusterCoordinator extends AutoCloseable {

  /**
   * Signals that new work is available somewhere in the cluster.
   *
   * <p>Wakeups are best-effort hints. Implementations should tolerate transport failures without
   * throwing to the caller; the local poll loop remains the source of truth.
   *
   * <p>The {@code source} identifies the sending node so subscribers can suppress self-wakeups
   * delivered back over a broadcast transport and so listener-side metrics can label notifications
   * by origin.
   *
   * <p>Callers must not invoke this method before the coordinator's {@link
   * SchedulerLifecycleHook#afterStart()} hook has run. The default CDI lifecycle guarantees this
   * ordering — application code that opens its own coordinator (tests, non-CDI integrations) must
   * uphold it manually, or the transport may be unavailable and the notification silently dropped.
   *
   * @param priority priority of the newly available work; never {@code null}
   * @param source identity of the node submitting this notification; never {@code null}
   */
  void notifyNewWork(JobPriority priority, NodeIdentity source);

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
   * <p>Listeners receive the priority of the newly available work and the {@link NodeIdentity} of
   * the sending node so they can label metrics and (when appropriate) suppress self-wakeups.
   *
   * @param listener callback to invoke when another node reports available work; never {@code null}
   */
  void registerWakeupListener(BiConsumer<JobPriority, NodeIdentity> listener);

  /**
   * Releases transport resources held by this coordinator. Must be idempotent: a second invocation
   * must complete without throwing. Called by the scheduler lifecycle during shutdown.
   *
   * <p>Implementations are expected to also implement {@link SchedulerLifecycleHook} and override
   * {@link SchedulerLifecycleHook#afterStop()} to invoke {@link #close()}; this routes shutdown
   * through the lifecycle hook chain so coordinator close() runs alongside other lifecycle-owned
   * shutdown actions. If the implementation does not implement {@link SchedulerLifecycleHook}, the
   * scheduler lifecycle will invoke {@link #close()} directly as a fallback during shutdown — the
   * idempotency requirement above keeps a fallback-and-hook double-invocation safe.
   */
  @Override
  void close();
}
