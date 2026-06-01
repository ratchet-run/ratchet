/*
 * Copyright 2026 Ratchet Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package run.ratchet.spi;

import java.util.function.Consumer;
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
   * <p>The {@code executionTarget} carries the routing label of the job that triggered the wakeup
   * (e.g. {@code "platform"} or {@code "virtual"}). It is informational only: receiving listeners
   * wake the local poller unconditionally and the claim-side filter on each node decides which pool
   * actually drains. A {@code null} value means the wakeup is not target-scoped.
   *
   * <p>Callers must not invoke this method before the coordinator's {@link
   * SchedulerLifecycleHook#afterStart()} hook has run. The default CDI lifecycle guarantees this
   * ordering — application code that opens its own coordinator (tests, non-CDI integrations) must
   * uphold it manually, or the transport may be unavailable and the notification silently dropped.
   *
   * @param priority priority of the newly available work; never {@code null}
   * @param source identity of the node submitting this notification; never {@code null}
   * @param executionTarget execution-target label of the originating job, or {@code null} when the
   *     wakeup is not target-scoped
   */
  void notifyNewWork(JobPriority priority, NodeIdentity source, String executionTarget);

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
   * <p>Listeners receive a {@link JobWakeupHint} carrying priority, origin {@link NodeIdentity},
   * and optional execution target so they can label metrics, suppress self-wakeups, and decide how
   * to react to a target-scoped notification.
   *
   * @param listener callback to invoke when another node reports available work; never {@code null}
   */
  void registerWakeupListener(Consumer<JobWakeupHint> listener);

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
