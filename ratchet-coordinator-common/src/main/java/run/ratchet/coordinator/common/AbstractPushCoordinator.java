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
package run.ratchet.coordinator.common;

import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.jboss.logging.Logger;
import run.ratchet.api.NodeIdentity;
import run.ratchet.coordinator.common.internal.NotifyPayloadCodec;
import run.ratchet.spi.JobWakeupHint;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.spi.NodeIdentityProvider;

/**
 * Shared push-dispatch machinery for the bundled cluster coordinators. Owns the listener registry,
 * the pre-registration buffer, the dispatch executor, and the self-suppression / metric tail that
 * is identical across every transport. Subclasses supply the transport-specific publish path and
 * the inbound message extraction, then hand a decoded {@link NotifyPayload} to {@link
 * #deliverDecodedPayload(NotifyPayload)}.
 *
 * <p>Each subclass MUST call {@link #configureDispatch} at the top of its {@code init()} — after
 * its config is resolved but before any transport setup — so the dispatch executor exists before
 * the transport can deliver an inbound message.
 */
public abstract class AbstractPushCoordinator {

  private static final Logger log = Logger.getLogger(AbstractPushCoordinator.class);

  private static final int PRE_REGISTRATION_BUFFER_CAPACITY = 256;

  protected final NotifyPayloadCodec codec = new NotifyPayloadCodec();
  private final CopyOnWriteArrayList<Consumer<JobWakeupHint>> listeners =
      new CopyOnWriteArrayList<>();
  private final BlockingQueue<NotifyPayload> preRegistrationBuffer =
      new ArrayBlockingQueue<>(PRE_REGISTRATION_BUFFER_CAPACITY);
  private final AtomicBoolean closed = new AtomicBoolean(false);

  // Written once in configureDispatch (the init thread) and read from the transport listener and
  // shutdown threads. volatile gives each read the most recent write and makes the 64-bit
  // shutdownGraceMs write atomic, so the dispatch config is safely published without leaning on
  // the transport library's internal synchronization for the happens-before.
  private volatile String coordinatorKind;
  private volatile String displayName;
  private volatile MetricsCollector metrics;
  private volatile NodeIdentityProvider identityProvider;
  private volatile int maxInboundPayloadChars;
  private volatile long shutdownGraceMs;
  private volatile ExecutorService listenerExecutor;

  protected AbstractPushCoordinator() {}

  /**
   * Wires the shared dispatch machinery and adopts the listener executor. Subclasses build the
   * bounded dispatch pool from their {@link CoordinatorThreading} and hand it in here, at the top
   * of {@code init()} after resolving their config and before any transport setup, so the executor
   * exists before the transport can deliver an inbound message.
   */
  protected final void configureDispatch(
      String coordinatorKind,
      String displayName,
      MetricsCollector metrics,
      NodeIdentityProvider identityProvider,
      int maxInboundPayloadChars,
      ExecutorService dispatchPool,
      long shutdownGraceMs) {
    this.coordinatorKind = coordinatorKind;
    this.displayName = displayName;
    this.metrics = metrics;
    this.identityProvider = identityProvider;
    this.maxInboundPayloadChars = maxInboundPayloadChars;
    this.shutdownGraceMs = shutdownGraceMs;
    this.listenerExecutor = Objects.requireNonNull(dispatchPool, "dispatchPool");
  }

  /**
   * The dispatch executor, for transports that need to schedule their own callbacks on it (e.g. an
   * async-publish completion handler). Available after {@link #configureDispatch} runs.
   */
  protected final ExecutorService listenerExecutor() {
    return listenerExecutor;
  }

  public void registerWakeupListener(Consumer<JobWakeupHint> listener) {
    Objects.requireNonNull(listener, "listener");
    if (closed.get()) {
      return;
    }
    listeners.add(listener);
    drainPreRegistrationBuffer();
  }

  /**
   * Self-suppression and metric tail shared by every transport. Resolves the local identity, drops
   * self-broadcasts, then either buffers (no listeners yet) or dispatches.
   */
  protected final void deliverDecodedPayload(NotifyPayload payload) {
    NodeIdentity local;
    try {
      local = new NodeIdentity(identityProvider.getNodeId());
    } catch (RuntimeException e) {
      onNodeIdentityProviderError(e);
      clusterWakeupReceived("ignored_provider_error");
      return;
    }
    if (payload.node().equals(local)) {
      clusterWakeupReceived("ignored_self");
      return;
    }
    clusterWakeupReceived("delivered");
    if (listeners.isEmpty()) {
      bufferOrDropOldest(payload);
      return;
    }
    dispatchToListeners(payload);
  }

  /**
   * Hook invoked when {@link NodeIdentityProvider#getNodeId()} throws while resolving the local
   * identity for self-suppression. Default is a no-op; transports that logged at this site before
   * the dispatch machinery was hoisted override this to preserve that log.
   */
  protected void onNodeIdentityProviderError(RuntimeException e) {
    // no-op by default
  }

  private void dispatchToListeners(NotifyPayload msg) {
    JobWakeupHint hint = new JobWakeupHint(msg.priority(), msg.node(), msg.executionTarget());
    for (Consumer<JobWakeupHint> listener : listeners) {
      try {
        listenerExecutor.execute(
            () -> {
              try {
                listener.accept(hint);
              } catch (RuntimeException listenerEx) {
                clusterWakeupReceived("listener_failure");
                log.warnf(
                    listenerEx,
                    "%s coordinator listener threw: %s — suppressing per SPI contract",
                    displayName,
                    listenerEx.getMessage());
              }
            });
      } catch (RuntimeException submitEx) {
        log.debugf(
            submitEx,
            "%s coordinator could not enqueue listener task: %s",
            displayName,
            submitEx.getMessage());
      }
    }
  }

  // Counts as overflow regardless of the second offer's outcome — the boolean is irrelevant.
  @SuppressWarnings("ResultOfMethodCallIgnored")
  private void bufferOrDropOldest(NotifyPayload msg) {
    if (preRegistrationBuffer.offer(msg)) {
      return;
    }
    preRegistrationBuffer.poll();
    preRegistrationBuffer.offer(msg);
    clusterWakeupReceived("pre_registration_overflow");
    log.warnf(
        "%s coordinator pre-registration buffer overflowed; oldest wakeup dropped", displayName);
  }

  private void drainPreRegistrationBuffer() {
    NotifyPayload msg;
    while ((msg = preRegistrationBuffer.poll()) != null) {
      dispatchToListeners(msg);
    }
  }

  protected final void clusterWakeupPublished(String outcome) {
    metrics.clusterWakeupPublished(coordinatorKind, outcome);
  }

  protected final void clusterWakeupReceived(String outcome) {
    metrics.clusterWakeupReceived(coordinatorKind, outcome);
  }

  /**
   * Rejects an inbound body that exceeds the configured character cap. Returns {@code true}
   * (already metric- and log-recorded) when the caller should drop the message.
   */
  protected final boolean rejectIfOversized(String body) {
    if (body != null && body.length() > maxInboundPayloadChars) {
      clusterWakeupReceived("parse_failure");
      log.warnf(
          "%s coordinator rejected oversized inbound payload (%d chars > cap %d)",
          displayName, body.length(), maxInboundPayloadChars);
      return true;
    }
    return false;
  }

  protected final void shutdownListenerExecutor() {
    ExecutorService executor = this.listenerExecutor;
    if (executor != null) {
      executor.shutdown();
      try {
        if (!executor.awaitTermination(shutdownGraceMs, TimeUnit.MILLISECONDS)) {
          executor.shutdownNow();
        }
      } catch (InterruptedException ignored) {
        Thread.currentThread().interrupt();
      }
    }
  }

  protected final boolean markClosed() {
    return closed.compareAndSet(false, true);
  }

  protected final boolean isClosed() {
    return closed.get();
  }
}
