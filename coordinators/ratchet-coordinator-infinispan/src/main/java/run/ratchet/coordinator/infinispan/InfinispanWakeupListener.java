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
package run.ratchet.coordinator.infinispan;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;
import org.jboss.logging.Logger;
import run.ratchet.coordinator.common.NotifyPayload;
import run.ratchet.coordinator.common.internal.NotifyPayloadCodec;

/**
 * Clustered Infinispan listener that decodes each created entry's value and forwards to the
 * coordinator's inbound dispatch path. Three load-bearing decisions encoded here:
 *
 * <ul>
 *   <li>{@code clustered = true} — required for cluster-wide event delivery; default {@code false}
 *       fires only on local-node events.
 *   <li>{@code includeCurrentState = false} — explicit so the listener does not replay pre-existing
 *       entries on registration (that would be replay, not signaling).
 *   <li>Null-guard on {@code event.getValue()} — entries can evict between creation and listener
 *       dispatch under memory pressure; without the guard the NPE propagates to Infinispan's
 *       dispatch thread, which can terminate the listener registration silently on Infinispan 14+.
 * </ul>
 */
// Package-private: Infinispan's @Listener annotation processor operates on the registered instance
// via reflection (addListenerAsync), NOT on the class's public modifier, so package-private is
// safe on Infinispan 14+. The class is an internal dispatch helper, not an extension point. The
// onEntryCreated method MUST remain public — Infinispan's annotation scan still requires public
// event-handler methods.
@Listener(clustered = true, includeCurrentState = false)
final class InfinispanWakeupListener {

  private static final Logger log = Logger.getLogger(InfinispanWakeupListener.class);

  private final NotifyPayloadCodec codec;
  private final int maxInboundPayloadChars;
  private final Consumer<NotifyPayload> inboundDispatch;
  private final Runnable onParseFailure;

  InfinispanWakeupListener(
      NotifyPayloadCodec codec,
      int maxInboundPayloadChars,
      Consumer<NotifyPayload> inboundDispatch,
      Runnable onParseFailure) {
    this.codec = codec;
    this.maxInboundPayloadChars = maxInboundPayloadChars;
    this.inboundDispatch = inboundDispatch;
    this.onParseFailure = onParseFailure;
  }

  @CacheEntryCreated
  public CompletionStage<Void> onEntryCreated(CacheEntryCreatedEvent<String, String> event) {
    if (event.isPre()) {
      return CompletableFuture.completedFuture(null);
    }
    String value = event.getValue();
    if (value == null) {
      // Entry evicted between creation and listener dispatch under memory pressure. Surface as
      // parse failure so the metric increments; do NOT let an NPE propagate, which can terminate
      // the listener registration under some Infinispan 14+ configurations.
      onParseFailure.run();
      return CompletableFuture.completedFuture(null);
    }
    if (value.length() > maxInboundPayloadChars) {
      onParseFailure.run();
      log.warnf(
          "Infinispan coordinator rejected oversized inbound payload (%d chars > cap %d)",
          value.length(), maxInboundPayloadChars);
      return CompletableFuture.completedFuture(null);
    }
    try {
      NotifyPayload payload = codec.decode(value);
      inboundDispatch.accept(payload);
    } catch (RuntimeException parseEx) {
      onParseFailure.run();
    }
    return CompletableFuture.completedFuture(null);
  }
}
