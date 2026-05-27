package run.ratchet.coordinator.infinispan;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;
import run.ratchet.coordinator.infinispan.InfinispanNotifyPayloadCodec.NotifyPayload;

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
@Listener(clustered = true, includeCurrentState = false)
public final class InfinispanWakeupListener {

  private final InfinispanNotifyPayloadCodec codec;
  private final Consumer<NotifyPayload> inboundDispatch;
  private final Runnable onParseFailure;

  InfinispanWakeupListener(
      InfinispanNotifyPayloadCodec codec,
      Consumer<NotifyPayload> inboundDispatch,
      Runnable onParseFailure) {
    this.codec = codec;
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
    try {
      NotifyPayload payload = codec.decode(value);
      inboundDispatch.accept(payload);
    } catch (RuntimeException parseEx) {
      onParseFailure.run();
    }
    return CompletableFuture.completedFuture(null);
  }
}
