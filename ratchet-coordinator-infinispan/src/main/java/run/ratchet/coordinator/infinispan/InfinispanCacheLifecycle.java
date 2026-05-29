package run.ratchet.coordinator.infinispan;

import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.infinispan.Cache;
import org.jboss.logging.Logger;
import run.ratchet.coordinator.common.NotifyPayload;
import run.ratchet.coordinator.common.internal.NotifyPayloadCodec;

/**
 * Owns the cache reference and the {@link InfinispanWakeupListener} registration for one
 * coordinator instance.
 *
 * <p>Listener registration is awaited at startup so {@code afterStart} cannot return until inbound
 * events are wired. Removal is best-effort because the {@code EmbeddedCacheManager} often outlives
 * the coordinator (WildFly subsystem-managed): explicit removal is hygiene, not correctness.
 */
final class InfinispanCacheLifecycle {

  private static final Logger log = Logger.getLogger(InfinispanCacheLifecycle.class);

  private final Cache<String, String> cache;
  private final InfinispanCoordinatorConfig config;
  private final NotifyPayloadCodec codec;
  private final Consumer<NotifyPayload> inboundDispatch;
  private final Runnable onParseFailure;
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private volatile InfinispanWakeupListener listener;

  InfinispanCacheLifecycle(
      Cache<String, String> cache,
      InfinispanCoordinatorConfig config,
      NotifyPayloadCodec codec,
      Consumer<NotifyPayload> inboundDispatch,
      Runnable onParseFailure) {
    this.cache = Objects.requireNonNull(cache, "cache");
    this.config = Objects.requireNonNull(config, "config");
    this.codec = Objects.requireNonNull(codec, "codec");
    this.inboundDispatch = Objects.requireNonNull(inboundDispatch, "inboundDispatch");
    this.onParseFailure = Objects.requireNonNull(onParseFailure, "onParseFailure");
  }

  /** Register the clustered listener and block until registration completes. */
  void start() {
    if (closed.get()) {
      return;
    }
    listener =
        new InfinispanWakeupListener(
            codec, config.maxInboundPayloadChars(), inboundDispatch, onParseFailure);
    try {
      cache
          .addListenerAsync(listener)
          .toCompletableFuture()
          .get(config.shutdownGraceMs(), TimeUnit.MILLISECONDS);
    } catch (TimeoutException te) {
      throw new IllegalStateException(
          "Infinispan listener registration timed out after " + config.shutdownGraceMs() + "ms",
          te);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted while registering Infinispan listener", ie);
    } catch (ExecutionException ee) {
      throw new IllegalStateException("Infinispan listener registration failed", ee);
    }
  }

  /**
   * Publish with a per-entry TTL and return the async completion so the caller can metric the real
   * outcome. The TTL is sourced from {@link InfinispanCoordinatorConfig#wakeupTtlSeconds()} so
   * cache definitions that omit a lifespan still bound storage.
   */
  CompletionStage<String> publish(String key, String value) {
    return cache.putAsync(key, value, config.wakeupTtlSeconds(), TimeUnit.SECONDS);
  }

  /** True after {@link #close()} has been called. */
  boolean isClosed() {
    return closed.get();
  }

  /**
   * Remove the listener with a bounded wait so close() does not stall on a stuck cache. Failures
   * are logged but never thrown — the cache manager's lifecycle (provider-owned) will release the
   * listener when it stops.
   */
  void close() {
    if (!closed.compareAndSet(false, true)) {
      return;
    }
    InfinispanWakeupListener l = this.listener;
    if (l == null) {
      return;
    }
    try {
      cache
          .removeListenerAsync(l)
          .toCompletableFuture()
          .get(config.shutdownGraceMs(), TimeUnit.MILLISECONDS);
    } catch (TimeoutException te) {
      log.warnf(
          "Infinispan listener removal timed out after %dms; relying on cache manager shutdown",
          config.shutdownGraceMs());
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
    } catch (ExecutionException ee) {
      log.warnf(ee, "Infinispan listener removal failed: %s", ee.getMessage());
    } finally {
      listener = null;
    }
  }
}
