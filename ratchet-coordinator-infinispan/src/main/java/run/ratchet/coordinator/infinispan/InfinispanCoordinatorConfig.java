package run.ratchet.coordinator.infinispan;

import java.util.Objects;
import java.util.Optional;

/**
 * Tunable configuration for {@link InfinispanClusterCoordinator}.
 *
 * <p>Infinispan does the heavy lifting on cluster transport (JGroups), serialization, and
 * replication strategy. The coordinator names a cache inside a cache container; the per-entry TTL
 * is supplied here so wakeup entries always expire — operator cache definitions that omit a
 * lifespan would otherwise grow without bound because the coordinator generates a fresh key per
 * notify.
 *
 * @param cacheName name of the wakeup cache inside the configured cache container; default {@code
 *     "wakeup"}
 * @param cellId optional per-cell suffix appended to {@link #cacheName} so multi-cell deployments
 *     sharing one cache container isolate wakeup traffic on separate cache definitions
 * @param wakeupTtlSeconds per-entry TTL applied to every {@code putAsync} so wakeup entries expire
 *     regardless of cache-level lifespan configuration. Must be {@code > 0}. Default 60s — long
 *     enough to survive a slow replication round-trip, short enough that any stuck entry evicts
 *     within a minute.
 * @param maxInboundPayloadChars hard cap on the character length of an inbound cache value before
 *     the codec rejects it as malformed. Wakeup envelopes are ~80 chars; the default 16384 leaves
 *     three orders of magnitude of headroom for future fields while bounding malformed JSON.
 * @param listenerExecutorThreads worker threads dispatching inbound cache events to registered
 *     listeners. Default 2 — keeps one slow listener from stalling all others.
 * @param shutdownGraceMs max wait for the @Listener removal on close. Default 5000.
 */
public record InfinispanCoordinatorConfig(
    String cacheName,
    Optional<String> cellId,
    long wakeupTtlSeconds,
    int maxInboundPayloadChars,
    int listenerExecutorThreads,
    long shutdownGraceMs) {

  public static final String DEFAULT_CACHE_NAME = "wakeup";

  public InfinispanCoordinatorConfig {
    Objects.requireNonNull(cacheName, "cacheName");
    if (cacheName.isBlank()) {
      throw new IllegalArgumentException("cacheName must be non-blank");
    }
    Objects.requireNonNull(cellId, "cellId");
    if (wakeupTtlSeconds <= 0) {
      throw new IllegalArgumentException("wakeupTtlSeconds must be > 0");
    }
    if (maxInboundPayloadChars <= 0) {
      throw new IllegalArgumentException("maxInboundPayloadChars must be > 0");
    }
    if (listenerExecutorThreads < 1) {
      throw new IllegalArgumentException("listenerExecutorThreads must be >= 1");
    }
    if (shutdownGraceMs <= 0) {
      throw new IllegalArgumentException("shutdownGraceMs must be > 0");
    }
  }

  /** Default tuning suitable for WildFly + standalone Infinispan deployments. */
  public static InfinispanCoordinatorConfig defaults() {
    return new InfinispanCoordinatorConfig(
        DEFAULT_CACHE_NAME, Optional.empty(), 60L, 16_384, 2, 5_000L);
  }

  /** The fully-qualified cache name after applying the optional {@code cellId} suffix. */
  public String effectiveCacheName() {
    return cellId.map(c -> cacheName + "_" + c).orElse(cacheName);
  }
}
