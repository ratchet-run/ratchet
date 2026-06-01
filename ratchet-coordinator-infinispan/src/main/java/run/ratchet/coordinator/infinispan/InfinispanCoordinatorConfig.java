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

import java.util.Objects;
import java.util.Optional;
import run.ratchet.coordinator.common.CoordinatorCells;
import run.ratchet.coordinator.common.CoordinatorConfigChecks;

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
 * @param listenerExecutorQueueCapacity bound on the dispatch pool's pending-task queue. When the
 *     bound is hit the oldest queued wakeup is discarded (wakeups are advisory and a fresher one
 *     supersedes a stale one). Default 1024; replaces the unbounded queue that risked OOM under
 *     sustained wakeup pressure.
 * @param shutdownGraceMs max wait for the @Listener removal on close. Default 5000.
 */
public record InfinispanCoordinatorConfig(
    String cacheName,
    Optional<String> cellId,
    long wakeupTtlSeconds,
    int maxInboundPayloadChars,
    int listenerExecutorThreads,
    int listenerExecutorQueueCapacity,
    long shutdownGraceMs) {

  public static final String DEFAULT_CACHE_NAME = "wakeup";

  public InfinispanCoordinatorConfig {
    Objects.requireNonNull(cacheName, "cacheName");
    if (cacheName.isBlank()) {
      throw new IllegalArgumentException("cacheName must be non-blank");
    }
    Objects.requireNonNull(cellId, "cellId");
    CoordinatorConfigChecks.requirePositive(wakeupTtlSeconds, "wakeupTtlSeconds");
    CoordinatorConfigChecks.requirePositive(maxInboundPayloadChars, "maxInboundPayloadChars");
    CoordinatorConfigChecks.requireAtLeastOne(listenerExecutorThreads, "listenerExecutorThreads");
    CoordinatorConfigChecks.requireAtLeastOne(
        listenerExecutorQueueCapacity, "listenerExecutorQueueCapacity");
    CoordinatorConfigChecks.requirePositive(shutdownGraceMs, "shutdownGraceMs");
  }

  /** Default tuning suitable for WildFly + standalone Infinispan deployments. */
  public static InfinispanCoordinatorConfig defaults() {
    return new InfinispanCoordinatorConfig(
        DEFAULT_CACHE_NAME, Optional.empty(), 60L, 16_384, 2, 1_024, 5_000L);
  }

  /** The fully-qualified cache name after applying the optional {@code cellId} suffix. */
  public String effectiveCacheName() {
    return CoordinatorCells.suffixed(cacheName, cellId);
  }
}
