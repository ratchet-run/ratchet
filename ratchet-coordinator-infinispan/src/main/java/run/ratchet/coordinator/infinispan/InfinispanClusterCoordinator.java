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

import static run.ratchet.coordinator.common.internal.JsonProviders.requireJsonProvider;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.interceptor.Interceptor;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import org.infinispan.Cache;
import org.infinispan.manager.EmbeddedCacheManager;
import org.jboss.logging.Logger;
import run.ratchet.api.JobPriority;
import run.ratchet.api.NodeIdentity;
import run.ratchet.coordinator.common.AbstractPushCoordinator;
import run.ratchet.coordinator.common.CoordinatorSupport;
import run.ratchet.coordinator.common.CoordinatorThreading;
import run.ratchet.coordinator.common.NotifyPayload;
import run.ratchet.spi.ClusterCoordinator;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.spi.SchedulerLifecycleHook;

/**
 * Infinispan embedded-cache {@link ClusterCoordinator}: publishes wakeup envelopes as {@code
 * putAsync} entries on a clustered cache and dispatches inbound {@code CacheEntryCreated} events to
 * registered listeners.
 *
 * <p>Adding this module to a deployment activates push-based cross-node wakeups in place of the
 * default {@code NoOpClusterCoordinator}. Activation is via {@link Alternative} + {@link Priority}.
 *
 * <p>Self-suppression is receive-side only — Infinispan has no cluster-side filter that can drop
 * events by source-node metadata. Cluster bandwidth carries every node's broadcast back to its
 * sender; the receive-side filter discards self-broadcasts.
 *
 * <p>{@link #close()} releases the coordinator's listener registration and dispatch executor. The
 * {@link EmbeddedCacheManager} is provider-owned (typically WildFly subsystem-managed) and is never
 * stopped here — doing so would corrupt other applications using the same cache container.
 */
@ApplicationScoped
@Alternative
@Priority(Interceptor.Priority.PLATFORM_BEFORE + 100)
// Coordinator @Priority order: see PostgresqlListenNotifyCoordinator. Operators MUST pull in
// exactly one coordinator module; distinct priorities only prevent CDI ambiguity errors on a
// transitive double-pull.
public class InfinispanClusterCoordinator extends AbstractPushCoordinator
    implements ClusterCoordinator, SchedulerLifecycleHook {

  private static final Logger log = Logger.getLogger(InfinispanClusterCoordinator.class);

  static final String COORDINATOR_KIND = "infinispan";

  @Inject NodeIdentityProvider identityProvider;

  /**
   * Resolved lazily in {@link #init()}. The config record has a {@code defaults()} factory but is
   * not a managed bean, so it is injected as an {@link Instance} with a defaults() fallback; a
   * direct {@code @Inject InfinispanCoordinatorConfig} would be an unsatisfied dependency that
   * fails deployment validation out of the box.
   */
  @Inject Instance<InfinispanCoordinatorConfig> configInstance;

  @Inject @Any Instance<InfinispanCacheManagerProvider> providerInstance;
  @Inject MetricsCollector metrics;

  private InfinispanCoordinatorConfig config;

  private final AtomicLong sendSequence = new AtomicLong();

  private InfinispanCacheLifecycle cacheLifecycle;
  private Cache<String, String> directCache;
  private CoordinatorThreading threading;

  protected InfinispanClusterCoordinator() {
    // CDI proxy constructor.
  }

  /** Test/non-CDI constructor that takes a pre-resolved cache directly. */
  InfinispanClusterCoordinator(
      NodeIdentityProvider identityProvider,
      InfinispanCoordinatorConfig config,
      Cache<String, String> cache,
      MetricsCollector metrics) {
    this.identityProvider = Objects.requireNonNull(identityProvider, "identityProvider");
    this.config = Objects.requireNonNull(config, "config");
    this.directCache = Objects.requireNonNull(cache, "cache");
    this.metrics = metrics;
    this.threading = CoordinatorThreading.standalone("ratchet-coordinator-infinispan");
  }

  @PostConstruct
  void init() {
    if (config == null) {
      config =
          CoordinatorSupport.resolveConfigOrDefault(
              configInstance, InfinispanCoordinatorConfig::defaults);
    }
    Objects.requireNonNull(config, "config");
    Objects.requireNonNull(identityProvider, "identityProvider");
    requireJsonProvider();
    if (threading == null) {
      // CDI/production path: route the dispatch pool through the container's managed thread
      // factory. Standalone is an explicit opt-in via the test constructor.
      threading = CoordinatorThreading.managed("ratchet-coordinator-infinispan");
    }
    configureDispatch(
        COORDINATOR_KIND,
        "Infinispan",
        metrics,
        identityProvider,
        config.maxInboundPayloadChars(),
        threading.newDispatchPool(
            "dispatch", config.listenerExecutorThreads(), config.listenerExecutorQueueCapacity()),
        config.shutdownGraceMs());
    Cache<String, String> cache;
    if (directCache != null) {
      cache = directCache;
    } else {
      InfinispanCacheManagerProvider provider =
          CoordinatorSupport.resolveRequired(
              providerInstance,
              "No InfinispanCacheManagerProvider available. Provide a @Produces"
                  + " InfinispanCacheManagerProvider or use the WildFly subsystem-bound default.",
              "Multiple InfinispanCacheManagerProvider beans visible; first match wins. Use"
                  + " @Alternative + @Priority for disambiguation.");
      EmbeddedCacheManager cacheManager = provider.cacheManager();
      cache = cacheManager.getCache(config.effectiveCacheName());
    }
    cacheLifecycle =
        new InfinispanCacheLifecycle(
            cache, config, codec, this::onInboundNotification, this::onParseFailure);
  }

  @Override
  public void afterStart() {
    if (isClosed()) {
      return;
    }
    if (cacheLifecycle == null) {
      throw new IllegalStateException("afterStart() called before init()");
    }
    cacheLifecycle.start();
  }

  @Override
  public void notifyNewWork(JobPriority priority, NodeIdentity source, String executionTarget) {
    Objects.requireNonNull(priority, "priority");
    Objects.requireNonNull(source, "source");
    if (isClosed()) {
      return;
    }
    try {
      String key = source.value() + ":" + sendSequence.incrementAndGet();
      String value = codec.encode(NotifyPayload.current(source, priority, executionTarget));
      cacheLifecycle
          .publish(key, value)
          .whenComplete(
              (v, throwable) -> clusterWakeupPublished(throwable == null ? "success" : "failure"));
    } catch (RuntimeException ex) {
      clusterWakeupPublished("failure");
      log.warnf(
          ex,
          "Infinispan coordinator notifyNewWork transport/encode failure: %s — wakeup dropped",
          ex.getMessage());
    }
  }

  /**
   * Hook chain entry point — runs during {@code RatchetLifecycle.onShutdown} after pollers and the
   * execution coordinator have stopped. Delegates to {@link #close()}, which is idempotent.
   */
  @Override
  public void afterStop() {
    close();
  }

  @Override
  public void close() {
    if (!markClosed()) {
      return;
    }
    InfinispanCacheLifecycle lifecycle = this.cacheLifecycle;
    if (lifecycle != null) {
      lifecycle.close();
    }
    shutdownListenerExecutor();
  }

  /** Dispatch path from the cache listener. Self-suppresses then routes to listeners. */
  void onInboundNotification(NotifyPayload msg) {
    deliverDecodedPayload(msg);
  }

  private void onParseFailure() {
    clusterWakeupReceived("parse_failure");
  }

  /** Test accessor for the harness — exposes the lifecycle so readiness can be polled. */
  InfinispanCacheLifecycle lifecycle() {
    return cacheLifecycle;
  }
}
