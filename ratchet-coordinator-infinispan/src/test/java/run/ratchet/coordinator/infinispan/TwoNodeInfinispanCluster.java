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

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;

/**
 * Two-node embedded Infinispan cluster for tests. Both cache managers live in the same JVM and join
 * via JGroups TCP loopback — fast (no Docker, no real network), deterministic, and small enough
 * that a hundred test cases finish in under a minute.
 */
public final class TwoNodeInfinispanCluster implements AutoCloseable {

  private final EmbeddedCacheManager managerA;
  private final EmbeddedCacheManager managerB;
  private final String cacheName;
  private final String clusterName;

  public TwoNodeInfinispanCluster(String cacheName) throws IOException {
    this.cacheName = cacheName;
    this.clusterName = "ratchet-tck-" + Long.toHexString(System.nanoTime());
    Configuration cacheConfig = newCacheConfig();
    this.managerA = new DefaultCacheManager(newGlobalConfig());
    this.managerA.defineConfiguration(cacheName, cacheConfig);
    this.managerB = new DefaultCacheManager(newGlobalConfig());
    this.managerB.defineConfiguration(cacheName, cacheConfig);
    // Touch the caches so they start and join the cluster.
    this.managerA.getCache(cacheName);
    this.managerB.getCache(cacheName);
    awaitClusterFormed();
  }

  public EmbeddedCacheManager managerA() {
    return managerA;
  }

  public EmbeddedCacheManager managerB() {
    return managerB;
  }

  public String cacheName() {
    return cacheName;
  }

  /** Stop manager A — used to force a cluster-side transport failure from B's perspective. */
  public void stopManagerA() {
    managerA.stop();
  }

  /** Stop manager B — used to force a cluster-side transport failure from A's perspective. */
  public void stopManagerB() {
    managerB.stop();
  }

  @Override
  public void close() {
    safeStop(managerA);
    safeStop(managerB);
  }

  private static void safeStop(EmbeddedCacheManager m) {
    try {
      if (m != null && m.getStatus().allowInvocations()) {
        m.stop();
      }
    } catch (Exception ignored) {
      // best-effort
    }
  }

  private void awaitClusterFormed() {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (System.nanoTime() < deadline) {
      if (managerA.getMembers() != null
          && managerA.getMembers().size() == 2
          && managerB.getMembers() != null
          && managerB.getMembers().size() == 2) {
        return;
      }
      try {
        Thread.sleep(20);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException("interrupted awaiting cluster formation", e);
      }
    }
    throw new IllegalStateException("cluster did not form 2 members within 10s");
  }

  private GlobalConfiguration newGlobalConfig() {
    GlobalConfigurationBuilder builder = GlobalConfigurationBuilder.defaultClusteredBuilder();
    builder
        .transport()
        .clusterName(clusterName)
        .nodeName("node-" + Long.toHexString(System.nanoTime()))
        .addProperty("jgroups.bind.address", "127.0.0.1");
    return builder.build();
  }

  private static Configuration newCacheConfig() {
    return new ConfigurationBuilder()
        .clustering()
        .cacheMode(CacheMode.REPL_ASYNC)
        .expiration()
        .lifespan(60, TimeUnit.SECONDS)
        .memory()
        .maxCount(1_000)
        .whenFull(EvictionStrategy.REMOVE)
        .build();
  }
}
