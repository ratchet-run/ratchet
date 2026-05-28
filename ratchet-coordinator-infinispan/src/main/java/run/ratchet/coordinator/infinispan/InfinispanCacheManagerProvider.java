package run.ratchet.coordinator.infinispan;

import org.infinispan.manager.EmbeddedCacheManager;

/**
 * Module-internal SPI for resolving the {@link EmbeddedCacheManager} the coordinator publishes /
 * subscribes against.
 *
 * <p>WildFly deployments use the default JNDI-driven provider; standalone deployments (Spring,
 * Micronaut, embedded test setups) supply a higher-priority {@code @Alternative} bean that
 * constructs the cache manager programmatically.
 */
public interface InfinispanCacheManagerProvider {

  /** The cache manager the coordinator will look up its wakeup cache from. */
  EmbeddedCacheManager cacheManager();
}
