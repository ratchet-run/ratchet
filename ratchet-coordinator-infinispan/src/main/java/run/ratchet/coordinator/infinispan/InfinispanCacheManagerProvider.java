package run.ratchet.coordinator.infinispan;

import org.infinispan.manager.EmbeddedCacheManager;

/**
 * SPI for resolving the {@link EmbeddedCacheManager} the coordinator publishes / subscribes
 * against.
 *
 * <p>WildFly deployments use the default JNDI-driven provider; standalone deployments (Spring,
 * Micronaut, embedded test setups) supply a higher-priority {@code @Alternative} bean that
 * constructs the cache manager programmatically.
 *
 * @apiNote Implementations MUST be CDI beans (typically {@code @ApplicationScoped} or
 *     {@code @Dependent}). Custom providers are selected by annotating the implementation with
 *     {@code @Alternative} and a {@code @Priority} higher than the bundled default; the
 *     coordinator's resolver picks the single highest-{@code @Priority} active alternative and
 *     fails fast if more than one is enabled with the same priority. There must be exactly one
 *     active provider at runtime — the single-active-provider invariant is enforced by {@code
 *     resolveProvider()} inside the coordinator. The coordinator does NOT own the returned {@link
 *     EmbeddedCacheManager}; lifecycle (start / stop) is the provider's responsibility.
 */
public interface InfinispanCacheManagerProvider {

  /** The cache manager the coordinator will look up its wakeup cache from. */
  EmbeddedCacheManager cacheManager();
}
