package run.ratchet.coordinator.hazelcast;

import com.hazelcast.core.HazelcastInstance;

/**
 * SPI for resolving the {@link HazelcastInstance} the coordinator publishes / subscribes on. Payara
 * deployments use the JNDI-driven default {@code payara/Hazelcast}; standalone deployments supply a
 * higher-priority {@code @Alternative} bean that wraps a programmatically-constructed instance.
 *
 * @apiNote Implementations MUST be CDI beans (typically {@code @ApplicationScoped} or
 *     {@code @Dependent}). The coordinator resolves the active provider via {@code Instance<T>} and
 *     selects the highest-{@code @Priority} {@code @Alternative}. The coordinator does NOT own the
 *     returned {@link HazelcastInstance}: the provider is responsible for the instance's lifecycle,
 *     and the coordinator MUST NOT call {@code shutdown()} on it. The {@link #hazelcastInstance()}
 *     method MUST return a non-null instance; returning {@code null} fails the coordinator's
 *     lifecycle hook.
 */
public interface HazelcastInstanceProvider {

  /** The Hazelcast instance the coordinator looks up its topic from. */
  HazelcastInstance hazelcastInstance();
}
