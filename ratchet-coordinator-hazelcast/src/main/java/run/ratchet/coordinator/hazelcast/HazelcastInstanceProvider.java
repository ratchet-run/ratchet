package run.ratchet.coordinator.hazelcast;

import com.hazelcast.core.HazelcastInstance;

/**
 * Module-internal SPI for resolving the {@link HazelcastInstance} the coordinator publishes /
 * subscribes on. Payara deployments use the JNDI-driven default {@code payara/Hazelcast};
 * standalone deployments supply a higher-priority {@code @Alternative} bean that wraps a
 * programmatically-constructed instance.
 */
public interface HazelcastInstanceProvider {

  /** The Hazelcast instance the coordinator looks up its topic from. */
  HazelcastInstance hazelcastInstance();
}
