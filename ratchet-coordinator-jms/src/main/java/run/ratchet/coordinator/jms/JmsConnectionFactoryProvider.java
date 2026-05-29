package run.ratchet.coordinator.jms;

import jakarta.jms.ConnectionFactory;
import jakarta.jms.Topic;

/**
 * Integration-extension SPI for resolving the JMS {@link ConnectionFactory} and {@link Topic} the
 * coordinator publishes / subscribes on.
 *
 * <p>This SPI is JMS-specific and intentionally NOT in {@code ratchet-api} — only the JMS
 * coordinator module knows what a {@code ConnectionFactory} is. It is a public extension point so
 * external deployments (Spring, Micronaut, embedded test brokers) can supply a programmatic
 * {@code @Alternative} implementation.
 *
 * <p>The default implementation ({@code JndiJmsConnectionFactoryProvider}) reads the JNDI names
 * from {@link JmsCoordinatorConfig} and looks them up via the platform's default {@code
 * InitialContext}. Standalone deployments (Spring, Micronaut, embedded broker for tests) override
 * by supplying a higher-priority {@code @Alternative} bean that constructs the {@link
 * ConnectionFactory} and {@link Topic} programmatically.
 *
 * @apiNote Implementations MUST be thread-safe: the coordinator may call {@link
 *     #connectionFactory()} and {@link #topic()} concurrently from publish and subscribe paths, and
 *     repeated calls during the bean's lifetime MUST return references the JMS provider considers
 *     equivalent (typically the same instance). Implementations SHOULD treat both methods as
 *     idempotent — the coordinator caches the resolved references for the lifetime of the
 *     coordinator bean and never closes them; lifecycle ownership of the {@link ConnectionFactory}
 *     and {@link Topic} rests with the provider (or its container).
 */
public interface JmsConnectionFactoryProvider {

  /**
   * The connection factory the coordinator will use to create its {@code JMSContext}.
   *
   * @return non-null {@link ConnectionFactory}; never returns {@code null}
   */
  ConnectionFactory connectionFactory();

  /**
   * The topic the coordinator publishes wakeup envelopes to and subscribes for inbound ones.
   *
   * @return non-null {@link Topic} reference; never returns {@code null}
   */
  Topic topic();
}
