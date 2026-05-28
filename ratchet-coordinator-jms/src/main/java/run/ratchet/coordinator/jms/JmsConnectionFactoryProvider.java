package run.ratchet.coordinator.jms;

import jakarta.jms.ConnectionFactory;
import jakarta.jms.Topic;

/**
 * Module-internal SPI for resolving the JMS {@link ConnectionFactory} and {@link Topic} the
 * coordinator publishes / subscribes on.
 *
 * <p>This SPI is JMS-specific and intentionally NOT in {@code ratchet-api} — only the JMS
 * coordinator module knows what a {@code ConnectionFactory} is.
 *
 * <p>The default implementation ({@code JndiJmsConnectionFactoryProvider}) reads the JNDI names
 * from {@link JmsCoordinatorConfig} and looks them up via the platform's default {@code
 * InitialContext}. Standalone deployments (Spring, Micronaut, embedded broker for tests) override
 * by supplying a higher-priority {@code @Alternative} bean that constructs the {@link
 * ConnectionFactory} and {@link Topic} programmatically.
 */
public interface JmsConnectionFactoryProvider {

  /** The connection factory the coordinator will use to create its {@code JMSContext}. */
  ConnectionFactory connectionFactory();

  /** The topic the coordinator publishes wakeup envelopes to and subscribes for inbound ones. */
  Topic topic();
}
