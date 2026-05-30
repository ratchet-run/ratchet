package run.ratchet.coordinator.jms;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import jakarta.annotation.Resource;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSConsumer;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSDestinationDefinition;
import jakarta.jms.JMSRuntimeException;
import jakarta.jms.Message;
import jakarta.jms.TextMessage;
import jakarta.jms.Topic;
import java.io.File;
import java.io.FileFilter;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit5.ArquillianExtension;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.EmptyAsset;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import run.ratchet.api.JobPriority;
import run.ratchet.api.NodeIdentity;
import run.ratchet.coordinator.common.NotifyPayload;
import run.ratchet.coordinator.common.internal.NotifyPayloadCodec;
import run.ratchet.spi.JobWakeupHint;
import run.ratchet.spi.NodeIdentityProvider;

/**
 * Deploys {@link JmsClusterCoordinator} into a managed WildFly instance booted with the {@code
 * standalone-full} profile (which enables the {@code messaging-activemq} subsystem and binds {@code
 * java:comp/DefaultJMSConnectionFactory}). This is the one test that exercises the coordinator's
 * startup and message paths against a strict Jakarta EE JMS provider — the embedded-Artemis ITs run
 * in Java SE mode, which does not enforce the §12.3 ban on asynchronous delivery.
 *
 * <p>The proof is two-sided so neither half is vacuous:
 *
 * <ol>
 *   <li>{@code containerEnforcesAsyncDeliveryBan} confirms WildFly really does reject {@code
 *       setExceptionListener}/{@code setMessageListener} on an application-created context in this
 *       web component. If this ever stops throwing, the container is not enforcing §12.3 and the
 *       second assertion proves nothing.
 *   <li>{@code coordinatorStartsAndRoundTripsAcrossNodes} confirms the coordinator starts, runs its
 *       synchronous receive loop, and delivers wakeups in both directions on that same strict
 *       container — which it can only do because it never registers asynchronous delivery.
 * </ol>
 */
@ExtendWith(ArquillianExtension.class)
public class JmsClusterCoordinatorWildflyContainerIT {

  private static final long RECEIVE_TIMEOUT_MS = 15_000L;
  private static final String LOCAL_NODE = "node-A";
  private static final String TOPIC_JNDI = "java:app/jms/ratchetWakeupContainerIt";

  @Deployment
  public static WebArchive deployment() {
    return ShrinkWrap.create(WebArchive.class, "ratchet-jms-coordinator-it.war")
        .addClasses(
            JmsClusterCoordinatorWildflyContainerIT.class,
            ContainerJmsProvider.class,
            ContainerSpiCollaborators.class)
        // Bundle the ratchet artifacts as built jars rather than re-resolving from the local
        // repository: this stays correct under a reactor (-am) build without an intervening
        // `install`, and each jar carries only ratchet code (no jakarta.* API jars that would
        // collide with the container's own modules).
        .addAsLibraries(
            reactorJar("../ratchet-api/target", "ratchet-api-"),
            reactorJar("../ratchet-coordinator-common/target", "ratchet-coordinator-common-"),
            reactorJar("target", "ratchet-coordinator-jms-"))
        .addAsWebInfResource(EmptyAsset.INSTANCE, "beans.xml");
  }

  @Inject JmsClusterCoordinator coordinator;

  @Resource(lookup = "java:comp/DefaultJMSConnectionFactory")
  ConnectionFactory connectionFactory;

  @Resource(lookup = TOPIC_JNDI)
  Topic topic;

  @AfterEach
  void stopCoordinator() {
    // This bare WAR has no RatchetLifecycle to drive afterStop(), so close the coordinator here.
    // Without it the synchronous receive thread outlives the CDI context at container shutdown and
    // logs a WELD-000229 when it next touches the MetricsCollector. close() is idempotent.
    coordinator.close();
  }

  @Test
  public void containerEnforcesAsyncDeliveryBan() {
    // Direct evidence the container is §12.3-strict. The methods the old async model called must
    // throw on an application-created context in this (web) component; if they do not, WildFly is
    // not enforcing the ban and the coordinator's compliance can't be demonstrated here.
    try (JMSContext ctx = connectionFactory.createContext(JMSContext.AUTO_ACKNOWLEDGE)) {
      assertThrows(
          JMSRuntimeException.class,
          () -> ctx.setExceptionListener(e -> {}),
          "a managed JMSContext must reject setExceptionListener per Jakarta Messaging §12.3");
      JMSConsumer consumer = ctx.createConsumer(topic);
      assertThrows(
          JMSRuntimeException.class,
          () -> consumer.setMessageListener(m -> {}),
          "a managed JMSConsumer must reject setMessageListener per Jakarta Messaging §12.3");
    }
  }

  @Test
  public void coordinatorStartsAndRoundTripsAcrossNodes() throws Exception {
    // afterStart() drives connect(): createContext + createConsumer + the synchronous receive
    // loop. The async model would have called the §12.3-banned setMessageListener here and the
    // coordinator would have failed to come up on this container.
    assertDoesNotThrow(coordinator::afterStart);

    // Inbound: a wakeup from a different node clears both the broker-side selector and the
    // receive-side self-filter and reaches a registered listener via the sync receive loop.
    BlockingQueue<JobWakeupHint> delivered = new LinkedBlockingQueue<>();
    coordinator.registerWakeupListener(delivered::add);
    publishWakeupAs("node-EXTERNAL", JobPriority.NORMAL, "inbound-target");

    JobWakeupHint inbound = delivered.poll(RECEIVE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    assertNotNull(
        inbound, "the synchronous receive loop must deliver a cross-node wakeup on a strict container");
    assertEquals("inbound-target", inbound.executionTarget());

    // Outbound: notifyNewWork publishes an envelope a plain consumer can read back. The
    // coordinator stamps the message with the local node, so its own selector drops the self-copy
    // while this unfiltered probe still receives it.
    try (JMSContext probe = connectionFactory.createContext(JMSContext.AUTO_ACKNOWLEDGE)) {
      JMSConsumer raw = probe.createConsumer(topic);
      coordinator.notifyNewWork(JobPriority.HIGH, new NodeIdentity(LOCAL_NODE), "outbound-target");
      Message received = raw.receive(RECEIVE_TIMEOUT_MS);
      assertNotNull(received, "notifyNewWork must publish a wakeup envelope to the topic");
      String body = ((TextMessage) received).getText();
      assertEquals("outbound-target", new NotifyPayloadCodec().decode(body).executionTarget());
    }
  }

  private void publishWakeupAs(String node, JobPriority priority, String target) {
    String body =
        new NotifyPayloadCodec().encode(NotifyPayload.current(new NodeIdentity(node), priority, target));
    try (JMSContext ctx = connectionFactory.createContext(JMSContext.AUTO_ACKNOWLEDGE)) {
      TextMessage message = ctx.createTextMessage(body);
      // The coordinator's consumer selects on `node <> '<localId>'`; stamp it so the broker-side
      // self-filter lets a foreign-node wakeup through.
      message.setStringProperty("node", node);
      message.setStringProperty("prio", priority.name());
      ctx.createProducer().send(topic, message);
    } catch (Exception e) {
      throw new IllegalStateException("failed to publish test wakeup", e);
    }
  }

  private static File reactorJar(String dir, String prefix) {
    File target = new File(dir);
    File[] matches =
        target.listFiles(
            (FileFilter)
                f ->
                    f.getName().startsWith(prefix)
                        && f.getName().endsWith(".jar")
                        && !f.getName().contains("-sources")
                        && !f.getName().contains("-javadoc")
                        && !f.getName().contains("-tests"));
    if (matches == null || matches.length == 0) {
      throw new IllegalStateException(
          "reactor jar " + prefix + "*.jar not found in " + target.getAbsolutePath());
    }
    return matches[0];
  }

  /**
   * Supplies the JMS resources from the container: the platform default connection factory and an
   * application-declared topic. {@link JMSDestinationDefinition} provisions the topic at deploy time
   * so the test needs no server-side CLI setup.
   */
  @JMSDestinationDefinition(
      name = TOPIC_JNDI,
      interfaceName = "jakarta.jms.Topic",
      destinationName = "ratchetWakeupContainerIt")
  @ApplicationScoped
  public static class ContainerJmsProvider implements JmsConnectionFactoryProvider {

    @Resource(lookup = "java:comp/DefaultJMSConnectionFactory")
    ConnectionFactory connectionFactory;

    @Resource(lookup = TOPIC_JNDI)
    Topic topic;

    @Override
    public ConnectionFactory connectionFactory() {
      return connectionFactory;
    }

    @Override
    public Topic topic() {
      return topic;
    }
  }

  /**
   * Produces the one SPI collaborator the WAR does not already carry. The coordinator's {@code
   * MetricsCollector} resolves to the bundled {@code NoOpMetricsCollector} (an {@code
   * @ApplicationScoped @Default} bean in ratchet-api) under the container's full bean discovery —
   * adding a producer for it here would make the type ambiguous (WELD-001409). {@code
   * NodeIdentityProvider} has no bundled default in this WAR (the RI is not deployed), so it is
   * produced explicitly.
   */
  @ApplicationScoped
  public static class ContainerSpiCollaborators {

    @Produces
    NodeIdentityProvider nodeIdentityProvider() {
      return () -> LOCAL_NODE;
    }
  }
}
