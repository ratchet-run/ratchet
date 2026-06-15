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
package run.ratchet.coordinator.jms;

import static run.ratchet.coordinator.common.internal.JsonProviders.requireJsonProvider;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.interceptor.Interceptor;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSException;
import jakarta.jms.JMSRuntimeException;
import jakarta.jms.Message;
import jakarta.jms.TextMessage;
import jakarta.jms.Topic;
import java.util.Objects;
import org.jboss.logging.Logger;
import run.ratchet.api.JobPriority;
import run.ratchet.api.NodeIdentity;
import run.ratchet.coordinator.common.AbstractPushCoordinator;
import run.ratchet.coordinator.common.CoordinatorSupport;
import run.ratchet.coordinator.common.CoordinatorThreading;
import run.ratchet.coordinator.common.DecodeException;
import run.ratchet.coordinator.common.NotifyPayload;
import run.ratchet.spi.ClusterCoordinator;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.spi.SchedulerLifecycleHook;

/**
 * Jakarta Messaging {@link ClusterCoordinator}: publishes wakeup envelopes as {@link TextMessage}s
 * on a single shared topic and dispatches inbound messages to registered listeners.
 *
 * <p>Adding this module to a Jakarta EE deployment activates push-based cross-node wakeups in place
 * of the default {@code NoOpClusterCoordinator}. Activation is via {@link Alternative} + {@link
 * Priority} — per CDI 4.0 §4.1.1 a {@code @Priority}-annotated alternative is selected globally
 * across all archives, so consumers do not need to edit application-side {@code beans.xml}.
 *
 * <p>Self-suppression is two-layered:
 *
 * <ol>
 *   <li>Broker-side: a JMS selector {@code node <> '<localId>'} is installed on the consumer when
 *       {@link JmsCoordinatorConfig#brokerSideSelfFilter()} is true (default). Saves bandwidth on
 *       brokers that implement selector filtering server-side.
 *   <li>Receive-side: every inbound envelope is compared to the local {@link NodeIdentity}; matches
 *       are dropped and counted as {@code ignored_self}. Always-on defense-in-depth that catches
 *       brokers with buggy selector implementations.
 * </ol>
 *
 * <p>{@link #close()} releases only resources this coordinator allocated: the {@link
 * JmsConnectionLifecycle}-owned context (which the JMS spec says transitively closes its producer
 * and consumer) and the listener executor. The {@link jakarta.jms.ConnectionFactory} and {@link
 * jakarta.jms.Topic} are provider-owned and never closed here.
 */
@ApplicationScoped
@Alternative
@Priority(Interceptor.Priority.PLATFORM_BEFORE + 300)
// Coordinator @Priority order: see PostgresqlListenNotifyCoordinator. Operators MUST pull in
// exactly one coordinator module; distinct priorities only prevent CDI ambiguity errors on a
// transitive double-pull.
public class JmsClusterCoordinator extends AbstractPushCoordinator
    implements ClusterCoordinator, SchedulerLifecycleHook {

  private static final Logger log = Logger.getLogger(JmsClusterCoordinator.class);

  static final String COORDINATOR_KIND = "jms";

  @Inject NodeIdentityProvider identityProvider;

  /**
   * Resolved lazily in {@link #init()}. The config record has a {@code defaults()} factory but is
   * not a managed bean, so it is injected as an {@link Instance} with a defaults() fallback; a
   * direct {@code @Inject JmsCoordinatorConfig} would be an unsatisfied dependency that fails
   * deployment validation out of the box.
   */
  @Inject Instance<JmsCoordinatorConfig> configInstance;

  @Inject @Any Instance<JmsConnectionFactoryProvider> providerInstance;
  @Inject MetricsCollector metrics;

  private JmsCoordinatorConfig config;

  private JmsConnectionLifecycle connectionLifecycle;
  private ConnectionFactory directConnectionFactory;
  private Topic topic;
  private NodeIdentity localIdentity;
  private CoordinatorThreading threading;

  protected JmsClusterCoordinator() {
    // CDI proxy constructor.
  }

  /**
   * Test/non-CDI constructor. Bypasses CDI provider resolution by accepting a fully-formed {@link
   * JmsConnectionLifecycle}. Callers must still invoke {@link #init()} before any SPI method.
   */
  JmsClusterCoordinator(
      NodeIdentityProvider identityProvider,
      JmsCoordinatorConfig config,
      JmsConnectionLifecycle lifecycle,
      Topic topic,
      MetricsCollector metrics) {
    this.identityProvider = Objects.requireNonNull(identityProvider, "identityProvider");
    this.config = Objects.requireNonNull(config, "config");
    this.connectionLifecycle = Objects.requireNonNull(lifecycle, "lifecycle");
    this.topic = Objects.requireNonNull(topic, "topic");
    this.metrics = metrics;
    this.threading = CoordinatorThreading.standalone("ratchet-coordinator-jms");
  }

  /**
   * Test/non-CDI constructor that defers lifecycle construction to {@link #init()} so the lifecycle
   * can be wired with the coordinator's inbound handler ({@code this::onJmsMessage}) — the same
   * path the CDI flow uses. This is the constructor the TCK harness uses.
   */
  JmsClusterCoordinator(
      NodeIdentityProvider identityProvider,
      JmsCoordinatorConfig config,
      ConnectionFactory connectionFactory,
      Topic topic,
      MetricsCollector metrics) {
    this.identityProvider = Objects.requireNonNull(identityProvider, "identityProvider");
    this.config = Objects.requireNonNull(config, "config");
    this.directConnectionFactory = Objects.requireNonNull(connectionFactory, "connectionFactory");
    this.topic = Objects.requireNonNull(topic, "topic");
    this.metrics = metrics;
    this.threading = CoordinatorThreading.standalone("ratchet-coordinator-jms");
  }

  @PostConstruct
  void init() {
    if (config == null) {
      config =
          CoordinatorSupport.resolveConfigOrDefault(configInstance, JmsCoordinatorConfig::defaults);
    }
    Objects.requireNonNull(config, "config");
    Objects.requireNonNull(identityProvider, "identityProvider");
    requireJsonProvider();
    if (threading == null) {
      // CDI/production path: route loop threads and the dispatch pool through the container's
      // managed thread factory. Standalone is an explicit opt-in via the test constructors.
      threading = CoordinatorThreading.managed("ratchet-coordinator-jms");
    }
    configureDispatch(
        COORDINATOR_KIND,
        "JMS",
        metrics,
        identityProvider,
        config.maxInboundPayloadChars(),
        threading.newDispatchPool(
            "dispatch", config.listenerExecutorThreads(), config.listenerExecutorQueueCapacity()),
        config.shutdownGraceMs());
    if (connectionLifecycle == null) {
      ConnectionFactory cf;
      if (directConnectionFactory != null) {
        cf = directConnectionFactory;
      } else {
        JmsConnectionFactoryProvider provider =
            CoordinatorSupport.resolveRequired(
                providerInstance,
                "No JmsConnectionFactoryProvider available for JmsClusterCoordinator. Provide a"
                    + " @Produces JmsConnectionFactoryProvider or use the default JNDI lookup bean.",
                "Multiple JmsConnectionFactoryProvider beans visible; first match wins. Use"
                    + " @Alternative + @Priority for disambiguation.");
        cf = provider.connectionFactory();
        if (topic == null) {
          this.topic = provider.topic();
        }
      }
      connectionLifecycle =
          new JmsConnectionLifecycle(
              cf, topic, config, this::onJmsMessage, this::onConnectionTransportFailure, threading);
    }
    localIdentity = new NodeIdentity(identityProvider.getNodeId());
  }

  @Override
  public void afterStart() {
    if (isClosed()) {
      return;
    }
    if (connectionLifecycle == null || localIdentity == null) {
      throw new IllegalStateException("afterStart() called before init()");
    }
    connectionLifecycle.start(localIdentity);
  }

  @Override
  public void notifyNewWork(JobPriority priority, NodeIdentity source, String executionTarget) {
    Objects.requireNonNull(priority, "priority");
    Objects.requireNonNull(source, "source");
    if (isClosed()) {
      return;
    }
    try {
      String body = codec.encode(NotifyPayload.current(source, priority, executionTarget));
      if (!connectionLifecycle.sendTextMessage(body, source.value(), priority.name())) {
        // Reconnect is in flight or initial connect failed — degrade to no-op and metric.
        clusterWakeupPublished("failure");
        return;
      }
      clusterWakeupPublished("success");
    } catch (JMSException jmsEx) {
      // TextMessage.setStringProperty declares JMSException (checked); provider returns it for
      // misuse like a closed session or invalid property type. Treat as transport failure and
      // trigger a reconnect so subsequent calls can recover.
      clusterWakeupPublished("failure");
      log.warnf(
          "JMS coordinator notifyNewWork checked-JMS failure: %s — wakeup dropped",
          jmsEx.getMessage());
      connectionLifecycle.triggerReconnect();
    } catch (JMSRuntimeException jmsRuntimeEx) {
      // Simplified API (jakarta.jms 3.x) raises JMSRuntimeException for transport-side failures
      // such as a dropped connection during send.
      clusterWakeupPublished("failure");
      log.warnf(
          "JMS coordinator notifyNewWork transport failure: %s — wakeup dropped",
          jmsRuntimeEx.getMessage());
      connectionLifecycle.triggerReconnect();
    } catch (RuntimeException runtimeEx) {
      // Defense-in-depth for runtime exceptions outside the JMS hierarchy: JSON-P providers
      // raise JsonException on encode, and vendor extensions may raise their own runtime
      // subclasses. SPI contract is "notifyNewWork never throws"; metric and move on without
      // triggering a reconnect because the fault is encode-side, not transport-side.
      clusterWakeupPublished("failure");
      log.warnf(
          runtimeEx,
          "JMS coordinator notifyNewWork encode/dispatch failure: %s — wakeup dropped",
          runtimeEx.getMessage());
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
    JmsConnectionLifecycle lifecycle = this.connectionLifecycle;
    if (lifecycle != null) {
      lifecycle.close();
    }
    shutdownListenerExecutor();
  }

  /** Dispatch path from the JMS provider's listener thread. */
  void onJmsMessage(Message message) {
    NotifyPayload payload;
    try {
      if (!(message instanceof TextMessage tm)) {
        clusterWakeupReceived("parse_failure");
        return;
      }
      String body = tm.getText();
      if (rejectIfOversized(body)) {
        // Hard cap on listener-thread allocation: a hostile or buggy producer can otherwise
        // attach a multi-MB body that the codec would happily decode into memory.
        return;
      }
      payload = codec.decode(body);
    } catch (DecodeException ex) {
      clusterWakeupReceived("parse_failure");
      log.debugf("JMS coordinator dropped malformed payload: %s", ex.getMessage());
      return;
    } catch (Exception ex) {
      clusterWakeupReceived("transport_failure");
      log.warnf("JMS coordinator dropped inbound message due to JMS error: %s", ex.getMessage());
      return;
    }
    deliverDecodedPayload(payload);
  }

  private void onConnectionTransportFailure() {
    clusterWakeupReceived("transport_failure");
  }

  /**
   * Test accessor: exposes the active connection lifecycle so the TCK harness can poll readiness.
   */
  JmsConnectionLifecycle lifecycle() {
    return connectionLifecycle;
  }
}
