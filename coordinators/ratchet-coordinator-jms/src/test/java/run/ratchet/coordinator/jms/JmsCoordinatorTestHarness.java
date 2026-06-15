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

import static org.awaitility.Awaitility.await;

import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSProducer;
import jakarta.jms.TextMessage;
import jakarta.jms.Topic;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;
import run.ratchet.api.NodeIdentity;
import run.ratchet.api.SignalDecision;
import run.ratchet.coordinator.common.NotifyPayload;
import run.ratchet.coordinator.common.internal.NotifyPayloadCodec;
import run.ratchet.spi.ClusterCoordinator;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.tck.coordinator.CoordinatorTestFixture;
import run.ratchet.tck.coordinator.CoordinatorTestHarness;
import run.ratchet.tck.coordinator.DeterministicNodeIdentityProvider;
import run.ratchet.tck.coordinator.RecordingMetricsCollector;

/**
 * Embedded-Artemis-backed {@link CoordinatorTestHarness}.
 *
 * <p>The {@link EmbeddedArtemisBroker} is provisioned and owned by the subclass (typically a
 * {@code @BeforeAll} static field) — harness construction is cheap and per-test. Each harness picks
 * a unique topic name so concurrent test invocations on the shared broker do not cross-talk.
 *
 * <p>Raw-wire injection is supported: the harness creates a separate JMS context and publishes the
 * raw payload string as a {@link TextMessage}, bypassing the codec.
 */
public final class JmsCoordinatorTestHarness implements CoordinatorTestHarness {

  private final EmbeddedArtemisBroker broker;
  private final String topicName;
  private final ActiveMQConnectionFactory connectionFactory;
  private final Topic topic;
  private final JmsCoordinatorConfig config;

  public JmsCoordinatorTestHarness(EmbeddedArtemisBroker broker) {
    this.broker = broker;
    this.topicName = "ratchet.tck." + Long.toHexString(System.nanoTime());
    this.connectionFactory = new ActiveMQConnectionFactory(broker.connectorUrl());
    this.topic = ActiveMQJMSClient.createTopic(topicName);
    this.config =
        new JmsCoordinatorConfig(
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            /* brokerSideSelfFilter= */ true,
            /* reconnectBackoffInitialMs= */ 25L,
            /* reconnectBackoffMaxMs= */ 250L,
            /* maxInboundPayloadChars= */ 16_384,
            /* listenerExecutorThreads= */ 2,
            /* listenerExecutorQueueCapacity= */ 1_024,
            /* shutdownGraceMs= */ 1_500L);
  }

  @Override
  public CoordinatorTestFixture twoNodeCluster() {
    NodeIdentity idA = new NodeIdentity("nodeA-" + UUID.randomUUID());
    NodeIdentity idB = new NodeIdentity("nodeB-" + UUID.randomUUID());
    RecordingMetrics metricsA = new RecordingMetrics();
    RecordingMetrics metricsB = new RecordingMetrics();
    JmsClusterCoordinator coordinatorA = newCoordinator(idA, metricsA);
    JmsClusterCoordinator coordinatorB = newCoordinator(idB, metricsB);
    awaitConnected(coordinatorA);
    awaitConnected(coordinatorB);
    return new CoordinatorTestFixture(coordinatorA, idA, coordinatorB, idB, metricsA, metricsB);
  }

  @Override
  public void forceTransportFailure() throws Exception {
    broker.stop();
  }

  @Override
  public void recoverTransport() throws Exception {
    broker.start();
  }

  @Override
  public Duration maxExpectedLatency() {
    return Duration.ofSeconds(5);
  }

  @Override
  public boolean supportsRawWireInjection() {
    return true;
  }

  @Override
  public void injectRawMessage(ClusterCoordinator receiver, String rawPayload) {
    try (JMSContext ctx = connectionFactory.createContext(JMSContext.AUTO_ACKNOWLEDGE)) {
      JMSProducer producer = ctx.createProducer();
      TextMessage msg = ctx.createTextMessage(rawPayload);
      // Mark the message as originating from a non-local node so receive-side self-suppression
      // does not pre-empt the parse-failure path under test.
      msg.setStringProperty("node", "external-injector-" + UUID.randomUUID());
      msg.setStringProperty("prio", JobPriority.HIGH.name());
      producer.send(topic, msg);
    } catch (Exception e) {
      throw new IllegalStateException("raw-wire injection failed", e);
    }
  }

  @Override
  public String futureVersionRawMessage(NodeIdentity source) {
    // Encode through the production codec with a version well clear of CURRENT_VERSION, so the
    // payload stays unsupported even after a future wire bump.
    return new NotifyPayloadCodec()
        .encode(
            new NotifyPayload(
                NotifyPayloadCodec.CURRENT_VERSION + 1000, source, JobPriority.HIGH, null));
  }

  @Override
  public void close() {
    // Broker and connection factory are class-scoped; nothing to release per-harness.
  }

  private JmsClusterCoordinator newCoordinator(NodeIdentity identity, MetricsCollector metrics) {
    NodeIdentityProvider provider = new DeterministicNodeIdentityProvider(identity.value());
    // CF+Topic constructor mirrors the CDI flow: the coordinator constructs its own lifecycle
    // inside init() with this::onJmsMessage as the inbound handler, breaking the
    // coordinator-needs-lifecycle / lifecycle-needs-coordinator-handler chicken-and-egg.
    JmsClusterCoordinator coordinator =
        new JmsClusterCoordinator(provider, config, connectionFactory, topic, metrics);
    coordinator.init();
    coordinator.afterStart();
    return coordinator;
  }

  private static void awaitConnected(JmsClusterCoordinator c) {
    // The lifecycle's currentProducer turns non-null once start() completes successfully. For
    // embedded Artemis this is essentially synchronous, but await a short window to absorb
    // listener-setup latency.
    await()
        .atMost(Duration.ofSeconds(5))
        .pollInterval(Duration.ofMillis(20))
        .until(() -> c.lifecycle().currentProducer() != null);
  }

  /**
   * MetricsCollector that ALSO implements {@link RecordingMetricsCollector} so the TCK can read
   * per-counter totals directly. Identical shape to the PG harness's recorder.
   */
  static final class RecordingMetrics implements MetricsCollector, RecordingMetricsCollector {
    private final AtomicLong publishedSuccess = new AtomicLong();
    private final AtomicLong publishedFailure = new AtomicLong();
    private final ConcurrentHashMap<String, AtomicLong> receivedByOutcome =
        new ConcurrentHashMap<>();

    @Override
    public void clusterWakeupPublished(String transport, String outcome) {
      if ("success".equals(outcome)) {
        publishedSuccess.incrementAndGet();
      } else {
        publishedFailure.incrementAndGet();
      }
    }

    @Override
    public void clusterWakeupReceived(String transport, String outcome) {
      receivedByOutcome.computeIfAbsent(outcome, k -> new AtomicLong()).incrementAndGet();
    }

    private long received(String outcome) {
      AtomicLong c = receivedByOutcome.get(outcome);
      return c == null ? 0L : c.get();
    }

    @Override
    public long sent() {
      return publishedSuccess.get() + publishedFailure.get();
    }

    @Override
    public long received() {
      return received("delivered");
    }

    @Override
    public long selfNotifySuppressed() {
      return received("ignored_self");
    }

    @Override
    public long transportFailure() {
      return publishedFailure.get()
          + received("transport_failure")
          + received("parse_failure")
          + received("ignored_provider_error");
    }

    @Override
    public long listenerFailure() {
      return received("listener_failure");
    }

    @Override
    public long preRegistrationOverflow() {
      return received("pre_registration_overflow");
    }

    // ---- unused MetricsCollector surface ----
    @Override
    public void jobStarted(java.util.UUID jobId, JobType type, JobPriority priority) {}

    @Override
    public void jobCompleted(java.util.UUID jobId, JobType type, long executionTimeMs) {}

    @Override
    public void jobFailed(java.util.UUID jobId, JobType type, Throwable cause, int attempt) {}

    @Override
    public void successFinalizationRetried(java.util.UUID jobId, JobType type) {}

    @Override
    public void successFinalizationMinimal(java.util.UUID jobId, JobType type) {}

    @Override
    public void successFinalizationStuck(java.util.UUID jobId, JobType type) {}

    @Override
    public void claimTransientFailure(String executionType) {}

    @Override
    public void jobsClaimed(String executionType, int claimedCount) {}

    @Override
    public void gateRejected(String executionType, String gateStatus) {}

    @Override
    public void localWakeup(String source) {}

    @Override
    public void callbackFailed(java.util.UUID jobId, JobType type, Throwable cause, int attempt) {}

    @Override
    public void signalWaiting(java.util.UUID jobId, JobType type, String signalKey) {}

    @Override
    public void signalDelivered(
        java.util.UUID jobId, JobType type, String signalKey, SignalDecision.Outcome outcome) {}

    @Override
    public void signalTimedOut(java.util.UUID jobId, JobType type, String signalKey) {}

    @Override
    public void signalCancelled(java.util.UUID jobId, JobType type, String signalKey) {}

    @Override
    public void storeOperation(
        String store, String operation, String outcome, long durationNanos) {}

    @Override
    public void pollerBreakerState(String breakerName, String state) {}
  }

  /** Lookup helper for tests that need to assert connection factory identity. */
  ConnectionFactory connectionFactory() {
    return connectionFactory;
  }

  /** Lookup helper for tests that need the topic name. */
  Topic topic() {
    return topic;
  }
}
