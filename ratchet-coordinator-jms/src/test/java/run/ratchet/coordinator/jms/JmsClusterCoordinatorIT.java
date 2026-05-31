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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.jms.Topic;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;
import run.ratchet.api.NodeIdentity;
import run.ratchet.api.SignalDecision;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.spi.NodeIdentityProvider;

/**
 * JMS-specific integration tests that the shared coordinator TCK cannot model in transport-neutral
 * terms — the {@code brokerSideSelfFilter} toggle has no analog on PG, Infinispan, or Hazelcast.
 *
 * <p>Cross-coordinator behavior runs in {@link
 * run.ratchet.coordinator.jms.tck.JmsCoordinatorContractIT}. The only scenarios in this class are
 * the two halves of the JMS-specific selector configuration: with the broker-side selector on, the
 * broker drops self-broadcasts before they reach the receiver (so the receive-side {@code
 * ignored_self} counter stays zero); with it off, the receive-side defense-in-depth filter does the
 * work and {@code ignored_self} increments per self-send.
 */
class JmsClusterCoordinatorIT {

  private static EmbeddedArtemisBroker broker;
  private ActiveMQConnectionFactory cf;
  private Topic topic;

  @BeforeAll
  static void startBroker() throws Exception {
    broker = new EmbeddedArtemisBroker();
    broker.start();
  }

  @AfterAll
  static void stopBroker() throws Exception {
    if (broker != null) {
      broker.stop();
    }
  }

  @BeforeEach
  void setUp() {
    cf = new ActiveMQConnectionFactory(broker.connectorUrl());
    topic = ActiveMQJMSClient.createTopic("ratchet.jms.specific." + System.nanoTime());
  }

  @AfterEach
  void tearDown() {
    cf.close();
  }

  @Test
  void brokerSideSelfFilterEnabledKeepsReceiveSideSelfSuppressMetricAtZero() {
    JmsCoordinatorConfig config = configWithBrokerSideFilter(true);
    RecordingMetrics metrics = new RecordingMetrics();
    JmsClusterCoordinator c = newCoordinator("nodeA", config, metrics);
    try {
      CopyOnWriteArrayList<NodeIdentity> seen = new CopyOnWriteArrayList<>();
      c.registerWakeupListener(hint -> seen.add(hint.source()));
      awaitReady(c);

      c.notifyNewWork(JobPriority.HIGH, new NodeIdentity("nodeA"), null);

      // Allow time for the broker to filter (and for any inbound to arrive on the rare buggy path).
      sleep(750);
      assertTrue(seen.isEmpty(), "self-broadcast must not fire local listener");
      // Receive-side suppress counter is the canary: when the broker filters, the receiver never
      // sees the message, so ignored_self stays at 0.
      assertEquals(
          0L,
          metrics.received("ignored_self"),
          "broker-side selector must drop self-broadcasts before receive-side filter sees them");
    } finally {
      c.close();
    }
  }

  @Test
  void brokerSideSelfFilterDisabledIncrementsReceiveSideSelfSuppressMetric() {
    JmsCoordinatorConfig config = configWithBrokerSideFilter(false);
    RecordingMetrics metrics = new RecordingMetrics();
    JmsClusterCoordinator c = newCoordinator("nodeA", config, metrics);
    try {
      CopyOnWriteArrayList<NodeIdentity> seen = new CopyOnWriteArrayList<>();
      c.registerWakeupListener(hint -> seen.add(hint.source()));
      awaitReady(c);

      c.notifyNewWork(JobPriority.HIGH, new NodeIdentity("nodeA"), null);

      await()
          .atMost(Duration.ofSeconds(5))
          .pollInterval(Duration.ofMillis(50))
          .until(() -> metrics.received("ignored_self") >= 1);
      assertTrue(seen.isEmpty(), "receive-side filter must still keep the listener silent");
    } finally {
      c.close();
    }
  }

  // ---- helpers ------------------------------------------------------------

  private JmsCoordinatorConfig configWithBrokerSideFilter(boolean enabled) {
    return new JmsCoordinatorConfig(
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        enabled,
        25L,
        250L,
        16_384,
        2,
        1_500L);
  }

  private JmsClusterCoordinator newCoordinator(
      String nodeId, JmsCoordinatorConfig config, MetricsCollector metrics) {
    NodeIdentityProvider provider = () -> nodeId;
    JmsClusterCoordinator c = new JmsClusterCoordinator(provider, config, cf, topic, metrics);
    c.init();
    c.afterStart();
    return c;
  }

  private static void awaitReady(JmsClusterCoordinator c) {
    await()
        .atMost(Duration.ofSeconds(5))
        .pollInterval(Duration.ofMillis(20))
        .until(() -> c.lifecycle().currentProducer() != null);
  }

  private static void sleep(long ms) {
    try {
      Thread.sleep(ms);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /** MetricsCollector recorder used by this IT. */
  static final class RecordingMetrics implements MetricsCollector {
    private final ConcurrentHashMap<String, AtomicLong> received = new ConcurrentHashMap<>();

    @Override
    public void clusterWakeupPublished(String transport, String outcome) {}

    @Override
    public void clusterWakeupReceived(String transport, String outcome) {
      received.computeIfAbsent(outcome, k -> new AtomicLong()).incrementAndGet();
    }

    long received(String outcome) {
      AtomicLong c = received.get(outcome);
      return c == null ? 0L : c.get();
    }

    @Override
    public void jobStarted(UUID jobId, JobType type, JobPriority priority) {}

    @Override
    public void jobCompleted(UUID jobId, JobType type, long executionTimeMs) {}

    @Override
    public void jobFailed(UUID jobId, JobType type, Throwable cause, int attempt) {}

    @Override
    public void successFinalizationRetried(UUID jobId, JobType type) {}

    @Override
    public void successFinalizationMinimal(UUID jobId, JobType type) {}

    @Override
    public void successFinalizationStuck(UUID jobId, JobType type) {}

    @Override
    public void claimTransientFailure(String executionType) {}

    @Override
    public void jobsClaimed(String executionType, int claimedCount) {}

    @Override
    public void gateRejected(String executionType, String gateStatus) {}

    @Override
    public void localWakeup(String source) {}

    @Override
    public void callbackFailed(UUID jobId, JobType type, Throwable cause, int attempt) {}

    @Override
    public void signalWaiting(UUID jobId, JobType type, String signalKey) {}

    @Override
    public void signalDelivered(
        UUID jobId, JobType type, String signalKey, SignalDecision.Outcome outcome) {}

    @Override
    public void signalTimedOut(UUID jobId, JobType type, String signalKey) {}

    @Override
    public void signalCancelled(UUID jobId, JobType type, String signalKey) {}

    @Override
    public void storeOperation(
        String store, String operation, String outcome, long durationNanos) {}

    @Override
    public void pollerBreakerState(String breakerName, String state) {}
  }
}
