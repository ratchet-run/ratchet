package run.ratchet.coordinator.hazelcast;

import com.hazelcast.core.HazelcastInstance;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;
import run.ratchet.api.NodeIdentity;
import run.ratchet.api.SignalDecision;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.tck.coordinator.CoordinatorTestFixture;
import run.ratchet.tck.coordinator.CoordinatorTestHarness;
import run.ratchet.tck.coordinator.DeterministicNodeIdentityProvider;
import run.ratchet.tck.coordinator.RecordingMetricsCollector;

/**
 * Two-member Hazelcast harness joined via TCP loopback. One cluster per harness so each test starts
 * with a clean slate.
 */
public final class HazelcastCoordinatorTestHarness implements CoordinatorTestHarness {

  private TwoNodeHazelcastCluster cluster;
  private final String topicName;
  private final HazelcastCoordinatorConfig config;

  public HazelcastCoordinatorTestHarness() {
    this.topicName = "ratchet-wakeup-" + Long.toHexString(System.nanoTime());
    this.config = new HazelcastCoordinatorConfig(topicName, Optional.empty(), 16_384, 2, 1_500L);
    this.cluster = new TwoNodeHazelcastCluster();
  }

  @Override
  public CoordinatorTestFixture twoNodeCluster() {
    NodeIdentity idA = new NodeIdentity("nodeA-" + UUID.randomUUID());
    NodeIdentity idB = new NodeIdentity("nodeB-" + UUID.randomUUID());
    RecordingMetrics metricsA = new RecordingMetrics();
    RecordingMetrics metricsB = new RecordingMetrics();
    HazelcastClusterCoordinator coordinatorA = newCoordinator(idA, cluster.memberA(), metricsA);
    HazelcastClusterCoordinator coordinatorB = newCoordinator(idB, cluster.memberB(), metricsB);
    return new CoordinatorTestFixture(coordinatorA, idA, coordinatorB, idB, metricsA, metricsB);
  }

  @Override
  public void forceTransportFailure() {
    cluster.shutdownMemberA();
  }

  @Override
  public void recoverTransport() {
    cluster.close();
    cluster = new TwoNodeHazelcastCluster();
  }

  @Override
  public Duration maxExpectedLatency() {
    return Duration.ofSeconds(5);
  }

  @Override
  public boolean supportsRawWireInjection() {
    return false;
  }

  @Override
  public boolean transportRecoverableWithinCoordinatorLifetime() {
    // Coordinator binds to an ITopic obtained from a HazelcastInstance; shutting down the
    // instance invalidates the coordinator's reference. Recovery is operator-driven.
    return false;
  }

  @Override
  public void close() {
    if (cluster != null) {
      cluster.close();
    }
  }

  private HazelcastClusterCoordinator newCoordinator(
      NodeIdentity identity, HazelcastInstance instance, MetricsCollector metrics) {
    NodeIdentityProvider provider = new DeterministicNodeIdentityProvider(identity.value());
    HazelcastClusterCoordinator c =
        new HazelcastClusterCoordinator(provider, config, instance, metrics);
    c.init();
    c.afterStart();
    return c;
  }

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
