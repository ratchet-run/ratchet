package run.ratchet.coordinator.infinispan;

import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.infinispan.Cache;
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
 * Two-node Infinispan harness backed by {@link TwoNodeInfinispanCluster}. One cluster instance is
 * created per harness so each test starts with a clean slate — Infinispan cluster bring-up is fast
 * enough (sub-second on the JGroups TCP loopback transport) to keep the per-test cost acceptable.
 *
 * <p>Raw-wire injection is unsupported: bypassing the codec requires writing a malformed value
 * directly to the cache, but the listener's null-guard and codec rejection are already covered by
 * the {@link InfinispanWakeupListenerTest} unit tests against mocked events. The TCK {@code
 * unknownEnvelopeVersionIsRejectedLoudly} contract is skipped via {@link
 * #supportsRawWireInjection()}.
 */
public final class InfinispanCoordinatorTestHarness implements CoordinatorTestHarness {

  private TwoNodeInfinispanCluster cluster;
  private final String cacheName;
  private final InfinispanCoordinatorConfig config;

  public InfinispanCoordinatorTestHarness() throws IOException {
    this.cacheName = "wakeup_" + Long.toHexString(System.nanoTime());
    this.config =
        new InfinispanCoordinatorConfig(cacheName, Optional.empty(), 60L, 16_384, 2, 1_500L);
    this.cluster = new TwoNodeInfinispanCluster(cacheName);
  }

  @Override
  public CoordinatorTestFixture twoNodeCluster() {
    NodeIdentity idA = new NodeIdentity("nodeA-" + UUID.randomUUID());
    NodeIdentity idB = new NodeIdentity("nodeB-" + UUID.randomUUID());
    RecordingMetrics metricsA = new RecordingMetrics();
    RecordingMetrics metricsB = new RecordingMetrics();
    Cache<String, String> cacheA = cluster.managerA().getCache(cacheName);
    Cache<String, String> cacheB = cluster.managerB().getCache(cacheName);
    InfinispanClusterCoordinator coordinatorA = newCoordinator(idA, cacheA, metricsA);
    InfinispanClusterCoordinator coordinatorB = newCoordinator(idB, cacheB, metricsB);
    return new CoordinatorTestFixture(coordinatorA, idA, coordinatorB, idB, metricsA, metricsB);
  }

  @Override
  public void forceTransportFailure() {
    // Stop manager A to force a cluster-side disruption visible to manager B's listener.
    cluster.stopManagerA();
  }

  @Override
  public void recoverTransport() throws IOException {
    // The previous cluster manager is gone; rebuild the whole cluster so the next test's
    // twoNodeCluster sees fresh state. The harness is a per-test instance, so this is safe.
    cluster.close();
    cluster = new TwoNodeInfinispanCluster(cacheName);
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
    // Coordinator binds to a Cache obtained from an EmbeddedCacheManager; stopping the manager
    // invalidates the coordinator's reference per PRD §Reconnect / cluster churn. Recovery is
    // operator-driven (restart the cache manager), not coordinator-driven.
    return false;
  }

  @Override
  public void close() {
    if (cluster != null) {
      cluster.close();
    }
  }

  private InfinispanClusterCoordinator newCoordinator(
      NodeIdentity identity, Cache<String, String> cache, MetricsCollector metrics) {
    NodeIdentityProvider provider = new DeterministicNodeIdentityProvider(identity.value());
    InfinispanClusterCoordinator c =
        new InfinispanClusterCoordinator(provider, config, cache, metrics);
    c.init();
    c.afterStart();
    await()
        .atMost(Duration.ofSeconds(5))
        .pollInterval(Duration.ofMillis(50))
        .until(() -> c.lifecycle() != null && !c.lifecycle().isClosed());
    return c;
  }

  /** Recording MetricsCollector mirroring the PG and JMS harnesses. */
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

    // unused MetricsCollector surface
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
