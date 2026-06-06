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
package run.ratchet.coordinator.postgresql;

import static org.awaitility.Awaitility.await;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;
import javax.sql.DataSource;
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
 * PostgreSQL-backed {@link CoordinatorTestHarness}.
 *
 * <p>One harness instance per TCK test: provisions two coordinators on a private NOTIFY channel (so
 * concurrent test invocations on the shared container do not cross-talk), exposes per-node metrics,
 * and translates {@code forceTransportFailure} into a {@code pg_terminate_backend} hammer that
 * drops every LISTEN backend except the admin connection used to fire it. The container itself is
 * provisioned and owned by the subclass (typically {@code @BeforeAll} static field) — harness
 * construction is cheap and per-test.
 *
 * <p>Lives in {@code run.ratchet.coordinator.postgresql} (not a sub-package) so it can reach the
 * package-private {@link PostgresqlConnectionLifecycle} type and the package-private {@code init()}
 * lifecycle hook on {@link PostgresqlListenNotifyCoordinator}.
 */
public final class PostgresqlCoordinatorTestHarness implements CoordinatorTestHarness {

  private final DataSource dataSource;
  private final PostgresqlCoordinatorConfig config;
  private final String channel;

  public PostgresqlCoordinatorTestHarness(DataSource dataSource) {
    this.dataSource = dataSource;
    this.channel = "ratchet_tck_" + Long.toHexString(System.nanoTime());
    this.config =
        new PostgresqlCoordinatorConfig(
            channel,
            Optional.empty(),
            /* receiveTimeoutMs= */ 500L,
            /* reconnectBackoffInitialMs= */ 50L,
            /* reconnectBackoffMaxMs= */ 500L,
            /* maxInboundPayloadChars= */ 16_384,
            /* listenerExecutorThreads= */ 1,
            /* listenerExecutorQueueCapacity= */ 1_024,
            /* shutdownGraceMs= */ 3_000L);
  }

  @Override
  public CoordinatorTestFixture twoNodeCluster() {
    NodeIdentity idA = new NodeIdentity("nodeA-" + UUID.randomUUID());
    NodeIdentity idB = new NodeIdentity("nodeB-" + UUID.randomUUID());
    RecordingMetrics metricsA = new RecordingMetrics();
    RecordingMetrics metricsB = new RecordingMetrics();
    PostgresqlConnectionLifecycle lifecycleA =
        new PostgresqlConnectionLifecycle(dataSource, config);
    PostgresqlConnectionLifecycle lifecycleB =
        new PostgresqlConnectionLifecycle(dataSource, config);
    PostgresqlListenNotifyCoordinator coordinatorA =
        newCoordinator(idA.value(), lifecycleA, metricsA);
    PostgresqlListenNotifyCoordinator coordinatorB =
        newCoordinator(idB.value(), lifecycleB, metricsB);
    awaitConnected(lifecycleA);
    awaitConnected(lifecycleB);
    return new CoordinatorTestFixture(coordinatorA, idA, coordinatorB, idB, metricsA, metricsB);
  }

  @Override
  public void forceTransportFailure() throws SQLException {
    try (Connection admin = dataSource.getConnection();
        Statement s = admin.createStatement()) {
      s.execute(
          "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
              + "WHERE query LIKE 'LISTEN%' AND pid <> pg_backend_pid()");
    }
  }

  @Override
  public void recoverTransport() {
    // No active recovery needed — the listen thread reconnect loop drives the recovery itself.
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
  public void injectRawMessage(ClusterCoordinator receiver, String rawPayload) throws Exception {
    try (Connection raw = dataSource.getConnection();
        Statement s = raw.createStatement()) {
      // Use NOTIFY with a parameter-escaped string payload.
      s.execute("NOTIFY " + channel + ", " + quote(rawPayload));
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
    // CoordinatorTestFixture.close() already closed the coordinators (which closed their
    // lifecycles). The container is shared and managed by the test class.
  }

  private PostgresqlListenNotifyCoordinator newCoordinator(
      String nodeId, PostgresqlConnectionLifecycle lifecycle, MetricsCollector metrics) {
    NodeIdentityProvider provider = new DeterministicNodeIdentityProvider(nodeId);
    PostgresqlListenNotifyCoordinator c =
        new PostgresqlListenNotifyCoordinator(
            provider, config, lifecycle, dataSource::getConnection, metrics);
    c.init();
    c.afterStart();
    return c;
  }

  private static void awaitConnected(PostgresqlConnectionLifecycle lifecycle) {
    await()
        .atMost(Duration.ofSeconds(10))
        .pollInterval(Duration.ofMillis(50))
        .until(() -> lifecycle.state() == PostgresqlConnectionLifecycle.State.CONNECTED);
  }

  private static String quote(String s) {
    return "'" + s.replace("'", "''") + "'";
  }

  /**
   * Test-only DataSource wrapper used by the IT bootstrap. Kept here for reuse from contract ITs.
   */
  public static DataSource newDataSource(String url, String user, String password) {
    return new SimpleDataSource(url, user, password);
  }

  private static final class SimpleDataSource implements DataSource {
    private final String url;
    private final String user;
    private final String password;

    SimpleDataSource(String url, String user, String password) {
      this.url = url;
      this.user = user;
      this.password = password;
    }

    @Override
    public Connection getConnection() throws SQLException {
      return DriverManager.getConnection(url, user, password);
    }

    @Override
    public Connection getConnection(String u, String p) throws SQLException {
      return DriverManager.getConnection(url, u, p);
    }

    @Override
    public PrintWriter getLogWriter() {
      return null;
    }

    @Override
    public void setLogWriter(PrintWriter out) {}

    @Override
    public void setLoginTimeout(int seconds) {}

    @Override
    public int getLoginTimeout() {
      return 0;
    }

    @Override
    public Logger getParentLogger() {
      return Logger.getLogger("global");
    }

    @Override
    public <T> T unwrap(Class<T> iface) {
      return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
      return false;
    }
  }

  /**
   * MetricsCollector that ALSO implements {@link RecordingMetricsCollector} so the TCK can read
   * per-counter totals directly from the recorder.
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
