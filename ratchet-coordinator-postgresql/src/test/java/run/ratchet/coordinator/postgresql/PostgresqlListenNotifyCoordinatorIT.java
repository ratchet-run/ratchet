package run.ratchet.coordinator.postgresql;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.postgresql.PostgreSQLContainer;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;
import run.ratchet.api.NodeIdentity;
import run.ratchet.api.SignalDecision;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.spi.NodeIdentityProvider;

/**
 * PostgreSQL-specific integration tests that the shared coordinator TCK cannot model in
 * transport-neutral terms.
 *
 * <p>Cross-coordinator behavior (round-trip, self-suppression, listener isolation, transport
 * failure tolerance, shutdown, envelope rejection, metric counters) runs in {@link
 * run.ratchet.coordinator.postgresql.tck.PostgresqlCoordinatorContractIT}. Only PG-specific
 * scenarios live here:
 *
 * <ul>
 *   <li>Non-JSON wire payload swallowed (raw {@code NOTIFY} from outside the codec — TCK only
 *       injects a future-version envelope).
 *   <li>Iterative reconnect-and-retry after a forced backend drop — the TCK has a one-shot recovery
 *       assertion; this exercises PG's listen-thread reconnect loop that converges over multiple
 *       attempts.
 * </ul>
 */
class PostgresqlListenNotifyCoordinatorIT {

  @SuppressWarnings({"resource", "rawtypes"})
  private static final PostgreSQLContainer CONTAINER =
      new PostgreSQLContainer("postgres:16")
          .withDatabaseName("ratchet_coord_pg_specific")
          .withUsername("ratchet")
          .withPassword("ratchet");

  private static DataSource dataSource;
  private static PostgresqlCoordinatorConfig config;

  private CoordinatorFixture nodeA;
  private CoordinatorFixture nodeB;

  @BeforeAll
  static void start() {
    CONTAINER.start();
    dataSource =
        new SimpleDataSource(
            CONTAINER.getJdbcUrl(), CONTAINER.getUsername(), CONTAINER.getPassword());
    config =
        new PostgresqlCoordinatorConfig(
            "ratchet_wakeup", Optional.empty(), 500L, 50L, 500L, 1, 3_000L);
  }

  @AfterAll
  static void stop() {
    CONTAINER.stop();
  }

  @BeforeEach
  void setUp() {
    nodeA = startCoordinator("nodeA-" + UUID.randomUUID());
    nodeB = startCoordinator("nodeB-" + UUID.randomUUID());
  }

  @AfterEach
  void tearDown() {
    if (nodeA != null) nodeA.coordinator.close();
    if (nodeB != null) nodeB.coordinator.close();
  }

  @Test
  void malformedNonJsonPayloadIsSwallowed() throws Exception {
    List<DeliveredEvent> deliveredToB = new CopyOnWriteArrayList<>();
    nodeB.coordinator.registerWakeupListener((p, s) -> deliveredToB.add(new DeliveredEvent(p, s)));
    awaitConnected(nodeB);

    // Send a deliberately non-JSON payload from an external connection. The TCK exercises the
    // "future envelope version" path (valid JSON, unknown v); this exercises the
    // JsonParsingException
    // path that the future-version test does not cover.
    try (Connection raw = dataSource.getConnection();
        Statement s = raw.createStatement()) {
      s.execute("NOTIFY ratchet_wakeup, 'this is not json'");
    }

    await().atMost(Duration.ofSeconds(5)).until(() -> nodeB.metrics.received("parse_failure") >= 1);
    assertTrue(deliveredToB.isEmpty(), "non-JSON payload must not fire listeners");
  }

  @Test
  void reconnectAfterServerSideDropRestoresDelivery() throws Exception {
    List<DeliveredEvent> delivered = new CopyOnWriteArrayList<>();
    nodeB.coordinator.registerWakeupListener((p, s) -> delivered.add(new DeliveredEvent(p, s)));
    awaitConnected(nodeA);
    awaitConnected(nodeB);

    // Sanity check that the round-trip works before we drop the connection.
    nodeA.coordinator.notifyNewWork(JobPriority.NORMAL, nodeA.identity);
    await().atMost(Duration.ofSeconds(5)).until(() -> delivered.size() >= 1);
    int baseline = delivered.size();

    terminateBackendOf(nodeB);

    // Outbound is on the sender side; sender's connection is intact. But the receiver must
    // reconnect to resume delivery. Iteratively retry until the listen-thread reconnect loop
    // converges — the TCK's one-shot recoverTransport() assertion does not exercise this loop.
    await()
        .atMost(Duration.ofSeconds(20))
        .pollInterval(Duration.ofMillis(200))
        .untilAsserted(
            () -> {
              nodeA.coordinator.notifyNewWork(JobPriority.NORMAL, nodeA.identity);
              assertTrue(
                  delivered.size() > baseline,
                  "no delivery observed yet (baseline="
                      + baseline
                      + ", delivered="
                      + delivered.size()
                      + ")");
            });
  }

  // ---- test fixture plumbing ------------------------------------------------

  private static CoordinatorFixture startCoordinator(String nodeId) {
    NodeIdentityProvider provider = () -> nodeId;
    RecordingMetrics metrics = new RecordingMetrics();
    PostgresqlConnectionLifecycle lifecycle = new PostgresqlConnectionLifecycle(dataSource, config);
    PostgresqlListenNotifyCoordinator c =
        new PostgresqlListenNotifyCoordinator(provider, config, lifecycle, metrics);
    c.init();
    c.afterStart();
    return new CoordinatorFixture(c, lifecycle, new NodeIdentity(nodeId), metrics);
  }

  private static void awaitConnected(CoordinatorFixture f) {
    await()
        .atMost(Duration.ofSeconds(10))
        .pollInterval(Duration.ofMillis(100))
        .until(() -> f.lifecycle.state() == PostgresqlConnectionLifecycle.State.CONNECTED);
  }

  /**
   * Kill every backend whose last query started with {@code LISTEN}. Both fixtures share the
   * container, so this is a deliberately coarse "drop all LISTEN sessions" hammer — the surviving
   * session is the {@code admin} connection that fired this. The reconnect assertion polls {@code
   * notifyNewWork} until delivery resumes, so any node that lost its connection succeeds as soon as
   * the listen-thread reconnect path completes. The argument {@code f} is unused but kept to
   * document the intended target.
   */
  @SuppressWarnings("unused")
  private static void terminateBackendOf(CoordinatorFixture f) throws SQLException {
    try (Connection admin = dataSource.getConnection();
        Statement s = admin.createStatement()) {
      s.execute(
          "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
              + "WHERE query LIKE 'LISTEN%' AND pid <> pg_backend_pid()");
    }
  }

  private record CoordinatorFixture(
      PostgresqlListenNotifyCoordinator coordinator,
      PostgresqlConnectionLifecycle lifecycle,
      NodeIdentity identity,
      RecordingMetrics metrics) {}

  private record DeliveredEvent(JobPriority priority, NodeIdentity source) {}

  /** Pool-less DataSource that delegates every getConnection() call to DriverManager. */
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

  /** Test MetricsCollector that records cluster_wakeup_received counters. */
  private static final class RecordingMetrics implements MetricsCollector {
    private final ConcurrentHashMap<String, AtomicInteger> received = new ConcurrentHashMap<>();

    @Override
    public void clusterWakeupPublished(String transport, String outcome) {}

    @Override
    public void clusterWakeupReceived(String transport, String outcome) {
      received.computeIfAbsent(outcome, k -> new AtomicInteger()).incrementAndGet();
    }

    int received(String outcome) {
      AtomicInteger c = received.get(outcome);
      return c == null ? 0 : c.get();
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
