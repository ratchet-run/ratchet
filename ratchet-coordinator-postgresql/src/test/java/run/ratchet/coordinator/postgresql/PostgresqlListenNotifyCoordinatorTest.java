package run.ratchet.coordinator.postgresql;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import jakarta.annotation.Priority;
import jakarta.interceptor.Interceptor;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.PGConnection;
import run.ratchet.api.JobPriority;
import run.ratchet.api.NodeIdentity;
import run.ratchet.coordinator.common.NotifyPayload;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.spi.NodeIdentityProvider;

class PostgresqlListenNotifyCoordinatorTest {

  private PostgresqlCoordinatorConfig config;
  private NodeIdentityProvider localProvider;
  private RecordingMetrics metrics;

  @BeforeEach
  void setUp() {
    config =
        new PostgresqlCoordinatorConfig(
            "ratchet_wakeup", Optional.empty(), 100L, 10L, 50L, 16_384, 1, 500L);
    localProvider = () -> "nodeA";
    metrics = new RecordingMetrics();
  }

  private PostgresqlListenNotifyCoordinator newCoordinator(
      PostgresqlConnectionLifecycle lifecycle) {
    PostgresqlListenNotifyCoordinator c =
        new PostgresqlListenNotifyCoordinator(localProvider, config, lifecycle, metrics);
    c.init();
    return c;
  }

  private static Connection stubPgConnection() throws SQLException {
    Connection raw = mock(Connection.class);
    PGConnection pg = mock(PGConnection.class);
    lenient().when(raw.unwrap(PGConnection.class)).thenReturn(pg);
    Statement stmt = mock(Statement.class);
    lenient().when(raw.createStatement()).thenReturn(stmt);
    return raw;
  }

  @Test
  void selfSuppressedNotificationDoesNotFireListener() {
    PostgresqlConnectionLifecycle lifecycle =
        new PostgresqlConnectionLifecycle(() -> stubPgConnection(), config, ms -> {});
    PostgresqlListenNotifyCoordinator c = newCoordinator(lifecycle);
    try {
      List<NodeIdentity> seen = new ArrayList<>();
      c.registerWakeupListener((p, s) -> seen.add(s));

      // Simulate inbound payload from ourselves
      c.dispatchInbound(new NotifyPayload(1, new NodeIdentity("nodeA"), JobPriority.HIGH));

      // Wait briefly to ensure no async dispatch happens
      sleep(50);
      assertTrue(seen.isEmpty(), "self-notification must not fire local listener");
      assertEquals(1, metrics.received("ignored_self"));
    } finally {
      c.close();
    }
  }

  @Test
  void foreignNotificationFiresListenerWithDecodedPriorityAndSource() throws Exception {
    PostgresqlConnectionLifecycle lifecycle =
        new PostgresqlConnectionLifecycle(() -> stubPgConnection(), config, ms -> {});
    PostgresqlListenNotifyCoordinator c = newCoordinator(lifecycle);
    try {
      CountDownLatch latch = new CountDownLatch(1);
      List<NotifyPayload> seen = new ArrayList<>();
      c.registerWakeupListener(
          (p, s) -> {
            seen.add(new NotifyPayload(1, s, p));
            latch.countDown();
          });

      c.dispatchInbound(new NotifyPayload(1, new NodeIdentity("nodeB"), JobPriority.CRITICAL));

      assertTrue(latch.await(2, TimeUnit.SECONDS), "listener never fired");
      assertEquals(1, seen.size());
      assertEquals(JobPriority.CRITICAL, seen.get(0).priority());
      assertEquals("nodeB", seen.get(0).node().value());
      assertEquals(1, metrics.received("delivered"));
    } finally {
      c.close();
    }
  }

  @Test
  void preRegistrationBufferDrainsToFirstListener() throws Exception {
    PostgresqlConnectionLifecycle lifecycle =
        new PostgresqlConnectionLifecycle(() -> stubPgConnection(), config, ms -> {});
    PostgresqlListenNotifyCoordinator c = newCoordinator(lifecycle);
    try {
      for (int i = 0; i < 5; i++) {
        c.dispatchInbound(new NotifyPayload(1, new NodeIdentity("nodeB"), JobPriority.NORMAL));
      }

      CountDownLatch latch = new CountDownLatch(5);
      AtomicInteger count = new AtomicInteger();
      c.registerWakeupListener(
          (p, s) -> {
            count.incrementAndGet();
            latch.countDown();
          });

      assertTrue(latch.await(2, TimeUnit.SECONDS), "buffered notifications never drained");
      assertEquals(5, count.get());
    } finally {
      c.close();
    }
  }

  @Test
  void preRegistrationBufferOverflowDropsOldest() {
    PostgresqlConnectionLifecycle lifecycle =
        new PostgresqlConnectionLifecycle(() -> stubPgConnection(), config, ms -> {});
    PostgresqlListenNotifyCoordinator c = newCoordinator(lifecycle);
    try {
      // Capacity is 256. Push 260 without a listener; expect the oldest to be dropped.
      for (int i = 0; i < 260; i++) {
        c.dispatchInbound(new NotifyPayload(1, new NodeIdentity("nodeB"), JobPriority.LOW));
      }
      AtomicInteger received = new AtomicInteger();
      c.registerWakeupListener((p, s) -> received.incrementAndGet());

      sleep(200);
      assertTrue(received.get() <= 256, "drained more than the buffer capacity: " + received.get());
      assertTrue(received.get() > 200, "drained too few: " + received.get());
    } finally {
      c.close();
    }
  }

  @Test
  void notifyNewWorkSwallowsRuntimeExceptionFromEncodeOrDriver() throws Exception {
    // SPI contract: notifyNewWork must never throw. SQLException is the obvious path,
    // but JSON-P providers can raise JsonException (RuntimeException) on encode and
    // misbehaving drivers can raise NPE on prepareStatement. Cover that broader catch here.
    Connection raw = mock(Connection.class);
    PGConnection pg = mock(PGConnection.class);
    when(raw.unwrap(PGConnection.class)).thenReturn(pg);
    Statement listenStmt = mock(Statement.class);
    when(raw.createStatement()).thenReturn(listenStmt);
    when(raw.prepareStatement(anyString()))
        .thenThrow(new RuntimeException("simulated runtime failure"));

    Connection listenRaw = stubPgConnection();
    PostgresqlConnectionLifecycle lifecycle =
        new PostgresqlConnectionLifecycle(() -> listenRaw, config, ms -> {});
    lifecycle.acquire();
    PostgresqlListenNotifyCoordinator c =
        new PostgresqlListenNotifyCoordinator(localProvider, config, lifecycle, () -> raw, metrics);
    c.init();
    try {
      assertDoesNotThrow(() -> c.notifyNewWork(JobPriority.HIGH, new NodeIdentity("nodeA")));
      assertEquals(1, metrics.published("failure"));
    } finally {
      c.close();
    }
  }

  @Test
  void notifyNewWorkSwallowsTransportException() throws Exception {
    AtomicInteger attempts = new AtomicInteger();
    PostgresqlConnectionLifecycle lifecycle =
        new PostgresqlConnectionLifecycle(() -> stubPgConnection(), config, ms -> {});
    PostgresqlListenNotifyCoordinator c =
        new PostgresqlListenNotifyCoordinator(
            localProvider,
            config,
            lifecycle,
            () -> {
              attempts.incrementAndGet();
              throw new SQLException("nope");
            },
            metrics);
    c.init();
    try {
      assertDoesNotThrow(() -> c.notifyNewWork(JobPriority.HIGH, new NodeIdentity("nodeA")));
      assertEquals(1, metrics.published("failure"));
    } finally {
      c.close();
    }
  }

  @Test
  void notifyNewWorkIssuesPgNotifyAndCountsSuccess() throws Exception {
    Connection raw = mock(Connection.class);
    PGConnection pg = mock(PGConnection.class);
    when(raw.unwrap(PGConnection.class)).thenReturn(pg);
    Statement listenStmt = mock(Statement.class);
    when(raw.createStatement()).thenReturn(listenStmt);
    PreparedStatement notifyStmt = mock(PreparedStatement.class);
    when(raw.prepareStatement(anyString())).thenReturn(notifyStmt);

    Connection listenRaw = stubPgConnection();
    PostgresqlConnectionLifecycle lifecycle =
        new PostgresqlConnectionLifecycle(() -> listenRaw, config, ms -> {});
    lifecycle.acquire();

    PostgresqlListenNotifyCoordinator c =
        new PostgresqlListenNotifyCoordinator(localProvider, config, lifecycle, () -> raw, metrics);
    c.init();
    try {
      c.notifyNewWork(JobPriority.NORMAL, new NodeIdentity("nodeA"));

      verify(raw).prepareStatement("SELECT pg_notify(?, ?)");
      verify(notifyStmt).setString(eq(1), eq("ratchet_wakeup"));
      verify(notifyStmt).setString(eq(2), anyString());
      verify(notifyStmt).execute();
      assertEquals(1, metrics.published("success"));
    } finally {
      c.close();
    }
  }

  @Test
  void closeIsIdempotent() throws Exception {
    Connection raw = stubPgConnection();
    PostgresqlConnectionLifecycle lifecycle =
        new PostgresqlConnectionLifecycle(() -> raw, config, ms -> {});
    lifecycle.acquire();
    PostgresqlListenNotifyCoordinator c = newCoordinator(lifecycle);

    assertDoesNotThrow(c::close);
    assertDoesNotThrow(c::close);
    verify(raw, atLeastOnce()).close();
  }

  @Test
  void notifyNewWorkPostCloseIsNoOp() throws Exception {
    PostgresqlConnectionLifecycle lifecycle =
        new PostgresqlConnectionLifecycle(() -> stubPgConnection(), config, ms -> {});
    PostgresqlListenNotifyCoordinator c = newCoordinator(lifecycle);
    c.close();

    long before = metrics.published("failure") + metrics.published("success");
    c.notifyNewWork(JobPriority.HIGH, new NodeIdentity("nodeA"));
    long after = metrics.published("failure") + metrics.published("success");

    assertEquals(before, after, "post-close notify must not emit metrics");
  }

  @Test
  void registerWakeupListenerPostCloseIsNoOp() {
    PostgresqlConnectionLifecycle lifecycle =
        new PostgresqlConnectionLifecycle(() -> stubPgConnection(), config, ms -> {});
    PostgresqlListenNotifyCoordinator c = newCoordinator(lifecycle);
    c.close();
    assertDoesNotThrow(() -> c.registerWakeupListener((p, s) -> {}));
  }

  @Test
  void throwingListenerDoesNotPreventOtherListeners() throws Exception {
    PostgresqlConnectionLifecycle lifecycle =
        new PostgresqlConnectionLifecycle(() -> stubPgConnection(), config, ms -> {});
    PostgresqlListenNotifyCoordinator c = newCoordinator(lifecycle);
    try {
      AtomicInteger goodCount = new AtomicInteger();
      CountDownLatch latch = new CountDownLatch(100);
      c.registerWakeupListener(
          (p, s) -> {
            throw new RuntimeException("boom");
          });
      c.registerWakeupListener(
          (p, s) -> {
            goodCount.incrementAndGet();
            latch.countDown();
          });

      for (int i = 0; i < 100; i++) {
        c.dispatchInbound(new NotifyPayload(1, new NodeIdentity("nodeB"), JobPriority.HIGH));
      }

      assertTrue(latch.await(5, TimeUnit.SECONDS), "good listener missed deliveries");
      assertEquals(100, goodCount.get());
    } finally {
      c.close();
    }
  }

  private static void sleep(long ms) {
    try {
      Thread.sleep(ms);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /** Minimal MetricsCollector that counts the two methods the coordinator uses. */
  private static final class RecordingMetrics implements MetricsCollector {
    private final java.util.concurrent.ConcurrentHashMap<String, AtomicInteger> published =
        new java.util.concurrent.ConcurrentHashMap<>();
    private final java.util.concurrent.ConcurrentHashMap<String, AtomicInteger> received =
        new java.util.concurrent.ConcurrentHashMap<>();

    @Override
    public void clusterWakeupPublished(String transport, String outcome) {
      published.computeIfAbsent(outcome, k -> new AtomicInteger()).incrementAndGet();
    }

    @Override
    public void clusterWakeupReceived(String transport, String outcome) {
      received.computeIfAbsent(outcome, k -> new AtomicInteger()).incrementAndGet();
    }

    int published(String outcome) {
      AtomicInteger c = published.get(outcome);
      return c == null ? 0 : c.get();
    }

    int received(String outcome) {
      AtomicInteger c = received.get(outcome);
      return c == null ? 0 : c.get();
    }

    @Override
    public void jobStarted(
        java.util.UUID jobId, run.ratchet.api.JobType type, JobPriority priority) {}

    @Override
    public void jobCompleted(
        java.util.UUID jobId, run.ratchet.api.JobType type, long executionTimeMs) {}

    @Override
    public void jobFailed(
        java.util.UUID jobId, run.ratchet.api.JobType type, Throwable cause, int attempt) {}

    @Override
    public void successFinalizationRetried(java.util.UUID jobId, run.ratchet.api.JobType type) {}

    @Override
    public void successFinalizationMinimal(java.util.UUID jobId, run.ratchet.api.JobType type) {}

    @Override
    public void successFinalizationStuck(java.util.UUID jobId, run.ratchet.api.JobType type) {}

    @Override
    public void claimTransientFailure(String executionType) {}

    @Override
    public void jobsClaimed(String executionType, int claimedCount) {}

    @Override
    public void gateRejected(String executionType, String gateStatus) {}

    @Override
    public void localWakeup(String source) {}
  }

  @Test
  void priorityIsPlatformBeforePlus400() {
    Priority p = PostgresqlListenNotifyCoordinator.class.getAnnotation(Priority.class);
    assertEquals(Interceptor.Priority.PLATFORM_BEFORE + 400, p.value());
  }
}
