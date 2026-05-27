package run.ratchet.coordinator.postgresql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.PGConnection;
import run.ratchet.coordinator.postgresql.PostgresqlConnectionLifecycle.State;

class PostgresqlConnectionLifecycleTest {

  private PostgresqlCoordinatorConfig config;

  @BeforeEach
  void setUp() {
    config =
        new PostgresqlCoordinatorConfig(
            "ratchet_wakeup", Optional.empty(), 5_000L, 10L, 50L, 1, 5_000L);
  }

  private static Connection mockPgConnection() throws SQLException {
    Connection raw = mock(Connection.class);
    PGConnection pg = mock(PGConnection.class);
    lenient().when(raw.unwrap(PGConnection.class)).thenReturn(pg);
    Statement stmt = mock(Statement.class);
    lenient().when(raw.createStatement()).thenReturn(stmt);
    return raw;
  }

  @Test
  void initialStateIsDisconnected() {
    PostgresqlConnectionLifecycle lifecycle =
        new PostgresqlConnectionLifecycle(
            () -> {
              throw new SQLException("unused");
            },
            config,
            ms -> {});
    assertEquals(State.DISCONNECTED, lifecycle.state());
  }

  @Test
  void currentThrowsWhenNotConnected() {
    PostgresqlConnectionLifecycle lifecycle =
        new PostgresqlConnectionLifecycle(
            () -> {
              throw new SQLException("unused");
            },
            config,
            ms -> {});
    assertThrows(SQLException.class, lifecycle::current);
  }

  @Test
  void acquireTransitionsToConnectedAndIssuesListen() throws Exception {
    Connection raw = mockPgConnection();
    PostgresqlConnectionLifecycle lifecycle =
        new PostgresqlConnectionLifecycle(() -> raw, config, ms -> {});

    lifecycle.acquire();

    assertEquals(State.CONNECTED, lifecycle.state());
    assertNotNull(lifecycle.current());
    verify(raw).setAutoCommit(true);
    verify(raw.createStatement()).execute("LISTEN \"ratchet_wakeup\"");
  }

  @Test
  void acquireAppliesCellIdSuffixToListenChannel() throws Exception {
    config =
        new PostgresqlCoordinatorConfig(
            "ratchet_wakeup", Optional.of("tenantA"), 5_000L, 10L, 50L, 1, 5_000L);
    Connection raw = mockPgConnection();
    PostgresqlConnectionLifecycle lifecycle =
        new PostgresqlConnectionLifecycle(() -> raw, config, ms -> {});

    lifecycle.acquire();

    verify(raw.createStatement()).execute("LISTEN \"ratchet_wakeup_tenantA\"");
  }

  @Test
  void acquireFailsLoudWhenUnwrapReturnsNull() throws Exception {
    Connection raw = mock(Connection.class);
    when(raw.unwrap(PGConnection.class)).thenReturn(null);
    PostgresqlConnectionLifecycle lifecycle =
        new PostgresqlConnectionLifecycle(() -> raw, config, ms -> {});

    SQLException ex = assertThrows(SQLException.class, lifecycle::acquire);

    assertTrue(ex.getMessage().contains("unwrap"), ex.getMessage());
    assertEquals(State.DISCONNECTED, lifecycle.state());
  }

  @Test
  void acquireFailurePathLeavesStateDisconnected() {
    PostgresqlConnectionLifecycle lifecycle =
        new PostgresqlConnectionLifecycle(
            () -> {
              throw new SQLException("connect refused");
            },
            config,
            ms -> {});

    assertThrows(SQLException.class, lifecycle::acquire);
    assertEquals(State.DISCONNECTED, lifecycle.state());
  }

  @Test
  void markFailedTransitionsConnectedToReconnecting() throws Exception {
    Connection raw = mockPgConnection();
    PostgresqlConnectionLifecycle lifecycle =
        new PostgresqlConnectionLifecycle(() -> raw, config, ms -> {});
    lifecycle.acquire();

    lifecycle.markFailed(new SQLException("dropped"));

    assertEquals(State.RECONNECTING, lifecycle.state());
    verify(raw).close();
  }

  @Test
  void markFailedIsNoOpOutsideConnected() {
    PostgresqlConnectionLifecycle lifecycle =
        new PostgresqlConnectionLifecycle(
            () -> {
              throw new SQLException("unused");
            },
            config,
            ms -> {});
    lifecycle.markFailed(new SQLException("ignored"));
    assertEquals(State.DISCONNECTED, lifecycle.state());
  }

  @Test
  void reconnectWithBackoffRetriesUntilSuccess() throws Exception {
    AtomicInteger attempts = new AtomicInteger();
    Connection raw = mockPgConnection();
    AtomicInteger sleeps = new AtomicInteger();

    PostgresqlConnectionLifecycle lifecycle =
        new PostgresqlConnectionLifecycle(
            () -> {
              if (attempts.incrementAndGet() < 3) {
                throw new SQLException("connect refused #" + attempts.get());
              }
              return raw;
            },
            config,
            ms -> sleeps.incrementAndGet());

    lifecycle.reconnectWithBackoff();

    assertEquals(State.CONNECTED, lifecycle.state());
    assertEquals(3, attempts.get());
    assertEquals(2, sleeps.get(), "two failures should produce two pre-success sleeps");
    assertEquals(2L, lifecycle.reconnectAttempts());
    assertEquals(
        0L, lifecycle.consecutiveFailures(), "consecutive counter resets after successful acquire");
  }

  @Test
  void consecutiveFailuresResetOnEachSuccess() throws Exception {
    AtomicInteger attempts = new AtomicInteger();
    Connection raw = mockPgConnection();
    // Sequence: fail, fail, success → fail, fail, fail, success
    PostgresqlConnectionLifecycle lifecycle =
        new PostgresqlConnectionLifecycle(
            () -> {
              int n = attempts.incrementAndGet();
              if (n == 1 || n == 2 || n == 4 || n == 5 || n == 6) {
                throw new SQLException("retry #" + n);
              }
              return raw;
            },
            config,
            ms -> {});

    lifecycle.reconnectWithBackoff();
    assertEquals(0L, lifecycle.consecutiveFailures(), "first connect resets to 0");

    // Force a fresh reconnect cycle.
    lifecycle.markFailed(new SQLException("dropped"));
    lifecycle.reconnectWithBackoff();
    assertEquals(0L, lifecycle.consecutiveFailures(), "second connect resets to 0 again");
    assertEquals(5L, lifecycle.reconnectAttempts(), "lifetime counter sums both cycles");
  }

  @Test
  void reconnectWithBackoffReturnsImmediatelyWhenClosed() throws Exception {
    PostgresqlConnectionLifecycle lifecycle =
        new PostgresqlConnectionLifecycle(
            () -> {
              throw new SQLException("unused");
            },
            config,
            ms -> {
              throw new AssertionError("sleeper should not be called after close()");
            });
    lifecycle.close();
    lifecycle.reconnectWithBackoff();
    assertEquals(State.CLOSED, lifecycle.state());
  }

  @Test
  void reconnectWithBackoffReturnsImmediatelyWhenAlreadyConnected() throws Exception {
    Connection raw = mockPgConnection();
    PostgresqlConnectionLifecycle lifecycle =
        new PostgresqlConnectionLifecycle(
            () -> raw,
            config,
            ms -> {
              throw new AssertionError("sleeper should not be called once CONNECTED");
            });
    lifecycle.acquire();
    lifecycle.reconnectWithBackoff();
    assertEquals(State.CONNECTED, lifecycle.state());
  }

  @Test
  void backoffGrowsExponentiallyAndCaps() throws Exception {
    AtomicInteger attempts = new AtomicInteger();
    java.util.List<Long> sleeps = new java.util.ArrayList<>();
    Connection raw = mockPgConnection();

    PostgresqlConnectionLifecycle lifecycle =
        new PostgresqlConnectionLifecycle(
            () -> {
              if (attempts.incrementAndGet() < 6) {
                throw new SQLException("retry #" + attempts.get());
              }
              return raw;
            },
            config,
            sleeps::add,
            // Identity jitter for deterministic ceiling assertion in tests.
            delay -> delay);

    lifecycle.reconnectWithBackoff();

    assertEquals(java.util.List.of(10L, 20L, 40L, 50L, 50L), sleeps);
  }

  @Test
  void backoffAppliesJitterCappedAtCeiling() throws Exception {
    AtomicInteger attempts = new AtomicInteger();
    java.util.List<Long> ceilings = new java.util.ArrayList<>();
    java.util.List<Long> sleeps = new java.util.ArrayList<>();
    Connection raw = mockPgConnection();

    PostgresqlConnectionLifecycle lifecycle =
        new PostgresqlConnectionLifecycle(
            () -> {
              if (attempts.incrementAndGet() < 6) {
                throw new SQLException("retry #" + attempts.get());
              }
              return raw;
            },
            config,
            sleeps::add,
            ceiling -> {
              ceilings.add(ceiling);
              return ceiling / 3; // arbitrary sub-ceiling that proves jitter ran
            });

    lifecycle.reconnectWithBackoff();

    assertEquals(java.util.List.of(10L, 20L, 40L, 50L, 50L), ceilings);
    for (int i = 0; i < sleeps.size(); i++) {
      long sleep = sleeps.get(i);
      long ceiling = ceilings.get(i);
      assertTrue(sleep >= 0 && sleep <= ceiling, "sleep " + sleep + " out of [0, " + ceiling + "]");
    }
  }

  @Test
  void closeIsIdempotent() throws Exception {
    Connection raw = mockPgConnection();
    PostgresqlConnectionLifecycle lifecycle =
        new PostgresqlConnectionLifecycle(() -> raw, config, ms -> {});
    lifecycle.acquire();

    lifecycle.close();
    lifecycle.close();

    assertEquals(State.CLOSED, lifecycle.state());
    verify(raw, atLeastOnce()).close();
  }

  @Test
  void closeIssuesUnlistenBestEffort() throws Exception {
    Connection raw = mockPgConnection();
    PostgresqlConnectionLifecycle lifecycle =
        new PostgresqlConnectionLifecycle(() -> raw, config, ms -> {});
    lifecycle.acquire();

    lifecycle.close();

    Statement unlistenStmt = raw.createStatement();
    verify(unlistenStmt, atLeastOnce()).execute(anyString());
    verify(raw, atLeastOnce()).close();
  }

  @Test
  void acquireAfterCloseFailsLoud() {
    PostgresqlConnectionLifecycle lifecycle =
        new PostgresqlConnectionLifecycle(
            () -> {
              throw new SQLException("unused");
            },
            config,
            ms -> {});
    lifecycle.close();
    assertThrows(SQLException.class, lifecycle::acquire);
  }

  @Test
  void quoteIdentifierEscapesEmbeddedDoubleQuotes() {
    assertEquals("\"plain\"", PostgresqlConnectionLifecycle.quoteIdentifier("plain"));
    assertEquals("\"weird\"\"name\"", PostgresqlConnectionLifecycle.quoteIdentifier("weird\"name"));
  }

  @Test
  void currentReturnsSamePgConnectionAcrossCalls() throws Exception {
    Connection raw = mockPgConnection();
    PostgresqlConnectionLifecycle lifecycle =
        new PostgresqlConnectionLifecycle(() -> raw, config, ms -> {});
    lifecycle.acquire();

    PGConnection first = lifecycle.current();
    PGConnection second = lifecycle.current();

    assertSame(first, second);
    assertSame(raw.unwrap(PGConnection.class), first);
  }

  @Test
  void reconnectAfterFailureRestoresUsability() throws Exception {
    Connection raw1 = mockPgConnection();
    Connection raw2 = mockPgConnection();
    java.util.Iterator<Connection> conns = java.util.List.of(raw1, raw2).iterator();
    PostgresqlConnectionLifecycle lifecycle =
        new PostgresqlConnectionLifecycle(conns::next, config, ms -> {});

    lifecycle.acquire();
    assertSame(raw1.unwrap(PGConnection.class), lifecycle.current());

    lifecycle.markFailed(new SQLException("dropped"));
    assertEquals(State.RECONNECTING, lifecycle.state());

    lifecycle.reconnectWithBackoff();
    assertEquals(State.CONNECTED, lifecycle.state());
    assertSame(raw2.unwrap(PGConnection.class), lifecycle.current());
  }

  @Test
  void acquirerOnlyReceivesAcquireCallsFromExpectedStates() throws Exception {
    AtomicInteger calls = new AtomicInteger();
    Connection raw = mockPgConnection();
    PostgresqlConnectionLifecycle lifecycle =
        new PostgresqlConnectionLifecycle(
            () -> {
              calls.incrementAndGet();
              return raw;
            },
            config,
            ms -> {});

    lifecycle.acquire();
    lifecycle.acquire(); // no-op while CONNECTED
    assertEquals(1, calls.get());
  }

  @Test
  void unlistenFailureOnCloseIsSwallowed() throws Exception {
    Connection raw = mock(Connection.class);
    PGConnection pg = mock(PGConnection.class);
    when(raw.unwrap(PGConnection.class)).thenReturn(pg);
    Statement listenStmt = mock(Statement.class);
    Statement unlistenStmt = mock(Statement.class);
    when(raw.createStatement()).thenReturn(listenStmt, unlistenStmt);
    when(unlistenStmt.execute(anyString())).thenThrow(new SQLException("server gone"));
    PostgresqlConnectionLifecycle lifecycle =
        new PostgresqlConnectionLifecycle(() -> raw, config, ms -> {});
    lifecycle.acquire();

    lifecycle.close();

    verify(raw, atLeastOnce()).close();
    assertEquals(State.CLOSED, lifecycle.state());
  }

  @Test
  void acquireUsesSuppliedConnectionAcquirer() throws Exception {
    Connection raw = mockPgConnection();
    PostgresqlConnectionLifecycle.ConnectionAcquirer acquirer =
        mock(PostgresqlConnectionLifecycle.ConnectionAcquirer.class);
    when(acquirer.acquire()).thenReturn(raw);
    PostgresqlConnectionLifecycle lifecycle =
        new PostgresqlConnectionLifecycle(acquirer, config, ms -> {});
    lifecycle.acquire();
    verify(acquirer).acquire();
  }

  @Test
  void sleeperInterruptionPropagatesAsInterruptedException() {
    Connection raw;
    try {
      raw = mockPgConnection();
    } catch (SQLException e) {
      throw new AssertionError(e);
    }
    AtomicInteger calls = new AtomicInteger();
    PostgresqlConnectionLifecycle lifecycle =
        new PostgresqlConnectionLifecycle(
            () -> {
              if (calls.getAndIncrement() == 0) {
                throw new SQLException("first fails");
              }
              return raw;
            },
            config,
            ms -> {
              throw new InterruptedException();
            });

    assertThrows(InterruptedException.class, lifecycle::reconnectWithBackoff);
  }

  @Test
  @SuppressWarnings("unused")
  void unwrapSqlExceptionPropagatesFromAcquire() {
    PostgresqlConnectionLifecycle lifecycle =
        new PostgresqlConnectionLifecycle(
            () -> {
              Connection raw = mock(Connection.class);
              when(raw.unwrap(any())).thenThrow(new SQLException("bad cast"));
              return raw;
            },
            config,
            ms -> {});
    assertThrows(SQLException.class, lifecycle::acquire);
    assertEquals(State.DISCONNECTED, lifecycle.state());
  }
}
