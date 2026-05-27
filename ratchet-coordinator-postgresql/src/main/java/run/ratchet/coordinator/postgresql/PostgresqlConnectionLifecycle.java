package run.ratchet.coordinator.postgresql;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongUnaryOperator;
import javax.sql.DataSource;
import org.jboss.logging.Logger;
import org.postgresql.PGConnection;

/**
 * Owns the dedicated long-lived {@link PGConnection} used by the PostgreSQL cluster coordinator for
 * {@code LISTEN}/{@code NOTIFY}. The connection is autocommit and never returned to a pool.
 *
 * <p>State machine:
 *
 * <pre>
 *    acquire()
 * DISCONNECTED ─────────────► CONNECTING
 *      ▲                          │
 *      │ failure / reconnect()    │ success → LISTEN channel; setAutoCommit(true)
 *      │                          ▼
 *  RECONNECTING ◄─────────► CONNECTED
 *      ▲          drop           │
 *      └──────────────────────────┘
 *               close() → CLOSED (terminal)
 * </pre>
 *
 * <p>Threading: {@link #acquire()}, {@link #reconnectWithBackoff()}, {@link
 * #markFailed(SQLException)}, and {@link #close()} are guarded by an intrinsic lock. {@link
 * #current()} returns the connection via a volatile read — callers must handle a stale-after-read
 * race the same way they handle any other transport error (catch {@link SQLException}, route
 * through {@code markFailed}).
 */
final class PostgresqlConnectionLifecycle {

  private static final Logger log = Logger.getLogger(PostgresqlConnectionLifecycle.class);

  enum State {
    DISCONNECTED,
    CONNECTING,
    CONNECTED,
    RECONNECTING,
    CLOSED
  }

  /** Hook so unit tests can inject a non-DataSource connection acquirer (mock or fake). */
  @FunctionalInterface
  interface ConnectionAcquirer {
    Connection acquire() throws SQLException;
  }

  /** Hook so unit tests can replace blocking {@code Thread.sleep} with a deterministic stub. */
  @FunctionalInterface
  interface Sleeper {
    void sleep(long millis) throws InterruptedException;
  }

  /**
   * Once {@code consecutiveFailures} reaches this threshold, reconnect log lines escalate from WARN
   * to ERROR so an oncall isn't drowned out by transient blips but a sustained outage paints red on
   * the dashboard.
   */
  static final int CONSECUTIVE_FAILURES_ERROR_THRESHOLD = 10;

  private final ConnectionAcquirer acquirer;
  private final PostgresqlCoordinatorConfig config;
  private final Sleeper sleeper;
  private final LongUnaryOperator jitter;
  private final Object lock = new Object();
  private final AtomicLong reconnectAttempts = new AtomicLong();
  private final AtomicLong consecutiveFailures = new AtomicLong();
  private volatile State state = State.DISCONNECTED;
  private volatile PGConnection connection;
  private volatile Connection rawConnection;

  /**
   * Production jitter source — uniform {@code [0, delay]} ("full jitter") via {@link
   * ThreadLocalRandom}. The {@code +1} on the upper bound is so {@code delay} itself remains
   * reachable; without it {@code nextLong(delay)} would cap at {@code delay - 1}.
   */
  private static final LongUnaryOperator DEFAULT_JITTER =
      delay -> ThreadLocalRandom.current().nextLong(delay + 1);

  PostgresqlConnectionLifecycle(DataSource dataSource, PostgresqlCoordinatorConfig config) {
    this(dataSource::getConnection, config, Thread::sleep, DEFAULT_JITTER);
  }

  PostgresqlConnectionLifecycle(
      ConnectionAcquirer acquirer, PostgresqlCoordinatorConfig config, Sleeper sleeper) {
    this(acquirer, config, sleeper, DEFAULT_JITTER);
  }

  PostgresqlConnectionLifecycle(
      ConnectionAcquirer acquirer,
      PostgresqlCoordinatorConfig config,
      Sleeper sleeper,
      LongUnaryOperator jitter) {
    this.acquirer = Objects.requireNonNull(acquirer, "acquirer");
    this.config = Objects.requireNonNull(config, "config");
    this.sleeper = Objects.requireNonNull(sleeper, "sleeper");
    this.jitter = Objects.requireNonNull(jitter, "jitter");
  }

  State state() {
    return state;
  }

  /** Number of completed reconnect attempts since construction. Exposed for tests/metrics. */
  long reconnectAttempts() {
    return reconnectAttempts.get();
  }

  /**
   * Failures since the last successful acquire — resets to zero on every successful CONNECTED
   * transition. Useful for distinguishing "blip" from "sustained outage" without scrolling logs.
   */
  long consecutiveFailures() {
    return consecutiveFailures.get();
  }

  /**
   * Returns the current {@link PGConnection}. Throws {@link SQLException} if no connection is held
   * (DISCONNECTED, CONNECTING, RECONNECTING, or CLOSED). Callers must catch and route through
   * {@link #markFailed(SQLException)}.
   */
  PGConnection current() throws SQLException {
    PGConnection c = connection;
    if (state != State.CONNECTED || c == null) {
      throw new SQLException(
          "PostgreSQL coordinator connection is not available (state=" + state + ")");
    }
    return c;
  }

  /**
   * Returns the same underlying connection as {@link #current()} viewed through the JDBC {@link
   * Connection} interface. Two views are exposed because {@link PGConnection} (a PG-specific SPI
   * interface) is not a subtype of {@link Connection}, even though every concrete implementation is
   * both. The {@code PGConnection} view is required for {@code getNotifications}; the {@code
   * Connection} view is required for {@code prepareStatement}/{@code createStatement}.
   */
  Connection currentRaw() throws SQLException {
    Connection c = rawConnection;
    if (state != State.CONNECTED || c == null) {
      throw new SQLException(
          "PostgreSQL coordinator connection is not available (state=" + state + ")");
    }
    return c;
  }

  /**
   * Acquires the initial dedicated connection. Transitions DISCONNECTED → CONNECTING → CONNECTED on
   * success. On failure, leaves state as DISCONNECTED and rethrows so the caller can route through
   * {@link #reconnectWithBackoff()}.
   *
   * <p>Idempotent only when already CONNECTED — calling from another state after a failure path
   * requires the caller to drive RECONNECTING via {@link #reconnectWithBackoff()}.
   */
  void acquire() throws SQLException {
    synchronized (lock) {
      if (state == State.CLOSED) {
        throw new SQLException("PostgreSQL coordinator lifecycle is CLOSED");
      }
      if (state == State.CONNECTED) {
        return;
      }
      if (state != State.DISCONNECTED && state != State.RECONNECTING) {
        throw new SQLException(
            "acquire() called from unexpected state "
                + state
                + " (expected DISCONNECTED/RECONNECTING)");
      }
      state = State.CONNECTING;
    }
    Connection raw = null;
    try {
      raw = acquirer.acquire();
      PGConnection pg = raw.unwrap(PGConnection.class);
      if (pg == null) {
        throw new SQLException(
            "DataSource returned a connection that does not unwrap to org.postgresql.PGConnection."
                + " Pools like DBCP2/c3p0 wrap connections opaquely; use HikariCP or a container-"
                + "managed DataSource for the LISTEN connection.");
      }
      raw.setAutoCommit(true);
      try (Statement s = raw.createStatement()) {
        s.execute("LISTEN " + quoteIdentifier(config.effectiveChannel()));
      }
      synchronized (lock) {
        if (state == State.CLOSED) {
          // close() raced us — drop the just-acquired connection without publishing.
          closeQuietly(raw);
          throw new SQLException("PostgreSQL coordinator lifecycle was CLOSED during acquire()");
        }
        this.rawConnection = raw;
        this.connection = pg;
        this.state = State.CONNECTED;
        consecutiveFailures.set(0);
      }
    } catch (SQLException e) {
      synchronized (lock) {
        if (state != State.CLOSED) {
          state = State.DISCONNECTED;
        }
      }
      if (raw != null && this.rawConnection != raw) {
        closeQuietly(raw);
      }
      throw e;
    }
  }

  /**
   * Marks the current connection as failed. CONNECTED → RECONNECTING. No-op from any other state so
   * callers can blindly route every transport error through this method.
   */
  void markFailed(SQLException cause) {
    Connection toClose = null;
    synchronized (lock) {
      if (state != State.CONNECTED) {
        return;
      }
      log.debugf(cause, "PostgreSQL coordinator connection failed; transitioning to RECONNECTING");
      state = State.RECONNECTING;
      toClose = this.rawConnection;
      this.rawConnection = null;
      this.connection = null;
    }
    closeQuietly(toClose);
  }

  /**
   * Reconnects with exponential backoff until {@link #acquire()} succeeds or {@link #close()} flips
   * state to CLOSED. Returns immediately if state is already CONNECTED or CLOSED.
   *
   * <p>The first {@link #acquire()} attempt fires immediately (no leading sleep). After a failed
   * attempt the thread sleeps for a jittered subset of {@code reconnectBackoffInitialMs} via {@link
   * Sleeper}, then retries; each subsequent failure doubles the delay ceiling, capped at {@code
   * reconnectBackoffMaxMs}. Full-jitter ({@code uniform [0, ceiling]}) keeps N nodes from marching
   * in lock-step out of a shared outage.
   *
   * @throws InterruptedException if the calling thread is interrupted between attempts; callers
   *     re-interrupt and return from their loop.
   */
  void reconnectWithBackoff() throws InterruptedException {
    long delay = config.reconnectBackoffInitialMs();
    while (true) {
      synchronized (lock) {
        if (state == State.CLOSED) {
          return;
        }
        if (state == State.CONNECTED) {
          return;
        }
        if (state == State.CONNECTING) {
          // Another thread is mid-acquire. Spin out and let the caller retry the read.
          return;
        }
        if (state != State.DISCONNECTED && state != State.RECONNECTING) {
          // Defensive: should never happen.
          return;
        }
      }
      try {
        acquire();
        return;
      } catch (SQLException e) {
        reconnectAttempts.incrementAndGet();
        long consecutive = consecutiveFailures.incrementAndGet();
        if (consecutive >= CONSECUTIVE_FAILURES_ERROR_THRESHOLD) {
          log.errorf(
              "PostgreSQL coordinator reconnect attempt %d failed (%d consecutive failures): %s"
                  + " — retrying within %d ms",
              reconnectAttempts.get(), consecutive, e.getMessage(), delay);
        } else {
          log.warnf(
              "PostgreSQL coordinator reconnect attempt %d failed: %s — retrying within %d ms",
              reconnectAttempts.get(), e.getMessage(), delay);
        }
      }
      synchronized (lock) {
        if (state == State.CLOSED) {
          return;
        }
        // After a failed acquire(), state is DISCONNECTED. Push it to RECONNECTING so the next
        // iteration's state guard treats this as a reconnect rather than initial acquire.
        if (state == State.DISCONNECTED) {
          state = State.RECONNECTING;
        }
      }
      sleeper.sleep(jitter.applyAsLong(delay));
      delay = Math.min(delay * 2, config.reconnectBackoffMaxMs());
    }
  }

  /**
   * Closes the lifecycle. Idempotent — a second invocation observes state == CLOSED and returns.
   * Releases the dedicated connection. After close, {@link #current()} throws and {@link
   * #reconnectWithBackoff()} returns immediately.
   */
  void close() {
    Connection toClose;
    synchronized (lock) {
      if (state == State.CLOSED) {
        return;
      }
      state = State.CLOSED;
      toClose = this.rawConnection;
      this.rawConnection = null;
      this.connection = null;
    }
    if (toClose != null) {
      // Best-effort UNLISTEN to be a polite citizen on the server side. Failure here is
      // irrelevant — closing the connection achieves the same effect server-side.
      try (Statement s = toClose.createStatement()) {
        s.execute("UNLISTEN " + quoteIdentifier(config.effectiveChannel()));
      } catch (SQLException unlistenEx) {
        log.debugf(unlistenEx, "PostgreSQL coordinator UNLISTEN on close failed; ignoring");
      } finally {
        closeQuietly(toClose);
      }
    }
  }

  private static void closeQuietly(Connection c) {
    if (c == null) {
      return;
    }
    try {
      c.close();
    } catch (SQLException e) {
      log.debugf(e, "PostgreSQL coordinator: closeQuietly suppressed exception");
    }
  }

  /**
   * Quotes a PostgreSQL identifier by wrapping in double quotes and doubling any embedded double
   * quote, matching the behavior of {@code org.postgresql.core.Utils.escapeIdentifier} but without
   * pulling in driver-internal API.
   */
  static String quoteIdentifier(String identifier) {
    Objects.requireNonNull(identifier, "identifier");
    StringBuilder sb = new StringBuilder(identifier.length() + 2);
    sb.append('"');
    for (int i = 0; i < identifier.length(); i++) {
      char c = identifier.charAt(i);
      if (c == '"') {
        sb.append('"');
      }
      sb.append(c);
    }
    sb.append('"');
    return sb.toString();
  }
}
