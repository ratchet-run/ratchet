package run.ratchet.coordinator.postgresql;

import java.sql.SQLException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.jboss.logging.Logger;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;
import run.ratchet.coordinator.common.DecodeException;
import run.ratchet.coordinator.common.NotifyPayload;
import run.ratchet.coordinator.common.internal.NotifyPayloadCodec;

/**
 * Single background daemon thread that pulls PostgreSQL {@code NOTIFY} messages off the dedicated
 * LISTEN connection and hands decoded payloads to the coordinator dispatcher.
 *
 * <p>Lifecycle:
 *
 * <ol>
 *   <li>Constructor allocates the thread but does not start it.
 *   <li>{@link #start()} drives initial connection acquire via {@link
 *       PostgresqlConnectionLifecycle#reconnectWithBackoff()}.
 *   <li>The {@link #run()} loop calls {@link PGConnection#getNotifications(int)} repeatedly until
 *       {@link #shutdown()} flips the close flag.
 *   <li>{@code SQLException}s from the loop route through {@code markFailed → reconnectWithBackoff}
 *       and the loop resumes on the new connection.
 *   <li>{@code DecodeException}s from malformed payloads are counted as parse failures and skipped;
 *       the connection stays up.
 * </ol>
 */
final class PostgresqlListenThread extends Thread {

  private static final Logger log = Logger.getLogger(PostgresqlListenThread.class);
  private static final AtomicLong THREAD_NUMBER = new AtomicLong();

  private final PostgresqlConnectionLifecycle lifecycle;
  private final NotifyPayloadCodec codec;
  private final PostgresqlCoordinatorConfig config;
  private final Consumer<NotifyPayload> dispatcher;
  private final Runnable onParseFailure;
  private final Runnable onTransportFailure;
  private volatile boolean closed;

  PostgresqlListenThread(
      PostgresqlConnectionLifecycle lifecycle,
      NotifyPayloadCodec codec,
      PostgresqlCoordinatorConfig config,
      Consumer<NotifyPayload> dispatcher,
      Runnable onParseFailure,
      Runnable onTransportFailure) {
    super("ratchet-coordinator-postgresql-listen-" + THREAD_NUMBER.incrementAndGet());
    setDaemon(true);
    this.lifecycle = Objects.requireNonNull(lifecycle, "lifecycle");
    this.codec = Objects.requireNonNull(codec, "codec");
    this.config = Objects.requireNonNull(config, "config");
    this.dispatcher = Objects.requireNonNull(dispatcher, "dispatcher");
    this.onParseFailure = Objects.requireNonNull(onParseFailure, "onParseFailure");
    this.onTransportFailure = Objects.requireNonNull(onTransportFailure, "onTransportFailure");
  }

  void shutdown() {
    closed = true;
    interrupt();
  }

  @Override
  public void run() {
    try {
      lifecycle.reconnectWithBackoff();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return;
    }

    int receiveTimeoutMs = clampReceiveTimeout(config.receiveTimeoutMs());

    while (!closed) {
      try {
        PGConnection pg = lifecycle.current();
        PGNotification[] notes = pg.getNotifications(receiveTimeoutMs);
        if (notes == null) {
          continue;
        }
        for (PGNotification n : notes) {
          dispatchOne(n);
        }
      } catch (SQLException sqlEx) {
        if (closed) {
          return;
        }
        onTransportFailure.run();
        lifecycle.markFailed(sqlEx);
        log.warnf("PostgreSQL coordinator listen failure: %s; reconnecting", sqlEx.getMessage());
        try {
          lifecycle.reconnectWithBackoff();
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          return;
        }
      }
    }
  }

  void dispatchOne(PGNotification n) {
    String parameter = n.getParameter();
    NotifyPayload payload;
    try {
      if (parameter != null && parameter.length() > config.maxInboundPayloadChars()) {
        onParseFailure.run();
        log.warnf(
            "PostgreSQL coordinator rejected oversized inbound payload (%d chars > cap %d)",
            parameter.length(), config.maxInboundPayloadChars());
        return;
      }
      payload = codec.decode(parameter);
    } catch (DecodeException parseEx) {
      onParseFailure.run();
      log.warnf("PostgreSQL coordinator dropped malformed payload: %s", parseEx.getMessage());
      return;
    }
    try {
      dispatcher.accept(payload);
    } catch (RuntimeException dispatchEx) {
      // Dispatcher exceptions should never escape and kill the listen thread. The coordinator
      // already wraps listener invocations in try/catch; this is defense-in-depth.
      log.warnf(dispatchEx, "PostgreSQL coordinator dispatcher threw: %s", dispatchEx.getMessage());
    }
  }

  /**
   * Pins the {@code getNotifications} timeout into the {@code int} range expected by the JDBC
   * driver. {@code PGConnection#getNotifications(int)} treats {@code 0} as "block forever," which
   * would prevent the loop from observing {@link #shutdown()}; we floor at 1ms.
   */
  private static int clampReceiveTimeout(long receiveTimeoutMs) {
    if (receiveTimeoutMs <= 0) {
      return 1;
    }
    if (receiveTimeoutMs > Integer.MAX_VALUE) {
      return Integer.MAX_VALUE;
    }
    return (int) receiveTimeoutMs;
  }
}
