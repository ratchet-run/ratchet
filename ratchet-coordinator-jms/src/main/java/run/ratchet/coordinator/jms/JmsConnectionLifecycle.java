package run.ratchet.coordinator.jms;

import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSConsumer;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSException;
import jakarta.jms.JMSProducer;
import jakarta.jms.Message;
import jakarta.jms.MessageListener;
import jakarta.jms.TextMessage;
import jakarta.jms.Topic;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.jboss.logging.Logger;
import run.ratchet.api.NodeIdentity;

/**
 * Owns the {@link JMSContext} and {@link JMSProducer} for one JMS coordinator instance. The {@code
 * JMSConsumer} the lifecycle creates is registered with the context via {@code setMessageListener}
 * and lives for the lifetime of that context — closing the context cascades to the consumer per the
 * JMS spec, so we do not hold a separate reference. Reads-after-write atomicity is preserved by
 * holding context and producer in a single {@link AtomicReference}: a reconnect path swaps both in
 * one publish, and {@link JmsClusterCoordinator#notifyNewWork} never observes a mixed pair.
 *
 * <p>Reconnect is driven by two paths:
 *
 * <ol>
 *   <li>JMS provider's {@code ExceptionListener} firing on a connection-level fault (the primary
 *       failure indicator the spec requires).
 *   <li>An active background retry loop started when reconnect first fails — the {@code
 *       ExceptionListener} only fires once per real fault, so an outage that outlives the first
 *       backoff window needs a self-paced retry source to recover.
 * </ol>
 *
 * <p>{@link #close()} is idempotent and releases the context the lifecycle owns. The {@link
 * ConnectionFactory} is provider-owned and never closed here.
 */
final class JmsConnectionLifecycle {

  private static final Logger log = Logger.getLogger(JmsConnectionLifecycle.class);

  /**
   * Once {@code consecutiveFailures} reaches this threshold, reconnect log lines escalate from WARN
   * to ERROR so an oncall isn't drowned out by transient blips but a sustained outage paints red on
   * the dashboard.
   */
  static final int CONSECUTIVE_FAILURES_ERROR_THRESHOLD = 10;

  private final ConnectionFactory connectionFactory;
  private final Topic topic;
  private final JmsCoordinatorConfig config;
  private final Consumer<Message> inboundHandler;
  private final Runnable onTransportFailure;

  private final AtomicReference<Pair> connectionRef = new AtomicReference<>();
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final AtomicBoolean reconnectInFlight = new AtomicBoolean(false);
  private final AtomicLong consecutiveFailures = new AtomicLong();

  private volatile NodeIdentity localIdentity;
  private volatile Thread reconnectThread;

  JmsConnectionLifecycle(
      ConnectionFactory connectionFactory,
      Topic topic,
      JmsCoordinatorConfig config,
      Consumer<Message> inboundHandler,
      Runnable onTransportFailure) {
    this.connectionFactory = Objects.requireNonNull(connectionFactory, "connectionFactory");
    this.topic = Objects.requireNonNull(topic, "topic");
    this.config = Objects.requireNonNull(config, "config");
    this.inboundHandler = Objects.requireNonNull(inboundHandler, "inboundHandler");
    this.onTransportFailure = Objects.requireNonNull(onTransportFailure, "onTransportFailure");
  }

  /**
   * Bind the lifecycle to a node identity and bring up the first context. Idempotent — invoked
   * during coordinator startup and never thereafter from the application side.
   */
  void start(NodeIdentity localIdentity) {
    this.localIdentity = Objects.requireNonNull(localIdentity, "localIdentity");
    if (!connectOnce(/* failureIsExpected= */ false)) {
      triggerReconnect();
    }
  }

  /** Current producer, or null if reconnect is in flight or the lifecycle is closed. */
  JMSProducer currentProducer() {
    Pair pair = connectionRef.get();
    return pair == null ? null : pair.producer();
  }

  /** Current context, or null if reconnect is in flight or the lifecycle is closed. */
  JMSContext currentContext() {
    Pair pair = connectionRef.get();
    return pair == null ? null : pair.context();
  }

  /**
   * Send a text message on the current coherent context/producer pair. Returns false when reconnect
   * is in flight or the initial connect has not yet succeeded.
   */
  boolean sendTextMessage(String body, String node, String priority) throws JMSException {
    Pair pair = connectionRef.get();
    if (pair == null) {
      return false;
    }
    TextMessage msg = pair.context().createTextMessage(body);
    msg.setStringProperty("node", node);
    msg.setStringProperty("prio", priority);
    pair.producer().send(topic, msg);
    return true;
  }

  /** True after {@link #close()} has been called. */
  boolean isClosed() {
    return closed.get();
  }

  /**
   * Failures since the last successful {@link #connectOnce(boolean)} — resets to zero on every
   * successful connect. Useful for distinguishing "blip" from "sustained outage".
   */
  long consecutiveFailures() {
    return consecutiveFailures.get();
  }

  /**
   * Trigger a reconnect cycle. Safe to call from any thread; one in-flight reconnect at a time. The
   * caller has already recorded a {@code transport_failure} metric for the originating fault.
   */
  void triggerReconnect() {
    if (closed.get()) {
      return;
    }
    if (!reconnectInFlight.compareAndSet(false, true)) {
      return; // already reconnecting
    }
    Thread t = new Thread(this::reconnectLoop, "ratchet-coordinator-jms-reconnect");
    t.setDaemon(true);
    this.reconnectThread = t;
    t.start();
  }

  /** Release the context the lifecycle owns. Idempotent. */
  void close() {
    if (!closed.compareAndSet(false, true)) {
      return;
    }
    Thread t = this.reconnectThread;
    if (t != null) {
      t.interrupt();
    }
    closeContextRef();
  }

  // ---- internals ----------------------------------------------------------

  private boolean connectOnce(boolean failureIsExpected) {
    if (closed.get()) {
      return false;
    }
    try {
      JMSContext ctx = connectionFactory.createContext(JMSContext.AUTO_ACKNOWLEDGE);
      ctx.setExceptionListener(this::onConnectionException);
      JMSProducer p = ctx.createProducer();
      String selector =
          config.brokerSideSelfFilter()
              ? "node <> '" + escapeSelector(localIdentity.value()) + "'"
              : null;
      JMSConsumer consumer =
          selector == null ? ctx.createConsumer(topic) : ctx.createConsumer(topic, selector);
      consumer.setMessageListener(new InboundListener(inboundHandler));

      // Close-during-reconnect race: if close() flipped `closed` after our guard above but
      // before we got here, the freshly-built context would leak — close() has already drained
      // contextRef and would not see this new one. Re-check and clean up if so.
      if (closed.get()) {
        closeQuietly(ctx);
        return false;
      }
      Pair stale = connectionRef.getAndSet(new Pair(ctx, p));
      consecutiveFailures.set(0);
      if (stale != null) {
        closeQuietly(stale.context());
      }
      return true;
    } catch (RuntimeException ex) {
      long consecutive = consecutiveFailures.incrementAndGet();
      if (consecutive >= CONSECUTIVE_FAILURES_ERROR_THRESHOLD) {
        log.errorf(
            "JMS coordinator connect failed (%d consecutive failures): %s",
            consecutive, ex.getMessage());
      } else if (!failureIsExpected) {
        log.warnf("JMS coordinator initial connect failed: %s", ex.getMessage());
      }
      // Leave the ref null so notifyNewWork degrades to no-op; metric was already recorded.
      connectionRef.set(null);
      return false;
    }
  }

  private void onConnectionException(JMSException ex) {
    if (closed.get()) {
      return;
    }
    onTransportFailure.run();
    log.warnf("JMS coordinator connection exception, scheduling reconnect: %s", ex.getMessage());
    Pair stale = connectionRef.getAndSet(null);
    if (stale != null) {
      closeQuietly(stale.context());
    }
    triggerReconnect();
  }

  private void reconnectLoop() {
    long delay = config.reconnectBackoffInitialMs();
    try {
      while (!closed.get()) {
        try {
          // Full jitter uniform [0, delay] so N nodes don't march in lock-step out of a shared
          // outage. The +1 keeps `delay` itself reachable.
          Thread.sleep(ThreadLocalRandom.current().nextLong(delay + 1));
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          return;
        }
        if (connectOnce(/* failureIsExpected= */ true)) {
          return;
        }
        onTransportFailure.run();
        delay = Math.min(delay * 2, config.reconnectBackoffMaxMs());
      }
    } finally {
      reconnectInFlight.set(false);
      reconnectThread = null;
    }
  }

  private void closeContextRef() {
    Pair pair = connectionRef.getAndSet(null);
    if (pair != null) {
      closeQuietly(pair.context());
    }
  }

  private record Pair(JMSContext context, JMSProducer producer) {}

  private static void closeQuietly(JMSContext ctx) {
    try {
      ctx.close();
    } catch (Exception ignored) {
      // best-effort; the context may already be in a half-broken state
    }
  }

  /**
   * Escape characters significant to JMS selector string literals: single-quote by doubling, and
   * backslash by doubling (JMS 3.0 §3.8.1.1 reserves both). {@link run.ratchet.api.NodeIdentity}'s
   * constructor already rejects values containing either, so in practice this is
   * belt-and-suspenders for selector strings derived from non-NodeIdentity inputs.
   */
  static String escapeSelector(String value) {
    return value.replace("\\", "\\\\").replace("'", "''");
  }

  /**
   * Adapter so the {@code MessageListener} signature can call a {@link Consumer} the coordinator
   * supplies. Catches every exception inside {@code onMessage} so a misbehaving handler does not
   * trigger the JMS provider's "close the connection on listener exception" behaviour.
   */
  static final class InboundListener implements MessageListener {
    private final Consumer<Message> handler;

    InboundListener(Consumer<Message> handler) {
      this.handler = handler;
    }

    @Override
    public void onMessage(Message message) {
      try {
        handler.accept(message);
      } catch (RuntimeException ignored) {
        // The handler already recorded any transport_failure metric. Swallow to avoid the
        // provider tearing down the connection on listener-throws.
      }
    }
  }
}
