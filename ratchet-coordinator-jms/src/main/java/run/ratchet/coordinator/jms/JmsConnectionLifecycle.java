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

import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSConsumer;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSException;
import jakarta.jms.JMSProducer;
import jakarta.jms.JMSRuntimeException;
import jakarta.jms.Message;
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
 * Owns the {@link JMSContext}, {@link JMSProducer}, and {@link JMSConsumer} for one JMS coordinator
 * instance, and runs a dedicated thread that pulls inbound messages with synchronous {@link
 * JMSConsumer#receive(long)}.
 *
 * <p>Inbound delivery is synchronous on purpose. Jakarta Messaging 3.0 §12.3 forbids an
 * application-created {@code JMSContext}/{@code JMSConsumer} in a web or EJB container from
 * registering asynchronous delivery — {@code setMessageListener} and {@code setExceptionListener}
 * must throw — so the async model only works in Java SE or against lenient brokers. A blocking
 * {@code receive(timeout)} on a dedicated thread is permitted everywhere and mirrors the PostgreSQL
 * coordinator's listen thread.
 *
 * <p>Reconnect is driven by two paths:
 *
 * <ol>
 *   <li>The receive thread observing a {@link JMSRuntimeException} from {@code receive()} on a
 *       connection-level fault (replacing the spec-illegal {@code ExceptionListener}).
 *   <li>An active background retry loop started when reconnect first fails — a fault surfaces once,
 *       so an outage that outlives the first backoff window needs a self-paced retry source.
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

  /**
   * Upper bound on a single {@code receive()} wait. It only bounds how quickly the receive thread
   * notices {@link #close()} / a pair swap on a quiet topic; real wakeups arrive as soon as the
   * broker pushes a message. Kept local because {@link JmsCoordinatorConfig} carries no receive
   * timeout (the PostgreSQL coordinator's default is the same 1s).
   */
  private static final long RECEIVE_TIMEOUT_MS = 1_000L;

  /**
   * Bound on how long {@link #close()} waits for the reconnect and receive threads to exit after
   * interrupting them. Long enough to cover a {@code receive()} poll plus an in-flight {@code
   * dispatch()} so the inbound handler stops touching CDI-managed beans before the context is
   * destroyed; short enough that a wedged handler cannot stall container shutdown.
   */
  private static final long SHUTDOWN_JOIN_TIMEOUT_MS = 2_000L;

  private final ConnectionFactory connectionFactory;
  private final Topic topic;
  private final JmsCoordinatorConfig config;
  private final Consumer<Message> inboundHandler;
  private final Runnable onTransportFailure;

  private final AtomicReference<Pair> connectionRef = new AtomicReference<>();
  // Serializes concurrent sends so only one thread ever touches the sender JMSContext at a time.
  // Jakarta Messaging 3.0 §6.1.1 limits a session (and its JMSContext) to a single thread of
  // control; sends arrive from arbitrary job-submission threads and JTA afterCompletion callbacks.
  private final Object sendLock = new Object();
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final AtomicBoolean reconnectInFlight = new AtomicBoolean(false);
  private final AtomicLong consecutiveFailures = new AtomicLong();

  private volatile NodeIdentity localIdentity;
  private volatile Thread reconnectThread;
  private volatile Thread receiveThread;

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

  /**
   * Current consumer (receive) context, or null if reconnect is in flight or the lifecycle is
   * closed.
   */
  JMSContext currentContext() {
    Pair pair = connectionRef.get();
    return pair == null ? null : pair.consumerContext();
  }

  /**
   * Send a text message on the current generation's dedicated sender context/producer. Returns
   * false when reconnect is in flight or the initial connect has not yet succeeded.
   *
   * <p>Held under {@link #sendLock} so concurrent senders are serialized — a JMSContext is
   * single-thread-of-control, and the sender context is distinct from the receive context so this
   * never contends with the blocking {@code receive()}.
   */
  boolean sendTextMessage(String body, String node, String priority) throws JMSException {
    Pair pair = connectionRef.get();
    if (pair == null) {
      return false;
    }
    synchronized (sendLock) {
      TextMessage msg = pair.senderContext().createTextMessage(body);
      msg.setStringProperty("node", node);
      msg.setStringProperty("prio", priority);
      pair.producer().send(topic, msg);
    }
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

  /**
   * Release the context the lifecycle owns and wait for the worker threads to exit. Idempotent.
   *
   * <p>The join matters: an in-flight {@link #dispatch(Message)} calls into the inbound handler,
   * which touches CDI-managed beans (the metrics collector among them). The caller is {@code
   * RatchetLifecycle#afterStop}, which lets the CDI context be destroyed once {@code close()}
   * returns. Returning while a receive thread is still inside the handler is what produces a {@code
   * WELD-000229} at container shutdown, so we drain the threads instead of only signalling them.
   * Joins are bounded so a wedged handler cannot stall shutdown.
   */
  void close() {
    if (!closed.compareAndSet(false, true)) {
      return;
    }
    closeContextRef();
    // Drain the reconnect thread first: an in-flight connectOnce may still be spawning a receive
    // thread, so settle it before reading and joining the receive thread itself.
    Thread reconnect = this.reconnectThread;
    if (reconnect != null) {
      reconnect.interrupt();
      joinQuietly(reconnect);
    }
    Thread receive = this.receiveThread;
    if (receive != null) {
      receive.interrupt();
      joinQuietly(receive);
    }
  }

  // ---- internals ----------------------------------------------------------

  private boolean connectOnce(boolean failureIsExpected) {
    if (closed.get()) {
      return false;
    }
    // Tracked so the finally can close any context created but not handed off to connectionRef.
    // Nulled the moment ownership transfers, so the finally is a no-op on the success paths.
    JMSContext consumerCtx = null;
    JMSContext senderCtx = null;
    try {
      consumerCtx = connectionFactory.createContext(JMSContext.AUTO_ACKNOWLEDGE);
      String selector =
          config.brokerSideSelfFilter()
              ? "node <> '" + escapeSelector(localIdentity.value()) + "'"
              : null;
      JMSConsumer consumer =
          selector == null
              ? consumerCtx.createConsumer(topic)
              : consumerCtx.createConsumer(topic, selector);
      // Dedicated sender context: sends run under sendLock on this context, never on the consumer
      // context that the receive thread blocks in. Created after the consumer so the receive side
      // is the first generation member published.
      senderCtx = connectionFactory.createContext(JMSContext.AUTO_ACKNOWLEDGE);
      JMSProducer p = senderCtx.createProducer();

      // Close-during-reconnect race: if close() flipped `closed` after our guard above but before
      // we got here, the freshly-built contexts would leak — close() has already drained
      // connectionRef and would not see them. The finally closes them on this early return.
      if (closed.get()) {
        return false;
      }
      Pair pair = new Pair(consumerCtx, consumer, senderCtx, p);
      Pair stale = connectionRef.getAndSet(pair);
      if (stale != null) {
        closeQuietly(stale.consumerContext());
        closeQuietly(stale.senderContext());
      }
      // close() may have flipped `closed` and drained the ref between the guard above and this
      // swap. If so, retract the pair we just published and tear it down (via the finally) rather
      // than start a receive thread that close() has already finished waiting for.
      if (closed.get()) {
        if (!connectionRef.compareAndSet(pair, null)) {
          // close() already took ownership of the pair and will close both contexts; the finally
          // must not double-close them.
          consumerCtx = null;
          senderCtx = null;
        }
        return false;
      }
      consecutiveFailures.set(0);
      startReceiveThread(pair);
      // Published: connectionRef and the receive thread now own both contexts.
      consumerCtx = null;
      senderCtx = null;
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
    } finally {
      // Close any context created but not handed off to connectionRef. No-op on the success and
      // close()-won-the-retract paths (locals nulled); on every throw or close-during-connect
      // return it stops a context (and its broker-side subscription) from leaking.
      closeQuietly(consumerCtx);
      closeQuietly(senderCtx);
    }
  }

  private void startReceiveThread(Pair pair) {
    Thread t = new Thread(() -> receiveLoop(pair), "ratchet-coordinator-jms-receive");
    t.setDaemon(true);
    this.receiveThread = t;
    t.start();
  }

  /**
   * Pulls inbound messages for {@code pair} until the lifecycle closes, the pair is swapped out by
   * a reconnect, or {@code receive()} reports a connection fault. A fault on the still-current pair
   * nulls the ref and kicks off reconnect — the synchronous-receive equivalent of the {@code
   * ExceptionListener} the EE spec forbids.
   */
  private void receiveLoop(Pair pair) {
    JMSConsumer consumer = pair.consumer();
    while (!closed.get() && connectionRef.get() == pair) {
      try {
        Message message = consumer.receive(RECEIVE_TIMEOUT_MS);
        if (message != null) {
          dispatch(message);
        }
      } catch (JMSRuntimeException fault) {
        // A context closed by close() or a reconnect swap also surfaces here; only a fault on the
        // pair that is still current is a real transport failure worth reconnecting.
        if (closed.get() || connectionRef.get() != pair) {
          return;
        }
        onTransportFailure.run();
        log.warnf("JMS coordinator receive failed, scheduling reconnect: %s", fault.getMessage());
        if (connectionRef.compareAndSet(pair, null)) {
          closeQuietly(pair.consumerContext());
          closeQuietly(pair.senderContext());
        }
        triggerReconnect();
        return;
      }
    }
  }

  /**
   * Hands one inbound message to the coordinator. Swallows every handler exception so a misbehaving
   * handler cannot kill the receive thread.
   */
  private void dispatch(Message message) {
    // A message can arrive in the window between close() flipping `closed` and the receive loop
    // re-checking it. Skip delivery once closed so the handler never touches a CDI context the
    // caller is about to destroy.
    if (closed.get()) {
      return;
    }
    try {
      inboundHandler.accept(message);
    } catch (RuntimeException ignored) {
      // The handler already recorded any transport_failure metric; swallow to keep receiving.
    }
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
      closeQuietly(pair.consumerContext());
      closeQuietly(pair.senderContext());
    }
  }

  /**
   * A coherent connection generation: a dedicated consumer context (touched only by the receive
   * thread) and a separate sender context (touched only under {@link #sendLock}). Keeping send and
   * receive on distinct contexts satisfies the single-thread-of-control rule without making sends
   * wait on the blocking {@code receive()}.
   */
  private record Pair(
      JMSContext consumerContext,
      JMSConsumer consumer,
      JMSContext senderContext,
      JMSProducer producer) {}

  private static void closeQuietly(JMSContext ctx) {
    if (ctx == null) {
      return;
    }
    try {
      ctx.close();
    } catch (Exception ignored) {
      // best-effort; the context may already be in a half-broken state
    }
  }

  private static void joinQuietly(Thread thread) {
    try {
      thread.join(SHUTDOWN_JOIN_TIMEOUT_MS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
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
}
