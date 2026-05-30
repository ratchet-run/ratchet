package run.ratchet.coordinator.jms;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import jakarta.jms.ConnectionFactory;
import jakarta.jms.ExceptionListener;
import jakarta.jms.JMSConsumer;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSProducer;
import jakarta.jms.JMSRuntimeException;
import jakarta.jms.Message;
import jakarta.jms.MessageListener;
import jakarta.jms.TextMessage;
import jakarta.jms.Topic;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.NodeIdentity;

class JmsConnectionLifecycleTest {

  private final List<JmsConnectionLifecycle> created = new ArrayList<>();

  private ConnectionFactory cf;
  private Topic topic;
  private JmsCoordinatorConfig config;

  private JMSContext ctx;
  private JMSProducer producer;
  private JMSConsumer consumer;

  @BeforeEach
  void setUp() {
    cf = mock(ConnectionFactory.class);
    topic = mock(Topic.class);
    config = newConfig(true);
    ctx = mock(JMSContext.class);
    producer = mock(JMSProducer.class);
    consumer = mock(JMSConsumer.class);
    when(cf.createContext(anyInt())).thenReturn(ctx);
    when(ctx.createProducer()).thenReturn(producer);
    when(ctx.createConsumer(any(Topic.class))).thenReturn(consumer);
    when(ctx.createConsumer(any(Topic.class), anyString())).thenReturn(consumer);
    // Default: a quiet topic. The receive loop blocks briefly and returns nothing, so it polls
    // without busy-spinning. Tests that need an inbound message or a fault override this.
    when(consumer.receive(anyLong())).thenAnswer(blockingQuietReceive());
  }

  @AfterEach
  void tearDown() {
    created.forEach(JmsConnectionLifecycle::close);
  }

  // ─── start() ─────────────────────────────────────────────────────────────────

  @Test
  void startCreatesContextWithAutoAcknowledge() {
    newLifecycle().start(identity("nodeA"));
    verify(cf).createContext(JMSContext.AUTO_ACKNOWLEDGE);
  }

  @Test
  void startDoesNotRegisterAsyncDelivery() {
    newLifecycle().start(identity("nodeA"));
    // Jakarta Messaging 3.0 §12.3 forbids setExceptionListener / setMessageListener on an
    // application-created context in a web or EJB container. Inbound delivery is synchronous.
    verify(ctx, never()).setExceptionListener(any(ExceptionListener.class));
    verify(consumer, never()).setMessageListener(any(MessageListener.class));
  }

  @Test
  void startCreatesProducerAndConsumer() {
    newLifecycle().start(identity("nodeA"));
    verify(ctx).createProducer();
    verify(ctx, atLeastOnce()).createConsumer(any(Topic.class), anyString());
  }

  @Test
  void startInstallsBrokerSideSelfFilterSelectorWhenEnabled() {
    newLifecycle().start(identity("nodeA"));
    verify(ctx)
        .createConsumer(any(Topic.class), org.mockito.ArgumentMatchers.eq("node <> 'nodeA'"));
  }

  @Test
  void startOmitsSelectorWhenBrokerSideSelfFilterDisabled() {
    config = newConfig(false);
    newLifecycle().start(identity("nodeA"));
    verify(ctx).createConsumer(any(Topic.class));
    verify(ctx, never()).createConsumer(any(Topic.class), anyString());
  }

  @Test
  void startBuildsSelectorFromNodeIdentity() {
    newLifecycle().start(identity("nodeA"));
    verify(ctx, atLeastOnce())
        .createConsumer(any(Topic.class), org.mockito.ArgumentMatchers.eq("node <> 'nodeA'"));
  }

  @Test
  void startWithFailingContextFactoryLeavesRefsNullAndDoesNotThrow() {
    when(cf.createContext(anyInt())).thenThrow(new JMSRuntimeException("boom"));
    AtomicInteger transportFailures = new AtomicInteger();
    JmsConnectionLifecycle lifecycle = newLifecycle(m -> {}, transportFailures::incrementAndGet);
    assertDoesNotThrow(() -> lifecycle.start(identity("nodeA")));
    assertNull(lifecycle.currentContext());
    assertNull(lifecycle.currentProducer());
  }

  @Test
  void startSetsContextAndProducerRefs() {
    JmsConnectionLifecycle lifecycle = newLifecycle();
    lifecycle.start(identity("nodeA"));
    assertNotNull(lifecycle.currentContext());
    assertNotNull(lifecycle.currentProducer());
  }

  // ─── receive-fault-driven reconnect ──────────────────────────────────────────

  @Test
  void receiveFaultNullsRefsAndCallsTransportFailure() {
    when(consumer.receive(anyLong())).thenThrow(new JMSRuntimeException("transport down"));
    AtomicInteger transportFailures = new AtomicInteger();
    JmsConnectionLifecycle lifecycle = newLifecycle(m -> {}, transportFailures::incrementAndGet);

    lifecycle.start(identity("nodeA"));

    await()
        .atMost(Duration.ofSeconds(2))
        .untilAsserted(
            () ->
                assertTrue(
                    transportFailures.get() >= 1,
                    "transport_failure must increment when receive() faults"));
  }

  @Test
  void receiveFaultClosesStaleContext() {
    when(consumer.receive(anyLong())).thenThrow(new JMSRuntimeException("transport down"));
    JmsConnectionLifecycle lifecycle = newLifecycle();
    lifecycle.start(identity("nodeA"));

    await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> verify(ctx, atLeastOnce()).close());
  }

  @Test
  void consecutiveFailuresResetOnSuccessfulConnect() {
    JmsConnectionLifecycle lifecycle = newLifecycle();
    lifecycle.start(identity("nodeA"));
    assertEquals(0L, lifecycle.consecutiveFailures(), "fresh successful connect resets to 0");
  }

  @Test
  void consecutiveFailuresIncrementOnFailedConnect() {
    when(cf.createContext(anyInt())).thenThrow(new JMSRuntimeException("down"));
    JmsConnectionLifecycle lifecycle = newLifecycle();
    lifecycle.start(identity("nodeA"));
    assertEquals(1L, lifecycle.consecutiveFailures(), "first failure increments");
  }

  @Test
  void reconnectRecoversAfterTransientReceiveFault() {
    JMSContext freshCtx = mock(JMSContext.class);
    JMSProducer freshProducer = mock(JMSProducer.class);
    JMSConsumer freshConsumer = mock(JMSConsumer.class);
    when(freshCtx.createProducer()).thenReturn(freshProducer);
    when(freshCtx.createConsumer(any(Topic.class), anyString())).thenReturn(freshConsumer);
    when(freshConsumer.receive(anyLong())).thenAnswer(blockingQuietReceive());
    // First context's receive() faults; the reconnect cycle then yields a fresh context.
    when(consumer.receive(anyLong())).thenThrow(new JMSRuntimeException("connection lost"));
    AtomicInteger calls = new AtomicInteger();
    when(cf.createContext(anyInt()))
        .thenAnswer(inv -> calls.incrementAndGet() == 1 ? ctx : freshCtx);

    JmsConnectionLifecycle lifecycle = newLifecycle();
    lifecycle.start(identity("nodeA"));

    await()
        .atMost(Duration.ofSeconds(2))
        .untilAsserted(
            () ->
                assertEquals(
                    freshCtx,
                    lifecycle.currentContext(),
                    "reconnect must publish a fresh context after a receive fault"));
    assertEquals(freshProducer, lifecycle.currentProducer());
  }

  // ─── close() ─────────────────────────────────────────────────────────────────

  @Test
  void closeReleasesContextOnce() {
    JmsConnectionLifecycle lifecycle = newLifecycle();
    lifecycle.start(identity("nodeA"));
    lifecycle.close();
    verify(ctx, times(1)).close();
  }

  @Test
  void closeIsIdempotent() {
    JmsConnectionLifecycle lifecycle = newLifecycle();
    lifecycle.start(identity("nodeA"));
    lifecycle.close();
    assertDoesNotThrow(lifecycle::close);
    verify(ctx, times(1)).close();
  }

  @Test
  void closeNullsAllRefs() {
    JmsConnectionLifecycle lifecycle = newLifecycle();
    lifecycle.start(identity("nodeA"));
    lifecycle.close();
    assertNull(lifecycle.currentContext());
    assertNull(lifecycle.currentProducer());
  }

  @Test
  void closeIsClosedReportsTrue() {
    JmsConnectionLifecycle lifecycle = newLifecycle();
    lifecycle.start(identity("nodeA"));
    assertFalse(lifecycle.isClosed());
    lifecycle.close();
    assertTrue(lifecycle.isClosed());
  }

  @Test
  void triggerReconnectIsNoOpAfterClose() {
    JmsConnectionLifecycle lifecycle = newLifecycle();
    lifecycle.start(identity("nodeA"));
    lifecycle.close();
    assertDoesNotThrow(lifecycle::triggerReconnect);
  }

  @Test
  void closeQuietlySwallowsCloseExceptions() {
    doAnswer(
            inv -> {
              throw new JMSRuntimeException("close blew up");
            })
        .when(ctx)
        .close();
    JmsConnectionLifecycle lifecycle = newLifecycle();
    lifecycle.start(identity("nodeA"));
    assertDoesNotThrow(lifecycle::close);
  }

  // ─── inbound dispatch ────────────────────────────────────────────────────────

  @Test
  void inboundHandlerIsInvokedForReceivedMessage() {
    TextMessage tm = mock(TextMessage.class);
    when(consumer.receive(anyLong())).thenReturn(tm).thenAnswer(blockingQuietReceive());
    AtomicReference<Message> received = new AtomicReference<>();
    JmsConnectionLifecycle lifecycle = newLifecycle(received::set, () -> {});

    lifecycle.start(identity("nodeA"));

    await()
        .atMost(Duration.ofSeconds(2))
        .untilAsserted(
            () -> assertEquals(tm, received.get(), "received message must reach handler"));
  }

  @Test
  void inboundHandlerExceptionsDoNotKillReceiveLoop() {
    TextMessage first = mock(TextMessage.class);
    TextMessage second = mock(TextMessage.class);
    when(consumer.receive(anyLong()))
        .thenReturn(first)
        .thenReturn(second)
        .thenAnswer(blockingQuietReceive());
    List<Message> seen = new java.util.concurrent.CopyOnWriteArrayList<>();
    JmsConnectionLifecycle lifecycle =
        newLifecycle(
            m -> {
              seen.add(m);
              throw new RuntimeException("handler blew up");
            },
            () -> {});

    lifecycle.start(identity("nodeA"));

    // A throwing handler on the first message must not stop the second from being delivered.
    await()
        .atMost(Duration.ofSeconds(2))
        .untilAsserted(
            () -> assertTrue(seen.contains(second), "receive loop must survive a throw"));
  }

  // ─── selector escaping helper ────────────────────────────────────────────────

  @Test
  void escapeSelectorDoublesSingleQuotes() {
    assertEquals("alice''s", JmsConnectionLifecycle.escapeSelector("alice's"));
    assertEquals("''", JmsConnectionLifecycle.escapeSelector("'"));
  }

  @Test
  void escapeSelectorDoublesBackslashes() {
    assertEquals("a\\\\b", JmsConnectionLifecycle.escapeSelector("a\\b"));
    assertEquals("\\\\\\\\", JmsConnectionLifecycle.escapeSelector("\\\\"));
  }

  @Test
  void escapeSelectorEscapesBackslashBeforeSingleQuote() {
    assertEquals("\\\\''", JmsConnectionLifecycle.escapeSelector("\\'"));
  }

  @Test
  void escapeSelectorLeavesUnquotedStringsAlone() {
    assertEquals("nodeA", JmsConnectionLifecycle.escapeSelector("nodeA"));
    assertEquals(
        "10.0.1.42:31415:abcd", JmsConnectionLifecycle.escapeSelector("10.0.1.42:31415:abcd"));
  }

  // ─── helpers ─────────────────────────────────────────────────────────────────

  private static org.mockito.stubbing.Answer<Message> blockingQuietReceive() {
    return inv -> {
      Thread.sleep(20);
      return null;
    };
  }

  private JmsConnectionLifecycle newLifecycle() {
    return newLifecycle(m -> {}, () -> {});
  }

  private JmsConnectionLifecycle newLifecycle(
      java.util.function.Consumer<Message> inboundHandler, Runnable onTransportFailure) {
    JmsConnectionLifecycle lifecycle =
        new JmsConnectionLifecycle(cf, topic, config, inboundHandler, onTransportFailure);
    created.add(lifecycle);
    return lifecycle;
  }

  private static JmsCoordinatorConfig newConfig(boolean brokerSideSelfFilter) {
    return new JmsCoordinatorConfig(
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        brokerSideSelfFilter,
        /* reconnectBackoffInitialMs= */ 25L,
        /* reconnectBackoffMaxMs= */ 100L,
        /* maxInboundPayloadChars= */ 16_384,
        /* listenerExecutorThreads= */ 2,
        /* shutdownGraceMs= */ 1_000L);
  }

  private static NodeIdentity identity(String value) {
    return new NodeIdentity(value);
  }
}
