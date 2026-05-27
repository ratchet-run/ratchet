package run.ratchet.coordinator.jms;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
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
import jakarta.jms.JMSException;
import jakarta.jms.JMSProducer;
import jakarta.jms.JMSRuntimeException;
import jakarta.jms.Message;
import jakarta.jms.MessageListener;
import jakarta.jms.TextMessage;
import jakarta.jms.Topic;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import run.ratchet.api.NodeIdentity;

class JmsConnectionLifecycleTest {

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
  }

  // ─── start() ─────────────────────────────────────────────────────────────────

  @Test
  void startCreatesContextWithAutoAcknowledge() {
    newLifecycle().start(identity("nodeA"));
    verify(cf).createContext(JMSContext.AUTO_ACKNOWLEDGE);
  }

  @Test
  void startInstallsExceptionListener() {
    newLifecycle().start(identity("nodeA"));
    verify(ctx).setExceptionListener(any(ExceptionListener.class));
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
    ArgumentCaptor<String> selector = ArgumentCaptor.forClass(String.class);
    verify(ctx).createConsumer(any(Topic.class), selector.capture());
    assertEquals("node <> 'nodeA'", selector.getValue());
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
    // NodeIdentity rejects single-quote / backslash at construction, so escapeSelector's escape
    // logic is exercised in isolation (see escapeSelectorDoublesSingleQuotes etc); this test
    // pins the rendered selector format for an unremarkable identity.
    newLifecycle().start(identity("nodeA"));
    ArgumentCaptor<String> selector = ArgumentCaptor.forClass(String.class);
    verify(ctx, atLeastOnce()).createConsumer(any(Topic.class), selector.capture());
    assertEquals("node <> 'nodeA'", selector.getValue());
  }

  @Test
  void startWithFailingContextFactoryLeavesRefsNullAndDoesNotThrow() {
    when(cf.createContext(anyInt())).thenThrow(new JMSRuntimeException("boom"));
    AtomicInteger transportFailures = new AtomicInteger();
    JmsConnectionLifecycle lifecycle =
        new JmsConnectionLifecycle(cf, topic, config, m -> {}, transportFailures::incrementAndGet);
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

  // ─── exception-listener-driven reconnect ─────────────────────────────────────

  @Test
  void exceptionListenerFiringNullsRefsAndCallsTransportFailure() throws Exception {
    AtomicInteger transportFailures = new AtomicInteger();
    JmsConnectionLifecycle lifecycle =
        new JmsConnectionLifecycle(cf, topic, config, m -> {}, transportFailures::incrementAndGet);
    ArgumentCaptor<ExceptionListener> listenerCaptor =
        ArgumentCaptor.forClass(ExceptionListener.class);
    lifecycle.start(identity("nodeA"));
    verify(ctx).setExceptionListener(listenerCaptor.capture());

    // Fire the listener with a JMSException.
    listenerCaptor.getValue().onException(new JMSException("transport down"));

    // Allow reconnect thread to attempt at least once before we assert.
    Thread.sleep(150);
    assertTrue(transportFailures.get() >= 1, "transport_failure must increment on disconnect");
  }

  @Test
  void exceptionListenerClosesStaleContext() {
    JmsConnectionLifecycle lifecycle = newLifecycle();
    ArgumentCaptor<ExceptionListener> listenerCaptor =
        ArgumentCaptor.forClass(ExceptionListener.class);
    lifecycle.start(identity("nodeA"));
    verify(ctx).setExceptionListener(listenerCaptor.capture());

    listenerCaptor.getValue().onException(new JMSException("transport down"));
    verify(ctx, atLeastOnce()).close();
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
  void reconnectRecoversAfterTransientFailure() throws Exception {
    JMSContext freshCtx = mock(JMSContext.class);
    JMSProducer freshProducer = mock(JMSProducer.class);
    JMSConsumer freshConsumer = mock(JMSConsumer.class);
    when(freshCtx.createProducer()).thenReturn(freshProducer);
    when(freshCtx.createConsumer(any(Topic.class), anyString())).thenReturn(freshConsumer);
    AtomicInteger calls = new AtomicInteger();
    when(cf.createContext(anyInt()))
        .thenAnswer(
            inv -> {
              int n = calls.incrementAndGet();
              if (n == 1) {
                return ctx;
              }
              if (n == 2) {
                throw new JMSRuntimeException("still down");
              }
              return freshCtx;
            });

    JmsConnectionLifecycle lifecycle = newLifecycle();
    ArgumentCaptor<ExceptionListener> listenerCaptor =
        ArgumentCaptor.forClass(ExceptionListener.class);
    lifecycle.start(identity("nodeA"));
    verify(ctx).setExceptionListener(listenerCaptor.capture());

    listenerCaptor.getValue().onException(new JMSException("connection lost"));

    long deadline = System.currentTimeMillis() + 2_000L;
    while (lifecycle.currentContext() != freshCtx && System.currentTimeMillis() < deadline) {
      Thread.sleep(20);
    }
    assertEquals(freshCtx, lifecycle.currentContext(), "reconnect must publish fresh context");
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
    // Second close must not call ctx.close again.
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
  void exceptionListenerAfterCloseIsNoOp() throws Exception {
    AtomicInteger transportFailures = new AtomicInteger();
    JmsConnectionLifecycle lifecycle =
        new JmsConnectionLifecycle(cf, topic, config, m -> {}, transportFailures::incrementAndGet);
    ArgumentCaptor<ExceptionListener> listenerCaptor =
        ArgumentCaptor.forClass(ExceptionListener.class);
    lifecycle.start(identity("nodeA"));
    verify(ctx).setExceptionListener(listenerCaptor.capture());

    lifecycle.close();
    listenerCaptor.getValue().onException(new JMSException("post-close fault"));

    // No reconnect attempt, no new transport-failure tick.
    Thread.sleep(50);
    assertEquals(0, transportFailures.get());
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
  void inboundHandlerIsInvokedWhenMessageListenerFires() throws Exception {
    AtomicReference<Message> received = new AtomicReference<>();
    JmsConnectionLifecycle lifecycle =
        new JmsConnectionLifecycle(cf, topic, config, received::set, () -> {});
    ArgumentCaptor<MessageListener> listenerCaptor = ArgumentCaptor.forClass(MessageListener.class);
    lifecycle.start(identity("nodeA"));
    verify(consumer).setMessageListener(listenerCaptor.capture());

    TextMessage tm = mock(TextMessage.class);
    listenerCaptor.getValue().onMessage(tm);

    assertEquals(tm, received.get());
  }

  @Test
  void inboundHandlerExceptionsAreSwallowed() throws Exception {
    JmsConnectionLifecycle lifecycle =
        new JmsConnectionLifecycle(
            cf,
            topic,
            config,
            m -> {
              throw new RuntimeException("handler blew up");
            },
            () -> {});
    ArgumentCaptor<MessageListener> listenerCaptor = ArgumentCaptor.forClass(MessageListener.class);
    lifecycle.start(identity("nodeA"));
    verify(consumer).setMessageListener(listenerCaptor.capture());

    assertDoesNotThrow(() -> listenerCaptor.getValue().onMessage(mock(TextMessage.class)));
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
    // Backslash must double first; otherwise a value of \' would become \''  → broken literal.
    assertEquals("\\\\''", JmsConnectionLifecycle.escapeSelector("\\'"));
  }

  @Test
  void escapeSelectorLeavesUnquotedStringsAlone() {
    assertEquals("nodeA", JmsConnectionLifecycle.escapeSelector("nodeA"));
    assertEquals(
        "10.0.1.42:31415:abcd", JmsConnectionLifecycle.escapeSelector("10.0.1.42:31415:abcd"));
  }

  // ─── helpers ─────────────────────────────────────────────────────────────────

  private JmsConnectionLifecycle newLifecycle() {
    return new JmsConnectionLifecycle(cf, topic, config, m -> {}, () -> {});
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
