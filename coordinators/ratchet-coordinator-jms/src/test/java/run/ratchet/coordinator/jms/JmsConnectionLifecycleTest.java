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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.NodeIdentity;
import run.ratchet.coordinator.common.CoordinatorThreading;

class JmsConnectionLifecycleTest {

  private final List<JmsConnectionLifecycle> created = new ArrayList<>();

  private ConnectionFactory cf;
  private Topic topic;
  private JmsCoordinatorConfig config;

  private JMSContext ctx; // consumer (receive) context
  private JMSContext senderCtx; // dedicated sender context
  private JMSProducer producer;
  private JMSConsumer consumer;

  @BeforeEach
  void setUp() {
    cf = mock(ConnectionFactory.class);
    topic = mock(Topic.class);
    config = newConfig(true);
    ctx = mock(JMSContext.class);
    senderCtx = mock(JMSContext.class);
    producer = mock(JMSProducer.class);
    consumer = mock(JMSConsumer.class);
    // connectOnce creates the consumer context first, then the dedicated sender context. Alternate
    // so a reconnect generation also gets (consumer ctx, sender ctx). Tests that need distinct
    // reconnect contexts re-stub this.
    AtomicInteger contextCalls = new AtomicInteger();
    when(cf.createContext(anyInt()))
        .thenAnswer(inv -> contextCalls.getAndIncrement() % 2 == 0 ? ctx : senderCtx);
    when(ctx.createConsumer(any(Topic.class))).thenReturn(consumer);
    when(ctx.createConsumer(any(Topic.class), anyString())).thenReturn(consumer);
    when(senderCtx.createProducer()).thenReturn(producer);
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
    // Two contexts per connect: one for receiving, one dedicated to sending.
    verify(cf, times(2)).createContext(JMSContext.AUTO_ACKNOWLEDGE);
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
  void publishUsesSynchronousSendNotAsyncCompletionListener() throws Exception {
    TextMessage outbound = mock(TextMessage.class);
    when(senderCtx.createTextMessage(anyString())).thenReturn(outbound);
    JmsConnectionLifecycle lifecycle = newLifecycle();
    lifecycle.start(identity("nodeA"));

    assertTrue(lifecycle.sendTextMessage("body", "nodeA", "NORMAL"));

    // §12.3 forbids asynchronous send (a CompletionListener) from a container-created producer just
    // as it forbids async receive. Publish must take the blocking send overload and never enable
    // async delivery on the producer.
    verify(producer).send(any(Topic.class), any(Message.class));
    verify(producer, never()).setAsync(any());
  }

  @Test
  void concurrentSendsAreSerializedOnTheSenderContext() throws Exception {
    when(senderCtx.createTextMessage(anyString())).thenReturn(mock(TextMessage.class));
    AtomicInteger inFlight = new AtomicInteger();
    AtomicInteger maxObserved = new AtomicInteger();
    // Record the peak number of threads simultaneously inside the producer send. With the per-send
    // lock and a dedicated sender context, only one thread may touch the context at a time, so the
    // peak must be 1; an unsynchronized send would overlap and push it above 1.
    doAnswer(
            inv -> {
              int now = inFlight.incrementAndGet();
              maxObserved.accumulateAndGet(now, Math::max);
              Thread.sleep(10); // widen the window so an unsynchronized send would overlap
              inFlight.decrementAndGet();
              return null;
            })
        .when(producer)
        .send(any(Topic.class), any(Message.class));

    JmsConnectionLifecycle lifecycle = newLifecycle();
    lifecycle.start(identity("nodeA"));

    int threads = 8;
    CountDownLatch ready = new CountDownLatch(threads);
    CountDownLatch go = new CountDownLatch(1);
    AtomicInteger successes = new AtomicInteger();
    List<Thread> workers = new ArrayList<>();
    for (int i = 0; i < threads; i++) {
      Thread t =
          new Thread(
              () -> {
                ready.countDown();
                try {
                  go.await();
                  if (lifecycle.sendTextMessage("body", "nodeA", "NORMAL")) {
                    successes.incrementAndGet();
                  }
                } catch (Exception ignored) {
                  // counted as a non-success
                }
              });
      t.start();
      workers.add(t);
    }
    assertTrue(ready.await(2, TimeUnit.SECONDS), "all senders must be ready");
    go.countDown();
    for (Thread t : workers) {
      t.join(Duration.ofSeconds(5).toMillis());
    }

    assertEquals(threads, successes.get(), "every concurrent send must succeed");
    assertEquals(1, maxObserved.get(), "concurrent sends must not overlap on the sender context");
  }

  @Test
  void connectClosesConsumerContextWhenConsumerCreationFails() {
    // If createConsumer throws mid-connect, the already-created consumer context must be closed
    // rather than leaked — it may hold a broker-side subscription. Pin a single context so every
    // retry consistently reaches the throwing createConsumer.
    when(cf.createContext(anyInt())).thenReturn(ctx);
    when(ctx.createConsumer(any(Topic.class), anyString()))
        .thenThrow(new JMSRuntimeException("consumer create failed"));
    JmsConnectionLifecycle lifecycle = newLifecycle();

    assertDoesNotThrow(() -> lifecycle.start(identity("nodeA")));
    lifecycle.close(); // stop the async reconnect retries before verifying

    verify(ctx, atLeastOnce()).close();
    assertNull(lifecycle.currentContext());
  }

  @Test
  void startCreatesProducerAndConsumer() {
    newLifecycle().start(identity("nodeA"));
    verify(senderCtx).createProducer();
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
    JMSContext freshConsumerCtx = mock(JMSContext.class);
    JMSContext freshSenderCtx = mock(JMSContext.class);
    JMSProducer freshProducer = mock(JMSProducer.class);
    JMSConsumer freshConsumer = mock(JMSConsumer.class);
    when(freshConsumerCtx.createConsumer(any(Topic.class), anyString())).thenReturn(freshConsumer);
    when(freshSenderCtx.createProducer()).thenReturn(freshProducer);
    when(freshConsumer.receive(anyLong())).thenAnswer(blockingQuietReceive());
    // First generation's receive() faults; the reconnect cycle then yields a fresh generation.
    when(consumer.receive(anyLong())).thenThrow(new JMSRuntimeException("connection lost"));
    // Each connect creates two contexts (consumer then sender): connect 1 -> ctx, senderCtx;
    // reconnect -> freshConsumerCtx, freshSenderCtx.
    when(cf.createContext(anyInt())).thenReturn(ctx, senderCtx, freshConsumerCtx, freshSenderCtx);

    JmsConnectionLifecycle lifecycle = newLifecycle();
    lifecycle.start(identity("nodeA"));

    await()
        .atMost(Duration.ofSeconds(2))
        .untilAsserted(
            () ->
                assertEquals(
                    freshConsumerCtx,
                    lifecycle.currentContext(),
                    "reconnect must publish a fresh consumer context after a receive fault"));
    assertEquals(freshProducer, lifecycle.currentProducer());
  }

  // ─── close() ─────────────────────────────────────────────────────────────────

  @Test
  void closeReleasesContextOnce() {
    JmsConnectionLifecycle lifecycle = newLifecycle();
    lifecycle.start(identity("nodeA"));
    lifecycle.close();
    verify(ctx, times(1)).close();
    verify(senderCtx, times(1)).close();
  }

  @Test
  void closeIsIdempotent() {
    JmsConnectionLifecycle lifecycle = newLifecycle();
    lifecycle.start(identity("nodeA"));
    lifecycle.close();
    assertDoesNotThrow(lifecycle::close);
    verify(ctx, times(1)).close();
    verify(senderCtx, times(1)).close();
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

  @Test
  void closeWaitsForInFlightDispatchToFinish() throws InterruptedException {
    TextMessage tm = mock(TextMessage.class);
    when(consumer.receive(anyLong())).thenReturn(tm).thenAnswer(blockingQuietReceive());

    CountDownLatch handlerEntered = new CountDownLatch(1);
    CountDownLatch releaseHandler = new CountDownLatch(1);
    AtomicBoolean handlerFinished = new AtomicBoolean(false);
    JmsConnectionLifecycle lifecycle =
        newLifecycle(
            m -> {
              handlerEntered.countDown();
              // Wait through interruption: close() interrupts the receive thread, but a real
              // handler doing synchronous work (e.g. a metrics call) would not unwind on it. This
              // models that so the test exercises close()'s join, not the interrupt.
              boolean released = false;
              while (!released) {
                try {
                  released = releaseHandler.await(2, TimeUnit.SECONDS);
                } catch (InterruptedException ignored) {
                  // keep waiting
                }
              }
              handlerFinished.set(true);
            },
            () -> {});

    lifecycle.start(identity("nodeA"));
    assertTrue(handlerEntered.await(2, TimeUnit.SECONDS), "handler must be entered before close()");

    AtomicBoolean closeReturned = new AtomicBoolean(false);
    Thread closer =
        new Thread(
            () -> {
              lifecycle.close();
              closeReturned.set(true);
            });
    closer.start();

    // close() must block in join() while the dispatch is still inside the handler.
    Thread.sleep(150);
    assertFalse(closeReturned.get(), "close() must not return while a dispatch is in flight");

    releaseHandler.countDown();
    closer.join(Duration.ofSeconds(3).toMillis());
    assertTrue(closeReturned.get(), "close() must return once the in-flight dispatch completes");
    assertTrue(handlerFinished.get(), "the in-flight handler must finish before close() returns");
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
        new JmsConnectionLifecycle(
            cf,
            topic,
            config,
            inboundHandler,
            onTransportFailure,
            CoordinatorThreading.standalone("ratchet-coordinator-jms-test"));
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
        /* listenerExecutorQueueCapacity= */ 1_024,
        /* shutdownGraceMs= */ 1_000L);
  }

  private static NodeIdentity identity(String value) {
    return new NodeIdentity(value);
  }
}
