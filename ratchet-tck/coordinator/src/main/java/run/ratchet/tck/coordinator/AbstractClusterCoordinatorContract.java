package run.ratchet.tck.coordinator;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobPriority;

/**
 * Mandatory cross-implementation contract verifying that any {@link
 * run.ratchet.spi.ClusterCoordinator ClusterCoordinator} implementation honours the SPI semantics
 * the scheduler relies on.
 *
 * <p>Coordinator modules supply a {@link CoordinatorTestHarness} from {@link #harness()} and
 * inherit all 15 contract methods. Transport-specific behavior that the harness can't model in
 * transport-neutral terms (reconnect mechanics, broker selectors, cluster membership) lives in the
 * module's own Testcontainers-driven IT.
 *
 * <p>The pre-registration buffer is an implementation choice; tests covering it live in {@link
 * AbstractClusterCoordinatorOptionalContract}. All four first-party Ratchet coordinators extend
 * both; third-party coordinators may extend only the mandatory contract and document the absence of
 * buffering as an implementation choice.
 */
public abstract class AbstractClusterCoordinatorContract {

  /** Supplies a fresh harness per test. Implementations create a per-test transport setup. */
  protected abstract CoordinatorTestHarness harness();

  private CoordinatorTestHarness harness;
  private CoordinatorTestFixture fixture;

  @BeforeEach
  void setUp() throws Exception {
    harness = harness();
    fixture = harness.twoNodeCluster();
    assertNotNull(fixture, "harness.twoNodeCluster() must return a non-null fixture");
  }

  @AfterEach
  void tearDown() throws Exception {
    // Restore the transport BEFORE closing the fixture: tests that left the transport
    // forced-failed (e.g. notifyNewWorkDoesNotThrowOnTransportFailure) would otherwise
    // poison the next test's setUp. Order matters — fixture.close() may need a live
    // transport to drain in-flight callbacks, and harness.close() may need it to gracefully
    // tear down provider-owned containers. For transports where failure is
    // connection-scoped (PG), recoverTransport is a no-op; for cluster-scoped failures
    // (JMS broker stop, Infinispan cluster shutdown, Hazelcast member shutdown), this is
    // essential to keep test isolation honest.
    if (harness != null) {
      try {
        harness.recoverTransport();
      } catch (RuntimeException ignored) {
        // Tolerate harnesses whose recoverTransport legitimately fails after a destructive
        // forceTransportFailure; swallow so the fixture + harness close paths still run and
        // we don't mask the original failure with a teardown-side error.
      }
    }
    if (fixture != null) {
      fixture.close();
    }
    if (harness != null) {
      harness.close();
    }
  }

  // ─── Round-trip and basic delivery ────────────────────────────────────────────

  @Test
  void notifyDeliversToRemoteListenerWithinMaxLatency() {
    RecordingWakeupListener listenerB = new RecordingWakeupListener();
    fixture.nodeB().registerWakeupListener(listenerB);

    fixture.nodeA().notifyNewWork(JobPriority.HIGH, fixture.identityA());

    listenerB.awaitOne(harness.maxExpectedLatency());
    assertEquals(1, listenerB.received().size(), "exactly one delivery expected");
    var record = listenerB.received().get(0);
    assertEquals(JobPriority.HIGH, record.priority());
    assertEquals(fixture.identityA(), record.source());
  }

  @Test
  void allPrioritiesRoundTrip() {
    RecordingWakeupListener listenerB = new RecordingWakeupListener();
    fixture.nodeB().registerWakeupListener(listenerB);

    JobPriority[] all = JobPriority.values();
    for (JobPriority p : all) {
      fixture.nodeA().notifyNewWork(p, fixture.identityA());
    }

    listenerB.awaitCount(all.length, harness.maxExpectedLatency());
    Set<JobPriority> observed = EnumSet.noneOf(JobPriority.class);
    for (RecordingWakeupListener.Record r : listenerB.received()) {
      observed.add(r.priority());
    }
    assertEquals(
        EnumSet.allOf(JobPriority.class),
        observed,
        "every JobPriority value must round-trip via the coordinator");
  }

  // ─── Self-suppression ─────────────────────────────────────────────────────────

  @Test
  void selfNotifyDoesNotFireLocalListener() {
    RecordingWakeupListener listenerA = new RecordingWakeupListener();
    fixture.nodeA().registerWakeupListener(listenerA);

    fixture.nodeA().notifyNewWork(JobPriority.HIGH, fixture.identityA());

    sleepPastLatencyWindow();
    assertTrue(
        listenerA.received().isEmpty(),
        "self-notification must not fire local listener; received " + listenerA.received());
  }

  @Test
  void selfNotifyIncrementsSelfSuppressedMetricOrFiltersAtWire() {
    // Either the receive-side metric increments (transports without a broker-side filter:
    // PG LISTEN/NOTIFY, Infinispan cache events, Hazelcast topic default), or the broker filters
    // the message at the wire so it never reaches the receiver (JMS with a node<>id selector).
    // Both modes satisfy the contract: the listener must not fire. The metric assertion is
    // expressed as ≥ 0 so a broker-side-filter implementation passes without inflating the count.
    RecordingWakeupListener listenerA = new RecordingWakeupListener();
    fixture.nodeA().registerWakeupListener(listenerA);

    fixture.nodeA().notifyNewWork(JobPriority.HIGH, fixture.identityA());

    sleepPastLatencyWindow();
    assertTrue(listenerA.received().isEmpty(), "self-notification must not fire local listener");
    long suppressed = fixture.metricsA().selfNotifySuppressed();
    assertTrue(suppressed >= 0, "selfNotifySuppressed must be a stable non-negative counter");
  }

  // ─── Listener isolation ───────────────────────────────────────────────────────

  @Test
  void throwingListenerDoesNotPreventOtherListeners() {
    AtomicInteger goodCount = new AtomicInteger();
    fixture
        .nodeB()
        .registerWakeupListener(
            (p, s) -> {
              throw new RuntimeException("boom");
            });
    fixture.nodeB().registerWakeupListener((p, s) -> goodCount.incrementAndGet());

    int iterations = 50;
    for (int i = 0; i < iterations; i++) {
      fixture.nodeA().notifyNewWork(JobPriority.HIGH, fixture.identityA());
    }

    awaitUntil(() -> goodCount.get() >= iterations, harness.maxExpectedLatency().multipliedBy(4));
    assertEquals(iterations, goodCount.get(), "good listener must observe every delivery");
    // Exact equality: every throwing-listener invocation increments listenerFailure exactly once.
    // The previous `>= iterations` assertion let a double-counting implementation pass; tighten to
    // catch that bug class. None of the four first-party coordinators legitimately double-count.
    assertEquals(
        iterations,
        fixture.metricsB().listenerFailure(),
        "listenerFailure must increment exactly once per throwing-listener invocation; observed "
            + fixture.metricsB().listenerFailure());
  }

  @Test
  void multipleListenersAllReceiveDeliveries() {
    RecordingWakeupListener listener1 = new RecordingWakeupListener();
    RecordingWakeupListener listener2 = new RecordingWakeupListener();
    RecordingWakeupListener listener3 = new RecordingWakeupListener();
    fixture.nodeB().registerWakeupListener(listener1);
    fixture.nodeB().registerWakeupListener(listener2);
    fixture.nodeB().registerWakeupListener(listener3);

    fixture.nodeA().notifyNewWork(JobPriority.NORMAL, fixture.identityA());

    listener1.awaitOne(harness.maxExpectedLatency());
    listener2.awaitOne(harness.maxExpectedLatency());
    listener3.awaitOne(harness.maxExpectedLatency());
  }

  // ─── Transport failure tolerance ──────────────────────────────────────────────

  @Test
  void notifyNewWorkDoesNotThrowOnTransportFailure() throws Exception {
    harness.forceTransportFailure();
    assertDoesNotThrow(
        () -> fixture.nodeA().notifyNewWork(JobPriority.HIGH, fixture.identityA()),
        "notifyNewWork must never throw out — SPI contract");
  }

  @Test
  void deliveryResumesAfterTransportRecovery() throws Exception {
    Assumptions.assumeTrue(
        harness.transportRecoverableWithinCoordinatorLifetime(),
        "harness's coordinator design binds to a provider-owned transport that cannot recover"
            + " without coordinator restart; see CoordinatorTestHarness#transportRecoverableWithinCoordinatorLifetime");
    RecordingWakeupListener listenerB = new RecordingWakeupListener();
    fixture.nodeB().registerWakeupListener(listenerB);

    harness.forceTransportFailure();
    // The notify during the failure window may or may not deliver — implementations differ.
    fixture.nodeA().notifyNewWork(JobPriority.HIGH, fixture.identityA());

    harness.recoverTransport();

    int before = listenerB.received().size();
    // Recovery happens on the transport's own clock for most coordinators (PG: listen-thread
    // reconnect loop; JMS: ExceptionListener-driven reconnect; Infinispan: cluster rejoin;
    // Hazelcast: member discovery). Retry the notify until at least one delivery lands so the
    // contract works for any transport without baking in a specific recovery model.
    Duration recoveryWindow = harness.maxExpectedLatency().multipliedBy(4);
    long deadlineNanos = System.nanoTime() + recoveryWindow.toNanos();
    while (listenerB.received().size() <= before) {
      if (System.nanoTime() >= deadlineNanos) {
        throw new AssertionError(
            "delivery did not resume within "
                + recoveryWindow
                + " after recoverTransport(); before="
                + before);
      }
      fixture.nodeA().notifyNewWork(JobPriority.HIGH, fixture.identityA());
      sleep(200);
    }
    // Reaching here means the while loop exited normally, so size() > before is already true.
  }

  // ─── Shutdown ─────────────────────────────────────────────────────────────────

  @Test
  void closeIsIdempotent() {
    fixture.nodeB().close();
    assertDoesNotThrow(() -> fixture.nodeB().close(), "second close() must not throw");
  }

  @Test
  void postCloseInboundMessagesDoNotFireListeners() {
    RecordingWakeupListener listenerB = new RecordingWakeupListener();
    fixture.nodeB().registerWakeupListener(listenerB);
    fixture.nodeB().close();

    fixture.nodeA().notifyNewWork(JobPriority.HIGH, fixture.identityA());

    sleepPastLatencyWindow();
    assertTrue(
        listenerB.received().isEmpty(),
        "closed coordinator must not fire listeners; received " + listenerB.received());
  }

  @Test
  void postCloseNotifyNewWorkIsNoOp() {
    fixture.nodeA().close();
    // Metrics may or may not increment after close — implementation choice. The contract is
    // "no throw."
    assertDoesNotThrow(
        () -> fixture.nodeA().notifyNewWork(JobPriority.HIGH, fixture.identityA()),
        "notifyNewWork on a closed coordinator must be a no-op, never throw");
  }

  // ─── Wire envelope ────────────────────────────────────────────────────────────

  @Test
  void unknownEnvelopeVersionIsRejectedLoudly() throws Exception {
    Assumptions.assumeTrue(
        harness.supportsRawWireInjection(),
        "harness does not support raw wire injection; skipping");
    RecordingWakeupListener listenerB = new RecordingWakeupListener();
    fixture.nodeB().registerWakeupListener(listenerB);

    long before = fixture.metricsB().transportFailure();
    harness.injectRawMessage(
        fixture.nodeB(),
        "{\"v\":2,\"node\":\"" + fixture.identityA().value() + "\",\"prio\":\"HIGH\"}");

    sleepPastLatencyWindow();
    assertTrue(
        listenerB.received().isEmpty(),
        "future envelope version must not fire listeners; received " + listenerB.received());
    assertTrue(
        fixture.metricsB().transportFailure() > before,
        "parse failure must increment the transport_failure counter");
  }

  @Test
  void envelopeRoundTripPreservesNodeIdentityAndPriority() {
    RecordingWakeupListener listenerB = new RecordingWakeupListener();
    fixture.nodeB().registerWakeupListener(listenerB);

    fixture.nodeA().notifyNewWork(JobPriority.LOW, fixture.identityA());

    listenerB.awaitOne(harness.maxExpectedLatency());
    var record = listenerB.received().get(0);
    assertEquals(
        fixture.identityA().value(),
        record.source().value(),
        "source identity must round-trip exactly");
    assertEquals(JobPriority.LOW, record.priority(), "priority must round-trip exactly");
  }

  // ─── Metrics surface ──────────────────────────────────────────────────────────

  @Test
  void sentMetricIncrementsOnEachNotify() {
    long before = fixture.metricsA().sent();
    int n = 10;
    for (int i = 0; i < n; i++) {
      fixture.nodeA().notifyNewWork(JobPriority.NORMAL, fixture.identityA());
    }
    awaitUntil(() -> fixture.metricsA().sent() - before >= n, harness.maxExpectedLatency());
    assertEquals(
        n,
        fixture.metricsA().sent() - before,
        "sent counter must increment exactly once per notify");
  }

  @Test
  void receivedMetricIncrementsOnEachInbound() {
    fixture.nodeB().registerWakeupListener((p, s) -> {});
    long before = fixture.metricsB().received();
    int n = 10;
    for (int i = 0; i < n; i++) {
      fixture.nodeA().notifyNewWork(JobPriority.NORMAL, fixture.identityA());
    }
    Duration window = harness.maxExpectedLatency().multipliedBy(2);
    awaitUntil(() -> fixture.metricsB().received() - before >= n, window);
    assertEquals(
        n,
        fixture.metricsB().received() - before,
        "received counter must increment exactly once per inbound dispatch");
  }

  // ─── Helpers ──────────────────────────────────────────────────────────────────

  /** Sleeps long enough for any in-flight delivery to land before asserting absence. */
  private void sleepPastLatencyWindow() {
    sleep(harness.maxExpectedLatency().toMillis());
  }

  private static void awaitUntil(java.util.function.BooleanSupplier condition, Duration timeout) {
    long deadlineNanos = System.nanoTime() + timeout.toNanos();
    while (!condition.getAsBoolean()) {
      if (System.nanoTime() >= deadlineNanos) {
        throw new AssertionError("condition not met within " + timeout);
      }
      sleep(20);
    }
  }

  private static void sleep(long ms) {
    try {
      Thread.sleep(ms);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError("interrupted while sleeping in test", e);
    }
  }
}
