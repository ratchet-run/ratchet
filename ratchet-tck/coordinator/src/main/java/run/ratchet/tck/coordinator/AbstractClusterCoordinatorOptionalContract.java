package run.ratchet.tck.coordinator;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobPriority;

/**
 * Optional contract covering the pre-registration buffer — the defense-in-depth choice that holds
 * inbound wakeups until at least one listener has registered.
 *
 * <p>All four first-party Ratchet coordinators implement this. Third-party coordinators with
 * stricter lifecycle integration (those that can guarantee {@code registerWakeupListener} is always
 * called before any inbound message can arrive) are not forced to — they extend only {@link
 * AbstractClusterCoordinatorContract} and document the absence of buffering.
 *
 * <p>Tests assume the buffer capacity is exactly 256 entries with drop-oldest overflow — the shape
 * the first-party modules implement. A coordinator with a different capacity should override the
 * constants by extending this class and shadowing the relevant test.
 */
public abstract class AbstractClusterCoordinatorOptionalContract {

  /** Capacity expected for the pre-registration buffer. First-party modules use 256. */
  protected static final int EXPECTED_BUFFER_CAPACITY = 256;

  /**
   * Supplies a fresh harness per test. Implementations create a per-test transport setup.
   *
   * @return a freshly initialised {@link CoordinatorTestHarness}; MUST NOT return {@code null};
   *     each invocation MUST return a new instance (no caching across tests).
   * @apiNote The contract harness lifecycle is owned by the {@code setUp()} method on the contract
   *     base class: the returned harness is started in {@code setUp()} and closed in {@code
   *     tearDown()}. Implementations MUST NOT close, share, or reuse the harness across invocations
   *     — each {@code @Test} method gets its own. Implementations MAY throw a {@link
   *     RuntimeException} from this method to fail the test if the transport cannot be stood up;
   *     the contract treats that as a setup failure.
   */
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
    // Order matches AbstractClusterCoordinatorContract.tearDown — recoverTransport first,
    // then fixture, then harness; see that class for the rationale.
    if (harness != null) {
      try {
        harness.recoverTransport();
      } catch (RuntimeException ignored) {
        // see AbstractClusterCoordinatorContract.tearDown — same rationale for swallowing
      }
    }
    if (fixture != null) {
      fixture.close();
    }
    if (harness != null) {
      harness.close();
    }
  }

  @Test
  void messageBeforeListenerRegistrationDeliversToLateListener() {
    // Send before any listener on nodeB is registered. The receiver must buffer.
    fixture.nodeA().notifyNewWork(JobPriority.HIGH, fixture.identityA());

    sleep(harness.maxExpectedLatency().toMillis()); // ensure the message has reached the buffer

    RecordingWakeupListener listenerB = new RecordingWakeupListener();
    fixture.nodeB().registerWakeupListener(listenerB);

    listenerB.awaitOne(harness.maxExpectedLatency());
    assertTrue(
        listenerB.received().stream().anyMatch(r -> r.priority() == JobPriority.HIGH),
        "buffered HIGH wakeup must dispatch to a late-registered listener; received "
            + listenerB.received());
  }

  @Test
  void preRegistrationBufferOverflowDropsOldestAndIncrementsMetric() {
    int over = EXPECTED_BUFFER_CAPACITY + 50;
    for (int i = 0; i < over; i++) {
      fixture.nodeA().notifyNewWork(JobPriority.NORMAL, fixture.identityA());
    }

    sleep(harness.maxExpectedLatency().toMillis()); // ensure all messages buffered or dropped

    RecordingWakeupListener listenerB = new RecordingWakeupListener();
    fixture.nodeB().registerWakeupListener(listenerB);

    Duration drainWindow = harness.maxExpectedLatency().multipliedBy(2);
    listenerB.awaitAtLeast(1, drainWindow);
    // Wait a beat for the drain to finish so the size check sees the final count.
    sleep(harness.maxExpectedLatency().toMillis());

    int delivered = listenerB.received().size();
    assertTrue(
        delivered >= 1 && delivered <= EXPECTED_BUFFER_CAPACITY,
        "buffered drain count must respect buffer capacity; delivered=" + delivered);
    assertTrue(
        fixture.metricsB().preRegistrationOverflow() > 0,
        "preRegistrationOverflow counter must increment when more than capacity arrive");
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
