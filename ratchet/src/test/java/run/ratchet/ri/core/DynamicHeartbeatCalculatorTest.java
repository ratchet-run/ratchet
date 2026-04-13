package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import run.ratchet.store.spi.JobCrudStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DynamicHeartbeatCalculatorTest {

  private static final long BASE_HEARTBEAT_SECONDS = 30;
  private static final long POLLER_MIN_DELAY_MS = 500;
  private static final long POLLER_MAX_DELAY_MS = 10_000;

  @Mock private JobCrudStore jobCrudStore;

  private DynamicHeartbeatCalculator calculator;

  @BeforeEach
  void setUp() {
    calculator =
        new DynamicHeartbeatCalculator(
            jobCrudStore, BASE_HEARTBEAT_SECONDS, POLLER_MIN_DELAY_MS, POLLER_MAX_DELAY_MS);
  }

  @Test
  void singleNode_zeroPending_increasesInterval() {
    when(jobCrudStore.countActiveNodes()).thenReturn(1L);
    when(jobCrudStore.countPendingJobs()).thenReturn(0L);

    long interval = calculator.calculateHeartbeatInterval();

    // singleNode: base * 1.5 = 45, zeroPending: * 1.2 = 54
    // max = base * 2 = 60, min = max(base/4, 5) = max(7, 5) = 7
    // bounded: min(54, 60) = 54, max(7, 54) = 54
    assertEquals(54, interval);
  }

  @Test
  void singleNode_highPending_decreasesInterval() {
    when(jobCrudStore.countActiveNodes()).thenReturn(1L);
    when(jobCrudStore.countPendingJobs()).thenReturn(300L);

    long interval = calculator.calculateHeartbeatInterval();

    // singleNode: base * 1.5 = 45, highPending (>200): * 0.5 = 22
    // bounded: max(7, min(22, 60)) = 22
    assertEquals(22, interval);
  }

  @Test
  void manyNodes_adjustsDownward() {
    when(jobCrudStore.countActiveNodes()).thenReturn(8L);
    when(jobCrudStore.countPendingJobs()).thenReturn(10L);

    long interval = calculator.calculateHeartbeatInterval();

    // 8 nodes (>6): base * 0.6 = 18, pending 10 (<=10): * 1.0 = 18
    // bounded: max(7, min(18, 60)) = 18
    assertEquals(18, interval);
  }

  @Test
  void mediumCluster_mediumLoad() {
    when(jobCrudStore.countActiveNodes()).thenReturn(5L);
    when(jobCrudStore.countPendingJobs()).thenReturn(100L);

    long interval = calculator.calculateHeartbeatInterval();

    // 5 nodes (4-6): base * 0.8 = 24, 100 pending (51-200): * 0.7 = 16
    // bounded: max(7, min(16, 60)) = 16
    assertEquals(16, interval);
  }

  @Test
  void interval_neverBelowMinimum() {
    when(jobCrudStore.countActiveNodes()).thenReturn(10L);
    when(jobCrudStore.countPendingJobs()).thenReturn(1000L);

    long interval = calculator.calculateHeartbeatInterval();

    long minInterval = Math.max(BASE_HEARTBEAT_SECONDS / 4, 5);
    assertTrue(interval >= minInterval, "Interval " + interval + " must be >= " + minInterval);
  }

  @Test
  void interval_neverAboveMaximum() {
    when(jobCrudStore.countActiveNodes()).thenReturn(1L);
    when(jobCrudStore.countPendingJobs()).thenReturn(0L);

    long interval = calculator.calculateHeartbeatInterval();

    long maxInterval = BASE_HEARTBEAT_SECONDS * 2;
    assertTrue(interval <= maxInterval, "Interval " + interval + " must be <= " + maxInterval);
  }

  @Test
  void pollerDelay_zeroPending_returnsMaxDelay() {
    when(jobCrudStore.countActiveNodes()).thenReturn(1L);
    when(jobCrudStore.countPendingJobs()).thenReturn(0L);

    assertEquals(POLLER_MAX_DELAY_MS, calculator.calculatePollerDelay());
  }

  @Test
  void pollerDelay_fewPending_returnsMidpoint() {
    when(jobCrudStore.countActiveNodes()).thenReturn(1L);
    when(jobCrudStore.countPendingJobs()).thenReturn(3L);

    long expected = (POLLER_MIN_DELAY_MS + POLLER_MAX_DELAY_MS) / 2;
    assertEquals(expected, calculator.calculatePollerDelay());
  }

  @Test
  void pollerDelay_manyPending_returnsMinDelay() {
    when(jobCrudStore.countActiveNodes()).thenReturn(1L);
    when(jobCrudStore.countPendingJobs()).thenReturn(50L);

    assertEquals(POLLER_MIN_DELAY_MS, calculator.calculatePollerDelay());
  }

  @Test
  void cacheTTL_secondCallWithinWindow_doesNotQueryStoreAgain() {
    when(jobCrudStore.countActiveNodes()).thenReturn(1L);
    when(jobCrudStore.countPendingJobs()).thenReturn(0L);

    calculator.calculateHeartbeatInterval();
    calculator.calculateHeartbeatInterval();
    calculator.calculatePollerDelay();

    // All three calls should use the cache from the first refresh
    verify(jobCrudStore, times(1)).countActiveNodes();
    verify(jobCrudStore, times(1)).countPendingJobs();
  }

  @Test
  void storeException_heartbeat_returnsBaseInterval() {
    when(jobCrudStore.countActiveNodes()).thenThrow(new RuntimeException("DB down"));

    long interval = calculator.calculateHeartbeatInterval();

    assertEquals(BASE_HEARTBEAT_SECONDS, interval);
  }

  @Test
  void storeException_pollerDelay_returnsMinDelay() {
    when(jobCrudStore.countActiveNodes()).thenThrow(new RuntimeException("DB down"));

    long delay = calculator.calculatePollerDelay();

    assertEquals(POLLER_MIN_DELAY_MS, delay);
  }
}
