package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import run.ratchet.store.spi.JobCrudStore;

@ExtendWith(MockitoExtension.class)
class DynamicHeartbeatCalculatorTest {

  private static final long BASE_HEARTBEAT_SECONDS = 30;
  private static final long POLLER_MIN_DELAY_MS = 500;
  private static final long POLLER_MAX_DELAY_MS = 10_000;
  private static final Instant FIXED_NOW = Instant.parse("2026-05-12T12:00:00Z");

  @Mock private JobCrudStore jobCrudStore;

  private MutableClock clock;
  private AtomicLong ticker;
  private DynamicHeartbeatCalculator calculator;

  @BeforeEach
  void setUp() {
    clock = new MutableClock(FIXED_NOW);
    ticker = new AtomicLong();
    calculator =
        new DynamicHeartbeatCalculator(
            jobCrudStore,
            BASE_HEARTBEAT_SECONDS,
            POLLER_MIN_DELAY_MS,
            POLLER_MAX_DELAY_MS,
            clock,
            ticker::get);
  }

  @Test
  void singleNode_zeroPending_increasesInterval() {
    when(jobCrudStore.countActiveNodes()).thenReturn(1L);
    when(jobCrudStore.countPendingJobs()).thenReturn(0L);

    long interval = calculator.calculateHeartbeatInterval();

    assertEquals(54, interval);
  }

  @Test
  void singleNode_highPending_decreasesInterval() {
    when(jobCrudStore.countActiveNodes()).thenReturn(1L);
    when(jobCrudStore.countPendingJobs()).thenReturn(300L);

    long interval = calculator.calculateHeartbeatInterval();

    assertEquals(22, interval);
  }

  @Test
  void manyNodes_adjustsDownward() {
    when(jobCrudStore.countActiveNodes()).thenReturn(8L);
    when(jobCrudStore.countPendingJobs()).thenReturn(10L);

    long interval = calculator.calculateHeartbeatInterval();

    assertEquals(18, interval);
  }

  @Test
  void mediumCluster_mediumLoad() {
    when(jobCrudStore.countActiveNodes()).thenReturn(5L);
    when(jobCrudStore.countPendingJobs()).thenReturn(100L);

    long interval = calculator.calculateHeartbeatInterval();

    assertEquals(16, interval);
  }

  @Test
  void loadThresholdBoundaries_useExpectedMultiplier() {
    assertEquals(45, heartbeatIntervalFor(1, 9));
    assertEquals(45, heartbeatIntervalFor(1, 10));
    assertEquals(40, heartbeatIntervalFor(1, 11));
    assertEquals(40, heartbeatIntervalFor(1, 49));
    assertEquals(40, heartbeatIntervalFor(1, 50));
    assertEquals(31, heartbeatIntervalFor(1, 51));
    assertEquals(31, heartbeatIntervalFor(1, 199));
    assertEquals(31, heartbeatIntervalFor(1, 200));
    assertEquals(22, heartbeatIntervalFor(1, 201));
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
  void cacheTTL_afterWindow_refreshesStoreCounts() {
    when(jobCrudStore.countActiveNodes()).thenReturn(1L, 8L);
    when(jobCrudStore.countPendingJobs()).thenReturn(0L, 300L);

    assertEquals(54, calculator.calculateHeartbeatInterval());
    ticker.addAndGet(TimeUnit.MILLISECONDS.toNanos(5001));

    assertEquals(9, calculator.calculateHeartbeatInterval());
    verify(jobCrudStore, times(2)).countActiveNodes();
    verify(jobCrudStore, times(2)).countPendingJobs();
  }

  @Test
  void storeException_heartbeat_returnsBaseInterval() {
    when(jobCrudStore.countActiveNodes()).thenThrow(new RuntimeException("DB down"));

    long interval = calculator.calculateHeartbeatInterval();

    assertEquals(BASE_HEARTBEAT_SECONDS, interval);
  }

  @Test
  void storeException_pollerDelay_returnsMaxDelay() {
    when(jobCrudStore.countActiveNodes()).thenThrow(new RuntimeException("DB down"));

    long delay = calculator.calculatePollerDelay();

    assertEquals(POLLER_MAX_DELAY_MS, delay);
  }

  @Test
  void cacheTTL_backwardWallClockStep_doesNotPinStaleCounts() {
    when(jobCrudStore.countActiveNodes()).thenReturn(1L, 8L);
    when(jobCrudStore.countPendingJobs()).thenReturn(0L, 300L);

    assertEquals(54, calculator.calculateHeartbeatInterval());

    clock.advance(Duration.ofSeconds(-30));
    ticker.addAndGet(TimeUnit.MILLISECONDS.toNanos(5001));

    assertEquals(9, calculator.calculateHeartbeatInterval());
    verify(jobCrudStore, times(2)).countActiveNodes();
    verify(jobCrudStore, times(2)).countPendingJobs();
  }

  private static long heartbeatIntervalFor(long activeNodes, long pendingJobs) {
    JobCrudStore store = mock(JobCrudStore.class);
    when(store.countActiveNodes()).thenReturn(activeNodes);
    when(store.countPendingJobs()).thenReturn(pendingJobs);
    return new DynamicHeartbeatCalculator(
            store,
            BASE_HEARTBEAT_SECONDS,
            POLLER_MIN_DELAY_MS,
            POLLER_MAX_DELAY_MS,
            Clock.fixed(FIXED_NOW, ZoneOffset.UTC))
        .calculateHeartbeatInterval();
  }

  private static final class MutableClock extends Clock {

    private Instant instant;

    private MutableClock(Instant instant) {
      this.instant = instant;
    }

    private void advance(Duration duration) {
      instant = instant.plus(duration);
    }

    @Override
    public ZoneId getZone() {
      return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(ZoneId zone) {
      return Clock.fixed(instant, zone);
    }

    @Override
    public Instant instant() {
      return instant;
    }
  }
}
