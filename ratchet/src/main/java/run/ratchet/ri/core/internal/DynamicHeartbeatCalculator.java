package run.ratchet.ri.core.internal;

import java.time.Clock;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import org.jboss.logging.Logger;
import run.ratchet.store.spi.JobCrudStore;

/**
 * Adaptive heartbeat and polling interval calculator. Heartbeat interval = Base x Node-Factor x
 * Load-Factor, clamped to [base/4, base*2]. Polling delay scales inversely with queue depth.
 */
public class DynamicHeartbeatCalculator {

  private static final Logger log = Logger.getLogger(DynamicHeartbeatCalculator.class);

  private static final long CACHE_TTL_MS = 5000;

  private final JobCrudStore jobCrudStore;
  private final long baseHeartbeatIntervalSeconds;
  private final long pollerMinDelayMs;
  private final long pollerMaxDelayMs;
  private final Clock clock;
  private final LongSupplier ticker;
  private final Object cacheRefreshLock = new Object();

  private volatile long cachedNodes;
  private volatile long cachedPending;
  private volatile long cacheExpiresAtNanos = Long.MIN_VALUE;

  protected DynamicHeartbeatCalculator() {
    this.jobCrudStore = null;
    this.baseHeartbeatIntervalSeconds = 0;
    this.pollerMinDelayMs = 0;
    this.pollerMaxDelayMs = 0;
    this.clock = null;
    this.ticker = null;
  }

  public DynamicHeartbeatCalculator(
      JobCrudStore jobCrudStore,
      long baseHeartbeatIntervalSeconds,
      long pollerMinDelayMs,
      long pollerMaxDelayMs) {
    this(
        jobCrudStore,
        baseHeartbeatIntervalSeconds,
        pollerMinDelayMs,
        pollerMaxDelayMs,
        Clock.systemUTC());
  }

  public DynamicHeartbeatCalculator(
      JobCrudStore jobCrudStore,
      long baseHeartbeatIntervalSeconds,
      long pollerMinDelayMs,
      long pollerMaxDelayMs,
      Clock clock) {
    this(
        jobCrudStore,
        baseHeartbeatIntervalSeconds,
        pollerMinDelayMs,
        pollerMaxDelayMs,
        clock,
        null);
  }

  DynamicHeartbeatCalculator(
      JobCrudStore jobCrudStore,
      long baseHeartbeatIntervalSeconds,
      long pollerMinDelayMs,
      long pollerMaxDelayMs,
      Clock clock,
      LongSupplier ticker) {
    this.jobCrudStore = jobCrudStore;
    this.baseHeartbeatIntervalSeconds = baseHeartbeatIntervalSeconds;
    this.pollerMinDelayMs = pollerMinDelayMs;
    this.pollerMaxDelayMs = pollerMaxDelayMs;
    this.clock = clock;
    this.ticker = ticker;
  }

  /**
   * @return heartbeat interval in seconds, clamped to [base/4, base*2]
   */
  public long calculateHeartbeatInterval() {
    try {
      CacheSnapshot snapshot = refreshCacheIfStale();
      long activeNodes = snapshot.activeNodes();
      long pendingJobs = snapshot.pendingJobs();

      long adjustedInterval =
          calculateNodeBasedInterval(baseHeartbeatIntervalSeconds, (int) activeNodes);
      adjustedInterval = calculateLoadBasedInterval(adjustedInterval, (int) pendingJobs);

      long minInterval = Math.max(baseHeartbeatIntervalSeconds / 4, 5);
      long maxInterval = baseHeartbeatIntervalSeconds * 2;

      long finalInterval = Math.max(minInterval, Math.min(adjustedInterval, maxInterval));

      log.debugf(
          "Calculated heartbeat interval: nodes=%d, pendingJobs=%d, "
              + "baseInterval=%ds, finalInterval=%ds",
          activeNodes, pendingJobs, baseHeartbeatIntervalSeconds, finalInterval);

      return finalInterval;

    } catch (Exception e) {
      log.error("Heartbeat interval calculation error, using default", e);
      return baseHeartbeatIntervalSeconds;
    }
  }

  public long calculatePollerDelay() {
    try {
      CacheSnapshot snapshot = refreshCacheIfStale();
      long pendingJobs = snapshot.pendingJobs();

      if (pendingJobs == 0) {
        return pollerMaxDelayMs;
      } else if (pendingJobs <= 5) {
        return (pollerMinDelayMs + pollerMaxDelayMs) / 2;
      } else {
        return pollerMinDelayMs;
      }
    } catch (Exception e) {
      log.error("Poller delay calculation error, backing off to maximum", e);
      return pollerMaxDelayMs;
    }
  }

  private CacheSnapshot refreshCacheIfStale() {
    long now = effectiveTicker().getAsLong();
    synchronized (cacheRefreshLock) {
      if (now >= cacheExpiresAtNanos) {
        long refreshedAt = effectiveTicker().getAsLong();
        if (refreshedAt >= cacheExpiresAtNanos) {
          cachedNodes = jobCrudStore.countActiveNodes();
          cachedPending = jobCrudStore.countPendingJobs();
          cacheExpiresAtNanos = refreshedAt + TimeUnit.MILLISECONDS.toNanos(CACHE_TTL_MS);
        }
      }
      return new CacheSnapshot(cachedNodes, cachedPending);
    }
  }

  private long calculateLoadBasedInterval(long currentInterval, int pendingJobs) {
    if (pendingJobs == 0) {
      return (long) (currentInterval * 1.2);
    } else if (pendingJobs <= 10) {
      return currentInterval;
    } else if (pendingJobs <= 50) {
      return (long) (currentInterval * 0.9);
    } else if (pendingJobs <= 200) {
      return (long) (currentInterval * 0.7);
    } else {
      return (long) (currentInterval * 0.5);
    }
  }

  private long calculateNodeBasedInterval(long baseInterval, int nodeCount) {
    if (nodeCount <= 1) {
      return (long) (baseInterval * 1.5);
    } else if (nodeCount <= 3) {
      return baseInterval;
    } else if (nodeCount <= 6) {
      return (long) (baseInterval * 0.8);
    } else {
      return (long) (baseInterval * 0.6);
    }
  }

  private Clock effective() {
    return clock != null ? clock : Clock.systemUTC();
  }

  private LongSupplier effectiveTicker() {
    return ticker != null ? ticker : System::nanoTime;
  }

  private record CacheSnapshot(long activeNodes, long pendingJobs) {}
}
