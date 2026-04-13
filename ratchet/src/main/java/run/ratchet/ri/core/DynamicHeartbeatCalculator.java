package run.ratchet.ri.core;

import run.ratchet.store.spi.JobCrudStore;
import org.jboss.logging.Logger;

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
  private final Object cacheRefreshLock = new Object();

  private volatile long cachedNodes;
  private volatile long cachedPending;
  private volatile long cacheTimestamp;

  protected DynamicHeartbeatCalculator() {
    this.jobCrudStore = null;
    this.baseHeartbeatIntervalSeconds = 0;
    this.pollerMinDelayMs = 0;
    this.pollerMaxDelayMs = 0;
  }

  public DynamicHeartbeatCalculator(
      JobCrudStore jobCrudStore,
      long baseHeartbeatIntervalSeconds,
      long pollerMinDelayMs,
      long pollerMaxDelayMs) {
    this.jobCrudStore = jobCrudStore;
    this.baseHeartbeatIntervalSeconds = baseHeartbeatIntervalSeconds;
    this.pollerMinDelayMs = pollerMinDelayMs;
    this.pollerMaxDelayMs = pollerMaxDelayMs;
  }

  /**
   * @return heartbeat interval in seconds, clamped to [base/4, base*2]
   */
  public long calculateHeartbeatInterval() {
    try {
      refreshCacheIfStale();
      long activeNodes = cachedNodes;
      long pendingJobs = cachedPending;

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
      log.error("Failed to calculate heartbeat interval, using default", e);
      return baseHeartbeatIntervalSeconds;
    }
  }

  public long calculatePollerDelay() {
    try {
      refreshCacheIfStale();
      long pendingJobs = cachedPending;

      if (pendingJobs == 0) {
        return pollerMaxDelayMs;
      } else if (pendingJobs <= 5) {
        return (pollerMinDelayMs + pollerMaxDelayMs) / 2;
      } else {
        return pollerMinDelayMs;
      }
    } catch (Exception e) {
      log.error("Failed to calculate poller delay, using minimum", e);
      return pollerMinDelayMs;
    }
  }

  private void refreshCacheIfStale() {
    long now = System.currentTimeMillis();
    if (now - cacheTimestamp <= CACHE_TTL_MS) {
      return;
    }

    synchronized (cacheRefreshLock) {
      long refreshedAt = System.currentTimeMillis();
      if (refreshedAt - cacheTimestamp <= CACHE_TTL_MS) {
        return;
      }

      cachedNodes = jobCrudStore.countActiveNodes();
      cachedPending = jobCrudStore.countPendingJobs();
      cacheTimestamp = refreshedAt;
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
}
