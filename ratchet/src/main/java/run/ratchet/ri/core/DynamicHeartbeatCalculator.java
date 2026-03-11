package run.ratchet.ri.core;

import run.ratchet.store.spi.JobCrudStore;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Provides adaptive heartbeat and polling interval calculations for the job scheduler cluster. This
 * component dynamically adjusts timing parameters based on real-time cluster conditions to optimize
 * performance, responsiveness, and resource utilization.
 *
 * <p>The calculator implements two key algorithms:
 *
 * <p><b>1. Heartbeat Interval Calculation:</b>
 *
 * <ul>
 *   <li>Monitors cluster size to adjust coordination frequency
 *   <li>Responds to job queue depth for load-based optimization
 *   <li>Ensures bounded intervals within configured min/max thresholds
 *   <li>Formula: Base Interval x Node Factor x Load Factor
 * </ul>
 *
 * <p><b>2. Polling Delay Calculation:</b>
 *
 * <ul>
 *   <li>Adjusts how frequently workers check for new jobs
 *   <li>Minimizes delay under high load for responsiveness
 *   <li>Maximizes delay when idle to reduce database load
 * </ul>
 *
 * @see Poller for polling delay usage
 */
public class DynamicHeartbeatCalculator {

  private static final Logger log = Logger.getLogger(DynamicHeartbeatCalculator.class.getName());

  private final JobCrudStore jobCrudStore;
  private final long baseHeartbeatIntervalSeconds;
  private final long pollerMinDelayMs;
  private final long pollerMaxDelayMs;

  // Required by CDI proxy
  protected DynamicHeartbeatCalculator() {
    this.jobCrudStore = null;
    this.baseHeartbeatIntervalSeconds = 0;
    this.pollerMinDelayMs = 0;
    this.pollerMaxDelayMs = 0;
  }

  /**
   * Creates a new DynamicHeartbeatCalculator.
   *
   * @param jobCrudStore store for querying cluster state metrics
   * @param baseHeartbeatIntervalSeconds the base heartbeat interval in seconds
   * @param pollerMinDelayMs minimum polling delay in milliseconds
   * @param pollerMaxDelayMs maximum polling delay in milliseconds
   */
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
   * Calculates the optimal heartbeat interval based on current cluster conditions.
   *
   * @return heartbeat interval in seconds, bounded between min and max thresholds
   */
  public long calculateHeartbeatInterval() {
    try {
      long activeNodes = jobCrudStore.countActiveNodes();
      long pendingJobs = jobCrudStore.countPendingJobs();

      long adjustedInterval =
          calculateNodeBasedInterval(baseHeartbeatIntervalSeconds, (int) activeNodes);
      adjustedInterval = calculateLoadBasedInterval(adjustedInterval, (int) pendingJobs);

      long minInterval = Math.max(baseHeartbeatIntervalSeconds / 4, 5);
      long maxInterval = baseHeartbeatIntervalSeconds * 2;

      long finalInterval = Math.max(minInterval, Math.min(adjustedInterval, maxInterval));

      log.fine(
          String.format(
              "Calculated heartbeat interval: nodes=%d, pendingJobs=%d, "
                  + "baseInterval=%ds, finalInterval=%ds",
              activeNodes, pendingJobs, baseHeartbeatIntervalSeconds, finalInterval));

      return finalInterval;

    } catch (Exception e) {
      log.log(Level.SEVERE, "Error calculating heartbeat interval, using default", e);
      return baseHeartbeatIntervalSeconds;
    }
  }

  /**
   * Calculates the recommended polling delay based on current job queue depth.
   *
   * @return polling delay in milliseconds
   */
  public long calculatePollerDelay() {
    try {
      long pendingJobs = jobCrudStore.countPendingJobs();

      if (pendingJobs == 0) {
        return pollerMaxDelayMs;
      } else if (pendingJobs <= 5) {
        return (pollerMinDelayMs + pollerMaxDelayMs) / 2;
      } else {
        return pollerMinDelayMs;
      }
    } catch (Exception e) {
      log.log(Level.SEVERE, "Error calculating poller delay, using minimum", e);
      return pollerMinDelayMs;
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
