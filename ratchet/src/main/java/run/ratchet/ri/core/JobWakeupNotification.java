package run.ratchet.ri.core;

import run.ratchet.api.JobPriority;
import java.io.Serializable;
import java.time.Instant;

/**
 * Lightweight notification used to wake up pollers across the cluster.
 *
 * <p>When a user-triggered or CRITICAL priority job is created, this notification is published to
 * the cluster. All nodes receive the notification and wake their pollers to immediately check for
 * available work.
 *
 * <p>This enables responsive job execution without constant database polling during quiet periods.
 *
 * @param originNodeId identifies which cluster node created the job requiring wakeup
 * @param timestamp when the notification was created
 * @param priority the priority level of the job triggering the wakeup
 * @param immediate flag indicating whether the job requires immediate pickup
 * @see JobWakeupService
 * @see PollerWakeupListener
 */
public record JobWakeupNotification(
    String originNodeId, Instant timestamp, JobPriority priority, boolean immediate)
    implements Serializable {

  /**
   * Creates a notification for immediate execution.
   *
   * @param nodeId the ID of the node creating the job
   * @param priority the job priority
   * @return a new notification marked for immediate processing
   */
  public static JobWakeupNotification immediate(String nodeId, JobPriority priority) {
    return new JobWakeupNotification(nodeId, Instant.now(), priority, true);
  }

  /**
   * Creates a notification for normal processing.
   *
   * @param nodeId the ID of the node creating the job
   * @param priority the job priority
   * @return a new notification for normal processing
   */
  public static JobWakeupNotification normal(String nodeId, JobPriority priority) {
    return new JobWakeupNotification(nodeId, Instant.now(), priority, false);
  }
}
