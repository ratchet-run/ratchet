package run.ratchet.ri.core;

import run.ratchet.api.JobPriority;
import java.io.Serializable;
import java.time.Instant;

/**
 * Published to the cluster when a high-priority job is created, causing all pollers to wake
 * immediately.
 */
public record JobWakeupNotification(
    String originNodeId, Instant timestamp, JobPriority priority, boolean immediate)
    implements Serializable {

  public static JobWakeupNotification immediate(String nodeId, JobPriority priority) {
    return new JobWakeupNotification(nodeId, Instant.now(), priority, true);
  }

  public static JobWakeupNotification normal(String nodeId, JobPriority priority) {
    return new JobWakeupNotification(nodeId, Instant.now(), priority, false);
  }
}
