package run.ratchet.ri.core;

import java.io.Serializable;
import java.time.Instant;
import run.ratchet.api.JobPriority;

/**
 * Published to the cluster when a high-priority job is created, causing all pollers to wake
 * immediately.
 */
public record JobWakeupNotification(
    String originNodeId, Instant timestamp, JobPriority priority, boolean immediate)
    implements Serializable {}
