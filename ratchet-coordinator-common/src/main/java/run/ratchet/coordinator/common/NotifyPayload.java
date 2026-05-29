package run.ratchet.coordinator.common;

import java.util.Objects;
import run.ratchet.api.JobPriority;
import run.ratchet.api.NodeIdentity;
import run.ratchet.coordinator.common.internal.NotifyPayloadCodec;

/**
 * Versioned wakeup envelope shared by every coordinator transport.
 *
 * <p>{@code executionTarget} carries the routing label of the job that triggered the wakeup. It is
 * nullable; a {@code null} value means the wakeup is not target-scoped and receiving listeners
 * should wake the poller unconditionally.
 */
public record NotifyPayload(
    int version, NodeIdentity node, JobPriority priority, String executionTarget) {

  public NotifyPayload {
    Objects.requireNonNull(node, "node");
    Objects.requireNonNull(priority, "priority");
  }

  public static NotifyPayload current(NodeIdentity node, JobPriority priority) {
    return current(node, priority, null);
  }

  public static NotifyPayload current(
      NodeIdentity node, JobPriority priority, String executionTarget) {
    return new NotifyPayload(NotifyPayloadCodec.CURRENT_VERSION, node, priority, executionTarget);
  }
}
