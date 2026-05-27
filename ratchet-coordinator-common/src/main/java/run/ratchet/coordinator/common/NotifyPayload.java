package run.ratchet.coordinator.common;

import java.util.Objects;
import run.ratchet.api.JobPriority;
import run.ratchet.api.NodeIdentity;

/** Versioned wakeup envelope shared by every coordinator transport. */
public record NotifyPayload(int version, NodeIdentity node, JobPriority priority) {

  public NotifyPayload {
    Objects.requireNonNull(node, "node");
    Objects.requireNonNull(priority, "priority");
  }

  public static NotifyPayload current(NodeIdentity node, JobPriority priority) {
    return new NotifyPayload(NotifyPayloadCodec.CURRENT_VERSION, node, priority);
  }
}
