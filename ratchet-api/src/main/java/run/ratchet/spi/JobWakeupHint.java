package run.ratchet.spi;

import java.util.Objects;
import run.ratchet.api.Incubating;
import run.ratchet.api.JobPriority;
import run.ratchet.api.NodeIdentity;

/**
 * Payload delivered to {@link ClusterCoordinator} wakeup listeners.
 *
 * <p>A wakeup hint carries the priority of the newly available work, the {@link NodeIdentity} of
 * the originating node, and (optionally) the {@code executionTarget} the work was routed to. The
 * target is informational metadata, not a routing filter — receiving listeners wake the local
 * poller unconditionally and let the claim-side filter pick which pool actually drains. A {@code
 * null} {@code executionTarget} means the wakeup is not target-scoped; listeners must wake the
 * poller as they would for any other notification.
 *
 * @param priority priority of the newly available work; never {@code null}
 * @param source identity of the node that originated this wakeup; never {@code null}
 * @param executionTarget execution-target label of the originating job, or {@code null} when the
 *     wakeup is not target-scoped
 * @since 0.1
 */
@Incubating
public record JobWakeupHint(JobPriority priority, NodeIdentity source, String executionTarget) {

  public JobWakeupHint {
    Objects.requireNonNull(priority, "priority");
    Objects.requireNonNull(source, "source");
  }
}
