package run.ratchet.api.event;

import java.io.Serial;
import java.time.Instant;
import java.util.UUID;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;

/**
 * Fired when all steps in a job chain complete.
 *
 * <p>{@code jobId} identifies the step whose completion closed the chain. {@code parentJobId}
 * identifies the root job that initiated the chain.
 */
public class ChainCompletedEvent extends AbstractJobSchedulerEvent {

  @Serial private static final long serialVersionUID = -8140882369003276835L;

  private final UUID parentJobId;

  public ChainCompletedEvent(
      UUID jobId,
      String businessKey,
      JobType jobType,
      JobPriority priority,
      String nodeId,
      Instant timestamp,
      UUID parentJobId) {
    super(jobId, businessKey, jobType, priority, nodeId, timestamp);
    this.parentJobId = parentJobId;
  }

  public ChainCompletedEvent(
      UUID jobId,
      String businessKey,
      JobType jobType,
      JobPriority priority,
      String nodeId,
      UUID parentJobId) {
    super(jobId, businessKey, jobType, priority, nodeId);
    this.parentJobId = parentJobId;
  }

  /** Returns the root job that initiated the completed chain. */
  public UUID getParentJobId() {
    return parentJobId;
  }
}
