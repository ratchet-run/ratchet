package run.ratchet.api.event;

import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;
import java.io.Serial;
import java.time.Instant;

/** Fired when all jobs in a chain have completed successfully. */
public class ChainCompletedEvent extends AbstractJobSchedulerEvent {

  @Serial private static final long serialVersionUID = -8140882369003276835L;

  /** The ID of the parent job that owns this chain. */
  private final Long parentJobId;

  public ChainCompletedEvent(
      Long jobId,
      String businessKey,
      JobType jobType,
      JobPriority priority,
      String nodeId,
      Instant timestamp,
      Long parentJobId) {
    super(jobId, businessKey, jobType, priority, nodeId, timestamp);
    this.parentJobId = parentJobId;
  }

  public ChainCompletedEvent(
      Long jobId,
      String businessKey,
      JobType jobType,
      JobPriority priority,
      String nodeId,
      Long parentJobId) {
    super(jobId, businessKey, jobType, priority, nodeId);
    this.parentJobId = parentJobId;
  }

  public Long getParentJobId() {
    return parentJobId;
  }
}
