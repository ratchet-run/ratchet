package run.ratchet.api.event;

import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;
import java.io.Serial;
import java.time.Instant;

/** Fired when a job chain begins execution. */
public class ChainStartedEvent extends AbstractJobSchedulerEvent {

  @Serial private static final long serialVersionUID = -4450548507481291423L;

  /** The ID of the parent job that owns this chain. */
  private final Long parentJobId;

  public ChainStartedEvent(
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

  public ChainStartedEvent(
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
