package run.ratchet.api.event;

import java.io.Serial;
import java.time.Instant;
import java.util.UUID;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;

/** Fired when a job chain begins execution. */
public class ChainStartedEvent extends AbstractJobSchedulerEvent {

  @Serial private static final long serialVersionUID = -4450548507481291423L;

  private final UUID parentJobId;

  public ChainStartedEvent(
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

  public ChainStartedEvent(
      UUID jobId,
      String businessKey,
      JobType jobType,
      JobPriority priority,
      String nodeId,
      UUID parentJobId) {
    super(jobId, businessKey, jobType, priority, nodeId);
    this.parentJobId = parentJobId;
  }

  public UUID getParentJobId() {
    return parentJobId;
  }
}
