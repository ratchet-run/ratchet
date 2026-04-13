package run.ratchet.api.event;

import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;
import java.io.Serial;
import java.time.Instant;

/** Fired when a job chain fails. */
public class ChainFailedEvent extends AbstractJobSchedulerEvent {

  @Serial private static final long serialVersionUID = 5542623623918947230L;

  private final Long parentJobId;
  private final String errorMessage;

  public ChainFailedEvent(
      Long jobId,
      String businessKey,
      JobType jobType,
      JobPriority priority,
      String nodeId,
      Instant timestamp,
      Long parentJobId,
      String errorMessage) {
    super(jobId, businessKey, jobType, priority, nodeId, timestamp);
    this.parentJobId = parentJobId;
    this.errorMessage = errorMessage;
  }

  public ChainFailedEvent(
      Long jobId,
      String businessKey,
      JobType jobType,
      JobPriority priority,
      String nodeId,
      Long parentJobId,
      String errorMessage) {
    super(jobId, businessKey, jobType, priority, nodeId);
    this.parentJobId = parentJobId;
    this.errorMessage = errorMessage;
  }

  public Long getParentJobId() {
    return parentJobId;
  }

  public String getErrorMessage() {
    return errorMessage;
  }
}
