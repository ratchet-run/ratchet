package run.ratchet.api.event;

import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;
import java.io.Serial;
import java.time.Instant;

/** Fired when a job has been cancelled. */
public class JobCancelledEvent extends AbstractJobSchedulerEvent {

  @Serial private static final long serialVersionUID = -3714116971496582534L;

  private final String previousStatus;
  private final Long executionTimeMs;

  public JobCancelledEvent(
      Long jobId,
      String businessKey,
      JobType jobType,
      JobPriority priority,
      String nodeId,
      Instant timestamp,
      String previousStatus,
      Long executionTimeMs) {
    super(jobId, businessKey, jobType, priority, nodeId, timestamp);
    this.previousStatus = previousStatus;
    this.executionTimeMs = executionTimeMs;
  }

  public JobCancelledEvent(
      Long jobId,
      String businessKey,
      JobType jobType,
      JobPriority priority,
      String nodeId,
      String previousStatus,
      Long executionTimeMs) {
    super(jobId, businessKey, jobType, priority, nodeId);
    this.previousStatus = previousStatus;
    this.executionTimeMs = executionTimeMs;
  }

  public String getPreviousStatus() {
    return previousStatus;
  }

  public Long getExecutionTimeMs() {
    return executionTimeMs;
  }
}
