package run.ratchet.api.event;

import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;
import java.io.Serial;
import java.time.Instant;

/** Fired synchronously when a job cancellation is being processed. */
public class JobCancellingEvent extends AbstractJobSchedulerEvent {

  @Serial private static final long serialVersionUID = -1807471708143349083L;

  /** The job's status before cancellation was initiated. */
  private final String previousStatus;

  /** The execution time in milliseconds if the job was running when cancelled. */
  private final Long executionTimeMs;

  public JobCancellingEvent(
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

  public JobCancellingEvent(
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
