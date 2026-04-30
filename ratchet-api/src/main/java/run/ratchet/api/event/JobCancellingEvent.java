package run.ratchet.api.event;

import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;
import java.io.Serial;
import java.time.Instant;
import java.util.UUID;

/**
 * Fired when a job cancellation is initiated.
 *
 * <p>Fired on the requesting thread before the job record is updated; the executor thread may still
 * be running.
 */
public class JobCancellingEvent extends AbstractJobSchedulerEvent {

  @Serial private static final long serialVersionUID = -1807471708143349083L;

  private final String previousStatus;
  private final Long executionTimeMs;

  public JobCancellingEvent(
      UUID jobId,
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
      UUID jobId,
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
