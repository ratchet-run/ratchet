package run.ratchet.api.event;

import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;
import java.io.Serial;
import java.time.Instant;

/** Fired when a job fails permanently and will not be retried. */
public class JobFailedEvent extends AbstractJobSchedulerEvent {

  @Serial private static final long serialVersionUID = -8745178784765705117L;

  /** The error message describing the failure. */
  private final String errorMessage;

  /** The final retry attempt number when the job failed. */
  private final Integer retryAttempt;

  public JobFailedEvent(
      Long jobId,
      String businessKey,
      JobType jobType,
      JobPriority priority,
      String nodeId,
      Instant timestamp,
      String errorMessage,
      Integer retryAttempt) {
    super(jobId, businessKey, jobType, priority, nodeId, timestamp);
    this.errorMessage = errorMessage;
    this.retryAttempt = retryAttempt;
  }

  public JobFailedEvent(
      Long jobId,
      String businessKey,
      JobType jobType,
      JobPriority priority,
      String nodeId,
      String errorMessage,
      Integer retryAttempt) {
    super(jobId, businessKey, jobType, priority, nodeId);
    this.errorMessage = errorMessage;
    this.retryAttempt = retryAttempt;
  }

  public String getErrorMessage() {
    return errorMessage;
  }

  public Integer getRetryAttempt() {
    return retryAttempt;
  }
}
