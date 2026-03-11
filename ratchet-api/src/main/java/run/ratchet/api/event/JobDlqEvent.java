package run.ratchet.api.event;

import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;
import java.io.Serial;
import java.time.Instant;

/** Fired when a job is moved to the Dead Letter Queue after exhausting all retry attempts. */
public class JobDlqEvent extends AbstractJobSchedulerEvent {

  @Serial private static final long serialVersionUID = -2578972098474327757L;

  /** The error message from the final failure. */
  private final String errorMessage;

  /** The total number of retry attempts before moving to DLQ. */
  private final Integer retryAttempt;

  public JobDlqEvent(
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

  public JobDlqEvent(
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
