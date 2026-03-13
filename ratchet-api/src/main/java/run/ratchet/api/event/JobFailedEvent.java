package run.ratchet.api.event;

import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;
import java.io.Serial;
import java.time.Instant;

/**
 * Represents an event signaling the failure of a job in the job scheduler.
 *
 * <p>This event contains additional information about the failure, such as an error message
 * describing the failure and the final retry attempt at which the job failed.
 *
 * <p>The event extends the {@code AbstractJobSchedulerEvent} and inherits metadata properties like
 * job ID, business key, job type, priority, node ID, and timestamp.
 */
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

  /**
   * Retrieves the error message associated with the failure of a job.
   *
   * @return the error message describing the failure.
   */
  public String getErrorMessage() {
    return errorMessage;
  }

  /**
   * Retrieves the number of retry attempts made before the job failed.
   *
   * @return the number of retry attempts associated with the job failure.
   */
  public Integer getRetryAttempt() {
    return retryAttempt;
  }
}
