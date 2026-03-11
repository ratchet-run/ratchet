package run.ratchet.api.event;

import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;
import java.io.Serial;
import java.time.Instant;

/** Fired when a job fails but will be retried based on its retry policy. */
public class JobRetryingEvent extends AbstractJobSchedulerEvent {

  @Serial private static final long serialVersionUID = -3500975641125637480L;

  /** The error message describing the failure that triggered the retry. */
  private final String errorMessage;

  /** The current retry attempt number. */
  private final Integer retryAttempt;

  /** The scheduled time for the retry attempt (after backoff). */
  private final Instant scheduledTime;

  public JobRetryingEvent(
      Long jobId,
      String businessKey,
      JobType jobType,
      JobPriority priority,
      String nodeId,
      Instant timestamp,
      String errorMessage,
      Integer retryAttempt,
      Instant scheduledTime) {
    super(jobId, businessKey, jobType, priority, nodeId, timestamp);
    this.errorMessage = errorMessage;
    this.retryAttempt = retryAttempt;
    this.scheduledTime = scheduledTime;
  }

  public JobRetryingEvent(
      Long jobId,
      String businessKey,
      JobType jobType,
      JobPriority priority,
      String nodeId,
      String errorMessage,
      Integer retryAttempt,
      Instant scheduledTime) {
    super(jobId, businessKey, jobType, priority, nodeId);
    this.errorMessage = errorMessage;
    this.retryAttempt = retryAttempt;
    this.scheduledTime = scheduledTime;
  }

  public String getErrorMessage() {
    return errorMessage;
  }

  public Integer getRetryAttempt() {
    return retryAttempt;
  }

  public Instant getScheduledTime() {
    return scheduledTime;
  }
}
