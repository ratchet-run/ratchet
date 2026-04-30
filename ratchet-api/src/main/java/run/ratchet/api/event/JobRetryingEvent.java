package run.ratchet.api.event;

import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;
import java.io.Serial;
import java.time.Instant;
import java.util.UUID;

/** Fired when a failed job is about to be retried. */
public class JobRetryingEvent extends AbstractJobSchedulerEvent {

  @Serial private static final long serialVersionUID = -3500975641125637480L;

  private final String errorMessage;
  private final Integer retryAttempt;
  private final Instant scheduledTime;

  public JobRetryingEvent(
      UUID jobId,
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
      UUID jobId,
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
