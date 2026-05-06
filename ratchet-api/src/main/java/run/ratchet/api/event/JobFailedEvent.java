package run.ratchet.api.event;

import java.io.Serial;
import java.time.Instant;
import java.util.UUID;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;

/** Fired when a job fails. */
public class JobFailedEvent extends AbstractJobSchedulerEvent {

  @Serial private static final long serialVersionUID = -8745178784765705117L;

  private final String errorMessage;
  private final Integer retryAttempt;

  public JobFailedEvent(
      UUID jobId,
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
      UUID jobId,
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
