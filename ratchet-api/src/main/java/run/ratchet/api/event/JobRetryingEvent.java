package run.ratchet.api.event;

import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;
import java.io.Serial;
import java.time.Instant;

/**
 * Represents an event that occurs when a job is being retried within the job scheduler system.
 *
 * <p>This event provides details about the reason for the retry, the current retry attempt number,
 * and the scheduled time for the next retry based on backoff logic.
 *
 * <p>It extends {@code AbstractJobSchedulerEvent}, inheriting common job metadata fields such as
 * job ID, business key, job type, priority, node identifier, and event timestamp.
 */
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

  /**
   * Retrieves the error message describing the failure that triggered the retry event.
   *
   * @return the error message associated with the retry.
   */
  public String getErrorMessage() {
    return errorMessage;
  }

  /**
   * Retrieves the current retry attempt number for the job retrying event.
   *
   * @return the current retry attempt number.
   */
  public Integer getRetryAttempt() {
    return retryAttempt;
  }

  /**
   * Retrieves the scheduled time for the retry attempt of a job.
   *
   * @return the {@code Instant} representing the scheduled time for the next retry based on backoff
   *     logic.
   */
  public Instant getScheduledTime() {
    return scheduledTime;
  }
}
