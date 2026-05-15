package run.ratchet.api.event;

import java.io.Serial;
import java.time.Instant;
import java.util.UUID;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;

/**
 * Fired when a job reaches terminal FAILED state.
 *
 * <p>Retryable per-attempt failures are reported through the metrics SPI and {@link
 * JobRetryingEvent}; they do not publish this event unless the failed attempt exhausts retry
 * handling and terminalizes the job.
 */
public class JobFailedEvent extends AbstractJobSchedulerEvent {

  @Serial private static final long serialVersionUID = -8745178784765705117L;

  private final String errorMessage;
  private final int retryAttempt;

  /**
   * Creates a failure event with an explicit timestamp.
   *
   * @param errorMessage sanitized failure message, or {@code null} when no message was recorded
   * @param retryAttempt 1-based execution attempt count that failed
   */
  public JobFailedEvent(
      UUID jobId,
      String businessKey,
      JobType jobType,
      JobPriority priority,
      String nodeId,
      Instant timestamp,
      String errorMessage,
      int retryAttempt) {
    super(jobId, businessKey, jobType, priority, nodeId, timestamp);
    this.errorMessage = errorMessage;
    this.retryAttempt = EventContract.requirePositive(retryAttempt, "retryAttempt");
  }

  /**
   * Creates a failure event using the current system clock instant.
   *
   * @param errorMessage sanitized failure message, or {@code null} when no message was recorded
   * @param retryAttempt 1-based execution attempt count that failed
   */
  public JobFailedEvent(
      UUID jobId,
      String businessKey,
      JobType jobType,
      JobPriority priority,
      String nodeId,
      String errorMessage,
      int retryAttempt) {
    super(jobId, businessKey, jobType, priority, nodeId);
    this.errorMessage = errorMessage;
    this.retryAttempt = EventContract.requirePositive(retryAttempt, "retryAttempt");
  }

  /** Returns the sanitized failure message, or {@code null} when no message was recorded. */
  public String getErrorMessage() {
    return errorMessage;
  }

  /** Returns the 1-based execution attempt count that failed. */
  public int getRetryAttempt() {
    return retryAttempt;
  }
}
