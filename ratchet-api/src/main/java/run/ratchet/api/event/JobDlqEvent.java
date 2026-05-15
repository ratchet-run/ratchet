package run.ratchet.api.event;

import java.io.Serial;
import java.time.Instant;
import java.util.UUID;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;

/** Fired after Ratchet moves a permanently failed job to the dead letter queue. */
public class JobDlqEvent extends AbstractJobSchedulerEvent {

  @Serial private static final long serialVersionUID = -2578972098474327757L;

  private final String errorMessage;
  private final int retryAttempt;

  /**
   * Creates an event with an explicit timestamp.
   *
   * @param errorMessage sanitized final failure message
   * @param retryAttempt final recorded retry attempt count before DLQ
   */
  public JobDlqEvent(
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
    this.retryAttempt = EventContract.requireNonNegative(retryAttempt, "retryAttempt");
  }

  /**
   * Creates an event using the current system clock instant.
   *
   * <p>Tests that assert event timestamps should use the constructor that accepts an explicit
   * {@link Instant}.
   *
   * @param errorMessage sanitized final failure message
   * @param retryAttempt final recorded retry attempt count before DLQ
   */
  public JobDlqEvent(
      UUID jobId,
      String businessKey,
      JobType jobType,
      JobPriority priority,
      String nodeId,
      String errorMessage,
      int retryAttempt) {
    super(jobId, businessKey, jobType, priority, nodeId);
    this.errorMessage = errorMessage;
    this.retryAttempt = EventContract.requireNonNegative(retryAttempt, "retryAttempt");
  }

  /** Returns the sanitized final failure message. */
  public String getErrorMessage() {
    return errorMessage;
  }

  /** Returns the final 1-based execution attempt count before DLQ. */
  public int getRetryAttempt() {
    return retryAttempt;
  }
}
