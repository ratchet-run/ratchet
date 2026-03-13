package run.ratchet.api.event;

import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;
import java.io.Serial;
import java.time.Instant;

/**
 * Represents an event that signifies the movement of a job to the Dead Letter Queue (DLQ) after
 * final failure.
 *
 * <p>This event provides specific metadata about the failure, including the associated error
 * message and the total number of retry attempts before the decision to move the job to the DLQ was
 * made.
 *
 * <p>It extends {@link AbstractJobSchedulerEvent}, inheriting common metadata fields shared by all
 * job scheduler events.
 */
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

  /**
   * Retrieves the error message associated with this event.
   *
   * @return the error message describing the failure.
   */
  public String getErrorMessage() {
    return errorMessage;
  }

  /**
   * Retrieves the total number of retry attempts made before the associated job moved to the Dead
   * Letter Queue (DLQ).
   *
   * @return the number of retry attempts before the job was deemed as failed permanently.
   */
  public Integer getRetryAttempt() {
    return retryAttempt;
  }
}
