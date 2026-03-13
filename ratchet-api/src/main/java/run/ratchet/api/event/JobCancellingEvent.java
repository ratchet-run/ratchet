package run.ratchet.api.event;

import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;
import java.io.Serial;
import java.time.Instant;

/**
 * Represents an event triggered when a job is being canceled in the job scheduler.
 *
 * <p>This event captures additional metadata such as the job's status prior to cancellation and the
 * execution time (if the job was running when the cancellation was initiated).
 *
 * <p>This is a subclass of {@code AbstractJobSchedulerEvent}.
 */
public class JobCancellingEvent extends AbstractJobSchedulerEvent {

  @Serial private static final long serialVersionUID = -1807471708143349083L;

  /** The job's status before cancellation was initiated. */
  private final String previousStatus;

  /** The execution time in milliseconds if the job was running when cancelled. */
  private final Long executionTimeMs;

  public JobCancellingEvent(
      Long jobId,
      String businessKey,
      JobType jobType,
      JobPriority priority,
      String nodeId,
      Instant timestamp,
      String previousStatus,
      Long executionTimeMs) {
    super(jobId, businessKey, jobType, priority, nodeId, timestamp);
    this.previousStatus = previousStatus;
    this.executionTimeMs = executionTimeMs;
  }

  public JobCancellingEvent(
      Long jobId,
      String businessKey,
      JobType jobType,
      JobPriority priority,
      String nodeId,
      String previousStatus,
      Long executionTimeMs) {
    super(jobId, businessKey, jobType, priority, nodeId);
    this.previousStatus = previousStatus;
    this.executionTimeMs = executionTimeMs;
  }

  /**
   * Retrieves the job's status prior to the cancellation event.
   *
   * @return the status of the job before the cancellation was initiated.
   */
  public String getPreviousStatus() {
    return previousStatus;
  }

  /**
   * Retrieves the execution time of the job in milliseconds.
   *
   * @return the execution time in milliseconds, or {@code null} if the job was not running when the
   *     cancellation was initiated.
   */
  public Long getExecutionTimeMs() {
    return executionTimeMs;
  }
}
