package run.ratchet.api.event;

import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;
import java.io.Serial;
import java.time.Instant;

/**
 * Represents an event that is triggered when a job is cancelled in the job scheduler.
 *
 * <p>This event extends the {@code AbstractJobSchedulerEvent} class, inheriting fields such as job
 * metadata and provides additional information specific to the cancellation of jobs.
 */
public class JobCancelledEvent extends AbstractJobSchedulerEvent {

  @Serial private static final long serialVersionUID = -3714116971496582534L;

  /** The job's status before cancellation. */
  private final String previousStatus;

  /** The execution time in milliseconds if the job was running when cancelled. */
  private final Long executionTimeMs;

  public JobCancelledEvent(
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

  public JobCancelledEvent(
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
   * Retrieves the status of the job before it was cancelled.
   *
   * @return the previous status of the job as a {@code String}.
   */
  public String getPreviousStatus() {
    return previousStatus;
  }

  public Long getExecutionTimeMs() {
    return executionTimeMs;
  }
}
