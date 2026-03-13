package run.ratchet.api.event;

import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;
import java.io.Serial;
import java.time.Instant;

/**
 * Represents an event signaling the completion of a job in the job scheduler.
 *
 * <p>This event provides information about the completed job, including its execution time in
 * milliseconds and other metadata inherited from {@link AbstractJobSchedulerEvent}.
 *
 * <p>It supports the creation of events with explicit timestamps or a default timestamp, which
 * defaults to the current instant.
 *
 * <p>This class is immutable and thread-safe.
 */
public class JobCompletedEvent extends AbstractJobSchedulerEvent {

  @Serial private static final long serialVersionUID = 6928539910648242733L;

  /** The total execution time in milliseconds for the completed job. */
  private final Long executionTimeMs;

  public JobCompletedEvent(
      Long jobId,
      String businessKey,
      JobType jobType,
      JobPriority priority,
      String nodeId,
      Instant timestamp,
      Long executionTimeMs) {
    super(jobId, businessKey, jobType, priority, nodeId, timestamp);
    this.executionTimeMs = executionTimeMs;
  }

  public JobCompletedEvent(
      Long jobId,
      String businessKey,
      JobType jobType,
      JobPriority priority,
      String nodeId,
      Long executionTimeMs) {
    super(jobId, businessKey, jobType, priority, nodeId);
    this.executionTimeMs = executionTimeMs;
  }

  /**
   * Returns the total execution time in milliseconds for the completed job.
   *
   * @return the execution time in milliseconds, or {@code null} if the execution time is not
   *     available.
   */
  public Long getExecutionTimeMs() {
    return executionTimeMs;
  }
}
