package run.ratchet.api.event;

import java.io.Serial;
import java.time.Instant;
import java.util.UUID;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;

/** Fired when a job completes successfully. */
public class JobCompletedEvent extends AbstractJobSchedulerEvent {

  @Serial private static final long serialVersionUID = 6928539910648242733L;

  private final Long executionTimeMs;

  /**
   * Creates a completion event with an explicit timestamp.
   *
   * @param executionTimeMs wall-clock execution duration in milliseconds, or {@code null} if not
   *     recorded
   */
  public JobCompletedEvent(
      UUID jobId,
      String businessKey,
      JobType jobType,
      JobPriority priority,
      String nodeId,
      Instant timestamp,
      Long executionTimeMs) {
    super(jobId, businessKey, jobType, priority, nodeId, timestamp);
    this.executionTimeMs = executionTimeMs;
  }

  /**
   * Creates a completion event using the current system clock instant.
   *
   * <p>Use the constructor that accepts an explicit {@link Instant} for tests, replay, or any path
   * that already has a scheduler-provided timestamp.
   *
   * @param executionTimeMs wall-clock execution duration in milliseconds, or {@code null} if not
   *     recorded
   */
  public JobCompletedEvent(
      UUID jobId,
      String businessKey,
      JobType jobType,
      JobPriority priority,
      String nodeId,
      Long executionTimeMs) {
    super(jobId, businessKey, jobType, priority, nodeId);
    this.executionTimeMs = executionTimeMs;
  }

  /** Returns the wall-clock execution duration in milliseconds, or {@code null} if not recorded. */
  public Long getExecutionTimeMs() {
    return executionTimeMs;
  }
}
