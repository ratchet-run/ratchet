package run.ratchet.api.event;

import java.io.Serial;
import java.time.Instant;
import java.util.UUID;
import run.ratchet.api.Incubating;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;

/** Base class for job cancellation lifecycle events. */
@Incubating
public abstract class AbstractJobCancellationEvent extends AbstractJobSchedulerEvent {

  @Serial private static final long serialVersionUID = 6253413059723522683L;

  private final String previousStatus;
  private final Long executionTimeMs;

  protected AbstractJobCancellationEvent(
      UUID jobId,
      String businessKey,
      JobType jobType,
      JobPriority priority,
      String nodeId,
      Instant timestamp,
      String previousStatus,
      Long executionTimeMs) {
    super(jobId, businessKey, jobType, priority, nodeId, timestamp);
    this.previousStatus = EventContract.requireNonBlank(previousStatus, "previousStatus");
    this.executionTimeMs = EventContract.requireNonNegative(executionTimeMs, "executionTimeMs");
  }

  /**
   * Creates a cancellation event using the current system clock instant.
   *
   * @param previousStatus status observed before the cancellation transition
   * @param executionTimeMs measured execution duration, or {@code null} if unavailable
   */
  protected AbstractJobCancellationEvent(
      UUID jobId,
      String businessKey,
      JobType jobType,
      JobPriority priority,
      String nodeId,
      String previousStatus,
      Long executionTimeMs) {
    super(jobId, businessKey, jobType, priority, nodeId);
    this.previousStatus = EventContract.requireNonBlank(previousStatus, "previousStatus");
    this.executionTimeMs = EventContract.requireNonNegative(executionTimeMs, "executionTimeMs");
  }

  /**
   * Returns the status observed before the cancellation transition.
   *
   * <p>The value is the persisted {@link run.ratchet.api.JobStatus#name()} string. It is kept as a
   * string so event consumers can read events produced by newer schedulers that introduce
   * additional statuses.
   */
  public String getPreviousStatus() {
    return previousStatus;
  }

  /**
   * Returns the measured execution duration in milliseconds.
   *
   * <p>This value is {@code null} when the job was canceled before execution began, or when Ratchet
   * could not reload the job details after a successful cancellation transition.
   */
  public Long getExecutionTimeMs() {
    return executionTimeMs;
  }
}
