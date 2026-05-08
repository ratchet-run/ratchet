package run.ratchet.api.event;

import java.io.Serial;
import java.time.Instant;
import java.util.UUID;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;

/** Base class for job cancellation lifecycle events. */
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
    this.previousStatus = previousStatus;
    this.executionTimeMs = executionTimeMs;
  }

  protected AbstractJobCancellationEvent(
      UUID jobId,
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

  public String getPreviousStatus() {
    return previousStatus;
  }

  public Long getExecutionTimeMs() {
    return executionTimeMs;
  }
}
