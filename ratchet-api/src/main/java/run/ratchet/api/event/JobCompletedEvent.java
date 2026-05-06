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

  public Long getExecutionTimeMs() {
    return executionTimeMs;
  }
}
