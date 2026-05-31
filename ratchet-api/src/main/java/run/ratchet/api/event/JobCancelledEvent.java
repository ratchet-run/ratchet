package run.ratchet.api.event;

import java.io.Serial;
import java.time.Instant;
import java.util.UUID;
import run.ratchet.api.Incubating;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;

/**
 * Fired when a job has been cancelled.
 *
 * <p>Fired after the job record reaches CANCELED state.
 */
@Incubating
public class JobCancelledEvent extends AbstractJobCancellationEvent {

  @Serial private static final long serialVersionUID = -3714116971496582534L;

  public JobCancelledEvent(
      UUID jobId,
      String businessKey,
      JobType jobType,
      JobPriority priority,
      String nodeId,
      Instant timestamp,
      String previousStatus,
      Long executionTimeMs) {
    super(
        jobId, businessKey, jobType, priority, nodeId, timestamp, previousStatus, executionTimeMs);
  }

  public JobCancelledEvent(
      UUID jobId,
      String businessKey,
      JobType jobType,
      JobPriority priority,
      String nodeId,
      String previousStatus,
      Long executionTimeMs) {
    super(jobId, businessKey, jobType, priority, nodeId, previousStatus, executionTimeMs);
  }
}
