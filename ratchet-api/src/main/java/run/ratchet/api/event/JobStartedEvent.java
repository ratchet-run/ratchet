package run.ratchet.api.event;

import java.io.Serial;
import java.time.Instant;
import java.util.UUID;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;

/**
 * Fired when a job starts executing.
 *
 * <p>The event is published after a job has been claimed and just before payload execution begins.
 * Inherited fields identify the job, business key, type, priority, executor node, and event
 * timestamp.
 */
public class JobStartedEvent extends AbstractJobSchedulerEvent {

  @Serial private static final long serialVersionUID = -1805923320335775574L;

  /** Creates a start event with an explicit timestamp. */
  public JobStartedEvent(
      UUID jobId,
      String businessKey,
      JobType jobType,
      JobPriority priority,
      String nodeId,
      Instant timestamp) {
    super(jobId, businessKey, jobType, priority, nodeId, timestamp);
  }

  /** Creates a start event using the current system clock instant. */
  public JobStartedEvent(
      UUID jobId, String businessKey, JobType jobType, JobPriority priority, String nodeId) {
    super(jobId, businessKey, jobType, priority, nodeId);
  }
}
