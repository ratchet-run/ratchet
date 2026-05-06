package run.ratchet.api.event;

import java.io.Serial;
import java.time.Instant;
import java.util.UUID;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;

/** Fired when a job starts executing. */
public class JobStartedEvent extends AbstractJobSchedulerEvent {

  @Serial private static final long serialVersionUID = -1805923320335775574L;

  public JobStartedEvent(
      UUID jobId,
      String businessKey,
      JobType jobType,
      JobPriority priority,
      String nodeId,
      Instant timestamp) {
    super(jobId, businessKey, jobType, priority, nodeId, timestamp);
  }

  public JobStartedEvent(
      UUID jobId, String businessKey, JobType jobType, JobPriority priority, String nodeId) {
    super(jobId, businessKey, jobType, priority, nodeId);
  }
}
