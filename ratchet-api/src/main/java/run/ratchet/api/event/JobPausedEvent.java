package run.ratchet.api.event;

import java.io.Serial;
import java.time.Instant;
import java.util.UUID;
import run.ratchet.api.Incubating;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;

/** Fired when a job is paused. @since 0.1 */
@Incubating
public class JobPausedEvent extends AbstractJobSchedulerEvent {

  @Serial private static final long serialVersionUID = -3743348949914580646L;

  public JobPausedEvent(
      UUID jobId,
      String businessKey,
      JobType jobType,
      JobPriority priority,
      String nodeId,
      Instant timestamp) {
    super(jobId, businessKey, jobType, priority, nodeId, timestamp);
  }

  public JobPausedEvent(
      UUID jobId, String businessKey, JobType jobType, JobPriority priority, String nodeId) {
    super(jobId, businessKey, jobType, priority, nodeId);
  }
}
