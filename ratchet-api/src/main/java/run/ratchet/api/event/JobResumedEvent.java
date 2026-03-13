package run.ratchet.api.event;

import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;
import java.io.Serial;
import java.time.Instant;

/**
 * Event representing the resumption of a previously paused job.
 *
 * <p>This event is triggered when a job in the scheduler transitions from a paused state to an
 * active or resumed state. It encapsulates metadata about the job, including its identifier, type,
 * priority, and the node handling the job.
 *
 * <p>This class extends {@code AbstractJobSchedulerEvent} and inherits its common fields and
 * functionality.
 *
 * <p>Thread-safety: Instances of this class are immutable and thread-safe.
 */
public class JobResumedEvent extends AbstractJobSchedulerEvent {

  @Serial private static final long serialVersionUID = -5969069800791733733L;

  public JobResumedEvent(
      Long jobId,
      String businessKey,
      JobType jobType,
      JobPriority priority,
      String nodeId,
      Instant timestamp) {
    super(jobId, businessKey, jobType, priority, nodeId, timestamp);
  }

  public JobResumedEvent(
      Long jobId, String businessKey, JobType jobType, JobPriority priority, String nodeId) {
    super(jobId, businessKey, jobType, priority, nodeId);
  }
}
