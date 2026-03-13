package run.ratchet.api.event;

import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;
import java.io.Serial;
import java.time.Instant;

/**
 * Event representing the pausing of a job within the job scheduler system.
 *
 * <p>This event is triggered when a job transitions to a paused state, indicating that it has been
 * temporarily halted and will not be executed until it is resumed. It encapsulates metadata about
 * the paused job, such as its identifier, type, priority, associated business key, the node that
 * processed the job, and the event timestamp.
 *
 * <p>Extends the {@link AbstractJobSchedulerEvent} class to inherit common job metadata fields.
 */
public class JobPausedEvent extends AbstractJobSchedulerEvent {

  @Serial private static final long serialVersionUID = -3743348949914580646L;

  public JobPausedEvent(
      Long jobId,
      String businessKey,
      JobType jobType,
      JobPriority priority,
      String nodeId,
      Instant timestamp) {
    super(jobId, businessKey, jobType, priority, nodeId, timestamp);
  }

  public JobPausedEvent(
      Long jobId, String businessKey, JobType jobType, JobPriority priority, String nodeId) {
    super(jobId, businessKey, jobType, priority, nodeId);
  }
}
