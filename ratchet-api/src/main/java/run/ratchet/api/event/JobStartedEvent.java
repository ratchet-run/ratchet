package run.ratchet.api.event;

import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;
import java.io.Serial;
import java.time.Instant;

/**
 * Represents an event indicating the start of a job in the scheduling system.
 *
 * <p>This event is triggered when a job begins execution, providing metadata such as job ID,
 * business key, job type, priority, and node identifier.
 *
 * <p>Two constructors are provided: one that allows explicitly specifying the event timestamp, and
 * another that defaults the timestamp to the value at the time of object creation.
 *
 * <p>This class extends {@code AbstractJobSchedulerEvent}, inheriting core metadata fields
 * representing details about the job.
 */
public class JobStartedEvent extends AbstractJobSchedulerEvent {

  @Serial private static final long serialVersionUID = -1805923320335775574L;

  public JobStartedEvent(
      Long jobId,
      String businessKey,
      JobType jobType,
      JobPriority priority,
      String nodeId,
      Instant timestamp) {
    super(jobId, businessKey, jobType, priority, nodeId, timestamp);
  }

  public JobStartedEvent(
      Long jobId, String businessKey, JobType jobType, JobPriority priority, String nodeId) {
    super(jobId, businessKey, jobType, priority, nodeId);
  }
}
