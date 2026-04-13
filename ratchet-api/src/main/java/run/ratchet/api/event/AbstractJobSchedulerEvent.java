package run.ratchet.api.event;

import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;
import java.io.Serial;
import java.io.Serializable;
import java.time.Instant;

/**
 * Base class for all job scheduler events.
 *
 * <p>Provides common job metadata fields shared by all scheduler event types.
 *
 * <p>Subclasses should declare constructors that delegate to {@code super(...)} and their own
 * {@code serialVersionUID}.
 */
public abstract class AbstractJobSchedulerEvent implements Serializable {

  @Serial private static final long serialVersionUID = 6853988277084004625L;

  /** The unique database identifier of the job that triggered this event. */
  private final Long jobId;

  /**
   * The business key associated with the job, providing a human-readable identifier that correlates
   * jobs with business operations (e.g., "user-import-12345").
   */
  private final String businessKey;

  /** The public job category (e.g., SINGLE, BATCH, CHAIN). */
  private final JobType jobType;

  /** The priority level of the job. */
  private final JobPriority priority;

  /** The identifier of the cluster node that processed this job. */
  private final String nodeId;

  /** The instant when this event was created. */
  private final Instant timestamp;

  /** Creates a new event with all fields including an explicit timestamp. */
  protected AbstractJobSchedulerEvent(
      Long jobId,
      String businessKey,
      JobType jobType,
      JobPriority priority,
      String nodeId,
      Instant timestamp) {
    this.jobId = jobId;
    this.businessKey = businessKey;
    this.jobType = jobType;
    this.priority = priority;
    this.nodeId = nodeId;
    this.timestamp = timestamp;
  }

  /** Creates a new event with timestamp defaulting to {@link Instant#now()}. */
  protected AbstractJobSchedulerEvent(
      Long jobId, String businessKey, JobType jobType, JobPriority priority, String nodeId) {
    this(jobId, businessKey, jobType, priority, nodeId, Instant.now());
  }

  public Long getJobId() {
    return jobId;
  }

  public String getBusinessKey() {
    return businessKey;
  }

  public JobType getJobType() {
    return jobType;
  }

  public JobPriority getPriority() {
    return priority;
  }

  public String getNodeId() {
    return nodeId;
  }

  public Instant getTimestamp() {
    return timestamp;
  }
}
