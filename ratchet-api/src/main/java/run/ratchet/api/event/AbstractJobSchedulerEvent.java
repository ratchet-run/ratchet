package run.ratchet.api.event;

import java.io.Serial;
import java.io.Serializable;
import java.time.Instant;
import java.util.UUID;
import run.ratchet.api.Incubating;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;

/**
 * Base class for per-job lifecycle events published by Ratchet.
 *
 * <p>Subclasses carry a snapshot of public job metadata at the time the event is created. Fields
 * such as {@code businessKey}, {@code jobType}, {@code priority}, and {@code nodeId} may be {@code
 * null} when Ratchet can no longer load the job row after a successful state transition.
 */
@Incubating
public abstract class AbstractJobSchedulerEvent implements Serializable {

  @Serial private static final long serialVersionUID = 6853988277084004625L;

  private final UUID jobId;
  private final String businessKey;
  private final JobType jobType;
  private final JobPriority priority;
  private final String nodeId;
  private final Instant timestamp;

  protected AbstractJobSchedulerEvent(
      UUID jobId,
      String businessKey,
      JobType jobType,
      JobPriority priority,
      String nodeId,
      Instant timestamp) {
    this.jobId = EventContract.requireNonNull(jobId, "jobId");
    this.businessKey = businessKey;
    this.jobType = jobType;
    this.priority = priority;
    this.nodeId = nodeId;
    this.timestamp = EventContract.requireNonNull(timestamp, "timestamp");
  }

  /**
   * Creates an event using {@link Instant#now()}.
   *
   * <p>Tests, replay code, and integrations that need a deterministic timestamp should call the
   * constructor that accepts an explicit {@link Instant}.
   */
  protected AbstractJobSchedulerEvent(
      UUID jobId, String businessKey, JobType jobType, JobPriority priority, String nodeId) {
    this(jobId, businessKey, jobType, priority, nodeId, Instant.now());
  }

  /** Returns the job id affected by this event. */
  public UUID getJobId() {
    return jobId;
  }

  /** Returns the business key captured for the job, or {@code null} when unavailable. */
  public String getBusinessKey() {
    return businessKey;
  }

  /** Returns the public job type captured for the job, or {@code null} when unavailable. */
  public JobType getJobType() {
    return jobType;
  }

  /** Returns the job priority captured for the job, or {@code null} when unavailable. */
  public JobPriority getPriority() {
    return priority;
  }

  /** Returns the node id associated with the event, or {@code null} when unavailable. */
  public String getNodeId() {
    return nodeId;
  }

  /** Returns the instant at which this event object was created. */
  public Instant getTimestamp() {
    return timestamp;
  }
}
