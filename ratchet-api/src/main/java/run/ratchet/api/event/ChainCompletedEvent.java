package run.ratchet.api.event;

import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;
import java.io.Serial;
import java.time.Instant;

/**
 * Represents an event signaling the completion of a job chain in the job scheduling system.
 *
 * <p>This class extends {@code AbstractJobSchedulerEvent} and adds specific metadata for
 * identifying the parent job associated with the completed chain. It serves as a domain model for
 * events related to the successful execution of job chains.
 *
 * <p>A job chain refers to a sequence of jobs that are logically connected, where the execution of
 * each job depends on certain conditions or completions of prior jobs in the chain.
 *
 * <p>Instances of this class capture key information such as the unique identifiers of the
 * completing job and its parent chain, the business context, processing priority, the node
 * responsible for execution, and the timestamp of event creation.
 *
 * <p>This event can be useful in scenarios such as auditing, logging, or triggering downstream
 * actions once a job chain has entirely completed.
 *
 * <p>Thread-safety: Instances of this class are immutable and inherently thread-safe.
 */
public class ChainCompletedEvent extends AbstractJobSchedulerEvent {

  @Serial private static final long serialVersionUID = -8140882369003276835L;

  /** The ID of the parent job that owns this chain. */
  private final Long parentJobId;

  public ChainCompletedEvent(
      Long jobId,
      String businessKey,
      JobType jobType,
      JobPriority priority,
      String nodeId,
      Instant timestamp,
      Long parentJobId) {
    super(jobId, businessKey, jobType, priority, nodeId, timestamp);
    this.parentJobId = parentJobId;
  }

  public ChainCompletedEvent(
      Long jobId,
      String businessKey,
      JobType jobType,
      JobPriority priority,
      String nodeId,
      Long parentJobId) {
    super(jobId, businessKey, jobType, priority, nodeId);
    this.parentJobId = parentJobId;
  }

  /**
   * Retrieves the ID of the parent job that owns this job chain.
   *
   * @return the parent job ID, or {@code null} if no parent job is associated.
   */
  public Long getParentJobId() {
    return parentJobId;
  }
}
