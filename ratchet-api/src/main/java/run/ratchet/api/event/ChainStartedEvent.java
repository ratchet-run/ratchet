package run.ratchet.api.event;

import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;
import java.io.Serial;
import java.time.Instant;

/**
 * Event representing the initiation of a chain job in the scheduler.
 *
 * <p>This event is triggered when a chain of jobs has started, providing additional context about
 * the parent job that owns the chain.
 *
 * <p>Extends {@link AbstractJobSchedulerEvent} to leverage common job metadata fields.
 */
public class ChainStartedEvent extends AbstractJobSchedulerEvent {

  @Serial private static final long serialVersionUID = -4450548507481291423L;

  /** The ID of the parent job that owns this chain. */
  private final Long parentJobId;

  public ChainStartedEvent(
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

  public ChainStartedEvent(
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
   * Retrieves the ID of the parent job that owns the chain of jobs.
   *
   * @return the ID of the parent job, or {@code null} if no parent job exists.
   */
  public Long getParentJobId() {
    return parentJobId;
  }
}
