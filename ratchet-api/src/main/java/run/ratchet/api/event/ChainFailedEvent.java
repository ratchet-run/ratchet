package run.ratchet.api.event;

import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;
import java.io.Serial;
import java.time.Instant;

/**
 * Represents an event triggered when a chain execution fails in the job scheduler.
 *
 * <p>This event contains metadata identifying the failed chain and provides additional details
 * about the failure such as the ID of the parent job and the specific error message associated with
 * the failure.
 *
 * <p>This class extends {@code AbstractJobSchedulerEvent} to include common scheduler event
 * metadata.
 */
public class ChainFailedEvent extends AbstractJobSchedulerEvent {

  @Serial private static final long serialVersionUID = 5542623623918947230L;

  /** The ID of the parent job that owns this chain. */
  private final Long parentJobId;

  /** The error message from the failed chain step. */
  private final String errorMessage;

  public ChainFailedEvent(
      Long jobId,
      String businessKey,
      JobType jobType,
      JobPriority priority,
      String nodeId,
      Instant timestamp,
      Long parentJobId,
      String errorMessage) {
    super(jobId, businessKey, jobType, priority, nodeId, timestamp);
    this.parentJobId = parentJobId;
    this.errorMessage = errorMessage;
  }

  public ChainFailedEvent(
      Long jobId,
      String businessKey,
      JobType jobType,
      JobPriority priority,
      String nodeId,
      Long parentJobId,
      String errorMessage) {
    super(jobId, businessKey, jobType, priority, nodeId);
    this.parentJobId = parentJobId;
    this.errorMessage = errorMessage;
  }

  /**
   * Retrieves the ID of the parent job associated with this chain event.
   *
   * @return the ID of the parent job, or {@code null} if no parent job is defined.
   */
  public Long getParentJobId() {
    return parentJobId;
  }

  /**
   * Retrieves the error message associated with the event.
   *
   * @return the error message describing the failure or issue.
   */
  public String getErrorMessage() {
    return errorMessage;
  }
}
