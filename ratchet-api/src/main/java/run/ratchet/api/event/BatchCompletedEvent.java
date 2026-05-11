package run.ratchet.api.event;

import java.io.Serial;
import java.time.Instant;
import java.util.UUID;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;

/**
 * Fired when all children of a batch reach a terminal state.
 *
 * <p>{@code completedItems} counts successful children and {@code failedItems} counts children that
 * ended in a failed state. The event is still emitted when one or more children failed, after the
 * batch has no remaining pending/running children.
 */
public class BatchCompletedEvent extends AbstractJobSchedulerEvent {

  @Serial private static final long serialVersionUID = 843735174177646423L;

  private final int totalItems;
  private final int completedItems;
  private final int failedItems;

  public BatchCompletedEvent(
      UUID jobId,
      String businessKey,
      JobType jobType,
      JobPriority priority,
      String nodeId,
      Instant timestamp,
      int totalItems,
      int completedItems,
      int failedItems) {
    super(jobId, businessKey, jobType, priority, nodeId, timestamp);
    this.totalItems = totalItems;
    this.completedItems = completedItems;
    this.failedItems = failedItems;
  }

  public BatchCompletedEvent(
      UUID jobId,
      String businessKey,
      JobType jobType,
      JobPriority priority,
      String nodeId,
      int totalItems,
      int completedItems,
      int failedItems) {
    super(jobId, businessKey, jobType, priority, nodeId);
    this.totalItems = totalItems;
    this.completedItems = completedItems;
    this.failedItems = failedItems;
  }

  /** Returns the total number of child jobs in the batch. */
  public int getTotalItems() {
    return totalItems;
  }

  /** Returns the number of child jobs that completed successfully. */
  public int getCompletedItems() {
    return completedItems;
  }

  /** Returns the number of child jobs that failed. */
  public int getFailedItems() {
    return failedItems;
  }
}
