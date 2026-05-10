package run.ratchet.api.event;

import java.io.Serial;
import java.time.Instant;
import java.util.UUID;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;

/** Fired when a batch is about to complete (before callbacks). */
public class BatchCompletingEvent extends AbstractJobSchedulerEvent {

  @Serial private static final long serialVersionUID = 2629383623872540166L;

  private final int totalItems;
  private final int completedItems;
  private final int failedItems;

  public BatchCompletingEvent(
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

  public BatchCompletingEvent(
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

  /** Returns the number of child jobs completed before callbacks run. */
  public int getCompletedItems() {
    return completedItems;
  }

  /** Returns the number of child jobs that failed before callbacks run. */
  public int getFailedItems() {
    return failedItems;
  }
}
