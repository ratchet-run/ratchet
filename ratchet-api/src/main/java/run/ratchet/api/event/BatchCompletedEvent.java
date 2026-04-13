package run.ratchet.api.event;

import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;
import java.io.Serial;
import java.time.Instant;

/** Fired when all children of a batch complete. */
public class BatchCompletedEvent extends AbstractJobSchedulerEvent {

  @Serial private static final long serialVersionUID = 843735174177646423L;

  private final Integer totalItems;
  private final Integer completedItems;
  private final Integer failedItems;

  public BatchCompletedEvent(
      Long jobId,
      String businessKey,
      JobType jobType,
      JobPriority priority,
      String nodeId,
      Instant timestamp,
      Integer totalItems,
      Integer completedItems,
      Integer failedItems) {
    super(jobId, businessKey, jobType, priority, nodeId, timestamp);
    this.totalItems = totalItems;
    this.completedItems = completedItems;
    this.failedItems = failedItems;
  }

  public BatchCompletedEvent(
      Long jobId,
      String businessKey,
      JobType jobType,
      JobPriority priority,
      String nodeId,
      Integer totalItems,
      Integer completedItems,
      Integer failedItems) {
    super(jobId, businessKey, jobType, priority, nodeId);
    this.totalItems = totalItems;
    this.completedItems = completedItems;
    this.failedItems = failedItems;
  }

  public Integer getTotalItems() {
    return totalItems;
  }

  public Integer getCompletedItems() {
    return completedItems;
  }

  public Integer getFailedItems() {
    return failedItems;
  }
}
