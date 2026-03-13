package run.ratchet.api.event;

import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;
import java.io.Serial;
import java.time.Instant;

/**
 * Represents an event indicating that a batch job has been completed.
 *
 * <p>This event contains information about the total items processed, as well as the count of
 * successfully completed items and items that failed during processing.
 *
 * <p>Extends {@code AbstractJobSchedulerEvent} to include metadata about the job, such as its ID,
 * type, priority, and associated business key.
 */
public class BatchCompletedEvent extends AbstractJobSchedulerEvent {

  @Serial private static final long serialVersionUID = 843735174177646423L;

  /** The total number of items in the batch. */
  private final Integer totalItems;

  /** The number of successfully completed items. */
  private final Integer completedItems;

  /** The number of failed items. */
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

  /**
   * Retrieves the total number of items in the batch.
   *
   * @return the total number of items as an Integer
   */
  public Integer getTotalItems() {
    return totalItems;
  }

  /**
   * Retrieves the number of successfully completed items in the batch.
   *
   * @return the number of completed items as an Integer
   */
  public Integer getCompletedItems() {
    return completedItems;
  }

  /**
   * Retrieves the number of items that failed during the batch process.
   *
   * @return the number of failed items as an Integer
   */
  public Integer getFailedItems() {
    return failedItems;
  }
}
