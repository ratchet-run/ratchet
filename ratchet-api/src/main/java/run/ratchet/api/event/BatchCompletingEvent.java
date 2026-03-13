package run.ratchet.api.event;

import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;
import java.io.Serial;
import java.time.Instant;

/**
 * Represents an event triggered upon the completion of a batch in a job scheduler. It provides
 * information about the batch, including the total items in the batch, the number of processed
 * items, and the number of failed items.
 *
 * <p>This event extends {@code AbstractJobSchedulerEvent}, inheriting common job metadata fields
 * such as job ID, business key, job type, priority, node identifier, and timestamp.
 */
public class BatchCompletingEvent extends AbstractJobSchedulerEvent {

  @Serial private static final long serialVersionUID = 2629383623872540166L;

  /** The total number of items in the batch. */
  private final Integer totalItems;

  /** The number of successfully completed items. */
  private final Integer completedItems;

  /** The number of failed items. */
  private final Integer failedItems;

  public BatchCompletingEvent(
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

  public BatchCompletingEvent(
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
   * Retrieves the total number of items in the batch associated with this event.
   *
   * @return the total number of items in the batch
   */
  public Integer getTotalItems() {
    return totalItems;
  }

  /**
   * Retrieves the number of successfully completed items in the batch associated with this event.
   *
   * @return the number of completed items
   */
  public Integer getCompletedItems() {
    return completedItems;
  }

  /**
   * Retrieves the number of failed items in the batch associated with this event.
   *
   * @return the number of failed items
   */
  public Integer getFailedItems() {
    return failedItems;
  }
}
