package run.ratchet.api;

import java.util.UUID;

/**
 * Immutable snapshot of batch execution progress, passed to progress hooks and workflow conditions.
 *
 * @param batchId UUIDv7 id of the batch parent job
 * @param totalItems total child jobs in the batch
 * @param completedItems successfully completed child jobs
 * @param failedItems failed child jobs
 * @see BatchBuilder#onProgress(SerializableConsumer)
 * @see WorkflowCondition#batchCustom(SerializablePredicate)
 */
public record BatchContext(UUID batchId, int totalItems, int completedItems, int failedItems) {

  /**
   * @return true if all items have been processed (completed + failed >= total)
   */
  public boolean isComplete() {
    return (completedItems + failedItems) >= totalItems;
  }

  /**
   * @return completion percentage (0–100); returns 100 for an empty batch
   */
  public int percentDone() {
    return totalItems == 0 ? 100 : (completedItems * 100) / totalItems;
  }

  /**
   * @return ratio of successful to total processed items (0.0–1.0); returns 1.0 if none yet
   */
  public double successRate() {
    int processed = completedItems + failedItems;
    return processed == 0 ? 1.0 : (double) completedItems / processed;
  }
}
