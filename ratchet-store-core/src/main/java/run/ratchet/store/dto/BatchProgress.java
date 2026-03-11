package run.ratchet.store.dto;

/**
 * Immutable snapshot of batch progress returned by atomic increment operations.
 *
 * <p>Captures the exact state of a batch at the moment of an atomic increment, enabling progress
 * hooks to be called with accurate, non-duplicated progress values even when multiple child jobs
 * complete simultaneously.
 *
 * @param batchId the unique identifier of the batch
 * @param totalItems the total number of items in the batch
 * @param completedItems the number of successfully completed items (as of this increment)
 * @param failedItems the number of failed items (as of this increment)
 */
public record BatchProgress(Long batchId, int totalItems, int completedItems, int failedItems) {

  /**
   * Returns true if the batch has finished processing all items (success or failure).
   *
   * @return true if completed + failed equals total
   */
  public boolean isComplete() {
    return completedItems + failedItems == totalItems;
  }

  /**
   * Returns the completion percentage (0-100).
   *
   * @return the percentage of items processed
   */
  public int percentComplete() {
    if (totalItems == 0) {
      return 100;
    }
    return (completedItems + failedItems) * 100 / totalItems;
  }
}
