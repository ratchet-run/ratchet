package run.ratchet.api;

/**
 * Immutable snapshot of batch execution progress, passed to progress hooks and workflow conditions.
 *
 * @param batchId the batch job identifier
 * @param totalItems total child jobs in the batch
 * @param completedItems successfully completed child jobs
 * @param failedItems failed child jobs
 * @see BatchBuilder#onProgress(SerializableConsumer)
 * @see WorkflowCondition#batchCustom(SerializablePredicate)
 */
public record BatchContext(long batchId, int totalItems, int completedItems, int failedItems) {

  /**
   * @return true if all items have been processed (completed + failed >= total)
   */
  public boolean isComplete() {
    return (completedItems + failedItems) >= totalItems;
  }

  /**
   * Calculates the completion percentage of the batch operation.
   *
   * <p>The percentage is calculated as (completedItems * 100) / totalItems. If totalItems is zero
   * (empty batch), returns 100% to indicate completion.
   *
   * @return the percentage of completion as an integer value between 0 and 100
   */
  public int percentDone() {
    return totalItems == 0 ? 100 : (completedItems * 100) / totalItems;
  }

  /**
   * Calculates the success rate of completed items in the batch.
   *
   * <p>The success rate is calculated as the ratio of successful completions to total completed
   * items (both successful and failed). Returns 1.0 if no items have been completed yet.
   *
   * @return the success rate as a double between 0.0 and 1.0
   */
  public double successRate() {
    int processed = completedItems + failedItems;
    return processed == 0 ? 1.0 : (double) completedItems / processed;
  }
}
