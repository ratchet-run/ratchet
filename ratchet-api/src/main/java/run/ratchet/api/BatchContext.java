package run.ratchet.api;

/**
 * Immutable context object representing the state and progress of a batch operation.
 *
 * <p>BatchContext provides real-time visibility into batch execution, capturing essential metrics
 * about the progress of child jobs within a batch. This context is passed to progress hooks and
 * workflow conditions, enabling monitoring and conditional logic based on batch state.
 *
 * <h2>Key Information Provided:</h2>
 *
 * <ul>
 *   <li>Unique batch identifier for tracking and correlation
 *   <li>Total number of child jobs in the batch
 *   <li>Count of successfully completed jobs
 *   <li>Count of failed jobs
 *   <li>Calculated completion percentage
 * </ul>
 *
 * <h2>Usage Example:</h2>
 *
 * <pre>{@code
 * // In a progress hook
 * .onProgress(context -> {
 *     log.info("Batch {} is {}% complete ({}/{} items)",
 *         context.batchId(),
 *         context.percentDone(),
 *         context.completedItems(),
 *         context.totalItems());
 *
 *     if (context.failedItems() > 0) {
 *         log.warn("Batch has {} failures", context.failedItems());
 *     }
 * })
 *
 * // In a workflow condition
 * .thenWhenBatch(context -> context.failedItems() == 0,
 *                () -> sendSuccessNotification())
 * }</pre>
 *
 * @param batchId the unique identifier of the batch job
 * @param totalItems the total number of child jobs in the batch
 * @param completedItems the number of child jobs that have completed successfully
 * @param failedItems the number of child jobs that have failed
 * @see BatchBuilder#onProgress(SerializableConsumer)
 * @see WorkflowCondition#batchCustom(SerializablePredicate)
 */
public record BatchContext(long batchId, int totalItems, int completedItems, int failedItems) {

  /**
   * Checks if the batch has completed processing all items.
   *
   * <p>A batch is considered complete when the sum of completed and failed items equals the total
   * number of items in the batch.
   *
   * @return true if all items have been processed, false otherwise
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
