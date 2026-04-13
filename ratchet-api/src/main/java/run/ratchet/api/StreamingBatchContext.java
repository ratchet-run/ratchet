package run.ratchet.api;

/**
 * Progress snapshot during the streaming phase of batch construction (before job execution). Unlike
 * {@link BatchContext}, the total item count is unknown because the stream has not been fully
 * consumed.
 *
 * @param batchId the batch parent job ID
 * @param processedItems cumulative items read from the stream
 * @param chunksInserted number of bulk insert operations performed
 * @see BatchContext
 * @see StreamingBatchBuilder#onProgress(java.util.function.Consumer)
 */
public record StreamingBatchContext(long batchId, int processedItems, int chunksInserted) {

  /** Alias for {@link #chunksInserted()}. */
  public int insertOperations() {
    return chunksInserted;
  }

  /** Alias for {@link #processedItems()}. */
  public int itemsStreamed() {
    return processedItems;
  }
}
