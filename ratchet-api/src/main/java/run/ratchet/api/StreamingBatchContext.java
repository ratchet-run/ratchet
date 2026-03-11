package run.ratchet.api;

/**
 * Context object provided during streaming batch construction.
 *
 * <p>Unlike {@link BatchContext} which is available during job execution, StreamingBatchContext is
 * available during the streaming phase when items are being read from the source stream and jobs
 * are being created. It provides information about the streaming progress.
 *
 * <h2>Key Characteristics:</h2>
 *
 * <ul>
 *   <li><b>Streaming Phase</b> - Available during stream consumption, not job execution
 *   <li><b>Progress Tracking</b> - Tracks items processed and chunks inserted
 *   <li><b>Unknown Total</b> - Total items are unknown during streaming (stream not exhausted)
 * </ul>
 *
 * <h2>Usage Example:</h2>
 *
 * <pre>{@code
 * scheduler.streamingBatch("Process Users")
 *     .fromStream(userRepository.streamActiveUsers())
 *     .process((ctx, userId) -> userService.processUser(userId))
 *     .onProgress(ctx -> {
 *         log.info("Streamed {} items in {} chunks",
 *                  ctx.processedItems(), ctx.chunksInserted());
 *     })
 *     .start();
 * }</pre>
 *
 * <h2>Comparison with BatchContext:</h2>
 *
 * <table border="1">
 *   <tr><th>Aspect</th><th>StreamingBatchContext</th><th>BatchContext</th></tr>
 *   <tr><td>Phase</td><td>Stream consumption (job creation)</td><td>Job execution</td></tr>
 *   <tr><td>Total known</td><td>No (stream not exhausted)</td><td>Yes</td></tr>
 *   <tr><td>Progress</td><td>Items streamed</td><td>Items completed/failed</td></tr>
 * </table>
 *
 * <h2>Thread Safety:</h2>
 *
 * <p>This record is immutable and inherently thread-safe. A new instance is created for each
 * progress callback invocation with the current state.
 *
 * @param batchId the unique identifier of the batch being created. This ID corresponds to the batch
 *     parent job's ID and can be used to query batch status or metrics after the streaming phase
 *     completes.
 * @param processedItems the cumulative number of items read from the stream so far. This count
 *     increases with each chunk and represents items that have been converted to child job entities
 *     and inserted into the database.
 * @param chunksInserted the number of bulk insert operations performed so far. Each chunk contains
 *     up to {@code chunkSize} items (default 500). This can be used to estimate database load or to
 *     log progress at regular intervals.
 * @see BatchContext
 * @see StreamingBatchBuilder#onProgress(java.util.function.Consumer)
 */
public record StreamingBatchContext(long batchId, int processedItems, int chunksInserted) {

  /**
   * Returns the approximate number of database insert operations performed.
   *
   * <p>Each chunk typically contains up to the configured chunk size (default 500). The final chunk
   * may contain fewer items.
   *
   * @return the count of bulk insert operations performed
   */
  public int insertOperations() {
    return chunksInserted;
  }

  /**
   * Returns the number of items streamed so far.
   *
   * <p>This is an alias for {@link #processedItems()} for API consistency.
   *
   * @return the count of items read from the stream
   */
  public int itemsStreamed() {
    return processedItems;
  }
}
