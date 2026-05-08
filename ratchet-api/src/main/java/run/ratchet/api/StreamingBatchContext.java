package run.ratchet.api;

import java.util.UUID;

/**
 * Progress snapshot during the streaming phase of batch construction (before job execution). Unlike
 * {@link BatchContext}, the total item count is unknown because the stream has not been fully
 * consumed.
 *
 * @param batchId UUIDv7 id of the batch parent job
 * @param processedItems cumulative items read from the stream
 * @param chunksInserted number of bulk insert operations performed
 * @see BatchContext
 * @see StreamingBatchBuilder#onProgress(java.util.function.Consumer)
 */
public record StreamingBatchContext(UUID batchId, int processedItems, int chunksInserted) {}
