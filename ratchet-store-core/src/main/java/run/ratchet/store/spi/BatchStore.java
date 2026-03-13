package run.ratchet.store.spi;

import run.ratchet.store.dto.BatchProgress;
import run.ratchet.store.entity.BatchEntity;
import java.util.List;
import java.util.Optional;

/** Batch lifecycle and progress tracking operations. */
public interface BatchStore {

  /** Creates or updates the batch progress row for a batch parent job. */
  BatchEntity saveBatch(BatchEntity batch);

  /** Returns the batch progress row for the given parent job ID when it exists. */
  Optional<BatchEntity> findBatchById(long batchId);

  /** Atomically increments the completed-child counter and returns the post-update snapshot. */
  BatchProgress incrementCompletedAtomic(long batchId);

  /** Atomically increments the failed-child counter and returns the post-update snapshot. */
  BatchProgress incrementFailedAtomic(long batchId);

  /** Marks a batch as completion-processed when all children are terminal. */
  boolean markBatchCompleteIfReady(long batchId);

  /** Finds batches that are complete but whose completion flow has not yet been processed. */
  List<Long> findRecoverableBatchIds(int limit);

  /** Updates the total expected child count for a batch parent. */
  boolean updateBatchTotalItems(long batchId, int totalItems);
}
