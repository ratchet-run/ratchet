package run.ratchet.store.spi;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import run.ratchet.api.Incubating;
import run.ratchet.store.dto.BatchProgress;
import run.ratchet.store.entity.BatchEntity;

/** Batch lifecycle and progress tracking operations. */
@Incubating
public interface BatchStore {

  BatchEntity saveBatch(BatchEntity batch);

  Optional<BatchEntity> findBatchById(UUID batchId);

  /** Atomically increments the completed-child counter and returns the post-update snapshot. */
  BatchProgress incrementCompletedAtomic(UUID batchId);

  /** Atomically increments the failed-child counter and returns the post-update snapshot. */
  BatchProgress incrementFailedAtomic(UUID batchId);

  /** Marks a batch as completion-processed when all children are terminal. */
  boolean markBatchCompleteIfReady(UUID batchId);

  /** Finds batches that are complete but whose completion flow has not yet been processed. */
  List<UUID> findRecoverableBatchIds(int limit);

  List<BatchEntity> findBatchesByIds(List<UUID> batchIds);

  /** Updates the total expected child count for a batch parent. */
  boolean updateBatchTotalItems(UUID batchId, int totalItems);
}
