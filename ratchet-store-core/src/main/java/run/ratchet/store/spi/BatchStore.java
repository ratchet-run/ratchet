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

  /**
   * Marks a batch as completion-processed when all children are terminal.
   *
   * @param batchId batch parent id
   * @return {@code true} when this call changed the batch to processed, {@code false} when the
   *     batch is missing, already processed, or not yet complete
   */
  boolean markBatchCompleteIfReady(UUID batchId);

  /**
   * Finds batches that are complete but whose completion flow has not yet been processed.
   *
   * @param limit maximum number of batch ids to return; implementations may return fewer
   * @return at most {@code limit} recoverable batch ids
   */
  List<UUID> findRecoverableBatchIds(int limit);

  List<BatchEntity> findBatchesByIds(List<UUID> batchIds);

  /**
   * Updates the total expected child count for a batch parent.
   *
   * @param batchId batch parent id
   * @param totalItems final expected child count
   * @return {@code true} when the batch was found and updated, {@code false} when no batch exists
   *     for {@code batchId}
   */
  boolean updateBatchTotalItems(UUID batchId, int totalItems);
}
