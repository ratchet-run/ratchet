package run.ratchet.store.spi;

import run.ratchet.store.dto.BatchProgress;
import run.ratchet.store.entity.BatchEntity;
import java.util.Optional;

/** Batch lifecycle and progress tracking operations. */
public interface BatchStore {

  BatchEntity saveBatch(BatchEntity batch);

  Optional<BatchEntity> findBatchById(long batchId);

  BatchProgress incrementCompletedAtomic(long batchId);

  BatchProgress incrementFailedAtomic(long batchId);

  boolean markBatchCompleteIfReady(long batchId);

  boolean updateBatchTotalItems(long batchId, int totalItems);
}
