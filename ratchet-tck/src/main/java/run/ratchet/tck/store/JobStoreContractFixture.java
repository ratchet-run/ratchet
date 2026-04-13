package run.ratchet.tck.store;

import run.ratchet.store.entity.BatchEntity;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.spi.JobStore;

/**
 * Fixture contract implemented by concrete TCK suites.
 *
 * <p>Implementors provide a fully configured {@link JobStore} plus factory methods that create
 * valid entities for the target database and persistence stack.
 */
public interface JobStoreContractFixture {

  JobStore store();

  JobEntity newPendingJob();

  JobEntity newBatchParentJob();

  void cleanupStore();

  /** Persists a job using the store under test. */
  default JobEntity persist(JobEntity job) {
    return store().save(job);
  }

  /** Creates and persists a new batch progress row for the supplied parent job ID. */
  default BatchEntity persistBatch(long batchId, int totalItems) {
    BatchEntity batch = new BatchEntity();
    batch.setId(batchId);
    batch.setTotalItems(totalItems);
    batch.setCompletedItems(0);
    batch.setFailedItems(0);
    batch.setCompletionProcessed(false);
    return store().saveBatch(batch);
  }

  /** Whether this fixture supports transactional rollback (false for MongoDB standalone). */
  default boolean supportsTransactionalRollback() {
    return true;
  }

  /** Returns true if the throwable represents this store's optimistic-lock failure. */
  default boolean isStaleWriteException(Throwable t) {
    return false;
  }
}
