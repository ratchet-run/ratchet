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

  /** Returns the store instance under test. */
  JobStore store();

  /** Creates a valid pending non-batch job with unique keys suitable for persistence. */
  JobEntity newPendingJob();

  /** Creates a valid pending batch-parent job with unique keys suitable for persistence. */
  JobEntity newBatchParentJob();

  /** Removes persisted state created by the current test. */
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
}
