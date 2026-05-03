package run.ratchet.tck.store;

import run.ratchet.store.entity.BatchEntity;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.spi.JobStore;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.UUID;

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

  default JobEntity newPendingJob(String... tags) {
    JobEntity job = newPendingJob();
    if (tags.length > 0) {
      job.setTags(new ArrayList<>(Arrays.asList(tags)));
    }
    return job;
  }

  default JobEntity persist(JobEntity job) {
    return store().save(job);
  }

  default BatchEntity persistBatch(UUID batchId, int totalItems) {
    BatchEntity batch = new BatchEntity();
    batch.setId(batchId);
    batch.setTotalItems(totalItems);
    batch.setCompletedItems(0);
    batch.setFailedItems(0);
    batch.setCompletionProcessed(false);
    return store().saveBatch(batch);
  }

  // false for MongoDB standalone
  default boolean supportsTransactionalRollback() {
    return true;
  }

  /** Returns true if the throwable represents this store's optimistic-lock failure. */
  default boolean isStaleWriteException(Throwable t) {
    return false;
  }
}
