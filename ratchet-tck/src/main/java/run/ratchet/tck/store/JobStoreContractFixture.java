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

  /**
   * Whether this fixture supports transactional rollback in tests. MongoDB standalone containers do
   * not (no replica set → no sessions → no multi-document transactions); JPA-backed stores do.
   * Contracts that wrap work in a rollback-only transaction must gate on this via JUnit
   * {@code @EnabledIf}.
   */
  default boolean supportsTransactionalRollback() {
    return true;
  }

  /**
   * Returns whether the given throwable represents this store's stale-write / optimistic-lock
   * failure. Lets concurrency contracts assert "exactly one thread observed a stale-write" without
   * coupling the TCK to a specific exception class, and without confusing genuine infrastructure
   * failures with stale-write evidence.
   *
   * <p>Default implementation returns {@code false} — a store MUST override to signal that its
   * {@code save()} throws on version mismatch. Until a store overrides, stale-write contracts will
   * fail their "exactly one" assertion, which is the correct signal that the store has no
   * optimistic-lock detection.
   */
  default boolean isStaleWriteException(Throwable t) {
    return false;
  }
}
