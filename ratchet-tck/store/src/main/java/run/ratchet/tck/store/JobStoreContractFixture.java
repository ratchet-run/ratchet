/*
 * Copyright 2026 Ratchet Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package run.ratchet.tck.store;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.UUID;
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

  /**
   * Returns the {@link JobStore} under test.
   *
   * @return non-null store instance.
   * @apiNote The returned store MUST be wrapped in the implementor's transactional boundary so
   *     mutations performed inside contract methods are persisted and visible to subsequent {@link
   *     #cleanupStore()} calls. Each invocation MAY return the same instance — contracts do not
   *     assume per-call fresh stores.
   */
  JobStore store();

  /**
   * Builds a new {@link JobEntity} in {@code PENDING} status, ready to {@link #persist(JobEntity)}.
   *
   * @return non-null transient {@link JobEntity}.
   * @apiNote Implementations MUST populate every NOT-NULL column required by the target database
   *     schema (typically: id, status, attempts, scheduled time, payload, priority, job type,
   *     created/updated timestamps). The contract suite relies on the returned entity being
   *     immediately persistable without further mutation.
   */
  JobEntity newPendingJob();

  /**
   * Builds a new batch-parent {@link JobEntity} ready for {@link #persist(JobEntity)}.
   *
   * @return non-null transient batch-parent {@link JobEntity}.
   * @apiNote Same field-population obligations as {@link #newPendingJob()}, plus the batch metadata
   *     (batch id, expected child count, completion fields) that the batch-store contracts require.
   */
  JobEntity newBatchParentJob();

  /**
   * Removes all rows the contract tests may have created so the next test starts on a clean store.
   *
   * @apiNote Cleanup MUST cover every persistence surface touched by the suite — at minimum the job
   *     queue, batch metadata, terminal/archive tables, lock state, and any signal-delivery
   *     records. Failing to truncate all surfaces produces leaky tests where prior runs surface as
   *     phantom rows.
   */
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
