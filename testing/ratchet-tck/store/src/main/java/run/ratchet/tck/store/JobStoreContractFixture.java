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
import run.ratchet.store.spi.ArchiveStore;
import run.ratchet.store.spi.BatchStore;
import run.ratchet.store.spi.DlqAlertStore;
import run.ratchet.store.spi.JobAnalyticsStore;
import run.ratchet.store.spi.JobAuditStore;
import run.ratchet.store.spi.JobExtensionStore;
import run.ratchet.store.spi.JobQueryStore;
import run.ratchet.store.spi.JobStore;
import run.ratchet.store.spi.LockStore;
import run.ratchet.store.spi.RecurringJobStore;
import run.ratchet.store.spi.ResourcePermitStore;
import run.ratchet.store.spi.SignalStore;
import run.ratchet.store.spi.WorkflowConditionStore;

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
   * Returns the store viewed as an optional capability, resolved through {@link
   * JobStore#capability}. A capability contract calls the matching accessor for its capability
   * methods (core lifecycle methods still go through {@link #store()}). When the store under test
   * does not advertise the capability the accessor throws — the conditional-conformance harness is
   * responsible for not running a capability's contract against a store that lacks it.
   */
  private <T> T capabilityView(Class<T> type) {
    return store()
        .capability(type)
        .orElseThrow(
            () ->
                // Abort (not fail): a store that does not advertise this optional capability makes
                // the capability's contract not-applicable. JUnit reports the test as skipped, and
                // the conformance report records it as N/A rather than MISSING.
                new org.opentest4j.TestAbortedException(
                    "store under test does not advertise the "
                        + type.getSimpleName()
                        + " capability — capability contract skipped (N/A)"));
  }

  default RecurringJobStore recurringStore() {
    return capabilityView(RecurringJobStore.class);
  }

  default BatchStore batchStore() {
    return capabilityView(BatchStore.class);
  }

  default WorkflowConditionStore workflowConditionStore() {
    return capabilityView(WorkflowConditionStore.class);
  }

  default SignalStore signalStore() {
    return capabilityView(SignalStore.class);
  }

  default ResourcePermitStore resourcePermitStore() {
    return capabilityView(ResourcePermitStore.class);
  }

  default LockStore lockStore() {
    return capabilityView(LockStore.class);
  }

  default ArchiveStore archiveStore() {
    return capabilityView(ArchiveStore.class);
  }

  default JobQueryStore queryStore() {
    return capabilityView(JobQueryStore.class);
  }

  default JobAnalyticsStore analyticsStore() {
    return capabilityView(JobAnalyticsStore.class);
  }

  default JobAuditStore auditStore() {
    return capabilityView(JobAuditStore.class);
  }

  default DlqAlertStore dlqAlertStore() {
    return capabilityView(DlqAlertStore.class);
  }

  default JobExtensionStore extensionStore() {
    return capabilityView(JobExtensionStore.class);
  }

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
    return batchStore().saveBatch(batch);
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
