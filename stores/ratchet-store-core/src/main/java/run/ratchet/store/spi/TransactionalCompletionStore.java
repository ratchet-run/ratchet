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
package run.ratchet.store.spi;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import run.ratchet.api.Incubating;
import run.ratchet.api.Nullable;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.WorkflowConditionEntity;
import run.ratchet.store.spi.JobStateStore.StateWriteResult;
import run.ratchet.store.util.ExtensionValidation;

/** Optional SQL capability for completing a job in the transaction that ran its body. */
@Incubating
public interface TransactionalCompletionStore {
  /**
   * Resolves an uncertain commit in a new transaction, locking the queue row before the cold row. A
   * running queue row with the requested claim means rolled back; any other queue row means
   * superseded. Without a queue row, success with that claim means committed, cancellation means
   * canceled, and every other state (including a missing job) means superseded.
   */
  TerminalOutcome terminalOutcome(UUID jobId, long claimSeq);

  /**
   * Requires an existing transaction and throws IllegalStateException if the store's entity manager
   * has not joined it. This method must never start a transaction itself.
   */
  void requireJoinedTransaction();

  /**
   * Writes job state in the caller's transaction as the last step of a transactional unit, for the
   * live claim only. Requires the caller's transaction and never starts one. Flushes pending entity
   * changes, then locks the job's queue row with the same exclusive lock as completion and keeps it
   * until the caller's transaction ends. Rows are read current, never from an earlier snapshot of
   * the same transaction. Never retries.
   *
   * <p>With {@code expectedVersion} null, inserts the state at version 0 only when no row exists.
   * Otherwise updates it only when the stored version equals {@code expectedVersion}, and
   * increments the version by one. State is encrypted at rest under the same rules as {@link
   * JobStateStore}.
   *
   * <p>The queue row is locked but not changed, so its version stays as it is. The write changes no
   * field that {@link JobCrudStore#save} writes back.
   *
   * <p>After {@code VERSION_CONFLICT} the caller must roll back its transaction and run nothing
   * more in it. The store may already have marked the transaction rollback-only, for example when
   * an insert fails on the existing row.
   *
   * @param jobId owning job id; never {@code null}
   * @param namespace state namespace; never {@code null} or blank, at most 64 characters
   * @param state serialized JSON blob; never {@code null}
   * @param expectedClaimSeq the executing attempt's claim sequence
   * @param expectedVersion the version the unit started from, or {@code null} when it started with
   *     no stored row; never negative
   * @return {@code UPDATED} when written; {@code VERSION_CONFLICT} when the stored row does not
   *     match {@code expectedVersion} (a row exists where none was expected, no row exists where
   *     one was expected, or the versions differ); {@code SUPERSEDED} when the queue row is
   *     missing, not RUNNING, or has another claim sequence. Nothing is written unless it returns
   *     {@code UPDATED}.
   * @throws IllegalArgumentException before any SQL runs, if the namespace is invalid, the version
   *     is negative, or the state exceeds {@link ExtensionValidation#MAX_STATE_BYTES} UTF-8 bytes
   */
  StateWriteResult writeClaimState(
      UUID jobId,
      String namespace,
      String state,
      long expectedClaimSeq,
      @Nullable Integer expectedVersion);

  /**
   * Reads a job like JobCrudStore.findById, using current rows for completion planning. Requires
   * the caller's transaction; MySQL takes shared locks until that transaction completes.
   */
  Optional<JobEntity> findByIdForCompletion(UUID id);

  /**
   * Reads jobs like JobCrudStore.findByIds, using current rows for completion planning. Requires
   * the caller's transaction; MySQL takes shared locks until that transaction completes.
   */
  List<JobEntity> findByIdsForCompletion(List<UUID> ids);

  /**
   * Reads a page like JobCrudStore.findDependants, with the same ordering and paging. Requires the
   * caller's transaction; MySQL takes shared locks to read current rows.
   */
  List<JobEntity> findDependantsForCompletion(UUID parentJobId, int limit, int offset);

  /**
   * Reads conditions like WorkflowConditionStore.findConditionsByParentJobId. Requires the caller's
   * transaction; MySQL takes shared locks to read current rows.
   */
  List<WorkflowConditionEntity> findConditionsForCompletion(UUID parentJobId);
}
