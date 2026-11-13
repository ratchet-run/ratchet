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
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.WorkflowConditionEntity;

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
