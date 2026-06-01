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
import run.ratchet.store.dto.BatchProgress;
import run.ratchet.store.entity.BatchEntity;

/** Batch lifecycle and progress tracking operations. */
@Incubating
public interface BatchStore {

  /** Persists a batch row. Transaction attribute: {@code REQUIRED}. */
  BatchEntity saveBatch(BatchEntity batch);

  /** Finds a batch by id. Transaction attribute: {@code SUPPORTS}. */
  Optional<BatchEntity> findBatchById(UUID batchId);

  /**
   * Atomically increments the completed-child counter and returns the post-update snapshot.
   * Transaction attribute: {@code REQUIRED}.
   */
  BatchProgress incrementCompletedAtomic(UUID batchId);

  /**
   * Atomically increments the failed-child counter and returns the post-update snapshot.
   * Transaction attribute: {@code REQUIRED}.
   */
  BatchProgress incrementFailedAtomic(UUID batchId);

  /**
   * Marks a batch as completion-processed when all children are terminal.
   *
   * @param batchId batch parent id
   * @return {@code true} when this call changed the batch to processed, {@code false} when the
   *     batch is missing, already processed, or not yet complete
   *     <p>Transaction attribute: {@code REQUIRED}.
   */
  boolean markBatchCompleteIfReady(UUID batchId);

  /**
   * Finds batches that are complete but whose completion flow has not yet been processed.
   *
   * @param limit maximum number of batch ids to return; implementations may return fewer
   * @return at most {@code limit} recoverable batch ids
   *     <p>Transaction attribute: {@code SUPPORTS}.
   */
  List<UUID> findRecoverableBatchIds(int limit);

  /**
   * Batch-loads batch rows by id. Transaction attribute: {@code SUPPORTS}.
   *
   * <p>Callers should keep {@code batchIds} sized for one backend {@code IN} predicate; stores may
   * split larger lists internally.
   */
  List<BatchEntity> findBatchesByIds(List<UUID> batchIds);

  /**
   * Updates the total expected child count for a batch parent.
   *
   * @param batchId batch parent id
   * @param totalItems final expected child count
   * @return {@code true} when the batch was found and updated, {@code false} when no batch exists
   *     for {@code batchId}
   *     <p>Transaction attribute: {@code REQUIRED}.
   */
  boolean updateBatchTotalItems(UUID batchId, int totalItems);
}
