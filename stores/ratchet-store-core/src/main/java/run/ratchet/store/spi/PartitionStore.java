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
import run.ratchet.store.dto.PartitionGenerationWrite;
import run.ratchet.store.entity.PartitionEntity;
import run.ratchet.store.entity.PartitionSetEntity;

/** Optional durable partition-set capability. Set and partition rows are never archived. */
@Incubating
public interface PartitionStore {

  /** Finds a set by its caller key. Transaction attribute: {@code SUPPORTS}. */
  Optional<PartitionSetEntity> findPartitionSet(String setKey);

  /** Finds a set by id. Transaction attribute: {@code SUPPORTS}. */
  Optional<PartitionSetEntity> findPartitionSetById(UUID setId);

  /**
   * Atomically creates or advances the set, partition rows, parent job and batch. A duplicate key
   * or stale version returns false without writing. SQL joins the caller transaction; MongoDB uses
   * one ClientSession transaction. Transaction attribute: {@code REQUIRED}.
   */
  boolean createOrAdvance(PartitionGenerationWrite write);

  /**
   * Pages partitions by ascending index, exclusively after the cursor (-1 initially). Transaction
   * attribute: {@code SUPPORTS}.
   */
  List<PartitionEntity> findPartitions(UUID setId, int afterIndex, int limit);

  /**
   * Pages partitions of a generation by ascending index, exclusively after the cursor. Transaction
   * attribute: {@code SUPPORTS}.
   */
  List<PartitionEntity> findGenerationPartitions(
      UUID setId, int generation, int afterIndex, int limit);

  /**
   * Idempotently records outcomes, errors and checkpoints only for matching generation and child
   * ids. Returns the number of rows updated. Transaction attribute: {@code REQUIRED}.
   */
  int sealPartitions(UUID setId, int generation, List<PartitionEntity> outcomes);

  /**
   * Moves the sealed generation forward, returning whether it changed. Transaction attribute:
   * {@code REQUIRED}.
   */
  boolean markSealed(UUID setId, int generation);

  /** Finds sets whose latest generation is not sealed. Transaction attribute: {@code SUPPORTS}. */
  List<PartitionSetEntity> findUnsealedSets(int limit);

  /** Finds a child partition row; empty when absent. Transaction attribute: {@code SUPPORTS}. */
  Optional<PartitionEntity> findPartitionByChild(UUID childJobId);

  /**
   * Counts only BATCH_CHILD jobs depending on the batch parent. Transaction attribute: {@code
   * SUPPORTS}.
   */
  long countBatchChildren(UUID batchId);

  /**
   * Counts only BATCH_CHILD jobs not SUCCEEDED, FAILED or CANCELED. Transaction attribute: {@code
   * SUPPORTS}.
   */
  long countUnfinishedBatchChildren(UUID batchId);

  /**
   * Deletes a fully sealed set and cascades its partition rows; refuses unsealed sets. Transaction
   * attribute: {@code REQUIRED}.
   */
  boolean deletePartitionSet(UUID setId);
}
