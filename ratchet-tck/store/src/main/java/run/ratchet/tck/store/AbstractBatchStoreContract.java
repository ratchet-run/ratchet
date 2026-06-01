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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.tck.util.ConcurrentTestRunner;

/** Base contract tests for {@code BatchStore}. */
public abstract class AbstractBatchStoreContract implements JobStoreContractFixture {

  @BeforeEach
  @AfterEach
  void cleanupBatchFixture() {
    cleanupStore();
  }

  @Test
  void incrementCompletedAtomic_returnsUpdatedSnapshot() {
    var parent = persist(newBatchParentJob());
    persistBatch(parent.getId(), 2);

    var progress = store().incrementCompletedAtomic(parent.getId());

    assertEquals(2, progress.totalItems());
    assertEquals(1, progress.completedItems());
    assertEquals(0, progress.failedItems());
  }

  @Test
  void findRecoverableBatchIds_exposesUnprocessedCompletedBatches() {
    var parent = persist(newBatchParentJob());
    persistBatch(parent.getId(), 2);
    store().incrementCompletedAtomic(parent.getId());
    store().incrementCompletedAtomic(parent.getId());

    List<UUID> recoverable = store().findRecoverableBatchIds(10);

    assertTrue(
        recoverable.contains(parent.getId()),
        "Completed batch should be visible to recovery before completion is marked");
    assertTrue(store().markBatchCompleteIfReady(parent.getId()));
    assertEquals(
        Boolean.TRUE, store().findBatchById(parent.getId()).orElseThrow().getCompletionProcessed());
  }

  @Test
  void incrementFailedAtomic_returnsUpdatedSnapshot() {
    var parent = persist(newBatchParentJob());
    persistBatch(parent.getId(), 2);

    var progress = store().incrementFailedAtomic(parent.getId());

    assertEquals(2, progress.totalItems());
    assertEquals(0, progress.completedItems());
    assertEquals(1, progress.failedItems());
  }

  @Test
  void incrementCompletedAtomic_unknownBatch_throwsIllegalStateException() {
    assertThrows(
        IllegalStateException.class,
        () -> store().incrementCompletedAtomic(new UUID(0L, Long.MAX_VALUE)),
        "Missing batch increments should fail with the BatchStore contract exception");
  }

  @Test
  void incrementCompletedAtomic_concurrent_allCountsCaptured() {
    int threadCount = 10;
    var parent = persist(newBatchParentJob());
    persistBatch(parent.getId(), threadCount);

    Runnable[] tasks = new Runnable[threadCount];
    for (int i = 0; i < threadCount; i++) {
      tasks[i] = () -> store().incrementCompletedAtomic(parent.getId());
    }
    ConcurrentTestRunner.runAll(Duration.ofSeconds(10), tasks);

    var batch = store().findBatchById(parent.getId()).orElseThrow();
    assertEquals(
        threadCount,
        batch.getCompletedItems(),
        "All concurrent increments must be captured — server-side atomicity required");
  }

  @Test
  void markBatchCompleteIfReady_returnsFalseWhenIncomplete() {
    var parent = persist(newBatchParentJob());
    persistBatch(parent.getId(), 3);
    store().incrementCompletedAtomic(parent.getId());

    boolean ready = store().markBatchCompleteIfReady(parent.getId());

    assertFalse(ready, "markBatchCompleteIfReady should return false when batch is not complete");
  }

  @Test
  void findBatchById_unknownId_returnsEmpty() {
    var result = store().findBatchById(new UUID(0L, Long.MAX_VALUE));

    assertTrue(result.isEmpty(), "findBatchById with unknown ID should return empty");
  }

  @Test
  void findBatchesByIds_returnsRequestedBatches() {
    var parent1 = persist(newBatchParentJob());
    persistBatch(parent1.getId(), 5);
    var parent2 = persist(newBatchParentJob());
    persistBatch(parent2.getId(), 3);

    var batches = store().findBatchesByIds(List.of(parent1.getId(), parent2.getId()));

    assertEquals(2, batches.size(), "findBatchesByIds should return both requested batches");
  }

  @Test
  void updateBatchTotalItems_changesTotalCount() {
    var parent = persist(newBatchParentJob());
    persistBatch(parent.getId(), 5);

    boolean updated = store().updateBatchTotalItems(parent.getId(), 10);

    assertTrue(updated, "updateBatchTotalItems should succeed");
    var batch = store().findBatchById(parent.getId()).orElseThrow();
    assertEquals(10, batch.getTotalItems(), "Total items should be updated to 10");
  }
}
