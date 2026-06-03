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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.store.entity.BatchMetricsEntity;
import run.ratchet.tck.util.ConcurrentTestRunner;

/** Base contract tests for {@code BatchStore}, including batch metrics. */
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

    var progress = batchStore().incrementCompletedAtomic(parent.getId());

    assertEquals(2, progress.totalItems());
    assertEquals(1, progress.completedItems());
    assertEquals(0, progress.failedItems());
  }

  @Test
  void findRecoverableBatchIds_exposesUnprocessedCompletedBatches() {
    var parent = persist(newBatchParentJob());
    persistBatch(parent.getId(), 2);
    batchStore().incrementCompletedAtomic(parent.getId());
    batchStore().incrementCompletedAtomic(parent.getId());

    List<UUID> recoverable = batchStore().findRecoverableBatchIds(10);

    assertTrue(
        recoverable.contains(parent.getId()),
        "Completed batch should be visible to recovery before completion is marked");
    assertTrue(batchStore().markBatchCompleteIfReady(parent.getId()));
    assertEquals(
        Boolean.TRUE,
        batchStore().findBatchById(parent.getId()).orElseThrow().getCompletionProcessed());
  }

  @Test
  void markBatchCompleteIfReady_isIdempotent_andLeavesRecoverableSet() {
    var parent = persist(newBatchParentJob());
    persistBatch(parent.getId(), 2);
    batchStore().incrementCompletedAtomic(parent.getId());
    batchStore().incrementCompletedAtomic(parent.getId());

    // First finalization wins and flips completion_processed -> TRUE.
    assertTrue(batchStore().markBatchCompleteIfReady(parent.getId()));

    // A second call — from the recovery sweep on another node, or a redelivered child completion —
    // must no-op. The completion_processed = FALSE guard is the only thing stopping a duplicate
    // BatchCompletedEvent / parent-terminal transition across nodes.
    assertFalse(
        batchStore().markBatchCompleteIfReady(parent.getId()),
        "Second markBatchCompleteIfReady must no-op once completion is processed");
    assertFalse(
        batchStore().findRecoverableBatchIds(10).contains(parent.getId()),
        "Finalized batch must leave the recoverable set so recovery cannot re-finalize it");
  }

  @Test
  void incrementFailedAtomic_returnsUpdatedSnapshot() {
    var parent = persist(newBatchParentJob());
    persistBatch(parent.getId(), 2);

    var progress = batchStore().incrementFailedAtomic(parent.getId());

    assertEquals(2, progress.totalItems());
    assertEquals(0, progress.completedItems());
    assertEquals(1, progress.failedItems());
  }

  @Test
  void incrementCompletedAtomic_unknownBatch_throwsIllegalStateException() {
    assertThrows(
        IllegalStateException.class,
        () -> batchStore().incrementCompletedAtomic(new UUID(0L, Long.MAX_VALUE)),
        "Missing batch increments should fail with the BatchStore contract exception");
  }

  @Test
  void incrementCompletedAtomic_concurrent_allCountsCaptured() {
    int threadCount = 10;
    var parent = persist(newBatchParentJob());
    persistBatch(parent.getId(), threadCount);

    Runnable[] tasks = new Runnable[threadCount];
    for (int i = 0; i < threadCount; i++) {
      tasks[i] = () -> batchStore().incrementCompletedAtomic(parent.getId());
    }
    ConcurrentTestRunner.runAll(Duration.ofSeconds(10), tasks);

    var batch = batchStore().findBatchById(parent.getId()).orElseThrow();
    assertEquals(
        threadCount,
        batch.getCompletedItems(),
        "All concurrent increments must be captured — server-side atomicity required");
  }

  @Test
  void markBatchCompleteIfReady_returnsFalseWhenIncomplete() {
    var parent = persist(newBatchParentJob());
    persistBatch(parent.getId(), 3);
    batchStore().incrementCompletedAtomic(parent.getId());

    boolean ready = batchStore().markBatchCompleteIfReady(parent.getId());

    assertFalse(ready, "markBatchCompleteIfReady should return false when batch is not complete");
  }

  @Test
  void findBatchById_unknownId_returnsEmpty() {
    var result = batchStore().findBatchById(new UUID(0L, Long.MAX_VALUE));

    assertTrue(result.isEmpty(), "findBatchById with unknown ID should return empty");
  }

  @Test
  void findBatchesByIds_returnsRequestedBatches() {
    var parent1 = persist(newBatchParentJob());
    persistBatch(parent1.getId(), 5);
    var parent2 = persist(newBatchParentJob());
    persistBatch(parent2.getId(), 3);

    var batches = batchStore().findBatchesByIds(List.of(parent1.getId(), parent2.getId()));

    assertEquals(2, batches.size(), "findBatchesByIds should return both requested batches");
  }

  @Test
  void updateBatchTotalItems_changesTotalCount() {
    var parent = persist(newBatchParentJob());
    persistBatch(parent.getId(), 5);

    boolean updated = batchStore().updateBatchTotalItems(parent.getId(), 10);

    assertTrue(updated, "updateBatchTotalItems should succeed");
    var batch = batchStore().findBatchById(parent.getId()).orElseThrow();
    assertEquals(10, batch.getTotalItems(), "Total items should be updated to 10");
  }

  @Test
  void saveBatchMetrics_andFindByBatchId() {
    var parent = persist(newBatchParentJob());
    persistBatch(parent.getId(), 3);

    BatchMetricsEntity metrics = new BatchMetricsEntity();
    metrics.setBatchId(parent.getId());
    metrics.setChildCount(0);
    metrics.setSuccessCount(0);
    metrics.setFailureCount(0);
    metrics.setStartedAt(Instant.now());

    batchStore().saveBatchMetrics(metrics);

    var found = batchStore().findBatchMetrics(parent.getId());
    assertTrue(found.isPresent(), "findBatchMetrics should return the persisted metrics");
    assertEquals(parent.getId(), found.get().getBatchId());
  }

  @Test
  void addChildExecutionTime_accumulates() {
    var parent = persist(newBatchParentJob());
    persistBatch(parent.getId(), 3);

    BatchMetricsEntity metrics = new BatchMetricsEntity();
    metrics.setBatchId(parent.getId());
    metrics.setChildCount(0);
    metrics.setSuccessCount(0);
    metrics.setFailureCount(0);
    metrics.setStartedAt(Instant.now());

    batchStore().saveBatchMetrics(metrics);

    batchStore().addChildExecutionTime(parent.getId(), 100L);
    batchStore().addChildExecutionTime(parent.getId(), 250L);

    var found = batchStore().findBatchMetrics(parent.getId());
    assertTrue(found.isPresent(), "Metrics should still exist after adding execution times");
    assertEquals(
        350L,
        found.get().getChildExecutionMs(),
        "childExecutionMs should accumulate both additions");
  }

  @Test
  void findBatchMetrics_unknownBatch_returnsEmpty() {
    var result = batchStore().findBatchMetrics(new UUID(0L, Long.MAX_VALUE));

    assertTrue(result.isEmpty(), "findBatchMetrics for unknown batch should return empty");
  }

  @Test
  void finalizeBatchMetrics_setsCompletionFields() {
    var parent = persist(newBatchParentJob());
    persistBatch(parent.getId(), 2);

    BatchMetricsEntity metrics = newMetrics(parent.getId());
    batchStore().saveBatchMetrics(metrics);

    batchStore().finalizeBatchMetrics(parent.getId());

    var found = batchStore().findBatchMetrics(parent.getId()).orElseThrow();
    assertNotNull(found.getCompletedAt(), "finalizeBatchMetrics should set completedAt");
  }

  @Test
  void finalizeBatchMetrics_preservesSubSecondPrecision() {
    var parent = persist(newBatchParentJob());
    persistBatch(parent.getId(), 1);

    BatchMetricsEntity metrics = newMetrics(parent.getId());
    metrics.setStartedAt(Instant.now().minusMillis(1_500));
    batchStore().saveBatchMetrics(metrics);
    batchStore().addChildExecutionTime(parent.getId(), 250L);

    batchStore().finalizeBatchMetrics(parent.getId());

    var found = batchStore().findBatchMetrics(parent.getId()).orElseThrow();
    assertTrue(
        found.getTotalDurationMs() >= 1_200L,
        "finalizeBatchMetrics should preserve elapsed milliseconds, not truncate to seconds");
    assertEquals(
        found.getTotalDurationMs() - 250L,
        found.getOverheadMs(),
        "overheadMs should be totalDurationMs minus accumulated child execution time");
  }

  @Test
  void finalizeBatchMetrics_isIdempotent() throws InterruptedException {
    var parent = persist(newBatchParentJob());
    persistBatch(parent.getId(), 1);

    BatchMetricsEntity metrics = newMetrics(parent.getId());
    metrics.setStartedAt(Instant.now().minusSeconds(5));
    batchStore().saveBatchMetrics(metrics);
    batchStore().addChildExecutionTime(parent.getId(), 100L);

    batchStore().finalizeBatchMetrics(parent.getId());
    var first = batchStore().findBatchMetrics(parent.getId()).orElseThrow();
    Instant completedAt = first.getCompletedAt();
    Long totalDurationMs = first.getTotalDurationMs();
    Long overheadMs = first.getOverheadMs();

    Thread.sleep(25);
    batchStore().finalizeBatchMetrics(parent.getId());

    var second = batchStore().findBatchMetrics(parent.getId()).orElseThrow();
    assertEquals(completedAt, second.getCompletedAt(), "completedAt should not be rewritten");
    assertEquals(
        totalDurationMs, second.getTotalDurationMs(), "totalDurationMs should not be rewritten");
    assertEquals(overheadMs, second.getOverheadMs(), "overheadMs should not be rewritten");
  }

  @Test
  void finalizeBatchMetrics_unknownBatch_isNoOp() {
    assertDoesNotThrow(
        () -> batchStore().finalizeBatchMetrics(new UUID(0L, Long.MAX_VALUE)),
        "finalizeBatchMetrics for unknown batch should not throw");
  }

  @Test
  void updateBatchMetricsChildCount_updatesCount() {
    var parent = persist(newBatchParentJob());
    persistBatch(parent.getId(), 5);

    BatchMetricsEntity metrics = newMetrics(parent.getId());
    batchStore().saveBatchMetrics(metrics);

    batchStore().updateBatchMetricsChildCount(parent.getId(), 5);

    var found = batchStore().findBatchMetrics(parent.getId()).orElseThrow();
    assertEquals(5, found.getChildCount(), "childCount should be updated");
  }

  @Test
  void addChildExecutionTime_concurrent_allTimesAccumulated() {
    int threadCount = 10;
    long timePerThread = 100L;
    var parent = persist(newBatchParentJob());
    persistBatch(parent.getId(), threadCount);

    BatchMetricsEntity metrics = newMetrics(parent.getId());
    batchStore().saveBatchMetrics(metrics);

    Runnable[] tasks = new Runnable[threadCount];
    for (int i = 0; i < threadCount; i++) {
      tasks[i] = () -> batchStore().addChildExecutionTime(parent.getId(), timePerThread);
    }
    ConcurrentTestRunner.runAll(Duration.ofSeconds(10), tasks);

    var found = batchStore().findBatchMetrics(parent.getId()).orElseThrow();
    assertEquals(
        threadCount * timePerThread,
        found.getChildExecutionMs(),
        "All concurrent time additions must be captured — server-side atomicity required");
  }

  @Test
  void saveBatchMetrics_idempotentOnDuplicate() {
    var parent = persist(newBatchParentJob());
    persistBatch(parent.getId(), 3);

    BatchMetricsEntity metrics = newMetrics(parent.getId());
    batchStore().saveBatchMetrics(metrics);
    batchStore().addChildExecutionTime(parent.getId(), 500L);

    // Second save should not lose accumulated data
    var existing = batchStore().findBatchMetrics(parent.getId()).orElseThrow();
    batchStore().saveBatchMetrics(existing);

    var found = batchStore().findBatchMetrics(parent.getId()).orElseThrow();
    assertEquals(
        500L, found.getChildExecutionMs(), "Second save should not reset accumulated data");
  }

  @Test
  void addChildExecutionTime_unknownBatch_isNoOp() {
    assertDoesNotThrow(
        () -> batchStore().addChildExecutionTime(new UUID(0L, Long.MAX_VALUE), 100L),
        "addChildExecutionTime for unknown batch should not throw");
  }

  private BatchMetricsEntity newMetrics(UUID batchId) {
    BatchMetricsEntity metrics = new BatchMetricsEntity();
    metrics.setBatchId(batchId);
    metrics.setChildCount(0);
    metrics.setSuccessCount(0);
    metrics.setFailureCount(0);
    metrics.setStartedAt(Instant.now());
    return metrics;
  }
}
