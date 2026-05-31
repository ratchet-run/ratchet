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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.store.entity.BatchMetricsEntity;
import run.ratchet.tck.util.ConcurrentTestRunner;

/** Base contract tests for {@code BatchMetricsStore}. */
public abstract class AbstractBatchMetricsStoreContract implements JobStoreContractFixture {

  @BeforeEach
  @AfterEach
  void cleanupBatchMetricsFixture() {
    cleanupStore();
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

    store().saveBatchMetrics(metrics);

    var found = store().findBatchMetrics(parent.getId());
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

    store().saveBatchMetrics(metrics);

    store().addChildExecutionTime(parent.getId(), 100L);
    store().addChildExecutionTime(parent.getId(), 250L);

    var found = store().findBatchMetrics(parent.getId());
    assertTrue(found.isPresent(), "Metrics should still exist after adding execution times");
    assertEquals(
        350L,
        found.get().getChildExecutionMs(),
        "childExecutionMs should accumulate both additions");
  }

  @Test
  void findBatchMetrics_unknownBatch_returnsEmpty() {
    var result = store().findBatchMetrics(new UUID(0L, Long.MAX_VALUE));

    assertTrue(result.isEmpty(), "findBatchMetrics for unknown batch should return empty");
  }

  @Test
  void finalizeBatchMetrics_setsCompletionFields() {
    var parent = persist(newBatchParentJob());
    persistBatch(parent.getId(), 2);

    BatchMetricsEntity metrics = newMetrics(parent.getId());
    store().saveBatchMetrics(metrics);

    store().finalizeBatchMetrics(parent.getId());

    var found = store().findBatchMetrics(parent.getId()).orElseThrow();
    assertNotNull(found.getCompletedAt(), "finalizeBatchMetrics should set completedAt");
  }

  @Test
  void finalizeBatchMetrics_preservesSubSecondPrecision() {
    var parent = persist(newBatchParentJob());
    persistBatch(parent.getId(), 1);

    BatchMetricsEntity metrics = newMetrics(parent.getId());
    metrics.setStartedAt(Instant.now().minusMillis(1_500));
    store().saveBatchMetrics(metrics);
    store().addChildExecutionTime(parent.getId(), 250L);

    store().finalizeBatchMetrics(parent.getId());

    var found = store().findBatchMetrics(parent.getId()).orElseThrow();
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
    store().saveBatchMetrics(metrics);
    store().addChildExecutionTime(parent.getId(), 100L);

    store().finalizeBatchMetrics(parent.getId());
    var first = store().findBatchMetrics(parent.getId()).orElseThrow();
    Instant completedAt = first.getCompletedAt();
    Long totalDurationMs = first.getTotalDurationMs();
    Long overheadMs = first.getOverheadMs();

    Thread.sleep(25);
    store().finalizeBatchMetrics(parent.getId());

    var second = store().findBatchMetrics(parent.getId()).orElseThrow();
    assertEquals(completedAt, second.getCompletedAt(), "completedAt should not be rewritten");
    assertEquals(
        totalDurationMs, second.getTotalDurationMs(), "totalDurationMs should not be rewritten");
    assertEquals(overheadMs, second.getOverheadMs(), "overheadMs should not be rewritten");
  }

  @Test
  void finalizeBatchMetrics_unknownBatch_isNoOp() {
    assertDoesNotThrow(
        () -> store().finalizeBatchMetrics(new UUID(0L, Long.MAX_VALUE)),
        "finalizeBatchMetrics for unknown batch should not throw");
  }

  @Test
  void updateBatchMetricsChildCount_updatesCount() {
    var parent = persist(newBatchParentJob());
    persistBatch(parent.getId(), 5);

    BatchMetricsEntity metrics = newMetrics(parent.getId());
    store().saveBatchMetrics(metrics);

    store().updateBatchMetricsChildCount(parent.getId(), 5);

    var found = store().findBatchMetrics(parent.getId()).orElseThrow();
    assertEquals(5, found.getChildCount(), "childCount should be updated");
  }

  @Test
  void addChildExecutionTime_concurrent_allTimesAccumulated() {
    int threadCount = 10;
    long timePerThread = 100L;
    var parent = persist(newBatchParentJob());
    persistBatch(parent.getId(), threadCount);

    BatchMetricsEntity metrics = newMetrics(parent.getId());
    store().saveBatchMetrics(metrics);

    Runnable[] tasks = new Runnable[threadCount];
    for (int i = 0; i < threadCount; i++) {
      tasks[i] = () -> store().addChildExecutionTime(parent.getId(), timePerThread);
    }
    ConcurrentTestRunner.runAll(Duration.ofSeconds(10), tasks);

    var found = store().findBatchMetrics(parent.getId()).orElseThrow();
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
    store().saveBatchMetrics(metrics);
    store().addChildExecutionTime(parent.getId(), 500L);

    // Second save should not lose accumulated data
    var existing = store().findBatchMetrics(parent.getId()).orElseThrow();
    store().saveBatchMetrics(existing);

    var found = store().findBatchMetrics(parent.getId()).orElseThrow();
    assertEquals(
        500L, found.getChildExecutionMs(), "Second save should not reset accumulated data");
  }

  @Test
  void addChildExecutionTime_unknownBatch_isNoOp() {
    assertDoesNotThrow(
        () -> store().addChildExecutionTime(new UUID(0L, Long.MAX_VALUE), 100L),
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
