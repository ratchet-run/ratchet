package run.ratchet.tck.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import run.ratchet.tck.util.ConcurrentTestRunner;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** Base contract tests for {@code BatchStore}. */
public abstract class AbstractBatchStoreContract implements JobStoreContractFixture {

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

    List<Long> recoverable = store().findRecoverableBatchIds(10);

    assertTrue(
        recoverable.contains(parent.getId()),
        "Completed batch should be visible to recovery before completion is marked");
    assertTrue(store().markBatchCompleteIfReady(parent.getId()));
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

  /**
   * N threads concurrently increment the completed counter. The final total must equal N, proving
   * the increment is a server-side atomic operation, not a load-then-save at the application level.
   */
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
    var result = store().findBatchById(Long.MAX_VALUE);

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
