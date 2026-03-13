package run.ratchet.tck.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
}
