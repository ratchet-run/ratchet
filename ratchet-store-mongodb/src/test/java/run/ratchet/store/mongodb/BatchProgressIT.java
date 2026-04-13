package run.ratchet.store.mongodb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import run.ratchet.store.dto.BatchProgress;
import run.ratchet.store.entity.BatchEntity;
import run.ratchet.store.entity.JobEntity;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class BatchProgressIT extends BaseDocumentStoreIT {

  @Test
  void batchProgressTracking_incrementsAtomically() {
    BatchEntity batch = new BatchEntity();
    batch.setId(1L);
    batch.setTotalItems(3);
    store().saveBatch(batch);

    JobEntity parent = newBatchParentJob();
    store().save(parent);

    for (int i = 0; i < 3; i++) {
      store().save(newBatchChildJob());
    }

    BatchProgress p1 = store().incrementCompletedAtomic(1L);
    assertNotNull(p1);
    assertEquals(1, p1.completedItems());
    assertEquals(3, p1.totalItems());

    BatchProgress p2 = store().incrementCompletedAtomic(1L);
    assertEquals(2, p2.completedItems());

    BatchProgress p3 = store().incrementCompletedAtomic(1L);
    assertEquals(3, p3.completedItems());
    assertEquals(3, p3.totalItems());
  }

  @Test
  void batchProgress_failedItemsTracked() {
    BatchEntity batch = new BatchEntity();
    batch.setId(2L);
    batch.setTotalItems(3);
    store().saveBatch(batch);

    store().incrementCompletedAtomic(2L);
    store().incrementFailedAtomic(2L);
    store().incrementCompletedAtomic(2L);

    Optional<BatchEntity> found = store().findBatchById(2L);
    assertTrue(found.isPresent());
    assertEquals(2, found.get().getCompletedItems());
    assertEquals(1, found.get().getFailedItems());
    assertEquals(3, found.get().getTotalItems());
  }

  @Test
  void markBatchCompleteIfReady_triggersWhenAllDone() {
    BatchEntity batch = new BatchEntity();
    batch.setId(3L);
    batch.setTotalItems(2);
    store().saveBatch(batch);

    store().incrementCompletedAtomic(3L);
    boolean ready1 = store().markBatchCompleteIfReady(3L);
    assertFalse(ready1);

    store().incrementCompletedAtomic(3L);
    boolean ready2 = store().markBatchCompleteIfReady(3L);
    assertTrue(ready2);

    boolean ready3 = store().markBatchCompleteIfReady(3L);
    assertFalse(ready3);
  }
}
