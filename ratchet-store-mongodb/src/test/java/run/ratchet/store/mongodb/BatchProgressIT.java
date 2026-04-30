package run.ratchet.store.mongodb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import run.ratchet.store.dto.BatchProgress;
import run.ratchet.store.entity.BatchEntity;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.id.UuidV7Factory;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class BatchProgressIT extends BaseDocumentStoreIT {

  @Test
  void batchProgressTracking_incrementsAtomically() {
    UUID batchId = UuidV7Factory.create();
    BatchEntity batch = new BatchEntity();
    batch.setId(batchId);
    batch.setTotalItems(3);
    store().saveBatch(batch);

    JobEntity parent = newBatchParentJob();
    store().save(parent);

    for (int i = 0; i < 3; i++) {
      store().save(newBatchChildJob());
    }

    BatchProgress p1 = store().incrementCompletedAtomic(batchId);
    assertNotNull(p1);
    assertEquals(1, p1.completedItems());
    assertEquals(3, p1.totalItems());

    BatchProgress p2 = store().incrementCompletedAtomic(batchId);
    assertEquals(2, p2.completedItems());

    BatchProgress p3 = store().incrementCompletedAtomic(batchId);
    assertEquals(3, p3.completedItems());
    assertEquals(3, p3.totalItems());
  }

  @Test
  void batchProgress_failedItemsTracked() {
    UUID batchId = UuidV7Factory.create();
    BatchEntity batch = new BatchEntity();
    batch.setId(batchId);
    batch.setTotalItems(3);
    store().saveBatch(batch);

    store().incrementCompletedAtomic(batchId);
    store().incrementFailedAtomic(batchId);
    store().incrementCompletedAtomic(batchId);

    Optional<BatchEntity> found = store().findBatchById(batchId);
    assertTrue(found.isPresent());
    assertEquals(2, found.get().getCompletedItems());
    assertEquals(1, found.get().getFailedItems());
    assertEquals(3, found.get().getTotalItems());
  }

  @Test
  void markBatchCompleteIfReady_triggersWhenAllDone() {
    UUID batchId = UuidV7Factory.create();
    BatchEntity batch = new BatchEntity();
    batch.setId(batchId);
    batch.setTotalItems(2);
    store().saveBatch(batch);

    store().incrementCompletedAtomic(batchId);
    boolean ready1 = store().markBatchCompleteIfReady(batchId);
    assertFalse(ready1);

    store().incrementCompletedAtomic(batchId);
    boolean ready2 = store().markBatchCompleteIfReady(batchId);
    assertTrue(ready2);

    boolean ready3 = store().markBatchCompleteIfReady(batchId);
    assertFalse(ready3);
  }
}
