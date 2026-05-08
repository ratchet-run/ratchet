package run.ratchet.store.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import run.ratchet.store.entity.JobPayload;

class BatchProgressRowsTest {

  private final UUID batchId = UUID.fromString("019ae3d1-3f82-7e18-9f09-a9f000000001");
  private final JobPayload progressHook = new JobPayload("Target", "hook", "()V", false, List.of());

  @Test
  void fromCurrentRowMapsReturnedCounterValues() {
    var progress = BatchProgressRows.fromCurrentRow(batchId, row(2, 1, 5), ignored -> progressHook);

    assertEquals(batchId, progress.batchId());
    assertEquals(5, progress.totalItems());
    assertEquals(2, progress.completedItems());
    assertEquals(1, progress.failedItems());
    assertSame(progressHook, progress.progressHook());
  }

  @Test
  void afterCompletedIncrementAddsOneCompletedItem() {
    var progress =
        BatchProgressRows.afterCompletedIncrement(batchId, row(2, 1, 5), ignored -> progressHook);

    assertEquals(3, progress.completedItems());
    assertEquals(1, progress.failedItems());
    assertEquals(5, progress.totalItems());
  }

  @Test
  void afterFailedIncrementAddsOneFailedItem() {
    var progress =
        BatchProgressRows.afterFailedIncrement(batchId, row(2, 1, 5), ignored -> progressHook);

    assertEquals(2, progress.completedItems());
    assertEquals(2, progress.failedItems());
    assertEquals(5, progress.totalItems());
  }

  private static Object[] row(int completedItems, int failedItems, int totalItems) {
    return new Object[] {completedItems, failedItems, totalItems, "{\"type\":\"hook\"}"};
  }
}
