package run.ratchet.store.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

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

  @Test
  void completedItemsReadsFirstColumn() {
    assertEquals(7, BatchProgressRows.completedItems(row(7, 3, 10)));
  }

  @Test
  void failedItemsReadsSecondColumn() {
    assertEquals(3, BatchProgressRows.failedItems(row(7, 3, 10)));
  }

  @Test
  void rejectsRowsWithMissingColumns() {
    var ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                BatchProgressRows.fromCurrentRow(batchId, new Object[] {1, 0, 2}, ignored -> null));

    assertEquals(
        "Batch progress row must contain completed, failed, total, and progress hook columns; got 3",
        ex.getMessage());
  }

  @Test
  void rejectsNullCounterColumns() {
    var ex =
        assertThrows(
            IllegalStateException.class,
            () -> BatchProgressRows.completedItems(new Object[] {null, 0, 1, "{}"}));

    assertEquals("Batch progress counter column is null", ex.getMessage());
  }

  @Test
  void rejectsNonNumericCounterColumns() {
    var ex =
        assertThrows(
            IllegalStateException.class,
            () -> BatchProgressRows.failedItems(new Object[] {0, "one", 1, "{}"}));

    assertEquals("Batch progress counter column is not numeric: java.lang.String", ex.getMessage());
  }

  @Test
  void rejectsMissingProgressHookParserByName() {
    NullPointerException ex =
        assertThrows(
            NullPointerException.class,
            () -> BatchProgressRows.fromCurrentRow(batchId, row(0, 0, 1), null));

    assertEquals("progressHookParser", ex.getMessage());
  }

  @Test
  void wrapsProgressHookParserFailuresWithContext() {
    IllegalStateException ex =
        assertThrows(
            IllegalStateException.class,
            () ->
                BatchProgressRows.afterCompletedIncrement(
                    batchId,
                    row(0, 0, 1),
                    ignored -> {
                      throw new IllegalArgumentException("bad progress hook");
                    }));

    assertEquals("Could not parse batch progress hook", ex.getMessage());
    assertEquals("bad progress hook", ex.getCause().getMessage());
  }

  private static Object[] row(int completedItems, int failedItems, int totalItems) {
    return new Object[] {completedItems, failedItems, totalItems, "{\"type\":\"hook\"}"};
  }
}
