package run.ratchet.store.util;

import java.util.UUID;
import java.util.function.Function;
import run.ratchet.store.dto.BatchProgress;
import run.ratchet.store.entity.JobPayload;

/** Maps SQL batch progress rows with columns completed, failed, total, progress hook. */
public final class BatchProgressRows {

  private BatchProgressRows() {}

  public static BatchProgress fromCurrentRow(
      UUID batchId, Object[] row, Function<Object, JobPayload> progressHookParser) {
    return progress(
        batchId,
        totalItems(row),
        completedItems(row),
        failedItems(row),
        row[3],
        progressHookParser);
  }

  public static BatchProgress afterCompletedIncrement(
      UUID batchId, Object[] row, Function<Object, JobPayload> progressHookParser) {
    return progress(
        batchId,
        totalItems(row),
        completedItems(row) + 1,
        failedItems(row),
        row[3],
        progressHookParser);
  }

  public static BatchProgress afterFailedIncrement(
      UUID batchId, Object[] row, Function<Object, JobPayload> progressHookParser) {
    return progress(
        batchId,
        totalItems(row),
        completedItems(row),
        failedItems(row) + 1,
        row[3],
        progressHookParser);
  }

  public static int completedItems(Object[] row) {
    return intValue(row[0]);
  }

  public static int failedItems(Object[] row) {
    return intValue(row[1]);
  }

  private static int totalItems(Object[] row) {
    return intValue(row[2]);
  }

  private static BatchProgress progress(
      UUID batchId,
      int totalItems,
      int completedItems,
      int failedItems,
      Object progressHook,
      Function<Object, JobPayload> progressHookParser) {
    return new BatchProgress(
        batchId, totalItems, completedItems, failedItems, progressHookParser.apply(progressHook));
  }

  private static int intValue(Object value) {
    return ((Number) value).intValue();
  }
}
