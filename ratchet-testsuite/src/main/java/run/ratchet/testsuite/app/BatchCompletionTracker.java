package run.ratchet.testsuite.app;

import run.ratchet.api.BatchContext;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

/** Tracks batch progress callbacks and completion status for integration tests. */
public class BatchCompletionTracker {

  private static final CopyOnWriteArrayList<BatchContext> PROGRESS_SNAPSHOTS =
      new CopyOnWriteArrayList<>();
  private static final AtomicBoolean BATCH_COMPLETE = new AtomicBoolean(false);

  public static void onProgress(BatchContext context) {
    PROGRESS_SNAPSHOTS.add(context);
    if (context.isComplete()) {
      BATCH_COMPLETE.set(true);
    }
  }

  public static List<BatchContext> progressSnapshots() {
    return List.copyOf(PROGRESS_SNAPSHOTS);
  }

  public static boolean isBatchComplete() {
    return BATCH_COMPLETE.get();
  }

  public static void reset() {
    PROGRESS_SNAPSHOTS.clear();
    BATCH_COMPLETE.set(false);
  }
}
