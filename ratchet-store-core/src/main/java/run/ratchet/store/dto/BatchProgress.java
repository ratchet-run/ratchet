package run.ratchet.store.dto;

import run.ratchet.store.entity.JobPayload;
import java.util.UUID;

/**
 * Immutable snapshot of batch progress returned by atomic increment operations.
 *
 * <p>Captures the exact state of a batch at the moment of an atomic increment, enabling progress
 * hooks to be called with accurate, non-duplicated progress values even when multiple child jobs
 * complete simultaneously.
 */
public record BatchProgress(
    UUID batchId, int totalItems, int completedItems, int failedItems, JobPayload progressHook) {

  /** Returns true if the batch has finished processing all items (success or failure). */
  public boolean isComplete() {
    return completedItems + failedItems == totalItems;
  }

  /** Returns the completion percentage (0-100). */
  public int percentComplete() {
    if (totalItems == 0) {
      return 100;
    }
    return (completedItems + failedItems) * 100 / totalItems;
  }
}
