package run.ratchet.store.spi;

import java.util.Optional;
import java.util.UUID;
import run.ratchet.api.Incubating;
import run.ratchet.store.entity.BatchMetricsEntity;

/**
 * Batch metrics tracking operations.
 *
 * <p>Metrics rows are keyed by the batch parent job id. Increment methods are atomic and are no-ops
 * when the metrics row does not exist. Finalization is idempotent: the first call records {@code
 * completedAt}, derives total duration from {@code startedAt}, and derives overhead as {@code
 * totalDurationMs - childExecutionMs}; later calls must leave those finalized values unchanged.
 * Unknown batch ids are ignored by finalization.
 */
@Incubating
public interface BatchMetricsStore {

  /** Persists batch metrics. Transaction attribute: {@code REQUIRED}. */
  BatchMetricsEntity saveBatchMetrics(BatchMetricsEntity metrics);

  /** Finds metrics for one batch. Transaction attribute: {@code SUPPORTS}. */
  Optional<BatchMetricsEntity> findBatchMetrics(UUID batchId);

  /**
   * Adds one successful child execution duration and increments the success count.
   *
   * <p>Unknown batch ids are ignored. Transaction attribute: {@code REQUIRED}.
   */
  void addChildExecutionTime(UUID batchId, long durationMs);

  /**
   * Finalizes metrics for a completed batch exactly once.
   *
   * <p>Unknown batch ids are ignored. Repeated calls after {@code completedAt} is set must not
   * recalculate completion time, duration, or overhead. Transaction attribute: {@code REQUIRED}.
   */
  void finalizeBatchMetrics(UUID batchId);

  /** Updates the expected child count. Transaction attribute: {@code REQUIRED}. */
  void updateBatchMetricsChildCount(UUID batchId, int childCount);
}
