package run.ratchet.store.spi;

import java.util.Optional;
import java.util.UUID;
import run.ratchet.api.Incubating;
import run.ratchet.store.entity.BatchMetricsEntity;

/** Batch metrics tracking operations. */
@Incubating
public interface BatchMetricsStore {

  /** Persists batch metrics. Transaction attribute: {@code REQUIRED}. */
  BatchMetricsEntity saveBatchMetrics(BatchMetricsEntity metrics);

  /** Finds metrics for one batch. Transaction attribute: {@code SUPPORTS}. */
  Optional<BatchMetricsEntity> findBatchMetrics(UUID batchId);

  /** Adds one child execution duration. Transaction attribute: {@code REQUIRED}. */
  void addChildExecutionTime(UUID batchId, long durationMs);

  /** Finalizes metrics for a completed batch. Transaction attribute: {@code REQUIRED}. */
  void finalizeBatchMetrics(UUID batchId);

  /** Updates the expected child count. Transaction attribute: {@code REQUIRED}. */
  void updateBatchMetricsChildCount(UUID batchId, int childCount);
}
