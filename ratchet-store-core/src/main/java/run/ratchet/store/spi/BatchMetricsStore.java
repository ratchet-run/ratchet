package run.ratchet.store.spi;

import run.ratchet.api.Incubating;
import run.ratchet.store.entity.BatchMetricsEntity;
import java.util.Optional;

/** Batch metrics tracking operations. */
@Incubating
public interface BatchMetricsStore {

  /** Creates or updates the metrics row associated with a batch parent. */
  BatchMetricsEntity saveBatchMetrics(BatchMetricsEntity metrics);

  /** Loads metrics for a batch parent when they exist. */
  Optional<BatchMetricsEntity> findBatchMetrics(long batchId);

  /** Adds one completed child execution duration to the batch aggregate counters. */
  void addChildExecutionTime(long batchId, long durationMs);

  /** Marks batch metrics as complete and computes any final aggregate values. */
  void finalizeBatchMetrics(long batchId);

  /** Updates the expected child count for a batch metrics record. */
  void updateBatchMetricsChildCount(long batchId, int childCount);
}
