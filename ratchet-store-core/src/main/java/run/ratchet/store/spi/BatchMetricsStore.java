package run.ratchet.store.spi;

import run.ratchet.api.Incubating;
import run.ratchet.store.entity.BatchMetricsEntity;
import java.util.Optional;

/** Batch metrics tracking operations. */
@Incubating
public interface BatchMetricsStore {

  BatchMetricsEntity saveBatchMetrics(BatchMetricsEntity metrics);

  Optional<BatchMetricsEntity> findBatchMetrics(long batchId);

  void addChildExecutionTime(long batchId, long durationMs);

  void finalizeBatchMetrics(long batchId);

  void updateBatchMetricsChildCount(long batchId, int childCount);
}
