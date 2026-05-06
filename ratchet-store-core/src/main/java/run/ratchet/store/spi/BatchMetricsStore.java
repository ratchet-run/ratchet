package run.ratchet.store.spi;

import java.util.Optional;
import java.util.UUID;
import run.ratchet.api.Incubating;
import run.ratchet.store.entity.BatchMetricsEntity;

/** Batch metrics tracking operations. */
@Incubating
public interface BatchMetricsStore {

  BatchMetricsEntity saveBatchMetrics(BatchMetricsEntity metrics);

  Optional<BatchMetricsEntity> findBatchMetrics(UUID batchId);

  void addChildExecutionTime(UUID batchId, long durationMs);

  void finalizeBatchMetrics(UUID batchId);

  void updateBatchMetricsChildCount(UUID batchId, int childCount);
}
