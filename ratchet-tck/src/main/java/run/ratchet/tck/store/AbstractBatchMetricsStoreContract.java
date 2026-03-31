package run.ratchet.tck.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import run.ratchet.store.entity.BatchMetricsEntity;
import java.time.Instant;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** Base contract tests for {@code BatchMetricsStore}. */
public abstract class AbstractBatchMetricsStoreContract implements JobStoreContractFixture {

  @AfterEach
  void cleanupBatchMetricsFixture() {
    cleanupStore();
  }

  @Test
  void saveBatchMetrics_andFindByBatchId() {
    var parent = persist(newBatchParentJob());
    persistBatch(parent.getId(), 3);

    BatchMetricsEntity metrics = new BatchMetricsEntity();
    metrics.setBatchId(parent.getId());
    metrics.setChildCount(0);
    metrics.setSuccessCount(0);
    metrics.setFailureCount(0);
    metrics.setStartedAt(Instant.now());

    store().saveBatchMetrics(metrics);

    var found = store().findBatchMetrics(parent.getId());
    assertTrue(found.isPresent(), "findBatchMetrics should return the persisted metrics");
    assertEquals(parent.getId(), found.get().getBatchId());
  }

  @Test
  void addChildExecutionTime_accumulates() {
    var parent = persist(newBatchParentJob());
    persistBatch(parent.getId(), 3);

    BatchMetricsEntity metrics = new BatchMetricsEntity();
    metrics.setBatchId(parent.getId());
    metrics.setChildCount(0);
    metrics.setSuccessCount(0);
    metrics.setFailureCount(0);
    metrics.setStartedAt(Instant.now());

    store().saveBatchMetrics(metrics);

    store().addChildExecutionTime(parent.getId(), 100L);
    store().addChildExecutionTime(parent.getId(), 250L);

    var found = store().findBatchMetrics(parent.getId());
    assertTrue(found.isPresent(), "Metrics should still exist after adding execution times");
    assertEquals(
        350L,
        found.get().getChildExecutionMs(),
        "childExecutionMs should accumulate both additions");
  }
}
