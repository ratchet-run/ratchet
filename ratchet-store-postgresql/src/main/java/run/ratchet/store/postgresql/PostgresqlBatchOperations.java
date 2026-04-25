package run.ratchet.store.postgresql;

import run.ratchet.store.converter.PayloadSerializerHolder;
import run.ratchet.store.dto.BatchProgress;
import run.ratchet.store.entity.BatchEntity;
import run.ratchet.store.entity.BatchMetricsEntity;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobPayload;
import run.ratchet.store.spi.BatchMetricsStore;
import run.ratchet.store.spi.BatchStore;
import java.util.List;
import java.util.Optional;
import org.jboss.logging.Logger;

final class PostgresqlBatchOperations implements BatchStore, BatchMetricsStore {

  private static final Logger log = Logger.getLogger(PostgresqlBatchOperations.class);

  private final PostgresqlStoreContext ctx;

  PostgresqlBatchOperations(PostgresqlStoreContext ctx) {
    this.ctx = ctx;
  }

  private JobPayload parseProgressHook(Object jsonValue) {
    if (jsonValue == null) {
      return null;
    }
    try {
      return PayloadSerializerHolder.get().deserialize(jsonValue.toString(), JobPayload.class);
    } catch (IllegalArgumentException e) {
      log.warnf("Bad progress_hook JSON: %s", e.getMessage());
      return null;
    }
  }

  @Override
  public BatchEntity saveBatch(BatchEntity batch) {
    if (ctx.em().find(BatchEntity.class, batch.getId()) == null) {
      ctx.em().persist(batch);
      return batch;
    }
    return ctx.em().merge(batch);
  }

  @Override
  public Optional<BatchEntity> findBatchById(long batchId) {
    BatchEntity batch = ctx.em().find(BatchEntity.class, batchId);
    if (batch != null) {
      ctx.em().refresh(batch);
    }
    return Optional.ofNullable(batch);
  }

  @Override
  public List<BatchEntity> findBatchesByIds(List<Long> batchIds) {
    if (batchIds == null || batchIds.isEmpty()) {
      return List.of();
    }
    List<BatchEntity> batches =
        ctx.em()
            .createQuery("SELECT b FROM BatchEntity b WHERE b.id IN :ids", BatchEntity.class)
            .setParameter("ids", batchIds)
            .getResultList();
    batches.forEach(ctx.em()::refresh);
    return batches;
  }

  @Override
  public BatchProgress incrementCompletedAtomic(long batchId) {
    Object[] row =
        (Object[])
            ctx.em()
                .createNativeQuery(
                    "UPDATE scheduler_batch SET completed_items = completed_items + 1 "
                        + "WHERE batch_id = ? "
                        + "RETURNING completed_items, failed_items, total_items, progress_hook")
                .setParameter(1, batchId)
                .getSingleResult();
    return new BatchProgress(
        batchId,
        ((Number) row[2]).intValue(),
        ((Number) row[0]).intValue(),
        ((Number) row[1]).intValue(),
        parseProgressHook(row[3]));
  }

  @Override
  public BatchProgress incrementFailedAtomic(long batchId) {
    Object[] row =
        (Object[])
            ctx.em()
                .createNativeQuery(
                    "UPDATE scheduler_batch SET failed_items = failed_items + 1 "
                        + "WHERE batch_id = ? "
                        + "RETURNING completed_items, failed_items, total_items, progress_hook")
                .setParameter(1, batchId)
                .getSingleResult();
    return new BatchProgress(
        batchId,
        ((Number) row[2]).intValue(),
        ((Number) row[0]).intValue(),
        ((Number) row[1]).intValue(),
        parseProgressHook(row[3]));
  }

  @Override
  public boolean markBatchCompleteIfReady(long batchId) {
    int updated =
        ctx.em()
            .createNativeQuery(
                "UPDATE scheduler_batch SET completion_processed = TRUE "
                    + "WHERE batch_id = ? AND completion_processed = FALSE "
                    + "AND (completed_items + failed_items) >= total_items")
            .setParameter(1, batchId)
            .executeUpdate();
    return updated > 0;
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<Long> findRecoverableBatchIds(int limit) {
    List<Number> results =
        ctx.em()
            .createNativeQuery(
                "SELECT batch_id FROM scheduler_batch "
                    + "WHERE completion_processed = FALSE "
                    + "AND (completed_items + failed_items) >= total_items "
                    + "LIMIT ?")
            .setParameter(1, limit)
            .getResultList();
    return results.stream().map(Number::longValue).toList();
  }

  @Override
  public boolean updateBatchTotalItems(long batchId, int totalItems) {
    int updated =
        ctx.em()
            .createNativeQuery("UPDATE scheduler_batch SET total_items = ? WHERE batch_id = ?")
            .setParameter(1, totalItems)
            .setParameter(2, batchId)
            .executeUpdate();
    return updated > 0;
  }

  @Override
  public BatchMetricsEntity saveBatchMetrics(BatchMetricsEntity metrics) {
    if (ctx.em().find(BatchMetricsEntity.class, metrics.getBatchId()) == null) {
      if (metrics.getBatchJob() == null) {
        metrics.setBatchJob(ctx.em().getReference(JobEntity.class, metrics.getBatchId()));
      }
      ctx.em().persist(metrics);
      return metrics;
    }
    return ctx.em().merge(metrics);
  }

  @Override
  public Optional<BatchMetricsEntity> findBatchMetrics(long batchId) {
    return Optional.ofNullable(ctx.em().find(BatchMetricsEntity.class, batchId));
  }

  @Override
  public void addChildExecutionTime(long batchId, long durationMs) {
    ctx.em()
        .createNativeQuery(
            "UPDATE scheduler_batch_metrics SET "
                + "child_execution_ms = COALESCE(child_execution_ms, 0) + ?, "
                + "success_count = success_count + 1 "
                + "WHERE batch_id = ?")
        .setParameter(1, durationMs)
        .setParameter(2, batchId)
        .executeUpdate();
  }

  @Override
  public void finalizeBatchMetrics(long batchId) {
    ctx.em()
        .createNativeQuery(
            "UPDATE scheduler_batch_metrics SET "
                + "completed_at = statement_timestamp(), "
                + "total_duration_ms = CASE WHEN started_at IS NOT NULL "
                + "  THEN EXTRACT(EPOCH FROM (statement_timestamp() - started_at))::bigint * 1000 "
                + "  ELSE NULL END, "
                + "overhead_ms = CASE WHEN started_at IS NOT NULL AND child_execution_ms IS NOT NULL "
                + "  THEN EXTRACT(EPOCH FROM (statement_timestamp() - started_at))::bigint * 1000 - child_execution_ms "
                + "  ELSE NULL END "
                + "WHERE batch_id = ?")
        .setParameter(1, batchId)
        .executeUpdate();
  }

  @Override
  public void updateBatchMetricsChildCount(long batchId, int childCount) {
    ctx.em()
        .createNativeQuery("UPDATE scheduler_batch_metrics SET child_count = ? WHERE batch_id = ?")
        .setParameter(1, childCount)
        .setParameter(2, batchId)
        .executeUpdate();
  }
}
