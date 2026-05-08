package run.ratchet.store.postgresql;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.jboss.logging.Logger;
import run.ratchet.store.converter.PayloadSerializerHolder;
import run.ratchet.store.dto.BatchProgress;
import run.ratchet.store.entity.BatchEntity;
import run.ratchet.store.entity.BatchMetricsEntity;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobPayload;
import run.ratchet.store.spi.BatchMetricsStore;
import run.ratchet.store.spi.BatchStore;
import run.ratchet.store.util.BatchProgressRows;

final class PostgresqlBatchOperations implements BatchStore, BatchMetricsStore {

  private static final Logger log = Logger.getLogger(PostgresqlBatchOperations.class);

  private final PostgresqlStoreContext ctx;

  PostgresqlBatchOperations(PostgresqlStoreContext ctx) {
    this.ctx = ctx;
  }

  @Override
  public BatchEntity saveBatch(BatchEntity batch) {
    // language=PostgreSQL
    String sql =
        """
        INSERT INTO scheduler_batch
          (batch_id, total_items, completed_items, failed_items,
           completion_processed, progress_hook)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT (batch_id) DO UPDATE SET
          total_items = EXCLUDED.total_items,
          completed_items = EXCLUDED.completed_items,
          failed_items = EXCLUDED.failed_items,
          completion_processed = EXCLUDED.completion_processed,
          progress_hook = EXCLUDED.progress_hook,
          version = scheduler_batch.version + 1
        """;
    ctx.em()
        .createNativeQuery(sql)
        .setParameter(1, batch.getId())
        .setParameter(2, batch.getTotalItems())
        .setParameter(3, batch.getCompletedItems())
        .setParameter(4, batch.getFailedItems())
        .setParameter(5, Boolean.TRUE.equals(batch.getCompletionProcessed()))
        .setParameter(6, progressHookJson(batch.getProgressHook()))
        .executeUpdate();
    ctx.em().flush();
    return findBatchById(batch.getId()).orElse(batch);
  }

  @Override
  public Optional<BatchEntity> findBatchById(UUID batchId) {
    BatchEntity batch = ctx.em().find(BatchEntity.class, batchId);
    refreshIfManaged(batch);
    return Optional.ofNullable(batch);
  }

  @Override
  public List<BatchEntity> findBatchesByIds(List<UUID> batchIds) {
    if (batchIds == null || batchIds.isEmpty()) {
      return List.of();
    }
    // language=JPAQL
    String jpql = "SELECT b FROM BatchEntity b WHERE b.id IN :ids";
    List<BatchEntity> batches =
        ctx.em().createQuery(jpql, BatchEntity.class).setParameter("ids", batchIds).getResultList();
    batches.forEach(this::refreshIfManaged);
    return batches;
  }

  @Override
  public BatchProgress incrementCompletedAtomic(UUID batchId) {
    return incrementAtomic(batchId, BatchCounter.COMPLETED);
  }

  @Override
  public BatchProgress incrementFailedAtomic(UUID batchId) {
    return incrementAtomic(batchId, BatchCounter.FAILED);
  }

  private BatchProgress incrementAtomic(UUID batchId, BatchCounter counter) {
    // language=PostgreSQL
    String sql =
        String.format(
            """
        UPDATE scheduler_batch SET %1$s = %1$s + 1
        WHERE batch_id = ?
        RETURNING completed_items, failed_items, total_items, progress_hook
        """,
            counter.columnName);
    Object[] row =
        (Object[]) ctx.em().createNativeQuery(sql).setParameter(1, batchId).getSingleResult();
    return BatchProgressRows.fromCurrentRow(batchId, row, this::parseProgressHook);
  }

  @Override
  public boolean markBatchCompleteIfReady(UUID batchId) {
    // language=PostgreSQL
    String sql =
        """
        UPDATE scheduler_batch SET completion_processed = TRUE
        WHERE batch_id = ? AND completion_processed = FALSE
          AND (completed_items + failed_items) >= total_items
        """;
    int updated = ctx.em().createNativeQuery(sql).setParameter(1, batchId).executeUpdate();
    return updated > 0;
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<UUID> findRecoverableBatchIds(int limit) {
    // language=PostgreSQL
    String sql =
        """
        SELECT batch_id FROM scheduler_batch
        WHERE completion_processed = FALSE
          AND (completed_items + failed_items) >= total_items
        LIMIT ?
        """;
    List<?> results = ctx.em().createNativeQuery(sql).setParameter(1, limit).getResultList();
    return results.stream().map(PostgresqlJobRowMapper::uuidOrNull).toList();
  }

  @Override
  public boolean updateBatchTotalItems(UUID batchId, int totalItems) {
    // language=PostgreSQL
    String sql = "UPDATE scheduler_batch SET total_items = ? WHERE batch_id = ?";
    int updated =
        ctx.em()
            .createNativeQuery(sql)
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
  public Optional<BatchMetricsEntity> findBatchMetrics(UUID batchId) {
    return Optional.ofNullable(ctx.em().find(BatchMetricsEntity.class, batchId));
  }

  @Override
  public void addChildExecutionTime(UUID batchId, long durationMs) {
    // language=PostgreSQL
    String sql =
        """
        UPDATE scheduler_batch_metrics
        SET child_execution_ms = COALESCE(child_execution_ms, 0) + ?,
            success_count = success_count + 1
        WHERE batch_id = ?
        """;
    ctx.em()
        .createNativeQuery(sql)
        .setParameter(1, durationMs)
        .setParameter(2, batchId)
        .executeUpdate();
  }

  @Override
  public void finalizeBatchMetrics(UUID batchId) {
    // language=PostgreSQL
    String sql =
        """
        UPDATE scheduler_batch_metrics
        SET completed_at = statement_timestamp(),
            total_duration_ms = CASE WHEN started_at IS NOT NULL
              THEN EXTRACT(EPOCH FROM (statement_timestamp() - started_at))::bigint * 1000
              ELSE NULL END,
            overhead_ms = CASE WHEN started_at IS NOT NULL AND child_execution_ms IS NOT NULL
              THEN EXTRACT(EPOCH FROM (statement_timestamp() - started_at))::bigint * 1000
                   - child_execution_ms
              ELSE NULL END
        WHERE batch_id = ?
        """;
    ctx.em().createNativeQuery(sql).setParameter(1, batchId).executeUpdate();
  }

  @Override
  public void updateBatchMetricsChildCount(UUID batchId, int childCount) {
    // language=PostgreSQL
    String sql = "UPDATE scheduler_batch_metrics SET child_count = ? WHERE batch_id = ?";
    ctx.em()
        .createNativeQuery(sql)
        .setParameter(1, childCount)
        .setParameter(2, batchId)
        .executeUpdate();
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

  private void refreshIfManaged(BatchEntity batch) {
    if (batch != null && ctx.em().contains(batch)) {
      ctx.em().refresh(batch);
    }
  }

  private String progressHookJson(JobPayload progressHook) {
    return progressHook == null ? null : PayloadSerializerHolder.get().serialize(progressHook);
  }

  private enum BatchCounter {
    COMPLETED("completed_items"),
    FAILED("failed_items");

    private final String columnName;

    BatchCounter(String columnName) {
      this.columnName = columnName;
    }
  }
}
