package run.ratchet.store.mysql;

import run.ratchet.store.converter.PayloadSerializerHolder;
import run.ratchet.store.dto.BatchProgress;
import run.ratchet.store.entity.BatchEntity;
import run.ratchet.store.entity.BatchMetricsEntity;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobPayload;
import run.ratchet.store.mysql.converter.UuidByteArrayConverter;
import run.ratchet.store.spi.BatchMetricsStore;
import run.ratchet.store.spi.BatchStore;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.jboss.logging.Logger;

final class MysqlBatchOperations implements BatchStore, BatchMetricsStore {

  private static final Logger log = Logger.getLogger(MysqlBatchOperations.class);

  private final MysqlStoreContext ctx;

  MysqlBatchOperations(MysqlStoreContext ctx) {
    this.ctx = ctx;
  }

  @Override
  public BatchEntity saveBatch(BatchEntity batch) {
    // language=MySQL
    String sql =
        """
        INSERT INTO scheduler_batch
          (batch_id, total_items, completed_items, failed_items,
           completion_processed, progress_hook)
        VALUES (?, ?, ?, ?, ?, CAST(? AS JSON))
        ON DUPLICATE KEY UPDATE
          total_items = VALUES(total_items),
          completed_items = VALUES(completed_items),
          failed_items = VALUES(failed_items),
          completion_processed = VALUES(completion_processed),
          progress_hook = VALUES(progress_hook),
          version = version + 1
        """;
    ctx.em()
        .createNativeQuery(sql)
        .setParameter(1, UuidByteArrayConverter.toBytes(batch.getId()))
        .setParameter(2, batch.getTotalItems())
        .setParameter(3, batch.getCompletedItems())
        .setParameter(4, batch.getFailedItems())
        .setParameter(5, Boolean.TRUE.equals(batch.getCompletionProcessed()) ? 1 : 0)
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

  private void refreshIfManaged(BatchEntity batch) {
    if (batch != null && ctx.em().contains(batch)) {
      ctx.em().refresh(batch);
    }
  }

  private String progressHookJson(JobPayload progressHook) {
    return progressHook == null ? null : PayloadSerializerHolder.get().serialize(progressHook);
  }

  @Override
  public BatchProgress incrementCompletedAtomic(UUID batchId) {
    // language=MySQL
    String selectSql =
        """
        SELECT completed_items, failed_items, total_items, progress_hook
        FROM scheduler_batch
        WHERE batch_id = ?
        FOR UPDATE
        """;
    Object[] locked =
        (Object[])
            ctx.em()
                .createNativeQuery(selectSql)
                .setParameter(1, UuidByteArrayConverter.toBytes(batchId))
                .getSingleResult();

    int newCompleted = ((Number) locked[0]).intValue() + 1;
    // language=MySQL
    String updateSql = "UPDATE scheduler_batch SET completed_items = ? WHERE batch_id = ?";
    ctx.em()
        .createNativeQuery(updateSql)
        .setParameter(1, newCompleted)
        .setParameter(2, UuidByteArrayConverter.toBytes(batchId))
        .executeUpdate();

    return new BatchProgress(
        batchId,
        ((Number) locked[2]).intValue(),
        newCompleted,
        ((Number) locked[1]).intValue(),
        parseProgressHook(locked[3]));
  }

  @Override
  public BatchProgress incrementFailedAtomic(UUID batchId) {
    // language=MySQL
    String selectSql =
        """
        SELECT completed_items, failed_items, total_items, progress_hook
        FROM scheduler_batch
        WHERE batch_id = ?
        FOR UPDATE
        """;
    Object[] locked =
        (Object[])
            ctx.em()
                .createNativeQuery(selectSql)
                .setParameter(1, UuidByteArrayConverter.toBytes(batchId))
                .getSingleResult();

    int newFailed = ((Number) locked[1]).intValue() + 1;
    // language=MySQL
    String updateSql = "UPDATE scheduler_batch SET failed_items = ? WHERE batch_id = ?";
    ctx.em()
        .createNativeQuery(updateSql)
        .setParameter(1, newFailed)
        .setParameter(2, UuidByteArrayConverter.toBytes(batchId))
        .executeUpdate();

    return new BatchProgress(
        batchId,
        ((Number) locked[2]).intValue(),
        ((Number) locked[0]).intValue(),
        newFailed,
        parseProgressHook(locked[3]));
  }

  @Override
  public boolean markBatchCompleteIfReady(UUID batchId) {
    // language=MySQL
    String sql =
        """
        UPDATE scheduler_batch SET completion_processed = 1
        WHERE batch_id = ? AND completion_processed = 0
          AND (completed_items + failed_items) >= total_items
        """;
    int updated =
        ctx.em()
            .createNativeQuery(sql)
            .setParameter(1, UuidByteArrayConverter.toBytes(batchId))
            .executeUpdate();
    return updated > 0;
  }

  @Override
  public List<UUID> findRecoverableBatchIds(int limit) {
    // language=MySQL
    String sql =
        """
        SELECT batch_id FROM scheduler_batch
        WHERE completion_processed = 0
          AND (completed_items + failed_items) >= total_items
        LIMIT ?
        """;
    @SuppressWarnings("unchecked")
    List<?> results = ctx.em().createNativeQuery(sql).setParameter(1, limit).getResultList();
    return results.stream().map(MysqlJobRowMapper::uuidOrNull).toList();
  }

  @Override
  public boolean updateBatchTotalItems(UUID batchId, int totalItems) {
    // language=MySQL
    String sql = "UPDATE scheduler_batch SET total_items = ? WHERE batch_id = ?";
    int updated =
        ctx.em()
            .createNativeQuery(sql)
            .setParameter(1, totalItems)
            .setParameter(2, UuidByteArrayConverter.toBytes(batchId))
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
    // language=MySQL
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
        .setParameter(2, UuidByteArrayConverter.toBytes(batchId))
        .executeUpdate();
  }

  @Override
  public void finalizeBatchMetrics(UUID batchId) {
    // language=MySQL
    String sql =
        """
        UPDATE scheduler_batch_metrics
        SET completed_at = NOW(3),
            total_duration_ms = TIMESTAMPDIFF(MICROSECOND, started_at, NOW(3)) / 1000,
            overhead_ms = COALESCE(
              TIMESTAMPDIFF(MICROSECOND, started_at, NOW(3)) / 1000 - child_execution_ms, 0)
        WHERE batch_id = ?
        """;
    ctx.em()
        .createNativeQuery(sql)
        .setParameter(1, UuidByteArrayConverter.toBytes(batchId))
        .executeUpdate();
  }

  @Override
  public void updateBatchMetricsChildCount(UUID batchId, int childCount) {
    // language=MySQL
    String sql = "UPDATE scheduler_batch_metrics SET child_count = ? WHERE batch_id = ?";
    ctx.em()
        .createNativeQuery(sql)
        .setParameter(1, childCount)
        .setParameter(2, UuidByteArrayConverter.toBytes(batchId))
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
}
