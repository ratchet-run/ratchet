package run.ratchet.store.postgresql;

import jakarta.persistence.NoResultException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
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
  @SuppressWarnings("unchecked")
  public List<BatchEntity> findBatchesByIds(List<UUID> batchIds) {
    if (batchIds == null || batchIds.isEmpty()) {
      return List.of();
    }
    String placeholders = "?,".repeat(batchIds.size());
    // language=PostgreSQL
    String sql =
        """
        SELECT batch_id, total_items, completed_items, failed_items,
               completion_processed, version, progress_hook
        FROM scheduler_batch
        WHERE batch_id IN (%s)
        """
            .formatted(placeholders.substring(0, placeholders.length() - 1));
    var query = ctx.em().createNativeQuery(sql);
    for (int i = 0; i < batchIds.size(); i++) {
      query.setParameter(i + 1, batchIds.get(i));
    }
    List<Object[]> rows = query.getResultList();
    List<BatchEntity> batches = new ArrayList<>(rows.size());
    for (Object[] row : rows) {
      batches.add(mapBatchRow(row));
    }
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
    try {
      Object result = ctx.em().createNativeQuery(sql).setParameter(1, batchId).getSingleResult();
      return mapIncrementResult(batchId, result, this::parseProgressHook);
    } catch (NoResultException e) {
      throw new IllegalStateException("Batch not found: " + batchId, e);
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("increment batch " + counter.operationName, e);
    }
  }

  static BatchProgress mapIncrementResult(
      UUID batchId, Object result, Function<Object, JobPayload> progressHookParser) {
    if (!(result instanceof Object[] row)) {
      throw new IllegalStateException(
          "Expected batch progress row for batch "
              + batchId
              + " to be Object[] but was "
              + typeName(result));
    }
    if (row.length < 4) {
      throw new IllegalStateException(
          "Expected batch progress row for batch "
              + batchId
              + " to contain at least 4 columns but found "
              + row.length);
    }
    requireNumber(batchId, row, 0, "completed_items");
    requireNumber(batchId, row, 1, "failed_items");
    requireNumber(batchId, row, 2, "total_items");
    return BatchProgressRows.fromCurrentRow(batchId, row, progressHookParser);
  }

  private static void requireNumber(UUID batchId, Object[] row, int index, String columnName) {
    if (!(row[index] instanceof Number)) {
      throw new IllegalStateException(
          "Expected batch progress column "
              + columnName
              + " for batch "
              + batchId
              + " to be numeric but was "
              + typeName(row[index]));
    }
  }

  private static String typeName(Object value) {
    return value == null ? "null" : value.getClass().getName();
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

  private BatchEntity mapBatchRow(Object[] row) {
    BatchEntity batch = new BatchEntity();
    batch.setId(PostgresqlJobRowMapper.uuidOrNull(row[0]));
    batch.setTotalItems(((Number) row[1]).intValue());
    batch.setCompletedItems(((Number) row[2]).intValue());
    batch.setFailedItems(((Number) row[3]).intValue());
    batch.setCompletionProcessed(asBoolean(row[4]));
    batch.setVersion(row[5] == null ? null : ((Number) row[5]).intValue());
    batch.setProgressHook(parseProgressHook(row[6]));
    return batch;
  }

  private static boolean asBoolean(Object value) {
    if (value instanceof Boolean bool) {
      return bool;
    }
    return value != null && ((Number) value).intValue() != 0;
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
    COMPLETED("completed_items", "completed counter"),
    FAILED("failed_items", "failed counter");

    private final String columnName;
    private final String operationName;

    BatchCounter(String columnName, String operationName) {
      this.columnName = columnName;
      this.operationName = operationName;
    }
  }
}
