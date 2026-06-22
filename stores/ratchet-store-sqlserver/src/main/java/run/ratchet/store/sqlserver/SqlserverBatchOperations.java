/*
 * Copyright 2026 Ratchet Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package run.ratchet.store.sqlserver;

import jakarta.persistence.NoResultException;
import jakarta.persistence.Query;
import java.sql.Timestamp;
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
import run.ratchet.store.spi.BatchStore;
import run.ratchet.store.sqlserver.converter.UuidByteArrayConverter;
import run.ratchet.store.util.BatchProgressRows;

final class SqlserverBatchOperations implements BatchStore {

  private static final Logger log = Logger.getLogger(SqlserverBatchOperations.class);

  private final SqlserverStoreContext ctx;

  SqlserverBatchOperations(SqlserverStoreContext ctx) {
    this.ctx = ctx;
  }

  @Override
  public BatchEntity saveBatch(BatchEntity batch) {
    // language=SQL Server
    String sql =
        """
        MERGE scheduler_batch WITH (HOLDLOCK) AS tgt
        USING (VALUES (?, ?, ?, ?, ?, ?))
          AS src(batch_id, total_items, completed_items, failed_items,
                 completion_processed, progress_hook)
          ON tgt.batch_id = src.batch_id
        WHEN MATCHED THEN UPDATE SET
          total_items = src.total_items,
          completed_items = src.completed_items,
          failed_items = src.failed_items,
          completion_processed = src.completion_processed,
          progress_hook = src.progress_hook,
          version = tgt.version + 1
        WHEN NOT MATCHED THEN INSERT
          (batch_id, total_items, completed_items, failed_items,
           completion_processed, progress_hook)
          VALUES (src.batch_id, src.total_items, src.completed_items, src.failed_items,
                  src.completion_processed, src.progress_hook)
        OUTPUT inserted.batch_id, inserted.total_items, inserted.completed_items,
               inserted.failed_items, inserted.completion_processed, inserted.version,
               inserted.progress_hook;
        """;
    Object row =
        ctx.em()
            .createNativeQuery(sql)
            .setParameter(1, UuidByteArrayConverter.toBytes(batch.getId()))
            .setParameter(2, batch.getTotalItems())
            .setParameter(3, batch.getCompletedItems())
            .setParameter(4, batch.getFailedItems())
            .setParameter(5, Boolean.TRUE.equals(batch.getCompletionProcessed()))
            .setParameter(6, progressHookJson(batch.getProgressHook()))
            .getSingleResult();
    return row instanceof Object[] values ? mapBatchRow(values) : batch;
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
    // language=SQL Server
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
      query.setParameter(i + 1, UuidByteArrayConverter.toBytes(batchIds.get(i)));
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
    // language=SQL Server
    String sql =
        String.format(
            """
        UPDATE scheduler_batch SET %1$s = %1$s + 1
        OUTPUT inserted.completed_items, inserted.failed_items, inserted.total_items,
               inserted.progress_hook
        WHERE batch_id = ?
        """,
            counter.columnName);
    try {
      Object result =
          ctx.em()
              .createNativeQuery(sql)
              .setParameter(1, UuidByteArrayConverter.toBytes(batchId))
              .getSingleResult();
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
    // language=SQL Server
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
  @SuppressWarnings("unchecked")
  public List<UUID> findRecoverableBatchIds(int limit) {
    // language=SQL Server
    String sql =
        """
        SELECT batch_id FROM scheduler_batch
        WHERE completion_processed = 0
          AND (completed_items + failed_items) >= total_items
        ORDER BY (SELECT 1) OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY
        """;
    List<?> results = ctx.em().createNativeQuery(sql).setParameter(1, limit).getResultList();
    return results.stream().map(SqlserverJobRowMapper::uuidOrNull).toList();
  }

  @Override
  public boolean updateBatchTotalItems(UUID batchId, int totalItems) {
    // language=SQL Server
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
    if (metrics.getBatchJob() == null) {
      metrics.setBatchJob(ctx.em().getReference(JobEntity.class, metrics.getBatchId()));
    }
    // language=SQL Server
    String sql =
        """
        MERGE scheduler_batch_metrics WITH (HOLDLOCK) AS tgt
        USING (VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?))
          AS src(batch_id, total_duration_ms, child_execution_ms, overhead_ms, child_count,
                 success_count, failure_count, started_at, completed_at)
          ON tgt.batch_id = src.batch_id
        WHEN MATCHED THEN UPDATE SET
          total_duration_ms = src.total_duration_ms,
          child_execution_ms = src.child_execution_ms,
          overhead_ms = src.overhead_ms,
          child_count = src.child_count,
          success_count = src.success_count,
          failure_count = src.failure_count,
          started_at = src.started_at,
          completed_at = src.completed_at,
          version = tgt.version + 1
        WHEN NOT MATCHED THEN INSERT
          (batch_id, total_duration_ms, child_execution_ms, overhead_ms, child_count,
           success_count, failure_count, started_at, completed_at)
          VALUES (src.batch_id, src.total_duration_ms, src.child_execution_ms, src.overhead_ms,
                  src.child_count, src.success_count, src.failure_count, src.started_at,
                  src.completed_at);
        """;
    Query query = ctx.em().createNativeQuery(sql);
    query.setParameter(1, UuidByteArrayConverter.toBytes(metrics.getBatchId()));
    query.setParameter(2, metrics.getTotalDurationMs());
    query.setParameter(3, metrics.getChildExecutionMs());
    query.setParameter(4, metrics.getOverheadMs());
    query.setParameter(5, metrics.getChildCount());
    query.setParameter(6, metrics.getSuccessCount());
    query.setParameter(7, metrics.getFailureCount());
    query.setParameter(8, timestampOrNull(metrics.getStartedAt()));
    query.setParameter(9, timestampOrNull(metrics.getCompletedAt()));
    query.executeUpdate();
    return metrics;
  }

  @Override
  public Optional<BatchMetricsEntity> findBatchMetrics(UUID batchId) {
    return Optional.ofNullable(ctx.em().find(BatchMetricsEntity.class, batchId));
  }

  @Override
  public void addChildExecutionTime(UUID batchId, long durationMs) {
    // language=SQL Server
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
    // language=SQL Server
    String sql =
        """
        UPDATE scheduler_batch_metrics
        SET completed_at = SYSUTCDATETIME(),
            total_duration_ms = CASE WHEN started_at IS NOT NULL
              THEN DATEDIFF_BIG(MILLISECOND, started_at, SYSUTCDATETIME())
              ELSE NULL END,
            overhead_ms = CASE WHEN started_at IS NOT NULL AND child_execution_ms IS NOT NULL
              THEN DATEDIFF_BIG(MILLISECOND, started_at, SYSUTCDATETIME())
                   - child_execution_ms
              ELSE NULL END
        WHERE batch_id = ? AND completed_at IS NULL
        """;
    ctx.em()
        .createNativeQuery(sql)
        .setParameter(1, UuidByteArrayConverter.toBytes(batchId))
        .executeUpdate();
  }

  @Override
  public void updateBatchMetricsChildCount(UUID batchId, int childCount) {
    // language=SQL Server
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
      log.warn("Failed to deserialize stored batch progress hook payload", e);
      throw new IllegalArgumentException("JobPayload deserialization error", e);
    }
  }

  private static Timestamp timestampOrNull(java.time.Instant instant) {
    return instant == null ? null : Timestamp.from(instant);
  }

  private BatchEntity mapBatchRow(Object[] row) {
    BatchEntity batch = new BatchEntity();
    batch.setId(SqlserverJobRowMapper.uuidOrNull(row[0]));
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
