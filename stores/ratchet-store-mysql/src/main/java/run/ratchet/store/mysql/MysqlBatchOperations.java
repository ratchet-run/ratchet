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
package run.ratchet.store.mysql;

import jakarta.persistence.NoResultException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import org.jboss.logging.Logger;
import run.ratchet.store.converter.JobPayloadConverter;
import run.ratchet.store.converter.PayloadSerializerHolder;
import run.ratchet.store.dto.BatchProgress;
import run.ratchet.store.entity.BatchEntity;
import run.ratchet.store.entity.BatchMetricsEntity;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobPayload;
import run.ratchet.store.mysql.converter.UuidByteArrayConverter;
import run.ratchet.store.spi.BatchStore;
import run.ratchet.store.util.BatchProgressRows;

final class MysqlBatchOperations implements BatchStore {

  private static final Logger log = Logger.getLogger(MysqlBatchOperations.class);
  private static final JobPayloadConverter JOB_PAYLOAD_CONVERTER = new JobPayloadConverter();

  private final MysqlStoreContext ctx;

  MysqlBatchOperations(MysqlStoreContext ctx) {
    this.ctx = ctx;
  }

  @Override
  public BatchEntity saveBatch(BatchEntity batch) {
    try {
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
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("save batch", e);
    }
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
    String placeholders = String.join(", ", Collections.nCopies(batchIds.size(), "?"));
    // language=MySQL
    String sql =
        """
        SELECT batch_id, total_items, completed_items, failed_items,
               completion_processed, version, progress_hook
        FROM scheduler_batch
        WHERE batch_id IN (%s)
        """
            .formatted(placeholders);
    var query = ctx.em().createNativeQuery(sql);
    for (int i = 0; i < batchIds.size(); i++) {
      query.setParameter(i + 1, UuidByteArrayConverter.toBytes(batchIds.get(i)));
    }
    @SuppressWarnings("unchecked")
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
    try {
      // MySQL 8.0 does not support UPDATE ... RETURNING. Keep the row locked from the
      // snapshot read through the counter update so the returned BatchProgress reflects
      // the exact increment applied by this transaction.
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

      int newValue = counter.nextValue(locked);
      // language=MySQL
      String updateSql =
          "UPDATE scheduler_batch SET " + counter.columnName + " = ? WHERE batch_id = ?";
      ctx.em()
          .createNativeQuery(updateSql)
          .setParameter(1, newValue)
          .setParameter(2, UuidByteArrayConverter.toBytes(batchId))
          .executeUpdate();

      return counter.progressAfterIncrement(batchId, locked, this::parseProgressHook);
    } catch (NoResultException e) {
      throw new IllegalStateException("Batch not found: " + batchId, e);
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("increment batch counter", e);
    }
  }

  @Override
  public boolean markBatchCompleteIfReady(UUID batchId) {
    try {
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
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("mark batch complete", e);
    }
  }

  @Override
  public List<UUID> findRecoverableBatchIds(int limit) {
    try {
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
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("find recoverable batch ids", e);
    }
  }

  @Override
  public boolean updateBatchTotalItems(UUID batchId, int totalItems) {
    try {
      // language=MySQL
      String sql = "UPDATE scheduler_batch SET total_items = ? WHERE batch_id = ?";
      int updated =
          ctx.em()
              .createNativeQuery(sql)
              .setParameter(1, totalItems)
              .setParameter(2, UuidByteArrayConverter.toBytes(batchId))
              .executeUpdate();
      return updated > 0;
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("update batch total items", e);
    }
  }

  @Override
  public BatchMetricsEntity saveBatchMetrics(BatchMetricsEntity metrics) {
    try {
      if (ctx.em().find(BatchMetricsEntity.class, metrics.getBatchId()) == null) {
        if (metrics.getBatchJob() == null) {
          metrics.setBatchJob(ctx.em().getReference(JobEntity.class, metrics.getBatchId()));
        }
        ctx.em().persist(metrics);
        return metrics;
      }
      return ctx.em().merge(metrics);
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("save batch metrics", e);
    }
  }

  @Override
  public Optional<BatchMetricsEntity> findBatchMetrics(UUID batchId) {
    return Optional.ofNullable(ctx.em().find(BatchMetricsEntity.class, batchId));
  }

  @Override
  public void addChildExecutionTime(UUID batchId, long durationMs) {
    try {
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
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("add child execution time", e);
    }
  }

  @Override
  public void finalizeBatchMetrics(UUID batchId) {
    try {
      // language=MySQL
      String sql =
          """
          UPDATE scheduler_batch_metrics
          SET completed_at = NOW(3),
              total_duration_ms = TIMESTAMPDIFF(MICROSECOND, started_at, NOW(3)) / 1000,
              overhead_ms = COALESCE(
                TIMESTAMPDIFF(MICROSECOND, started_at, NOW(3)) / 1000 - child_execution_ms, 0)
          WHERE batch_id = ? AND completed_at IS NULL
          """;
      ctx.em()
          .createNativeQuery(sql)
          .setParameter(1, UuidByteArrayConverter.toBytes(batchId))
          .executeUpdate();
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("finalize batch metrics", e);
    }
  }

  @Override
  public void updateBatchMetricsChildCount(UUID batchId, int childCount) {
    try {
      // language=MySQL
      String sql = "UPDATE scheduler_batch_metrics SET child_count = ? WHERE batch_id = ?";
      ctx.em()
          .createNativeQuery(sql)
          .setParameter(1, childCount)
          .setParameter(2, UuidByteArrayConverter.toBytes(batchId))
          .executeUpdate();
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("update batch metrics child count", e);
    }
  }

  private void refreshIfManaged(BatchEntity batch) {
    if (batch != null && ctx.em().contains(batch)) {
      ctx.em().refresh(batch);
    }
  }

  private BatchEntity mapBatchRow(Object[] row) {
    BatchEntity batch = new BatchEntity();
    batch.setId(MysqlJobRowMapper.uuidOrNull(row[0]));
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

  private String progressHookJson(JobPayload progressHook) {
    return JOB_PAYLOAD_CONVERTER.convertToDatabaseColumn(progressHook);
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

  private enum BatchCounter {
    COMPLETED("completed_items") {
      @Override
      int nextValue(Object[] row) {
        return BatchProgressRows.completedItems(row) + 1;
      }

      @Override
      BatchProgress progressAfterIncrement(
          UUID batchId, Object[] row, Function<Object, JobPayload> progressHookParser) {
        return BatchProgressRows.afterCompletedIncrement(batchId, row, progressHookParser);
      }
    },
    FAILED("failed_items") {
      @Override
      int nextValue(Object[] row) {
        return BatchProgressRows.failedItems(row) + 1;
      }

      @Override
      BatchProgress progressAfterIncrement(
          UUID batchId, Object[] row, Function<Object, JobPayload> progressHookParser) {
        return BatchProgressRows.afterFailedIncrement(batchId, row, progressHookParser);
      }
    };

    private final String columnName;

    BatchCounter(String columnName) {
      this.columnName = columnName;
    }

    abstract int nextValue(Object[] row);

    abstract BatchProgress progressAfterIncrement(
        UUID batchId, Object[] row, Function<Object, JobPayload> progressHookParser);
  }
}
