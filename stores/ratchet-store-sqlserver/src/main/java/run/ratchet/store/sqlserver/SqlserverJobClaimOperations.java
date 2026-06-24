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

import jakarta.persistence.Query;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobStatus;
import run.ratchet.api.NodeTagFilter;
import run.ratchet.store.dto.JobClaimDto;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.spi.ExecutionTargetFilter;
import run.ratchet.store.spi.JobClaimStore;
import run.ratchet.store.sqlserver.converter.UuidByteArrayConverter;
import run.ratchet.store.util.JobClaimSqlSupport;
import run.ratchet.store.util.RowValues;
import run.ratchet.store.util.StatusClassifier;

final class SqlserverJobClaimOperations implements JobClaimStore {

  private static final String CLAIM_SELECT_COLUMNS = ClaimColumn.selectClause();

  private final SqlserverStoreContext ctx;
  private final SqlserverJobReadOperations reads;

  SqlserverJobClaimOperations(SqlserverStoreContext ctx, SqlserverJobReadOperations reads) {
    this.ctx = ctx;
    this.reads = reads;
  }

  // Overdue minutes since the row was due: DATEDIFF(SECOND, ...) is seconds, /60.0 converts to
  // minutes.
  private static String overdueMinutes(String timeColumn) {
    return "DATEDIFF(SECOND, " + timeColumn + ", SYSUTCDATETIME())/60.0";
  }

  /**
   * Selects due PENDING rows in effective-priority order with {@code FOR UPDATE SKIP LOCKED}. The
   * caller transitions the locked rows to RUNNING via a separate UPDATE; the priority ordering
   * established here is preserved through that UPDATE because the caller iterates the SELECT rows
   * directly. {@code UPDATE…RETURNING} from a CTE would emit rows in heap order instead.
   *
   * <p>Placeholder order: typeFilter params → executionTargetFilter params → tagFilter params →
   * boostInterval (if &gt; 0) → limit.
   */
  // language=SQL Server
  private static String buildQueueSelectSql(
      String selectColumns,
      String typeFilter,
      String executionTargetFilterSql,
      String tagFilterSql,
      String timeColumn,
      int boostInterval) {
    return """
        SELECT %s
        FROM scheduler_job_queue WITH (UPDLOCK, READPAST, ROWLOCK)
        WHERE status = 'PENDING'
          AND %s <= SYSUTCDATETIME()
          AND %s%s%s
        ORDER BY %s
        OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY
        """
        .formatted(
            selectColumns,
            timeColumn,
            typeFilter,
            executionTargetFilterSql,
            tagFilterSql,
            JobClaimSqlSupport.buildBoostedOrderBy(
                timeColumn, overdueMinutes(timeColumn), boostInterval));
  }

  // language=SQL Server
  private static String buildHydratedQueueSelectSql(
      String selectColumns,
      String typeFilter,
      String tagFilterSql,
      String timeColumn,
      int boostInterval) {
    return """
        SELECT %s
        FROM scheduler_job c
        JOIN scheduler_job_queue q WITH (UPDLOCK, READPAST, ROWLOCK) ON q.job_id = c.job_id
        WHERE q.status = 'PENDING'
          AND %s <= SYSUTCDATETIME()
          AND %s%s
        ORDER BY %s
        OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY
        """
        .formatted(
            selectColumns,
            timeColumn,
            typeFilter,
            tagFilterSql,
            JobClaimSqlSupport.buildBoostedOrderBy(
                timeColumn, overdueMinutes(timeColumn), boostInterval, "q."));
  }

  // SQL template is a compile-time constant defined in this package; runtime values are bound as
  // JDBC parameters via setParameter.
  @Override
  @SuppressWarnings("SqlSourceToSinkFlow")
  public List<JobEntity> claimNextBatch(int limit, String nodeId, NodeTagFilter tagFilter) {
    if (limit <= 0) {
      return List.of();
    }
    try {
      int boostInterval = ctx.priorityBoostIntervalMinutes();
      String tagSql = JobClaimSqlSupport.buildTagFilterSql(tagFilter, "q");
      Query selectQuery =
          ctx.em()
              .createNativeQuery(
                  buildHydratedQueueSelectSql(
                      SqlserverJobRowMapper.hydrationSelect(),
                      "q.job_type IN ('SINGLE','BATCH_CHILD','CHAIN_STEP','WORKFLOW_BRANCH')",
                      tagSql,
                      "q.scheduled_time",
                      boostInterval));
      int parameter = 1;
      parameter = JobClaimSqlSupport.bindTagFilter(selectQuery, tagFilter, parameter);
      if (boostInterval > 0) {
        selectQuery.setParameter(parameter++, boostInterval);
      }
      selectQuery.setParameter(parameter, limit);
      @SuppressWarnings("unchecked")
      List<Object[]> rows =
          ctx.timedStoreOperation(
              "claim_lookup",
              selectQuery::getResultList,
              result -> result.isEmpty() ? "empty" : "hit");
      if (rows.isEmpty()) {
        return List.of();
      }
      List<JobEntity> ordered = reads.hydrateRowsWithTags(rows);
      List<UUID> ids = new ArrayList<>(ordered.size());
      for (JobEntity job : ordered) {
        ids.add(job.getId());
      }
      Instant now = Instant.now();
      markPendingClaimsRunning(ids, nodeId, now);
      for (JobEntity job : ordered) {
        job.setStatus(JobStatus.RUNNING);
        job.setPickedBy(nodeId);
        job.setPickedAt(now);
      }
      return ordered;
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("claim jobs", e);
    }
  }

  // SQL template is a compile-time constant defined in this package; runtime values are bound as
  // JDBC parameters via setParameter.
  @Override
  @SuppressWarnings("SqlSourceToSinkFlow")
  public List<JobClaimDto> claimNextBatchOptimized(
      JobExecutionType jobType,
      int limit,
      String nodeId,
      NodeTagFilter tagFilter,
      ExecutionTargetFilter executionTargetFilter) {
    if (limit <= 0 || !StatusClassifier.isPollerExecutable(jobType)) {
      return List.of();
    }
    try {
      int boostInterval = ctx.priorityBoostIntervalMinutes();
      String tagSql = JobClaimSqlSupport.buildTagFilterSql(tagFilter, "scheduler_job_queue");
      String executionTargetSql =
          JobClaimSqlSupport.buildExecutionTargetFilterSql(
              executionTargetFilter, "execution_target");
      Query selectQuery =
          ctx.em()
              .createNativeQuery(
                  buildQueueSelectSql(
                      CLAIM_SELECT_COLUMNS,
                      "job_type = ?",
                      executionTargetSql,
                      tagSql,
                      "scheduled_time",
                      boostInterval));
      int parameter = 1;
      selectQuery.setParameter(parameter++, jobType.name());
      parameter =
          JobClaimSqlSupport.bindExecutionTargetFilter(
              selectQuery, executionTargetFilter, parameter);
      parameter = JobClaimSqlSupport.bindTagFilter(selectQuery, tagFilter, parameter);
      if (boostInterval > 0) {
        selectQuery.setParameter(parameter++, boostInterval);
      }
      selectQuery.setParameter(parameter, limit);
      @SuppressWarnings("unchecked")
      List<Object[]> rows = selectQuery.getResultList();
      if (rows.isEmpty()) {
        return List.of();
      }

      List<UUID> ids = new ArrayList<>(rows.size());
      for (Object[] row : rows) {
        ids.add(new ClaimRow(row).jobId());
      }
      return claimOptimizedRows(rows, ids, nodeId);
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("optimized claim", e);
    }
  }

  private List<JobClaimDto> claimOptimizedRows(List<Object[]> rows, List<UUID> ids, String nodeId) {
    return ctx.timedStoreOperation(
        "claim_mark_running_batch",
        () -> {
          Instant now = Instant.now();
          markPendingClaimsRunning(ids, nodeId, now);

          List<JobClaimDto> claims = new ArrayList<>(rows.size());
          for (int i = 0; i < rows.size(); i++) {
            ClaimRow row = new ClaimRow(rows.get(i));
            claims.add(
                new JobClaimDto(
                    ids.get(i),
                    JobStatus.RUNNING,
                    row.jobType(),
                    row.priority(),
                    row.scheduledTime(),
                    row.version(),
                    row.timeoutSeconds(),
                    nodeId,
                    now,
                    row.businessKey(),
                    row.attempts(),
                    row.maxRetries(),
                    row.executionTarget()));
          }
          return claims;
        },
        claims -> claims.isEmpty() ? "miss" : "updated");
  }

  private void markPendingClaimsRunning(List<UUID> ids, String nodeId, Instant now) {
    String placeholders = String.join(",", Collections.nCopies(ids.size(), "?"));
    // language=SQL Server
    String sql =
        """
        UPDATE scheduler_job_queue
        SET status = 'RUNNING',
            picked_by = ?,
            picked_at = ?,
            updated_at = ?,
            version = version + 1
        WHERE job_id IN (%s) AND status = 'PENDING'
        """
            .formatted(placeholders);
    Query query = ctx.em().createNativeQuery(sql);
    Timestamp nowTs = Timestamp.from(now);
    int p = 1;
    query.setParameter(p++, nodeId);
    query.setParameter(p++, nowTs);
    query.setParameter(p++, nowTs);
    for (UUID id : ids) {
      query.setParameter(p++, UuidByteArrayConverter.toBytes(id));
    }
    query.executeUpdate();
  }

  private enum ClaimColumn {
    JOB_ID("job_id"),
    STATUS("status"),
    JOB_TYPE("job_type"),
    PRIORITY("priority"),
    SCHEDULED_TIME("scheduled_time"),
    VERSION("version"),
    TIMEOUT_SEC("timeout_sec"),
    PICKED_BY("picked_by"),
    PICKED_AT("picked_at"),
    BUSINESS_KEY("business_key"),
    ATTEMPTS("attempts"),
    MAX_RETRIES("max_retries"),
    EXECUTION_TARGET("execution_target");

    private final String sqlName;

    ClaimColumn(String sqlName) {
      this.sqlName = sqlName;
    }

    static String selectClause() {
      return Arrays.stream(values()).map(ClaimColumn::sqlName).collect(Collectors.joining(", "));
    }

    String sqlName() {
      return sqlName;
    }
  }

  private record ClaimRow(Object[] values) {
    ClaimRow {
      Objects.requireNonNull(values, "values");
    }

    UUID jobId() {
      return SqlserverJobRowMapper.uuidOrNull(value(ClaimColumn.JOB_ID));
    }

    JobExecutionType jobType() {
      return JobExecutionType.valueOf((String) value(ClaimColumn.JOB_TYPE));
    }

    JobPriority priority() {
      return SqlserverJobRowMapper.safeJobPriority(number(ClaimColumn.PRIORITY).intValue());
    }

    Instant scheduledTime() {
      return RowValues.instantOrNull(value(ClaimColumn.SCHEDULED_TIME));
    }

    int version() {
      return number(ClaimColumn.VERSION).intValue();
    }

    int timeoutSeconds() {
      return number(ClaimColumn.TIMEOUT_SEC).intValue();
    }

    String businessKey() {
      return (String) value(ClaimColumn.BUSINESS_KEY);
    }

    String executionTarget() {
      return (String) value(ClaimColumn.EXECUTION_TARGET);
    }

    int attempts() {
      return number(ClaimColumn.ATTEMPTS).intValue();
    }

    int maxRetries() {
      return number(ClaimColumn.MAX_RETRIES).intValue();
    }

    private Number number(ClaimColumn column) {
      return (Number) value(column);
    }

    private Object value(ClaimColumn column) {
      return values[column.ordinal()];
    }
  }
}
