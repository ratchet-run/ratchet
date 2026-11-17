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
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobStatus;
import run.ratchet.api.NodeTagFilter;
import run.ratchet.store.dto.JobClaimDto;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.spi.ClaimState;
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

  /**
   * Selects due PENDING rows in priority order, locking them with {@code WITH (UPDLOCK, READPAST,
   * ROWLOCK)} so concurrent claimers skip the already-locked rows. The caller transitions the
   * locked rows to RUNNING via a separate UPDATE; the priority ordering established here is
   * preserved through that UPDATE because the caller iterates the SELECT rows directly. A single
   * {@code UPDATE … OUTPUT} from a CTE would emit rows in heap order instead.
   *
   * <p>Placeholder order: typeFilter params → executionTargetFilter params → tagFilter params →
   * limit.
   */
  // language=SQL Server
  private static String buildQueueSelectSql(
      String selectColumns,
      String typeFilter,
      String executionTargetFilterSql,
      String tagFilterSql,
      String timeColumn) {
    return """
    SELECT %s
    FROM scheduler_job_queue WITH (UPDLOCK, READPAST, ROWLOCK)
    WHERE status = 'PENDING'
      AND %s <= SYSUTCDATETIME()
      AND %s%s%s
    ORDER BY priority DESC, %s ASC, job_id ASC
    OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY
    """
        .formatted(
            selectColumns,
            timeColumn,
            typeFilter,
            executionTargetFilterSql,
            tagFilterSql,
            timeColumn);
  }

  // language=SQL Server
  private static String buildHydratedQueueSelectSql(
      String selectColumns, String typeFilter, String tagFilterSql, String timeColumn) {
    return """
    SELECT %s
    FROM scheduler_job c
    JOIN scheduler_job_queue q WITH (UPDLOCK, READPAST, ROWLOCK) ON q.job_id = c.job_id
    WHERE q.status = 'PENDING'
      AND %s <= SYSUTCDATETIME()
      AND %s%s
    ORDER BY q.priority DESC, %s ASC, q.job_id ASC
    OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY
    """
        .formatted(selectColumns, timeColumn, typeFilter, tagFilterSql, timeColumn);
  }

  // SQL template is a compile-time constant defined in this package; runtime values are bound as
  // JDBC parameters via setParameter.
  @Override
  public Optional<ClaimState> currentClaim(UUID jobId) {
    return reads.currentClaim(jobId);
  }

  @Override
  @SuppressWarnings("SqlSourceToSinkFlow")
  public List<JobEntity> claimNextBatch(int limit, String nodeId, NodeTagFilter tagFilter) {
    if (limit <= 0) {
      return List.of();
    }
    try {
      int boostInterval = ctx.priorityBoostIntervalMinutes();
      if (boostInterval > 0) {
        return claimBoostedHydrated(limit, nodeId, tagFilter);
      }
      String tagSql = JobClaimSqlSupport.buildTagFilterSql(tagFilter, "q");
      Query selectQuery =
          ctx.em()
              .createNativeQuery(
                  buildHydratedQueueSelectSql(
                      SqlserverJobRowMapper.hydrationSelect(),
                      "q.job_type IN ('SINGLE','BATCH_CHILD','CHAIN_STEP','WORKFLOW_BRANCH')",
                      tagSql,
                      "q.scheduled_time"));
      int parameter = 1;
      parameter = JobClaimSqlSupport.bindTagFilter(selectQuery, tagFilter, parameter);
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
        job.setClaimSeq(job.getClaimSeq() + 1);
        job.setStatus(JobStatus.RUNNING);
        job.setPickedBy(nodeId);
        job.setPickedAt(now);
        job.setExecutionStartTime(now);
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
      if (boostInterval > 0) {
        List<Object[]> rows =
            new SqlserverBoostedClaimCandidates(
                    ctx, jobType, limit, tagFilter, executionTargetFilter, databaseNow())
                .lock(CLAIM_SELECT_COLUMNS);
        if (rows.isEmpty()) return List.of();
        List<UUID> ids = rows.stream().map(row -> new ClaimRow(row).jobId()).toList();
        return claimOptimizedRows(rows, ids, nodeId);
      }
      String tagSql = JobClaimSqlSupport.buildTagFilterSql(tagFilter, "scheduler_job_queue");
      String executionTargetSql =
          JobClaimSqlSupport.buildExecutionTargetFilterSql(
              executionTargetFilter, "execution_target");
      // The driver binds strings as NVARCHAR. Cast the type so SQL Server can seek the claim
      // index. Without the cast, even an unboosted claim scans and U-locks every pending job of
      // the type until commit.
      Query selectQuery =
          ctx.em()
              .createNativeQuery(
                  buildQueueSelectSql(
                      CLAIM_SELECT_COLUMNS,
                      "job_type = CAST(? AS VARCHAR(8000))",
                      executionTargetSql,
                      tagSql,
                      "scheduled_time"));
      int parameter = 1;
      selectQuery.setParameter(parameter++, jobType.name());
      parameter =
          JobClaimSqlSupport.bindExecutionTargetFilter(
              selectQuery, executionTargetFilter, parameter);
      parameter = JobClaimSqlSupport.bindTagFilter(selectQuery, tagFilter, parameter);
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

  // A boosted claim binds one claim time to all its statements. Read it from the database so the
  // due check and boost age use the database clock, like the unboosted claim's SYSUTCDATETIME().
  private Instant databaseNow() {
    return RowValues.instantOrNull(
        ctx.em().createNativeQuery("SELECT SYSUTCDATETIME()").getSingleResult());
  }

  private List<JobEntity> claimBoostedHydrated(int limit, String nodeId, NodeTagFilter tags) {
    List<Object[]> locked =
        ctx.timedStoreOperation(
            "claim_lookup",
            () ->
                new SqlserverBoostedClaimCandidates(ctx, null, limit, tags, null, databaseNow())
                    .lock("job_id, scheduled_time"),
            rows -> rows.isEmpty() ? "empty" : "hit");
    if (locked.isEmpty()) return List.of();
    List<UUID> ids = locked.stream().map(row -> SqlserverJobRowMapper.uuidOrNull(row[0])).toList();
    String placeholders = String.join(",", Collections.nCopies(ids.size(), "?"));
    // language=SQL Server
    String sql =
        """
        SELECT %s
        FROM scheduler_job c
        JOIN scheduler_job_queue q ON q.job_id = c.job_id
        WHERE q.job_id IN (%s)
        """
            .formatted(SqlserverJobRowMapper.hydrationSelect(), placeholders);
    Query query = ctx.em().createNativeQuery(sql);
    int p = 1;
    for (UUID id : ids) query.setParameter(p++, UuidByteArrayConverter.toBytes(id));
    @SuppressWarnings("unchecked")
    List<Object[]> rows = query.getResultList();
    List<JobEntity> ordered =
        JobClaimSqlSupport.reorderById(reads.hydrateRowsWithTags(rows), ids, JobEntity::getId);
    if (ordered.size() != ids.size()) {
      throw new IllegalStateException("Locked claim rows disappeared during hydration");
    }
    Instant now = Instant.now();
    markPendingClaimsRunning(ids, nodeId, now);
    for (JobEntity job : ordered) {
      job.setClaimSeq(job.getClaimSeq() + 1);
      job.setStatus(JobStatus.RUNNING);
      job.setPickedBy(nodeId);
      job.setPickedAt(now);
      job.setExecutionStartTime(now);
    }
    return ordered;
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
                    row.executionTarget(),
                    row.dependsOn(),
                    row.claimSeq() + 1));
          }
          return claims;
        },
        claims -> claims.isEmpty() ? "miss" : "updated");
  }

  static String claimSelectClause() {
    return CLAIM_SELECT_COLUMNS;
  }

  private void markPendingClaimsRunning(List<UUID> ids, String nodeId, Instant now) {
    String placeholders = String.join(",", Collections.nCopies(ids.size(), "?"));
    // Seek the primary key, as the batch sweep does. Without the hint the optimizer can read
    // through idx_claim_executable or idx_queue_orphan, which matches status = 'PENDING', and
    // lock other jobs' index entries; that deadlocked with single-job pauses and other claims.
    // SQL Server rejects FORCESEEK on an UPDATE target, so the hint goes on the alias.
    // language=SQL Server
    String sql =
        """
        UPDATE q
        SET status = 'RUNNING',
            picked_by = ?,
            picked_at = ?,
            updated_at = ?,
            claim_seq = claim_seq + 1,
            version = version + 1
        FROM scheduler_job_queue q WITH (ROWLOCK, FORCESEEK, INDEX(pk_scheduler_job_queue))
        WHERE q.job_id IN (%s) AND q.status = 'PENDING'
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
    int updated = query.executeUpdate();
    if (updated != ids.size()) {
      throw new IllegalStateException(
          "Expected to mark " + ids.size() + " locked claims RUNNING, updated " + updated);
    }
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
    EXECUTION_TARGET("execution_target"),
    DEPENDS_ON("depends_on"),
    CLAIM_SEQ("claim_seq");

    private final String sqlName;

    ClaimColumn(String sqlName) {
      this.sqlName = sqlName;
    }

    static String selectClause() {
      return Arrays.stream(values())
          .map(ClaimColumn::selectExpression)
          .collect(Collectors.joining(", "));
    }

    String sqlName() {
      return sqlName;
    }

    String selectExpression() {
      if (this == DEPENDS_ON) {
        // UPDLOCK/READPAST remain scoped to scheduler_job_queue; cold metadata is a non-locking
        // scalar lookup by the scheduler_job primary key.
        return "(SELECT cold_job.depends_on FROM scheduler_job cold_job"
            + " WHERE cold_job.job_id = scheduler_job_queue.job_id) AS depends_on";
      }
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

    long claimSeq() {
      return number(ClaimColumn.CLAIM_SEQ).longValue();
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

    UUID dependsOn() {
      return SqlserverJobRowMapper.uuidOrNull(value(ClaimColumn.DEPENDS_ON));
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
