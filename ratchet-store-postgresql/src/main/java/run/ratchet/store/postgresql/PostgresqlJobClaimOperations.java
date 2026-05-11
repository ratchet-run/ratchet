package run.ratchet.store.postgresql;

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
import run.ratchet.store.spi.JobClaimStore;
import run.ratchet.store.util.JobClaimSqlSupport;

final class PostgresqlJobClaimOperations implements JobClaimStore {

  static final String EXECUTABLE_JOB_TYPE_FILTER =
      "job_type IN ('SINGLE','BATCH_CHILD','CHAIN_STEP','WORKFLOW_BRANCH')";

  private static final String CLAIM_SELECT_COLUMNS = ClaimColumn.selectClause();

  private final PostgresqlStoreContext ctx;
  private final PostgresqlJobReadOperations reads;

  PostgresqlJobClaimOperations(PostgresqlStoreContext ctx, PostgresqlJobReadOperations reads) {
    this.ctx = ctx;
    this.reads = reads;
  }

  static String buildBoostOrderBy(String timeColumn, int boostInterval) {
    return boostInterval > 0
        ? "(priority + FLOOR(GREATEST(0, EXTRACT(EPOCH FROM (statement_timestamp() - "
            + timeColumn
            + "))) / (60.0 * ?))) DESC, "
            + timeColumn
            + " ASC, job_id ASC"
        : "priority DESC, " + timeColumn + " ASC, job_id ASC";
  }

  /**
   * Selects due PENDING rows in effective-priority order with {@code FOR UPDATE SKIP LOCKED}. The
   * caller transitions the locked rows to RUNNING via a separate UPDATE; the priority ordering
   * established here is preserved through that UPDATE because the caller iterates the SELECT rows
   * directly. {@code UPDATE…RETURNING} from a CTE would emit rows in heap order instead.
   *
   * <p>Placeholder order: typeFilter params → tagFilter params → boostInterval (if &gt; 0) → limit.
   */
  // language=PostgreSQL
  private static String buildQueueSelectSql(
      String selectColumns,
      String typeFilter,
      String tagFilterSql,
      String timeColumn,
      int boostInterval) {
    return """
        SELECT %s
        FROM scheduler_job_queue
        WHERE status = 'PENDING'
          AND %s <= statement_timestamp()
          AND %s%s
        ORDER BY %s
        LIMIT ?
        FOR UPDATE SKIP LOCKED
        """
        .formatted(
            selectColumns,
            timeColumn,
            typeFilter,
            tagFilterSql,
            buildBoostOrderBy(timeColumn, boostInterval));
  }

  // language=PostgreSQL
  private static String buildHydratedQueueSelectSql(
      String selectColumns,
      String typeFilter,
      String tagFilterSql,
      String timeColumn,
      int boostInterval) {
    return """
        SELECT %s
        FROM scheduler_job c
        JOIN scheduler_job_queue q ON q.job_id = c.job_id
        WHERE q.status = 'PENDING'
          AND %s <= statement_timestamp()
          AND %s%s
        ORDER BY %s
        LIMIT ?
        FOR UPDATE SKIP LOCKED
        """
        .formatted(
            selectColumns,
            timeColumn,
            typeFilter,
            tagFilterSql,
            buildHydratedBoostOrderBy(timeColumn, boostInterval));
  }

  private static String buildHydratedBoostOrderBy(String timeColumn, int boostInterval) {
    return boostInterval > 0
        ? "(q.priority + FLOOR(GREATEST(0, EXTRACT(EPOCH FROM (statement_timestamp() - "
            + timeColumn
            + "))) / (60.0 * ?))) DESC, "
            + timeColumn
            + " ASC, q.job_id ASC"
        : "q.priority DESC, " + timeColumn + " ASC, q.job_id ASC";
  }

  @Override
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
                      PostgresqlJobRowMapper.hydrationSelect(),
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
      List<Object[]> rows = selectQuery.getResultList();
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

  @Override
  public List<JobClaimDto> claimNextBatchOptimized(
      JobExecutionType jobType, int limit, String nodeId, NodeTagFilter tagFilter) {
    if (limit <= 0 || !PostgresqlStoreContext.isPollerExecutable(jobType)) {
      return List.of();
    }
    try {
      int boostInterval = ctx.priorityBoostIntervalMinutes();
      String tagSql = JobClaimSqlSupport.buildTagFilterSql(tagFilter, "scheduler_job_queue");
      Query selectQuery =
          ctx.em()
              .createNativeQuery(
                  buildQueueSelectSql(
                      CLAIM_SELECT_COLUMNS,
                      "job_type = ?",
                      tagSql,
                      "scheduled_time",
                      boostInterval));
      int parameter = 1;
      selectQuery.setParameter(parameter++, jobType.name());
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
                row.maxRetries()));
      }
      return claims;
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("optimized claim", e);
    }
  }

  private void markPendingClaimsRunning(List<UUID> ids, String nodeId, Instant now) {
    String placeholders = String.join(",", Collections.nCopies(ids.size(), "?"));
    // language=PostgreSQL
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
      query.setParameter(p++, id);
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
    MAX_RETRIES("max_retries");

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
      return PostgresqlJobRowMapper.uuidOrNull(value(ClaimColumn.JOB_ID));
    }

    JobExecutionType jobType() {
      return JobExecutionType.valueOf((String) value(ClaimColumn.JOB_TYPE));
    }

    JobPriority priority() {
      return PostgresqlJobRowMapper.safeJobPriority(number(ClaimColumn.PRIORITY).intValue());
    }

    Instant scheduledTime() {
      return PostgresqlJobRowMapper.toInstant(value(ClaimColumn.SCHEDULED_TIME));
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

  @Override
  @SuppressWarnings("unchecked")
  public List<JobEntity> claimDueRecurring(int limit, String nodeId, NodeTagFilter tagFilter) {
    if (limit <= 0) {
      return List.of();
    }
    try {
      int boostInterval = ctx.priorityBoostIntervalMinutes();
      String tagSql = JobClaimSqlSupport.buildTagFilterSql(tagFilter, "c");
      // language=PostgreSQL
      String sql =
          """
          SELECT %s
          FROM scheduler_job c
          LEFT JOIN scheduler_job_queue q ON q.job_id = c.job_id
          WHERE c.job_type = 'RECURRING'
            AND c.rec_status = 'P'
            AND c.next_fire <= statement_timestamp()%s
          ORDER BY %s
          LIMIT ?
          FOR UPDATE OF c SKIP LOCKED
          """
              .formatted(
                  PostgresqlJobRowMapper.hydrationSelect(),
                  tagSql,
                  buildRecurringBoostOrderBy(boostInterval));
      Query selectQuery = ctx.em().createNativeQuery(sql);
      int parameter = 1;
      parameter = JobClaimSqlSupport.bindTagFilter(selectQuery, tagFilter, parameter);
      if (boostInterval > 0) {
        selectQuery.setParameter(parameter++, boostInterval);
      }
      selectQuery.setParameter(parameter, limit);
      List<Object[]> rows = selectQuery.getResultList();
      if (rows.isEmpty()) {
        return List.of();
      }
      List<JobEntity> ordered = reads.hydrateRowsWithTags(rows);
      Instant now = Instant.now();
      for (JobEntity job : ordered) {
        job.setStatus(JobStatus.RUNNING);
        job.setPickedBy(nodeId);
        job.setPickedAt(now);
      }
      return ordered;
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("claim recurring jobs", e);
    }
  }

  private static String buildRecurringBoostOrderBy(int boostInterval) {
    return boostInterval > 0
        ? "(c.priority + FLOOR(GREATEST(0, EXTRACT(EPOCH FROM (statement_timestamp() - "
            + "c.next_fire))) / (60.0 * ?))) DESC, c.next_fire ASC, c.job_id ASC"
        : "c.priority DESC, c.next_fire ASC, c.job_id ASC";
  }
}
