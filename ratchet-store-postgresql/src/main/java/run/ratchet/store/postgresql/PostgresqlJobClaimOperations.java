package run.ratchet.store.postgresql;

import jakarta.persistence.Query;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import run.ratchet.api.JobStatus;
import run.ratchet.api.NodeTagFilter;
import run.ratchet.store.dto.JobClaimDto;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.spi.JobClaimStore;

final class PostgresqlJobClaimOperations implements JobClaimStore {

  static final String EXECUTABLE_JOB_TYPE_FILTER =
      "job_type IN ('SINGLE','BATCH_CHILD','CHAIN_STEP','WORKFLOW_BRANCH')";

  private static final String CLAIM_SELECT_COLUMNS =
      "job_id, status, job_type, priority, scheduled_time, version, "
          + "timeout_sec, picked_by, picked_at, business_key, attempts, max_retries";

  private final PostgresqlStoreContext ctx;
  private final PostgresqlJobCrudOperations jobs;

  PostgresqlJobClaimOperations(PostgresqlStoreContext ctx, PostgresqlJobCrudOperations jobs) {
    this.ctx = ctx;
    this.jobs = jobs;
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

  /**
   * Builds a SQL fragment (empty string or starting with newline+AND) for tag affinity filtering.
   * Guards each list independently to avoid empty {@code IN ()}.
   */
  private static String buildTagFilterSql(NodeTagFilter filter, String tableAlias) {
    if (filter.isUnfiltered()) {
      return "";
    }
    StringBuilder sb = new StringBuilder();
    if (!filter.requireTags().isEmpty()) {
      String placeholders = "?,".repeat(filter.requireTags().size());
      sb.append("\n  AND EXISTS (SELECT 1 FROM scheduler_job_tag t WHERE t.job_id = ")
          .append(tableAlias)
          .append(".job_id AND t.tag IN (")
          .append(placeholders, 0, placeholders.length() - 1)
          .append("))");
    }
    if (!filter.excludeTags().isEmpty()) {
      String placeholders = "?,".repeat(filter.excludeTags().size());
      sb.append("\n  AND NOT EXISTS (SELECT 1 FROM scheduler_job_tag t WHERE t.job_id = ")
          .append(tableAlias)
          .append(".job_id AND t.tag IN (")
          .append(placeholders, 0, placeholders.length() - 1)
          .append("))");
    }
    return sb.toString();
  }

  private static int bindTagFilter(Query query, NodeTagFilter filter, int startParam) {
    int p = startParam;
    for (String tag : filter.requireTags()) {
      query.setParameter(p++, tag);
    }
    for (String tag : filter.excludeTags()) {
      query.setParameter(p++, tag);
    }
    return p;
  }

  private static List<JobEntity> reorderById(List<JobEntity> jobs, List<UUID> orderedIds) {
    Map<UUID, JobEntity> byId = new HashMap<>(jobs.size());
    for (JobEntity j : jobs) {
      byId.put(j.getId(), j);
    }
    List<JobEntity> ordered = new ArrayList<>(jobs.size());
    for (UUID id : orderedIds) {
      JobEntity j = byId.get(id);
      if (j != null) {
        ordered.add(j);
      }
    }
    return ordered;
  }

  @Override
  public List<JobEntity> claimNextBatch(int limit, String nodeId, NodeTagFilter tagFilter) {
    if (limit <= 0) {
      return List.of();
    }
    try {
      int boostInterval = ctx.priorityBoostIntervalMinutes();
      String tagSql = buildTagFilterSql(tagFilter, "scheduler_job_queue");
      Query selectQuery =
          ctx.em()
              .createNativeQuery(
                  buildQueueSelectSql(
                      "job_id",
                      EXECUTABLE_JOB_TYPE_FILTER,
                      tagSql,
                      "scheduled_time",
                      boostInterval));
      int parameter = 1;
      parameter = bindTagFilter(selectQuery, tagFilter, parameter);
      if (boostInterval > 0) {
        selectQuery.setParameter(parameter++, boostInterval);
      }
      selectQuery.setParameter(parameter, limit);
      @SuppressWarnings("unchecked")
      List<?> idRows = selectQuery.getResultList();
      if (idRows.isEmpty()) {
        return List.of();
      }
      List<UUID> ids = new ArrayList<>(idRows.size());
      for (Object idRow : idRows) {
        ids.add(PostgresqlJobRowMapper.uuidOrNull(idRow));
      }
      markPendingClaimsRunning(ids, nodeId, Instant.now());
      return reorderById(jobs.findByIds(ids), ids);
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
      String tagSql = buildTagFilterSql(tagFilter, "scheduler_job_queue");
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
      parameter = bindTagFilter(selectQuery, tagFilter, parameter);
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
        ids.add(PostgresqlJobRowMapper.uuidOrNull(row[0]));
      }
      Instant now = Instant.now();
      markPendingClaimsRunning(ids, nodeId, now);

      List<JobClaimDto> claims = new ArrayList<>(rows.size());
      for (int i = 0; i < rows.size(); i++) {
        Object[] row = rows.get(i);
        claims.add(
            new JobClaimDto(
                ids.get(i),
                JobStatus.RUNNING,
                JobExecutionType.valueOf((String) row[2]),
                PostgresqlJobRowMapper.safeJobPriority(((Number) row[3]).intValue()),
                PostgresqlJobRowMapper.toInstant(row[4]),
                row[5] == null ? null : ((Number) row[5]).intValue(),
                ((Number) row[6]).intValue(),
                nodeId,
                now,
                (String) row[9],
                ((Number) row[10]).intValue(),
                ((Number) row[11]).intValue()));
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

  @Override
  @SuppressWarnings("unchecked")
  public List<JobEntity> claimDueRecurring(int limit, String nodeId, NodeTagFilter tagFilter) {
    if (limit <= 0) {
      return List.of();
    }
    try {
      int boostInterval = ctx.priorityBoostIntervalMinutes();
      String tagSql = buildTagFilterSql(tagFilter, "scheduler_job");
      // language=PostgreSQL
      String sql =
          """
          SELECT job_id FROM scheduler_job
          WHERE job_type = 'RECURRING'
            AND rec_status = 'P'
            AND next_fire <= statement_timestamp()%s
          ORDER BY %s
          LIMIT ?
          FOR UPDATE SKIP LOCKED
          """
              .formatted(tagSql, buildBoostOrderBy("next_fire", boostInterval));
      Query selectQuery = ctx.em().createNativeQuery(sql);
      int parameter = 1;
      parameter = bindTagFilter(selectQuery, tagFilter, parameter);
      if (boostInterval > 0) {
        selectQuery.setParameter(parameter++, boostInterval);
      }
      selectQuery.setParameter(parameter, limit);
      List<?> idRows = selectQuery.getResultList();
      if (idRows.isEmpty()) {
        return List.of();
      }
      List<UUID> ids = new ArrayList<>(idRows.size());
      for (Object n : idRows) {
        ids.add(PostgresqlJobRowMapper.uuidOrNull(n));
      }
      List<JobEntity> ordered = reorderById(jobs.findByIds(ids), ids);
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
}
