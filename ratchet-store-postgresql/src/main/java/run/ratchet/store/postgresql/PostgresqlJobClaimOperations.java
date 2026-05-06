package run.ratchet.store.postgresql;

import jakarta.persistence.Query;
import java.time.Instant;
import java.util.ArrayList;
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
   * Atomic claim against {@code scheduler_job_queue}: select due PENDING rows with {@code FOR
   * UPDATE SKIP LOCKED}, then UPDATE to RUNNING in a single CTE, returning the claimed columns.
   *
   * <p>Placeholder order: typeFilter params → tagFilter params → boostInterval (if &gt; 0) → limit
   * → nodeId.
   */
  // language=PostgreSQL
  private static String buildQueueClaimSql(
      String typeFilter, String tagFilterSql, String timeColumn, int boostInterval) {
    return """
        WITH picked AS (
          SELECT job_id FROM scheduler_job_queue
          WHERE status = 'PENDING'
            AND %s <= statement_timestamp()
            AND %s%s
          ORDER BY %s
          FOR UPDATE SKIP LOCKED
          LIMIT ?
        )
        UPDATE scheduler_job_queue AS q
        SET status = 'RUNNING', picked_by = ?,
            picked_at = statement_timestamp(), updated_at = statement_timestamp(),
            version = version + 1
        FROM picked
        WHERE q.job_id = picked.job_id
        RETURNING q.job_id, q.status, q.job_type, q.priority, q.scheduled_time, q.version,
                  q.timeout_sec, q.picked_by, q.picked_at, q.business_key, q.attempts, q.max_retries
        """
        .formatted(
            timeColumn, typeFilter, tagFilterSql, buildBoostOrderBy(timeColumn, boostInterval));
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
  @SuppressWarnings("unchecked")
  public List<JobEntity> claimNextBatch(int limit, String nodeId, NodeTagFilter tagFilter) {
    if (limit <= 0) {
      return List.of();
    }
    try {
      int boostInterval = ctx.priorityBoostIntervalMinutes();
      String tagSql = buildTagFilterSql(tagFilter, "scheduler_job_queue");
      Query claimQuery =
          ctx.em()
              .createNativeQuery(
                  buildQueueClaimSql(
                      EXECUTABLE_JOB_TYPE_FILTER, tagSql, "scheduled_time", boostInterval));
      int parameter = 1;
      parameter = bindTagFilter(claimQuery, tagFilter, parameter);
      if (boostInterval > 0) {
        claimQuery.setParameter(parameter++, boostInterval);
      }
      claimQuery.setParameter(parameter++, limit);
      claimQuery.setParameter(parameter, nodeId);
      List<Object[]> rows = claimQuery.getResultList();
      if (rows.isEmpty()) {
        return List.of();
      }
      List<UUID> ids = new ArrayList<>(rows.size());
      for (Object[] row : rows) {
        ids.add(PostgresqlJobRowMapper.uuidOrNull(row[0]));
      }
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
      Query claimQuery =
          ctx.em()
              .createNativeQuery(
                  buildQueueClaimSql("job_type = ?", tagSql, "scheduled_time", boostInterval));
      int parameter = 1;
      claimQuery.setParameter(parameter++, jobType.name());
      parameter = bindTagFilter(claimQuery, tagFilter, parameter);
      if (boostInterval > 0) {
        claimQuery.setParameter(parameter++, boostInterval);
      }
      claimQuery.setParameter(parameter++, limit);
      claimQuery.setParameter(parameter, nodeId);
      @SuppressWarnings("unchecked")
      List<Object[]> rows = claimQuery.getResultList();

      List<JobClaimDto> claims = new ArrayList<>(rows.size());
      for (Object[] row : rows) {
        claims.add(
            new JobClaimDto(
                PostgresqlJobRowMapper.uuidOrNull(row[0]),
                JobStatus.RUNNING,
                JobExecutionType.valueOf((String) row[2]),
                PostgresqlJobRowMapper.safeJobPriority(((Number) row[3]).intValue()),
                PostgresqlJobRowMapper.toInstant(row[4]),
                row[5] == null ? null : ((Number) row[5]).intValue(),
                ((Number) row[6]).intValue(),
                nodeId,
                PostgresqlJobRowMapper.toInstant(row[8]),
                (String) row[9],
                ((Number) row[10]).intValue(),
                ((Number) row[11]).intValue()));
      }
      return claims;
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("optimized claim", e);
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
