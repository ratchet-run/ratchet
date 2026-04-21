package run.ratchet.store.postgresql;

import run.ratchet.api.JobPriority;
import run.ratchet.store.dto.JobClaimDto;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.entity.JobStatus;
import run.ratchet.store.spi.JobClaimStore;
import java.util.ArrayList;
import java.util.List;

final class PostgresqlJobClaimOperations implements JobClaimStore {

  static final String EXECUTABLE_JOB_TYPE_FILTER =
      "job_type IN ('SINGLE','BATCH_CHILD','CHAIN_STEP','WORKFLOW_BRANCH')";
  static final String RECURRING_JOB_TYPE_FILTER = "job_type = 'RECURRING'";

  private final PostgresqlStoreContext ctx;

  PostgresqlJobClaimOperations(PostgresqlStoreContext ctx) {
    this.ctx = ctx;
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
   * Builds the "claim jobs" CTE+UPDATE SQL using positional {@code ?} placeholders.
   *
   * <p>Placeholder order in the returned SQL (caller must bind in this exact order):
   *
   * <ol>
   *   <li>Any placeholders already present in {@code typeFilter} (e.g. a single {@code ?} for a
   *       jobType value)
   *   <li>{@code boostInterval} — only if {@code boostInterval > 0}
   *   <li>{@code limit}
   *   <li>{@code nodeId}
   * </ol>
   */
  static String buildClaimReturningSql(
      String typeFilter, String timeColumn, int boostInterval, String returningClause) {
    return "WITH picked AS ("
        + "  SELECT job_id FROM scheduler_job"
        + "  WHERE status = 'PENDING'"
        + "    AND "
        + timeColumn
        + " <= statement_timestamp()"
        + "    AND "
        + typeFilter
        + "  ORDER BY "
        + buildBoostOrderBy(timeColumn, boostInterval)
        + "  FOR UPDATE SKIP LOCKED"
        + "  LIMIT ?"
        + ") "
        + "UPDATE scheduler_job AS j SET status = 'RUNNING', picked_by = ?, "
        + "picked_at = statement_timestamp(), updated_at = statement_timestamp(), "
        + "version = version + 1 "
        + "FROM picked WHERE j.job_id = picked.job_id "
        + "RETURNING "
        + returningClause;
  }

  private static JobPriority safeJobPriority(int ordinal) {
    JobPriority[] values = JobPriority.values();
    if (ordinal < 0 || ordinal >= values.length) {
      return JobPriority.NORMAL;
    }
    return values[ordinal];
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<JobEntity> claimNextBatch(int limit, String nodeId) {
    try {
      int boostInterval = ctx.priorityBoostIntervalMinutes();
      var claimQuery =
          ctx.em()
              .createNativeQuery(
                  buildClaimReturningSql(
                      EXECUTABLE_JOB_TYPE_FILTER, "scheduled_time", boostInterval, "j.*"),
                  JobEntity.class);
      int parameter = 1;
      if (boostInterval > 0) {
        claimQuery.setParameter(parameter++, boostInterval);
      }
      claimQuery.setParameter(parameter++, limit);
      claimQuery.setParameter(parameter++, nodeId);
      return claimQuery.getResultList();
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("claim jobs", e);
    }
  }

  @Override
  public List<JobClaimDto> claimNextBatchOptimized(
      JobExecutionType jobType, int limit, String nodeId) {
    if (limit <= 0 || !PostgresqlStoreContext.isPollerExecutable(jobType)) {
      return List.of();
    }
    try {
      int boostInterval = ctx.priorityBoostIntervalMinutes();
      String selectColumns =
          "j.job_id, j.status, j.job_type, j.priority, j.scheduled_time, j.version, "
              + "j.timeout_sec, j.picked_by, j.picked_at, j.business_key, j.attempts, j.max_retries";
      var claimQuery =
          ctx.em()
              .createNativeQuery(
                  buildClaimReturningSql(
                      "job_type = ?", "scheduled_time", boostInterval, selectColumns));
      int parameter = 1;
      claimQuery.setParameter(parameter++, jobType.name());
      if (boostInterval > 0) {
        claimQuery.setParameter(parameter++, boostInterval);
      }
      claimQuery.setParameter(parameter++, limit);
      claimQuery.setParameter(parameter++, nodeId);
      @SuppressWarnings("unchecked")
      List<Object[]> rows = claimQuery.getResultList();

      List<JobClaimDto> claims = new ArrayList<>(rows.size());
      for (Object[] row : rows) {
        claims.add(
            new JobClaimDto(
                ((Number) row[0]).longValue(),
                JobStatus.RUNNING,
                JobExecutionType.valueOf((String) row[2]),
                safeJobPriority(((Number) row[3]).intValue()),
                PostgresqlJobCrudOperations.toInstant(row[4]),
                row[5] == null ? null : ((Number) row[5]).intValue(),
                ((Number) row[6]).intValue(),
                nodeId,
                PostgresqlJobCrudOperations.toInstant(row[8]),
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
  public List<JobEntity> claimDueRecurring(int limit, String nodeId) {
    try {
      int boostInterval = ctx.priorityBoostIntervalMinutes();
      var claimQuery =
          ctx.em()
              .createNativeQuery(
                  buildClaimReturningSql(
                      RECURRING_JOB_TYPE_FILTER, "next_fire", boostInterval, "j.*"),
                  JobEntity.class);
      int parameter = 1;
      if (boostInterval > 0) {
        claimQuery.setParameter(parameter++, boostInterval);
      }
      claimQuery.setParameter(parameter++, limit);
      claimQuery.setParameter(parameter++, nodeId);
      return claimQuery.getResultList();
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("claim recurring jobs", e);
    }
  }
}
