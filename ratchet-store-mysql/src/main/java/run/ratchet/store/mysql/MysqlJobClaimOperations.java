package run.ratchet.store.mysql;

import run.ratchet.store.dto.JobClaimDto;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.entity.JobStatus;
import run.ratchet.store.spi.JobClaimStore;
import jakarta.persistence.Query;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

final class MysqlJobClaimOperations implements JobClaimStore {

  private static final String EXECUTABLE_JOB_TYPE_FILTER =
      "job_type IN ('SINGLE','BATCH_CHILD','CHAIN_STEP','WORKFLOW_BRANCH')";

  private static final String CLAIM_SELECT_COLUMNS =
      """
      job_id, status, job_type, priority, scheduled_time,
      version, timeout_sec, picked_by, picked_at, business_key,
      attempts, max_retries
      """;

  private final MysqlStoreContext ctx;
  private final MysqlJobCrudOperations jobs;

  MysqlJobClaimOperations(MysqlStoreContext ctx, MysqlJobCrudOperations jobs) {
    this.ctx = ctx;
    this.jobs = jobs;
  }

  private static List<JobEntity> reorderById(List<JobEntity> jobs, List<Long> orderedIds) {
    Map<Long, JobEntity> byId = new HashMap<>(jobs.size());
    for (JobEntity j : jobs) {
      byId.put(j.getId(), j);
    }
    List<JobEntity> ordered = new ArrayList<>(jobs.size());
    for (Long id : orderedIds) {
      JobEntity j = byId.get(id);
      if (j != null) {
        ordered.add(j);
      }
    }
    return ordered;
  }

  // language=MySQL
  private static String buildClaimSql(
      String selectClause, String typeFilter, String timeColumn, int boostInterval) {
    return """
        SELECT %s FROM scheduler_job_queue FORCE INDEX (idx_claim_executable)
        WHERE status = 'PENDING'
          AND %s <= NOW(3)
          AND %s
        ORDER BY %s
        LIMIT ?
        FOR UPDATE SKIP LOCKED"""
        .formatted(
            selectClause, timeColumn, typeFilter, buildBoostedOrderBy(timeColumn, boostInterval));
  }

  private static String buildBoostedOrderBy(String timeColumn, int boostInterval) {
    return boostInterval > 0
        ? "(priority + FLOOR(GREATEST(0, TIMESTAMPDIFF(MINUTE, "
            + timeColumn
            + ", NOW(3))) / ?)) DESC, "
            + timeColumn
            + " ASC, job_id ASC"
        : "priority DESC, " + timeColumn + " ASC, job_id ASC";
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<JobEntity> claimNextBatch(int limit, String nodeId) {
    try {
      int boostInterval = ctx.priorityBoostIntervalMinutes();
      var query =
          ctx.em()
              .createNativeQuery(
                  buildClaimSql(
                      CLAIM_SELECT_COLUMNS,
                      EXECUTABLE_JOB_TYPE_FILTER,
                      "scheduled_time",
                      boostInterval));
      int parameter = 1;
      if (boostInterval > 0) {
        query.setParameter(parameter++, boostInterval);
      }
      query.setParameter(parameter++, limit);

      List<Object[]> candidateRows = query.getResultList();

      if (candidateRows.isEmpty()) {
        return List.of();
      }

      List<Long> candidateIds = new ArrayList<>(candidateRows.size());
      for (Object[] row : candidateRows) {
        candidateIds.add(((Number) row[0]).longValue());
      }
      boolean[] updated = batchClaimRowsJpa(candidateIds, nodeId, Instant.now());

      List<Long> claimedIds = new ArrayList<>(candidateIds.size());
      for (int i = 0; i < candidateIds.size(); i++) {
        if (updated[i]) {
          claimedIds.add(candidateIds.get(i));
        }
      }
      if (claimedIds.isEmpty()) {
        return List.of();
      }
      return jobs.findByIds(claimedIds);
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("claim jobs", e);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<JobClaimDto> claimNextBatchOptimized(
      JobExecutionType jobType, int limit, String nodeId) {
    if (limit <= 0 || !MysqlJobRowMapper.isPollerExecutable(jobType)) {
      return List.of();
    }

    try {
      int boostInterval = ctx.priorityBoostIntervalMinutes();
      var query =
          ctx.em()
              .createNativeQuery(
                  buildClaimSql(
                      CLAIM_SELECT_COLUMNS, "job_type = ?", "scheduled_time", boostInterval));
      int parameter = 1;
      query.setParameter(parameter++, jobType.name());
      if (boostInterval > 0) {
        query.setParameter(parameter++, boostInterval);
      }
      query.setParameter(parameter++, limit);

      List<Object[]> rows =
          ctx.timedStoreOperation(
              "claim_lookup", query::getResultList, result -> result.isEmpty() ? "empty" : "hit");

      if (rows.isEmpty()) {
        return List.of();
      }

      return claimOptimizedRows(rows, nodeId);
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("optimized claim", e);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<JobEntity> claimDueRecurring(int limit, String nodeId) {
    try {
      int boostInterval = ctx.priorityBoostIntervalMinutes();
      // language=MySQL
      String sql =
          """
          SELECT job_id, next_fire, priority, business_key
          FROM scheduler_job
          WHERE job_type = 'RECURRING'
            AND rec_status = 'P'
            AND next_fire <= NOW(3)
          ORDER BY %s
          LIMIT ?
          FOR UPDATE SKIP LOCKED
          """
              .formatted(buildBoostedOrderBy("next_fire", boostInterval));
      var query = ctx.em().createNativeQuery(sql);
      int parameter = 1;
      if (boostInterval > 0) {
        query.setParameter(parameter++, boostInterval);
      }
      List<Object[]> rows = query.setParameter(parameter, limit).getResultList();
      if (rows.isEmpty()) {
        return List.of();
      }
      List<Long> ids = new ArrayList<>(rows.size());
      for (Object[] row : rows) {
        ids.add(((Number) row[0]).longValue());
      }
      List<JobEntity> ordered = reorderById(jobs.findByIds(ids), ids);
      for (JobEntity job : ordered) {
        job.setStatus(JobStatus.RUNNING);
      }
      return ordered;
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("claim recurring jobs", e);
    }
  }

  private List<JobClaimDto> claimOptimizedRows(List<Object[]> rows, String nodeId) {
    return ctx.timedStoreOperation(
        "claim_mark_running_batch",
        () -> {
          Instant now = Instant.now();
          List<Long> jobIds = new ArrayList<>(rows.size());
          for (Object[] row : rows) {
            jobIds.add(((Number) row[0]).longValue());
          }
          boolean[] updated = batchClaimRowsJpa(jobIds, nodeId, now);
          List<JobClaimDto> claims = new ArrayList<>(rows.size());
          for (int i = 0; i < rows.size(); i++) {
            if (!updated[i]) {
              continue;
            }
            Object[] row = rows.get(i);
            claims.add(
                new JobClaimDto(
                    jobIds.get(i),
                    JobStatus.RUNNING,
                    JobExecutionType.valueOf((String) row[2]),
                    MysqlJobRowMapper.safeJobPriority(((Number) row[3]).intValue()),
                    MysqlJobRowMapper.toInstant(row[4]),
                    ((Number) row[5]).intValue(),
                    ((Number) row[6]).intValue(),
                    nodeId,
                    now,
                    (String) row[9],
                    ((Number) row[10]).intValue(),
                    ((Number) row[11]).intValue()));
          }
          return claims;
        },
        claims -> claims.isEmpty() ? "miss" : "updated");
  }

  private boolean[] batchClaimRowsJpa(List<Long> jobIds, String nodeId, Instant now) {
    Timestamp nowTs = Timestamp.from(now);
    try {
      String placeholders = String.join(",", Collections.nCopies(jobIds.size(), "?"));
      // language=MySQL
      String updateSql =
          """
          UPDATE scheduler_job_queue
          SET status = 'RUNNING', picked_by = ?, picked_at = ?, updated_at = ?,
              version = version + 1
          WHERE job_id IN (%s) AND status = 'PENDING'
          ORDER BY job_id ASC
          """
              .formatted(placeholders);
      Query updateQuery = ctx.em().createNativeQuery(updateSql);
      int parameter = 1;
      updateQuery.setParameter(parameter++, nodeId);
      updateQuery.setParameter(parameter++, nowTs);
      updateQuery.setParameter(parameter++, nowTs);
      for (Long id : jobIds) {
        updateQuery.setParameter(parameter++, id);
      }
      updateQuery.executeUpdate();

      // language=MySQL
      String selectSql =
          """
          SELECT job_id FROM scheduler_job_queue
          WHERE job_id IN (%s) AND status = 'RUNNING' AND picked_by = ?
          ORDER BY job_id ASC
          """
              .formatted(placeholders);
      Query selectQuery = ctx.em().createNativeQuery(selectSql);
      parameter = 1;
      for (Long id : jobIds) {
        selectQuery.setParameter(parameter++, id);
      }
      selectQuery.setParameter(parameter++, nodeId);
      @SuppressWarnings("unchecked")
      List<Number> claimedRows = selectQuery.getResultList();

      Set<Long> claimedIds = new HashSet<>(claimedRows.size());
      for (Number claimedRow : claimedRows) {
        claimedIds.add(claimedRow.longValue());
      }

      boolean[] updated = new boolean[jobIds.size()];
      for (int i = 0; i < jobIds.size(); i++) {
        updated[i] = claimedIds.contains(jobIds.get(i));
      }
      return updated;
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("claim jobs", e);
    }
  }
}
