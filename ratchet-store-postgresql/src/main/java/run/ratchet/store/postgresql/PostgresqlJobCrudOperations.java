package run.ratchet.store.postgresql;

import run.ratchet.api.JobPriority;
import run.ratchet.api.exception.RatchetOptimisticLockException;
import run.ratchet.store.converter.JobPayloadConverter;
import run.ratchet.store.converter.JsonMapConverter;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.entity.JobPayload;
import run.ratchet.store.entity.JobStatus;
import run.ratchet.store.id.TsidFactory;
import run.ratchet.store.spi.JobBulkStore;
import run.ratchet.store.spi.JobCrudStore;
import jakarta.persistence.OptimisticLockException;
import jakarta.persistence.Query;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

final class PostgresqlJobCrudOperations implements JobCrudStore, JobBulkStore {

  private static final JobPayloadConverter JOB_PAYLOAD_CONVERTER = new JobPayloadConverter();
  private static final JsonMapConverter JSON_MAP_CONVERTER = new JsonMapConverter();

  private static final String INSERT_SQL =
      "INSERT INTO scheduler_job "
          + "(job_id, status, paused_from_status, scheduled_time, job_type, priority, "
          + "attempts, max_retries, backoff_policy, backoff_param_ms, timeout_sec, "
          + "cron_expr, zone_id, next_fire, payload, params, "
          + "idempotency_key, business_key, resource_name, "
          + "on_success_payload, on_failure_payload, "
          + "depends_on, superseded_by, "
          + "picked_by, picked_at, last_error, created_at, created_by, "
          + "updated_at, execution_start_time, execution_end_time, execution_duration_ms, "
          + "queue_wait_ms, job_result, result_type, version) "
          + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,CAST(? AS jsonb),CAST(? AS jsonb),?,?,?,"
          + "CAST(? AS jsonb),CAST(? AS jsonb),?,?,?,?,?,?,?,?,?,?,?,?,CAST(? AS jsonb),?,0)";

  private static final String UPDATE_SQL =
      "UPDATE scheduler_job SET "
          + "status = ?, paused_from_status = ?, scheduled_time = ?, job_type = ?, "
          + "priority = ?, attempts = ?, max_retries = ?, backoff_policy = ?, "
          + "backoff_param_ms = ?, timeout_sec = ?, cron_expr = ?, zone_id = ?, "
          + "next_fire = ?, payload = CAST(? AS jsonb), params = CAST(? AS jsonb), "
          + "idempotency_key = ?, business_key = ?, resource_name = ?, "
          + "on_success_payload = CAST(? AS jsonb), on_failure_payload = CAST(? AS jsonb), "
          + "depends_on = ?, superseded_by = ?, picked_by = ?, picked_at = ?, last_error = ?, "
          + "updated_at = ?, execution_start_time = ?, execution_end_time = ?, "
          + "execution_duration_ms = ?, queue_wait_ms = ?, job_result = CAST(? AS jsonb), "
          + "result_type = ?, version = version + 1 "
          + "WHERE job_id = ? AND version = ?";

  private final PostgresqlStoreContext ctx;
  private final PostgresqlBusinessKeyReservations reservations;
  private final PostgresqlTagOperations tags;

  PostgresqlJobCrudOperations(
      PostgresqlStoreContext ctx,
      PostgresqlBusinessKeyReservations reservations,
      PostgresqlTagOperations tags) {
    this.ctx = ctx;
    this.reservations = reservations;
    this.tags = tags;
  }

  static Instant toInstant(Object value) {
    if (value instanceof Instant i) {
      return i;
    }
    if (value instanceof Timestamp t) {
      return t.toInstant();
    }
    if (value instanceof OffsetDateTime odt) {
      return odt.toInstant();
    }
    return null;
  }

  static String payloadToJson(JobEntity job) {
    return JOB_PAYLOAD_CONVERTER.convertToDatabaseColumn(job.getPayload());
  }

  static String paramsToJson(JobEntity job) {
    return JSON_MAP_CONVERTER.convertToDatabaseColumn(job.getParams());
  }

  static String callbackPayloadToJson(JobPayload payload) {
    return JOB_PAYLOAD_CONVERTER.convertToDatabaseColumn(payload);
  }

  @Override
  public JobEntity save(JobEntity job) {
    try {
      if (job.getId() == null || job.getId() == 0L) {
        Instant now = Instant.now();
        job.setId(TsidFactory.next());
        if (job.getCreatedAt() == null) {
          job.setCreatedAt(now);
        }
        job.setUpdatedAt(now);
        job.setVersion(0);
        bulkInsert(List.of(job));
        tags.insertTags(job.getId(), job.getTags());
        return job;
      }
      return updateJob(job);
    } catch (OptimisticLockException e) {
      throw new RatchetOptimisticLockException("Concurrent modification on job " + job.getId(), e);
    }
  }

  @Override
  public Optional<JobEntity> findById(long id) {
    return Optional.ofNullable(ctx.em().find(JobEntity.class, id));
  }

  @Override
  @SuppressWarnings("unchecked")
  public Optional<JobEntity> findByIdLatest(long id) {
    List<JobEntity> results =
        ctx.em()
            .createNativeQuery("SELECT * FROM scheduler_job WHERE job_id = ?", JobEntity.class)
            .setParameter(1, id)
            .getResultList();
    return results.isEmpty() ? Optional.empty() : Optional.of(results.get(0));
  }

  @Override
  public void delete(long id) {
    ctx.em()
        .createNativeQuery("DELETE FROM scheduler_job WHERE job_id = ?")
        .setParameter(1, id)
        .executeUpdate();
  }

  @Override
  @SuppressWarnings("unchecked")
  public JobStatus getJobStatus(long id) {
    List<Object> results =
        ctx.em()
            .createNativeQuery("SELECT status FROM scheduler_job WHERE job_id = ?")
            .setParameter(1, id)
            .getResultList();
    if (results.isEmpty()) {
      return null;
    }
    return JobStatus.valueOf((String) results.get(0));
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<JobEntity> findByIds(List<Long> ids) {
    if (ids.isEmpty()) {
      return List.of();
    }
    String placeholders = String.join(",", Collections.nCopies(ids.size(), "?"));
    Query query =
        ctx.em()
            .createNativeQuery(
                "SELECT * FROM scheduler_job WHERE job_id IN (" + placeholders + ")",
                JobEntity.class);
    int parameter = 1;
    for (Long id : ids) {
      query.setParameter(parameter++, id);
    }
    return query.getResultList();
  }

  @Override
  @SuppressWarnings("unchecked")
  public Optional<JobEntity> findActiveByBusinessKey(String businessKey) {
    List<JobEntity> results =
        ctx.em()
            .createNativeQuery(
                "SELECT j.* FROM scheduler_business_key_reservation br "
                    + "JOIN scheduler_job j ON j.job_id = br.owner_job_id "
                    + "WHERE br.business_key = ? LIMIT 1",
                JobEntity.class)
            .setParameter(1, businessKey)
            .getResultList();
    return results.isEmpty() ? Optional.empty() : Optional.of(results.get(0));
  }

  @Override
  @SuppressWarnings("unchecked")
  public Optional<JobEntity> findByIdempotencyKey(String idempotencyKey) {
    List<JobEntity> results =
        ctx.em()
            .createNativeQuery(
                "SELECT * FROM scheduler_job WHERE idempotency_key = ?", JobEntity.class)
            .setParameter(1, idempotencyKey)
            .getResultList();
    return results.isEmpty() ? Optional.empty() : Optional.of(results.get(0));
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<JobEntity> findDependants(long parentJobId) {
    return ctx.em()
        .createNativeQuery("SELECT * FROM scheduler_job WHERE depends_on = ?", JobEntity.class)
        .setParameter(1, parentJobId)
        .getResultList();
  }

  @Override
  @SuppressWarnings("unchecked")
  public Optional<Instant> findEarliestRecurringNextFire() {
    List<Object> results =
        ctx.em()
            .createNativeQuery(
                "SELECT MIN(next_fire) FROM scheduler_job "
                    + "WHERE job_type = 'RECURRING' AND status = 'PENDING' AND next_fire IS NOT NULL")
            .getResultList();
    if (results.isEmpty() || results.get(0) == null) {
      return Optional.empty();
    }
    return Optional.of(toInstant(results.get(0)));
  }

  @Override
  public long countPendingJobs() {
    return countByNative("SELECT COUNT(*) FROM scheduler_job WHERE status = 'PENDING'");
  }

  @Override
  public long countJobsByStatus(JobStatus status) {
    return countByNative("SELECT COUNT(*) FROM scheduler_job WHERE status = ?", status.name());
  }

  @Override
  public long countActiveJobs(JobExecutionType jobType) {
    return countByNative(
        "SELECT COUNT(*) FROM scheduler_job WHERE job_type = ? AND status IN ('PENDING','RUNNING')",
        jobType.name());
  }

  @Override
  public long countActiveNodes() {
    return countByNative("SELECT COUNT(*) FROM scheduler_node");
  }

  @Override
  public long countReadyJobs(Instant now) {
    return countByNative(
        "SELECT COUNT(*) FROM scheduler_job WHERE status = 'PENDING' AND scheduled_time <= ?",
        Timestamp.from(now));
  }

  @Override
  public long countStuckJobs(Instant stuckThreshold) {
    return countByNative(
        "SELECT COUNT(*) FROM scheduler_job WHERE status = 'RUNNING' AND picked_at < ?",
        Timestamp.from(stuckThreshold));
  }

  @Override
  public long countLongRunningJobs(Instant threshold) {
    return countByNative(
        "SELECT COUNT(*) FROM scheduler_job WHERE status = 'RUNNING' AND execution_start_time < ?",
        Timestamp.from(threshold));
  }

  @Override
  public long countPendingBatchChildren() {
    return countByNative(
        "SELECT COUNT(*) FROM scheduler_job WHERE job_type = 'BATCH_CHILD' AND status = 'PENDING'");
  }

  @Override
  public long countPendingJobsByPriority(JobPriority priority) {
    return countByNative(
        "SELECT COUNT(*) FROM scheduler_job WHERE status = 'PENDING' AND priority = ?",
        priority.ordinal());
  }

  @Override
  public long countPendingJobsByType(JobExecutionType jobType) {
    return countByNative(
        "SELECT COUNT(*) FROM scheduler_job WHERE status = 'PENDING' AND job_type = ?",
        jobType.name());
  }

  @Override
  public long countJobsByStatusSince(JobStatus status, Instant since) {
    return countByNative(
        "SELECT COUNT(*) FROM scheduler_job WHERE status = ? AND updated_at >= ?",
        status.name(),
        Timestamp.from(since));
  }

  @Override
  public long countJobsWithRetries() {
    return countByNative("SELECT COUNT(*) FROM scheduler_job WHERE attempts > 0");
  }

  @Override
  public double getRetryRateStats(Instant since) {
    Object result =
        ctx.em()
            .createNativeQuery(
                "SELECT CASE WHEN COUNT(*) = 0 THEN 0.0 "
                    + "ELSE CAST(SUM(CASE WHEN attempts > 0 THEN 1 ELSE 0 END) AS DOUBLE PRECISION) / COUNT(*) END "
                    + "FROM scheduler_job WHERE updated_at >= ?")
            .setParameter(1, Timestamp.from(since))
            .getSingleResult();
    return result == null ? 0.0 : ((Number) result).doubleValue();
  }

  @Override
  public double getAverageProcessingTime(Instant since) {
    Object result =
        ctx.em()
            .createNativeQuery(
                "SELECT COALESCE(AVG(execution_duration_ms), 0) "
                    + "FROM scheduler_job WHERE status = 'SUCCEEDED' AND updated_at >= ?")
            .setParameter(1, Timestamp.from(since))
            .getSingleResult();
    return result == null ? 0.0 : ((Number) result).doubleValue();
  }

  @Override
  public double getAverageBatchSize(Instant since) {
    Object result =
        ctx.em()
            .createNativeQuery(
                "SELECT COALESCE(AVG(total_items), 0) "
                    + "FROM scheduler_batch b "
                    + "INNER JOIN scheduler_job j ON b.batch_id = j.job_id "
                    + "WHERE j.updated_at >= ?")
            .setParameter(1, Timestamp.from(since))
            .getSingleResult();
    return result == null ? 0.0 : ((Number) result).doubleValue();
  }

  @Override
  @SuppressWarnings("unchecked")
  public Optional<Instant> getOldestPendingJobTime() {
    List<Object> results =
        ctx.em()
            .createNativeQuery(
                "SELECT MIN(scheduled_time) FROM scheduler_job WHERE status = 'PENDING'")
            .getResultList();
    if (results.isEmpty() || results.get(0) == null) {
      return Optional.empty();
    }
    return Optional.of(toInstant(results.get(0)));
  }

  @Override
  public long getQueueWaitTimePercentile(double percentile) {
    Object result =
        ctx.em()
            .createNativeQuery(
                "SELECT COALESCE(PERCENTILE_CONT(?) WITHIN GROUP (ORDER BY queue_wait_ms), 0) "
                    + "FROM scheduler_job WHERE queue_wait_ms IS NOT NULL AND status = 'SUCCEEDED'")
            .setParameter(1, percentile)
            .getSingleResult();
    return result == null ? 0L : ((Number) result).longValue();
  }

  @Override
  public void bulkInsert(List<JobEntity> jobs) {
    if (jobs.isEmpty()) {
      return;
    }
    try {
      for (JobEntity job : jobs) {
        Query query = ctx.em().createNativeQuery(INSERT_SQL);
        setBulkInsertParameters(query, job, Instant.now());
        query.executeUpdate();
        reservations.syncForJob(job);
      }
    } catch (Exception e) {
      throw new RuntimeException("Bulk insert error: " + e.getMessage(), e);
    } finally {
      ctx.em().clear();
    }
  }

  @Override
  public int deleteJobsByIds(List<Long> ids) {
    if (ids.isEmpty()) {
      return 0;
    }
    String placeholders = String.join(",", Collections.nCopies(ids.size(), "?"));
    Query query =
        ctx.em()
            .createNativeQuery("DELETE FROM scheduler_job WHERE job_id IN (" + placeholders + ")");
    int parameter = 1;
    for (Long id : ids) {
      query.setParameter(parameter++, id);
    }
    return query.executeUpdate();
  }

  @Override
  public int deleteDlqOlderThan(Instant cutoff) {
    return ctx.em()
        .createNativeQuery(
            "DELETE FROM scheduler_job WHERE status = 'FAILED' "
                + "AND attempts >= max_retries AND updated_at < ?")
        .setParameter(1, Timestamp.from(cutoff))
        .executeUpdate();
  }

  @Override
  public int resetOrphanJobs(Duration grace) {
    long graceSeconds = grace.toSeconds();
    return ctx.em()
        .createNativeQuery(
            "UPDATE scheduler_job SET status = 'PENDING', "
                + "picked_by = NULL, picked_at = NULL, "
                + "updated_at = statement_timestamp() "
                + "WHERE status = 'RUNNING' "
                + "AND picked_by NOT IN ("
                + "  SELECT node_id FROM scheduler_node "
                + "  WHERE heartbeat_ts > statement_timestamp() - ? * interval '1 second'"
                + ") "
                + "AND extract(epoch from (statement_timestamp() - picked_at))::bigint >= ?")
        .setParameter(1, graceSeconds)
        .setParameter(2, graceSeconds)
        .executeUpdate();
  }

  @Override
  public int resetOrphanJobsForNode(String nodeId) {
    return ctx.em()
        .createNativeQuery(
            "UPDATE scheduler_job SET status = 'PENDING', "
                + "picked_by = NULL, picked_at = NULL, "
                + "updated_at = statement_timestamp() "
                + "WHERE status = 'RUNNING' AND picked_by = ?")
        .setParameter(1, nodeId)
        .executeUpdate();
  }

  JobEntity hydrateForArchive(JobEntity job) {
    return ctx.em()
        .createQuery(
            "SELECT DISTINCT j FROM JobEntity j LEFT JOIN FETCH j.tags WHERE j.id = :id",
            JobEntity.class)
        .setParameter("id", job.getId())
        .getSingleResult();
  }

  private JobEntity updateJob(JobEntity job) {
    if (ctx.em().contains(job)) {
      ctx.em().detach(job);
    }

    Instant updatedAt = job.getUpdatedAt() != null ? job.getUpdatedAt() : Instant.now();
    int expectedVersion = job.getVersion() == null ? 0 : job.getVersion();

    Query query = ctx.em().createNativeQuery(UPDATE_SQL);
    int parameter = 1;
    query.setParameter(parameter++, job.getStatus() == null ? "PENDING" : job.getStatus().name());
    query.setParameter(
        parameter++, job.getPausedFromStatus() == null ? null : job.getPausedFromStatus().name());
    query.setParameter(parameter++, Timestamp.from(job.getScheduledTime()));
    query.setParameter(parameter++, job.getJobType().name());
    query.setParameter(parameter++, job.getPriority().ordinal());
    query.setParameter(parameter++, job.getAttempts());
    query.setParameter(parameter++, job.getMaxRetries());
    query.setParameter(parameter++, job.getBackoffPolicy().name());
    query.setParameter(parameter++, job.getBackoffParamMs());
    query.setParameter(parameter++, job.getTimeoutSec());
    query.setParameter(parameter++, job.getCronExpr() == null ? "" : job.getCronExpr());
    query.setParameter(parameter++, job.getZoneId() == null ? "UTC" : job.getZoneId());
    query.setParameter(
        parameter++, job.getNextFire() == null ? null : Timestamp.from(job.getNextFire()));
    query.setParameter(parameter++, payloadToJson(job));
    query.setParameter(parameter++, paramsToJson(job));
    query.setParameter(parameter++, job.getIdempotencyKey());
    query.setParameter(parameter++, job.getBusinessKey());
    query.setParameter(parameter++, job.getResourceName());
    query.setParameter(parameter++, callbackPayloadToJson(job.getOnSuccessPayload()));
    query.setParameter(parameter++, callbackPayloadToJson(job.getOnFailurePayload()));
    query.setParameter(parameter++, job.getDependsOn());
    query.setParameter(parameter++, job.getSupersededBy());
    query.setParameter(parameter++, job.getPickedBy());
    query.setParameter(
        parameter++, job.getPickedAt() == null ? null : Timestamp.from(job.getPickedAt()));
    query.setParameter(parameter++, job.getLastError());
    query.setParameter(parameter++, Timestamp.from(updatedAt));
    query.setParameter(
        parameter++,
        job.getExecutionStartTime() == null ? null : Timestamp.from(job.getExecutionStartTime()));
    query.setParameter(
        parameter++,
        job.getExecutionEndTime() == null ? null : Timestamp.from(job.getExecutionEndTime()));
    query.setParameter(parameter++, job.getExecutionDurationMs());
    query.setParameter(parameter++, job.getQueueWaitMs());
    query.setParameter(parameter++, job.getJobResult());
    query.setParameter(parameter++, job.getResultType());
    query.setParameter(parameter++, job.getId());
    query.setParameter(parameter, expectedVersion);

    int updated = query.executeUpdate();
    if (updated == 0) {
      throw new RatchetOptimisticLockException("Concurrent modification on job " + job.getId());
    }
    reservations.syncForJob(job);
    job.setUpdatedAt(updatedAt);
    job.setVersion(expectedVersion + 1);
    return job;
  }

  private void setBulkInsertParameters(Query query, JobEntity job, Instant now) {
    int parameter = 1;
    query.setParameter(parameter++, job.getId());
    query.setParameter(parameter++, job.getStatus() == null ? "PENDING" : job.getStatus().name());
    query.setParameter(
        parameter++, job.getPausedFromStatus() == null ? null : job.getPausedFromStatus().name());
    query.setParameter(parameter++, Timestamp.from(job.getScheduledTime()));
    query.setParameter(parameter++, job.getJobType().name());
    query.setParameter(parameter++, job.getPriority().ordinal());
    query.setParameter(parameter++, job.getAttempts());
    query.setParameter(parameter++, job.getMaxRetries());
    query.setParameter(parameter++, job.getBackoffPolicy().name());
    query.setParameter(parameter++, job.getBackoffParamMs());
    query.setParameter(parameter++, job.getTimeoutSec());
    query.setParameter(parameter++, job.getCronExpr() == null ? "" : job.getCronExpr());
    query.setParameter(parameter++, job.getZoneId() == null ? "UTC" : job.getZoneId());
    query.setParameter(
        parameter++, job.getNextFire() == null ? null : Timestamp.from(job.getNextFire()));
    query.setParameter(parameter++, payloadToJson(job));
    query.setParameter(parameter++, paramsToJson(job));
    query.setParameter(parameter++, job.getIdempotencyKey());
    query.setParameter(parameter++, job.getBusinessKey());
    query.setParameter(parameter++, job.getResourceName());
    query.setParameter(parameter++, callbackPayloadToJson(job.getOnSuccessPayload()));
    query.setParameter(parameter++, callbackPayloadToJson(job.getOnFailurePayload()));
    query.setParameter(parameter++, job.getDependsOn());
    query.setParameter(parameter++, job.getSupersededBy());
    query.setParameter(parameter++, job.getPickedBy());
    query.setParameter(
        parameter++, job.getPickedAt() == null ? null : Timestamp.from(job.getPickedAt()));
    query.setParameter(parameter++, job.getLastError());
    query.setParameter(
        parameter++, Timestamp.from(job.getCreatedAt() != null ? job.getCreatedAt() : now));
    query.setParameter(parameter++, job.getCreatedBy());
    query.setParameter(parameter++, Timestamp.from(now));
    query.setParameter(
        parameter++,
        job.getExecutionStartTime() == null ? null : Timestamp.from(job.getExecutionStartTime()));
    query.setParameter(
        parameter++,
        job.getExecutionEndTime() == null ? null : Timestamp.from(job.getExecutionEndTime()));
    query.setParameter(parameter++, job.getExecutionDurationMs());
    query.setParameter(parameter++, job.getQueueWaitMs());
    query.setParameter(parameter++, job.getJobResult());
    query.setParameter(parameter, job.getResultType());
  }

  private long countByNative(String sql, Object... params) {
    var query = ctx.em().createNativeQuery(sql);
    for (int i = 0; i < params.length; i++) {
      query.setParameter(i + 1, params[i]);
    }
    return ((Number) query.getSingleResult()).longValue();
  }
}
