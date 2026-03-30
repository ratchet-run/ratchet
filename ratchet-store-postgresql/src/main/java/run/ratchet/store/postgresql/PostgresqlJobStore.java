package run.ratchet.store.postgresql;

import com.fasterxml.jackson.databind.ObjectMapper;
import run.ratchet.api.JobPriority;
import run.ratchet.api.WorkflowCondition;
import run.ratchet.store.dto.BatchProgress;
import run.ratchet.store.dto.JobClaimDto;
import run.ratchet.store.entity.ArchivedJobEntity;
import run.ratchet.store.entity.BatchEntity;
import run.ratchet.store.entity.BatchMetricsEntity;
import run.ratchet.store.entity.DlqAlertEntity;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.entity.JobLogEntity;
import run.ratchet.store.entity.JobPayload;
import run.ratchet.store.entity.JobStatus;
import run.ratchet.store.entity.NodeEntity;
import run.ratchet.store.entity.ResourcePermitEntity;
import run.ratchet.store.entity.WorkflowConditionEntity;
import run.ratchet.store.spi.JobStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.persistence.EntityManager;
import jakarta.persistence.NoResultException;
import jakarta.persistence.PersistenceContext;
import jakarta.transaction.Transactional;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * PostgreSQL implementation of the {@link JobStore} SPI.
 *
 * <p>Uses JPA EntityManager for simple CRUD and native SQL for PostgreSQL-specific operations such
 * as {@code FOR UPDATE SKIP LOCKED}, {@code ON CONFLICT}, and {@code RETURNING} clauses.
 */
@ApplicationScoped
@Transactional
public class PostgresqlJobStore implements JobStore {

  private static final Logger log = Logger.getLogger(PostgresqlJobStore.class.getName());
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final String EXECUTABLE_JOB_TYPE_FILTER =
      "job_type IN ('SINGLE','BATCH_CHILD','CHAIN_STEP','WORKFLOW_BRANCH')";
  private static final String RECURRING_JOB_TYPE_FILTER = "job_type = 'RECURRING'";

  @PersistenceContext private EntityManager em;

  // ──────────────────────────────────────────────
  // JobCrudStore
  // ──────────────────────────────────────────────

  @Override
  public JobEntity save(JobEntity job) {
    if (job.getId() == null) {
      em.persist(job);
      return job;
    }
    return em.merge(job);
  }

  @Override
  public Optional<JobEntity> findById(long id) {
    return Optional.ofNullable(em.find(JobEntity.class, id));
  }

  @Override
  public Optional<JobEntity> findByIdForUpdate(long id) {
    @SuppressWarnings("unchecked")
    List<JobEntity> results =
        em.createNativeQuery(
                "SELECT * FROM scheduler_job WHERE job_id = ? FOR UPDATE", JobEntity.class)
            .setParameter(1, id)
            .getResultList();
    return results.isEmpty() ? Optional.empty() : Optional.of(results.get(0));
  }

  @Override
  public void delete(long id) {
    em.createNativeQuery("DELETE FROM scheduler_job WHERE job_id = ?")
        .setParameter(1, id)
        .executeUpdate();
  }

  @Override
  public JobStatus getJobStatus(long id) {
    @SuppressWarnings("unchecked")
    List<Object> results =
        em.createNativeQuery("SELECT status FROM scheduler_job WHERE job_id = ?")
            .setParameter(1, id)
            .getResultList();
    if (results.isEmpty()) {
      return null;
    }
    return JobStatus.valueOf((String) results.get(0));
  }

  @Override
  public List<JobEntity> findByIds(List<Long> ids) {
    if (ids.isEmpty()) {
      return List.of();
    }
    @SuppressWarnings("unchecked")
    List<JobEntity> results =
        em.createNativeQuery("SELECT * FROM scheduler_job WHERE job_id IN (:ids)", JobEntity.class)
            .setParameter("ids", ids)
            .getResultList();
    return results;
  }

  @Override
  public Optional<JobEntity> findActiveByBusinessKey(String businessKey) {
    @SuppressWarnings("unchecked")
    List<JobEntity> results =
        em.createNativeQuery(
                "SELECT * FROM scheduler_job WHERE business_key = ? AND status IN ('PENDING','RUNNING','PAUSED') LIMIT 1",
                JobEntity.class)
            .setParameter(1, businessKey)
            .getResultList();
    return results.isEmpty() ? Optional.empty() : Optional.of(results.get(0));
  }

  @Override
  public Optional<JobEntity> findByIdempotencyKey(String idempotencyKey) {
    @SuppressWarnings("unchecked")
    List<JobEntity> results =
        em.createNativeQuery(
                "SELECT * FROM scheduler_job WHERE idempotency_key = ?", JobEntity.class)
            .setParameter(1, idempotencyKey)
            .getResultList();
    return results.isEmpty() ? Optional.empty() : Optional.of(results.get(0));
  }

  @Override
  public List<JobEntity> findDependants(long parentJobId) {
    @SuppressWarnings("unchecked")
    List<JobEntity> results =
        em.createNativeQuery("SELECT * FROM scheduler_job WHERE depends_on = ?", JobEntity.class)
            .setParameter(1, parentJobId)
            .getResultList();
    return results;
  }

  @Override
  public Optional<Instant> findEarliestRecurringNextFire() {
    @SuppressWarnings("unchecked")
    List<Object> results =
        em.createNativeQuery(
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
        em.createNativeQuery(
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
        em.createNativeQuery(
                "SELECT COALESCE(AVG(execution_duration_ms), 0) "
                    + "FROM scheduler_job WHERE status = 'SUCCEEDED' AND updated_at >= ?")
            .setParameter(1, Timestamp.from(since))
            .getSingleResult();
    return result == null ? 0.0 : ((Number) result).doubleValue();
  }

  @Override
  public double getAverageBatchSize(Instant since) {
    Object result =
        em.createNativeQuery(
                "SELECT COALESCE(AVG(total_items), 0) "
                    + "FROM scheduler_batch b "
                    + "INNER JOIN scheduler_job j ON b.batch_id = j.job_id "
                    + "WHERE j.updated_at >= ?")
            .setParameter(1, Timestamp.from(since))
            .getSingleResult();
    return result == null ? 0.0 : ((Number) result).doubleValue();
  }

  @Override
  public Optional<Instant> getOldestPendingJobTime() {
    @SuppressWarnings("unchecked")
    List<Object> results =
        em.createNativeQuery(
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
        em.createNativeQuery(
                "SELECT COALESCE(PERCENTILE_CONT(?) WITHIN GROUP (ORDER BY queue_wait_ms), 0) "
                    + "FROM scheduler_job WHERE queue_wait_ms IS NOT NULL AND status = 'SUCCEEDED'")
            .setParameter(1, percentile)
            .getSingleResult();
    return result == null ? 0L : ((Number) result).longValue();
  }

  // ──────────────────────────────────────────────
  // JobClaimStore
  // ──────────────────────────────────────────────

  @Override
  public List<JobEntity> claimNextBatch(int limit, String nodeId) {
    int boostInterval = getPriorityBoostIntervalMinutes();
    var updateQuery =
        em.createNativeQuery(
                buildClaimUpdateSql(EXECUTABLE_JOB_TYPE_FILTER, "scheduled_time", boostInterval))
            .setParameter(1, limit)
            .setParameter(3, nodeId);
    if (boostInterval > 0) {
      updateQuery.setParameter(2, boostInterval);
    }

    int claimed = updateQuery.executeUpdate();

    if (claimed == 0) {
      return List.of();
    }

    em.clear();
    var readQuery =
        em.createNativeQuery(
                buildClaimReadBackSql("*", "scheduled_time", boostInterval), JobEntity.class)
            .setParameter(1, nodeId);
    if (boostInterval > 0) {
      readQuery.setParameter(2, boostInterval).setParameter(3, limit);
    } else {
      readQuery.setParameter(2, limit);
    }
    @SuppressWarnings("unchecked")
    List<JobEntity> jobs = readQuery.getResultList();
    return jobs;
  }

  @Override
  public List<JobClaimDto> claimNextBatchOptimized(int limit, String nodeId) {
    int boostInterval = getPriorityBoostIntervalMinutes();
    var updateQuery =
        em.createNativeQuery(
                buildClaimUpdateSql(EXECUTABLE_JOB_TYPE_FILTER, "scheduled_time", boostInterval))
            .setParameter(1, limit)
            .setParameter(3, nodeId);
    if (boostInterval > 0) {
      updateQuery.setParameter(2, boostInterval);
    }

    int claimed = updateQuery.executeUpdate();

    if (claimed == 0) {
      return List.of();
    }

    String selectColumns =
        "job_id, status, job_type, priority, scheduled_time, version, "
            + "timeout_sec, picked_by, picked_at, business_key, attempts, max_retries";
    var readQuery =
        em.createNativeQuery(buildClaimReadBackSql(selectColumns, "scheduled_time", boostInterval));
    readQuery.setParameter(1, nodeId);
    if (boostInterval > 0) {
      readQuery.setParameter(2, boostInterval).setParameter(3, limit);
    } else {
      readQuery.setParameter(2, limit);
    }
    @SuppressWarnings("unchecked")
    List<Object[]> rows = readQuery.getResultList();

    List<JobClaimDto> claims = new ArrayList<>(rows.size());
    for (Object[] row : rows) {
      claims.add(
          new JobClaimDto(
              ((Number) row[0]).longValue(),
              JobStatus.RUNNING,
              JobExecutionType.valueOf((String) row[2]),
              safeJobPriority(((Number) row[3]).intValue()),
              toInstant(row[4]),
              row[5] == null ? null : ((Number) row[5]).intValue(),
              ((Number) row[6]).intValue(),
              nodeId,
              toInstant(row[8]),
              (String) row[9],
              ((Number) row[10]).intValue(),
              ((Number) row[11]).intValue()));
    }
    return claims;
  }

  @Override
  public List<JobEntity> claimDueRecurring(int limit, String nodeId) {
    int boostInterval = getPriorityBoostIntervalMinutes();
    var updateQuery =
        em.createNativeQuery(
                buildClaimUpdateSql(RECURRING_JOB_TYPE_FILTER, "next_fire", boostInterval))
            .setParameter(1, limit)
            .setParameter(3, nodeId);
    if (boostInterval > 0) {
      updateQuery.setParameter(2, boostInterval);
    }

    int claimed = updateQuery.executeUpdate();

    if (claimed == 0) {
      return List.of();
    }

    em.clear();
    var readQuery =
        em.createNativeQuery(
                buildClaimReadBackSql("*", "next_fire", boostInterval), JobEntity.class)
            .setParameter(1, nodeId);
    if (boostInterval > 0) {
      readQuery.setParameter(2, boostInterval).setParameter(3, limit);
    } else {
      readQuery.setParameter(2, limit);
    }
    @SuppressWarnings("unchecked")
    List<JobEntity> jobs = readQuery.getResultList();
    return jobs;
  }

  // ──────────────────────────────────────────────
  // JobStatusStore
  // ──────────────────────────────────────────────

  @Override
  public void updateJobStatus(long id, JobStatus status, String errorMessage) {
    em.createNativeQuery(
            "UPDATE scheduler_job SET status = ?, last_error = ?, "
                + "updated_at = statement_timestamp() WHERE job_id = ?")
        .setParameter(1, status.name())
        .setParameter(2, errorMessage)
        .setParameter(3, id)
        .executeUpdate();
  }

  @Override
  public boolean compareAndSwapStatus(
      long id, JobStatus expected, JobStatus newStatus, String error) {
    int updated =
        em.createNativeQuery(
                "UPDATE scheduler_job SET status = ?, last_error = ?, "
                    + "updated_at = statement_timestamp() "
                    + "WHERE job_id = ? AND status = ?")
            .setParameter(1, newStatus.name())
            .setParameter(2, error)
            .setParameter(3, id)
            .setParameter(4, expected.name())
            .executeUpdate();
    return updated > 0;
  }

  @Override
  public int incrementRetryAttempt(long id) {
    List<?> results =
        em.createNativeQuery(
                "UPDATE scheduler_job SET attempts = attempts + 1, "
                    + "updated_at = statement_timestamp() "
                    + "WHERE job_id = ? "
                    + "AND status = 'RUNNING' "
                    + "RETURNING attempts")
            .setParameter(1, id)
            .getResultList();
    if (results.isEmpty()) {
      return -1;
    }
    return ((Number) results.get(0)).intValue();
  }

  @Override
  public boolean tryPickUpJob(long id, String nodeId) {
    int updated =
        em.createNativeQuery(
                "UPDATE scheduler_job SET status = 'RUNNING', picked_by = ?, "
                    + "picked_at = statement_timestamp(), updated_at = statement_timestamp() "
                    + "WHERE job_id = ? AND status = 'PENDING'")
            .setParameter(1, nodeId)
            .setParameter(2, id)
            .executeUpdate();
    return updated > 0;
  }

  @Override
  public boolean markJobSucceeded(
      long id,
      String resultJson,
      String resultType,
      Instant start,
      Instant end,
      Long durationMs,
      Long queueWaitMs) {
    int updated =
        em.createNativeQuery(
                "UPDATE scheduler_job SET status = 'SUCCEEDED', "
                    + "job_result = ?, result_type = ?, "
                    + "execution_start_time = ?, execution_end_time = ?, "
                    + "execution_duration_ms = ?, queue_wait_ms = ?, "
                    + "last_error = NULL, updated_at = statement_timestamp() "
                    + "WHERE job_id = ? AND status = 'RUNNING'")
            .setParameter(1, resultJson)
            .setParameter(2, resultType)
            .setParameter(3, start == null ? null : Timestamp.from(start))
            .setParameter(4, end == null ? null : Timestamp.from(end))
            .setParameter(5, durationMs)
            .setParameter(6, queueWaitMs)
            .setParameter(7, id)
            .executeUpdate();
    return updated > 0;
  }

  @Override
  public boolean markJobSucceededAndUpdateBatch(
      long jobId,
      String resultJson,
      String resultType,
      Instant start,
      Instant end,
      Long durationMs,
      Long queueWaitMs,
      long batchId) {
    boolean jobUpdated =
        markJobSucceeded(jobId, resultJson, resultType, start, end, durationMs, queueWaitMs);
    if (jobUpdated) {
      incrementCompletedAtomic(batchId);
    }
    return jobUpdated;
  }

  @Override
  public boolean scheduleJobRetry(long id, String error, Instant newScheduledTime, int attempts) {
    int updated =
        em.createNativeQuery(
                "UPDATE scheduler_job SET status = 'PENDING', "
                    + "scheduled_time = ?, attempts = ?, last_error = ?, "
                    + "picked_by = NULL, picked_at = NULL, "
                    + "updated_at = statement_timestamp() "
                    + "WHERE job_id = ? AND status IN ('RUNNING','FAILED')")
            .setParameter(1, Timestamp.from(newScheduledTime))
            .setParameter(2, attempts)
            .setParameter(3, error)
            .setParameter(4, id)
            .executeUpdate();
    return updated > 0;
  }

  @Override
  public boolean resetRunningJob(long id, String nodeId) {
    int updated =
        em.createNativeQuery(
                "UPDATE scheduler_job SET status = 'PENDING', "
                    + "picked_by = NULL, picked_at = NULL, "
                    + "updated_at = statement_timestamp() "
                    + "WHERE job_id = ? AND status = 'RUNNING' AND picked_by = ?")
            .setParameter(1, id)
            .setParameter(2, nodeId)
            .executeUpdate();
    return updated > 0;
  }

  @Override
  public int resetRunningJobs(String nodeId) {
    return em.createNativeQuery(
            "UPDATE scheduler_job SET status = 'PENDING', "
                + "picked_by = NULL, picked_at = NULL, "
                + "updated_at = statement_timestamp() "
                + "WHERE status = 'RUNNING' AND picked_by = ?")
        .setParameter(1, nodeId)
        .executeUpdate();
  }

  @Override
  public int cancelRecurringJobsByTag(String tag) {
    return em.createNativeQuery(
            "UPDATE scheduler_job SET status = 'CANCELED', "
                + "updated_at = statement_timestamp() "
                + "WHERE job_id IN ("
                + "  SELECT j.job_id FROM scheduler_job j "
                + "  INNER JOIN scheduler_job_tag t ON j.job_id = t.job_id "
                + "  WHERE t.tag = ? AND j.job_type = 'RECURRING' "
                + "  AND j.status IN ('PENDING','RUNNING','PAUSED')"
                + ")")
        .setParameter(1, tag)
        .executeUpdate();
  }

  @Override
  public int cancelRecurringJobByBusinessKey(String businessKey) {
    return em.createNativeQuery(
            "UPDATE scheduler_job SET status = 'CANCELED', "
                + "updated_at = statement_timestamp() "
                + "WHERE business_key = ? AND job_type = 'RECURRING' "
                + "AND status IN ('PENDING','RUNNING','PAUSED')")
        .setParameter(1, businessKey)
        .executeUpdate();
  }

  @Override
  public int cancelOrphanedRecurringAnnotationJobs(
      Set<String> registeredIds, Instant nodeStartTime) {
    if (registeredIds.isEmpty()) {
      return 0;
    }
    return em.createNativeQuery(
            "UPDATE scheduler_job SET status = 'CANCELED', "
                + "updated_at = statement_timestamp() "
                + "WHERE job_type = 'RECURRING' "
                + "AND status IN ('PENDING','RUNNING','PAUSED') "
                + "AND created_at < :cutoff "
                + "AND business_key IS NOT NULL "
                + "AND business_key NOT IN (:ids)")
        .setParameter("cutoff", Timestamp.from(nodeStartTime))
        .setParameter("ids", registeredIds)
        .executeUpdate();
  }

  @Override
  public boolean resetFailedToPending(long id) {
    int updated =
        em.createNativeQuery(
                "UPDATE scheduler_job SET status = 'PENDING', attempts = 0, "
                    + "last_error = NULL, scheduled_time = statement_timestamp(), "
                    + "picked_by = NULL, picked_at = NULL, "
                    + "updated_at = statement_timestamp() "
                    + "WHERE job_id = ? AND status = 'FAILED'")
            .setParameter(1, id)
            .executeUpdate();
    return updated > 0;
  }

  @Override
  public boolean transitionToPaused(long id, JobStatus expected) {
    int updated =
        em.createNativeQuery(
                "UPDATE scheduler_job SET status = 'PAUSED', "
                    + "paused_from_status = ?, "
                    + "updated_at = statement_timestamp() "
                    + "WHERE job_id = ? AND status = ?")
            .setParameter(1, expected.name())
            .setParameter(2, id)
            .setParameter(3, expected.name())
            .executeUpdate();
    return updated > 0;
  }

  @Override
  public boolean transitionFromPaused(long id, JobStatus target) {
    int updated =
        em.createNativeQuery(
                "UPDATE scheduler_job SET status = ?, "
                    + "paused_from_status = NULL, "
                    + "updated_at = statement_timestamp() "
                    + "WHERE job_id = ? AND status = 'PAUSED'")
            .setParameter(1, target.name())
            .setParameter(2, id)
            .executeUpdate();
    return updated > 0;
  }

  // ──────────────────────────────────────────────
  // JobBulkStore
  // ──────────────────────────────────────────────

  @Override
  public void bulkInsert(List<JobEntity> jobs) {
    if (jobs.isEmpty()) {
      return;
    }
    em.unwrap(Connection.class);
    // Use JDBC batch insert via the EntityManager's unwrapped connection
    em.createNativeQuery("SELECT 1").getSingleResult(); // force connection
    var sessionImpl = em.getDelegate();
    try {
      @SuppressWarnings("java:S3011")
      Connection conn = em.unwrap(Connection.class);
      String sql =
          "INSERT INTO scheduler_job "
              + "(job_id, status, paused_from_status, scheduled_time, job_type, priority, "
              + "attempts, max_retries, backoff_policy, backoff_param_ms, timeout_sec, "
              + "cron_expr, zone_id, next_fire, payload, params, "
              + "idempotency_key, business_key, resource_name, depends_on, superseded_by, "
              + "picked_by, picked_at, last_error, created_at, created_by, "
              + "updated_at, execution_start_time, execution_end_time, execution_duration_ms, "
              + "queue_wait_ms, job_result, result_type, version) "
              + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0)";
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        for (JobEntity job : jobs) {
          Instant now = Instant.now();
          ps.setLong(1, job.getId());
          ps.setString(2, job.getStatus() == null ? "PENDING" : job.getStatus().name());
          ps.setString(
              3, job.getPausedFromStatus() == null ? null : job.getPausedFromStatus().name());
          ps.setTimestamp(4, Timestamp.from(job.getScheduledTime()));
          ps.setString(5, job.getJobType().name());
          ps.setInt(6, job.getPriority().ordinal());
          ps.setInt(7, job.getAttempts());
          ps.setInt(8, job.getMaxRetries());
          ps.setString(9, job.getBackoffPolicy().name());
          ps.setInt(10, job.getBackoffParamMs());
          ps.setInt(11, job.getTimeoutSec());
          ps.setString(12, job.getCronExpr() == null ? "" : job.getCronExpr());
          ps.setString(13, job.getZoneId() == null ? "UTC" : job.getZoneId());
          if (job.getNextFire() != null) {
            ps.setTimestamp(14, Timestamp.from(job.getNextFire()));
          } else {
            ps.setNull(14, Types.TIMESTAMP);
          }
          ps.setString(15, payloadToJson(job));
          ps.setString(16, paramsToJson(job));
          ps.setString(17, job.getIdempotencyKey());
          ps.setString(18, job.getBusinessKey());
          ps.setString(19, job.getResourceName());
          if (job.getDependsOn() != null) {
            ps.setLong(20, job.getDependsOn());
          } else {
            ps.setNull(20, Types.BIGINT);
          }
          if (job.getSupersededBy() != null) {
            ps.setLong(21, job.getSupersededBy());
          } else {
            ps.setNull(21, Types.BIGINT);
          }
          ps.setString(22, job.getPickedBy());
          if (job.getPickedAt() != null) {
            ps.setTimestamp(23, Timestamp.from(job.getPickedAt()));
          } else {
            ps.setNull(23, Types.TIMESTAMP);
          }
          ps.setString(24, job.getLastError());
          ps.setTimestamp(
              25, Timestamp.from(job.getCreatedAt() != null ? job.getCreatedAt() : now));
          ps.setString(26, job.getCreatedBy());
          ps.setTimestamp(27, Timestamp.from(now));
          if (job.getExecutionStartTime() != null) {
            ps.setTimestamp(28, Timestamp.from(job.getExecutionStartTime()));
          } else {
            ps.setNull(28, Types.TIMESTAMP);
          }
          if (job.getExecutionEndTime() != null) {
            ps.setTimestamp(29, Timestamp.from(job.getExecutionEndTime()));
          } else {
            ps.setNull(29, Types.TIMESTAMP);
          }
          if (job.getExecutionDurationMs() != null) {
            ps.setLong(30, job.getExecutionDurationMs());
          } else {
            ps.setNull(30, Types.BIGINT);
          }
          if (job.getQueueWaitMs() != null) {
            ps.setLong(31, job.getQueueWaitMs());
          } else {
            ps.setNull(31, Types.BIGINT);
          }
          ps.setString(32, job.getJobResult());
          ps.setString(33, job.getResultType());
          ps.addBatch();
        }
        ps.executeBatch();
      }
    } catch (Exception e) {
      throw new RuntimeException("Bulk insert failed", e);
    } finally {
      em.clear();
    }
  }

  @Override
  public int deleteJobsByIds(List<Long> ids) {
    if (ids.isEmpty()) {
      return 0;
    }
    return em.createNativeQuery("DELETE FROM scheduler_job WHERE job_id IN (:ids)")
        .setParameter("ids", ids)
        .executeUpdate();
  }

  @Override
  public int deleteDlqOlderThan(Instant cutoff) {
    return em.createNativeQuery(
            "DELETE FROM scheduler_job WHERE status = 'FAILED' "
                + "AND attempts >= max_retries AND updated_at < ?")
        .setParameter(1, Timestamp.from(cutoff))
        .executeUpdate();
  }

  @Override
  public int resetOrphanJobs(Duration grace) {
    long graceMinutes = grace.toMinutes();
    return em.createNativeQuery(
            "UPDATE scheduler_job SET status = 'PENDING', "
                + "picked_by = NULL, picked_at = NULL, "
                + "updated_at = statement_timestamp() "
                + "WHERE status = 'RUNNING' "
                + "AND picked_by NOT IN ("
                + "  SELECT node_id FROM scheduler_node "
                + "  WHERE heartbeat_ts > statement_timestamp() - ? * interval '1 minute'"
                + ") "
                + "AND floor(extract(epoch from (statement_timestamp() - picked_at))/60)::bigint >= ?")
        .setParameter(1, graceMinutes)
        .setParameter(2, graceMinutes)
        .executeUpdate();
  }

  // ──────────────────────────────────────────────
  // BatchStore
  // ──────────────────────────────────────────────

  @Override
  public BatchEntity saveBatch(BatchEntity batch) {
    if (em.find(BatchEntity.class, batch.getId()) == null) {
      em.persist(batch);
      return batch;
    }
    return em.merge(batch);
  }

  @Override
  public Optional<BatchEntity> findBatchById(long batchId) {
    return Optional.ofNullable(em.find(BatchEntity.class, batchId));
  }

  @Override
  public List<BatchEntity> findBatchesByIds(List<Long> batchIds) {
    if (batchIds == null || batchIds.isEmpty()) {
      return List.of();
    }
    return em.createQuery("SELECT b FROM BatchEntity b WHERE b.id IN :ids", BatchEntity.class)
        .setParameter("ids", batchIds)
        .getResultList();
  }

  @Override
  public BatchProgress incrementCompletedAtomic(long batchId) {
    Object[] row =
        (Object[])
            em.createNativeQuery(
                    "UPDATE scheduler_batch SET completed_items = completed_items + 1 "
                        + "WHERE batch_id = ? "
                        + "RETURNING completed_items, failed_items, total_items, progress_hook")
                .setParameter(1, batchId)
                .getSingleResult();
    return new BatchProgress(
        batchId,
        ((Number) row[2]).intValue(),
        ((Number) row[0]).intValue(),
        ((Number) row[1]).intValue(),
        parseProgressHook(row[3]));
  }

  @Override
  public BatchProgress incrementFailedAtomic(long batchId) {
    Object[] row =
        (Object[])
            em.createNativeQuery(
                    "UPDATE scheduler_batch SET failed_items = failed_items + 1 "
                        + "WHERE batch_id = ? "
                        + "RETURNING completed_items, failed_items, total_items, progress_hook")
                .setParameter(1, batchId)
                .getSingleResult();
    return new BatchProgress(
        batchId,
        ((Number) row[2]).intValue(),
        ((Number) row[0]).intValue(),
        ((Number) row[1]).intValue(),
        parseProgressHook(row[3]));
  }

  private JobPayload parseProgressHook(Object jsonValue) {
    if (jsonValue == null) {
      return null;
    }
    try {
      return OBJECT_MAPPER.readValue(jsonValue.toString(), JobPayload.class);
    } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
      log.warning("Failed to parse progress_hook JSON: " + e.getMessage());
      return null;
    }
  }

  @Override
  public boolean markBatchCompleteIfReady(long batchId) {
    int updated =
        em.createNativeQuery(
                "UPDATE scheduler_batch SET completion_processed = TRUE "
                    + "WHERE batch_id = ? AND completion_processed = FALSE "
                    + "AND (completed_items + failed_items) >= total_items")
            .setParameter(1, batchId)
            .executeUpdate();
    return updated > 0;
  }

  @Override
  public List<Long> findRecoverableBatchIds(int limit) {
    @SuppressWarnings("unchecked")
    List<Number> results =
        em.createNativeQuery(
                "SELECT batch_id FROM scheduler_batch "
                    + "WHERE completion_processed = FALSE "
                    + "AND (completed_items + failed_items) >= total_items "
                    + "LIMIT ?")
            .setParameter(1, limit)
            .getResultList();
    return results.stream().map(Number::longValue).toList();
  }

  @Override
  public boolean updateBatchTotalItems(long batchId, int totalItems) {
    int updated =
        em.createNativeQuery("UPDATE scheduler_batch SET total_items = ? WHERE batch_id = ?")
            .setParameter(1, totalItems)
            .setParameter(2, batchId)
            .executeUpdate();
    return updated > 0;
  }

  // ──────────────────────────────────────────────
  // LockStore
  // ──────────────────────────────────────────────

  @Override
  public boolean tryLock(String name, Duration ttl, String nodeId) {
    long ttlSeconds = ttl.toSeconds();
    int updated =
        em.createNativeQuery(
                "INSERT INTO scheduler_lock (lock_name, owner_node, locked_at, expires_at) "
                    + "VALUES (?, ?, statement_timestamp(), statement_timestamp() + ? * interval '1 second') "
                    + "ON CONFLICT (lock_name) DO UPDATE SET "
                    + "  owner_node = EXCLUDED.owner_node, "
                    + "  locked_at = statement_timestamp(), "
                    + "  expires_at = statement_timestamp() + ? * interval '1 second' "
                    + "WHERE scheduler_lock.expires_at < statement_timestamp()")
            .setParameter(1, name)
            .setParameter(2, nodeId)
            .setParameter(3, ttlSeconds)
            .setParameter(4, ttlSeconds)
            .executeUpdate();
    return updated > 0;
  }

  @Override
  public void unlock(String name, String nodeId) {
    em.createNativeQuery("DELETE FROM scheduler_lock WHERE lock_name = ? AND owner_node = ?")
        .setParameter(1, name)
        .setParameter(2, nodeId)
        .executeUpdate();
  }

  @Override
  public boolean renewLock(String name, Duration extension, String nodeId) {
    long extensionSeconds = extension.toSeconds();
    int updated =
        em.createNativeQuery(
                "UPDATE scheduler_lock SET "
                    + "expires_at = statement_timestamp() + ? * interval '1 second' "
                    + "WHERE lock_name = ? AND owner_node = ?")
            .setParameter(1, extensionSeconds)
            .setParameter(2, name)
            .setParameter(3, nodeId)
            .executeUpdate();
    return updated > 0;
  }

  // ──────────────────────────────────────────────
  // NodeStore
  // ──────────────────────────────────────────────

  @Override
  public void upsertHeartbeat(String nodeId, Instant ts) {
    em.createNativeQuery(
            "INSERT INTO scheduler_node (node_id, heartbeat_ts, started_at) "
                + "VALUES (?, ?, ?) "
                + "ON CONFLICT (node_id) DO UPDATE SET heartbeat_ts = EXCLUDED.heartbeat_ts")
        .setParameter(1, nodeId)
        .setParameter(2, Timestamp.from(ts))
        .setParameter(3, Timestamp.from(ts))
        .executeUpdate();
  }

  @Override
  public Optional<NodeEntity> findNodeById(String nodeId) {
    return Optional.ofNullable(em.find(NodeEntity.class, nodeId));
  }

  @Override
  public List<NodeEntity> findInactiveNodesSince(Instant cutoff) {
    @SuppressWarnings("unchecked")
    List<NodeEntity> results =
        em.createNativeQuery(
                "SELECT * FROM scheduler_node WHERE heartbeat_ts < ?", NodeEntity.class)
            .setParameter(1, Timestamp.from(cutoff))
            .getResultList();
    return results;
  }

  @Override
  public int deleteInactiveNodesSince(Instant cutoff) {
    return em.createNativeQuery("DELETE FROM scheduler_node WHERE heartbeat_ts < ?")
        .setParameter(1, Timestamp.from(cutoff))
        .executeUpdate();
  }

  @Override
  public Instant getDatabaseTime() {
    Timestamp ts =
        (Timestamp) em.createNativeQuery("SELECT statement_timestamp()").getSingleResult();
    return ts.toInstant();
  }

  // ──────────────────────────────────────────────
  // ArchiveStore
  // ──────────────────────────────────────────────

  @Override
  public ArchivedJobEntity archiveJob(JobEntity job, String reason, String archivedBy) {
    ArchivedJobEntity archive = buildArchive(job, reason, archivedBy);
    em.persist(archive);
    return archive;
  }

  @Override
  public int archiveJobsBatch(List<JobEntity> jobs, String reason, String archivedBy) {
    int count = 0;
    for (JobEntity job : jobs) {
      archiveJob(job, reason, archivedBy);
      count++;
    }
    return count;
  }

  @Override
  public List<JobEntity> findJobsForArchiving(Instant olderThan, int limit) {
    return em.createQuery(
            "SELECT DISTINCT j FROM JobEntity j LEFT JOIN FETCH j.tags WHERE j.status IN ("
                + "run.ratchet.store.entity.JobStatus.SUCCEEDED, "
                + "run.ratchet.store.entity.JobStatus.FAILED, "
                + "run.ratchet.store.entity.JobStatus.CANCELED) "
                + "AND j.updatedAt < :cutoff "
                + "ORDER BY j.updatedAt ASC",
            JobEntity.class)
        .setParameter("cutoff", olderThan)
        .setMaxResults(limit)
        .getResultList();
  }

  @Override
  public long countJobsForArchiving(Instant olderThan) {
    return countByNative(
        "SELECT COUNT(*) FROM scheduler_job "
            + "WHERE status IN ('SUCCEEDED','FAILED','CANCELED') AND updated_at < ?",
        Timestamp.from(olderThan));
  }

  @Override
  public List<ArchivedJobEntity> findArchivedJobs(
      String targetClass, String businessKey, Instant from, Instant to, int limit) {
    StringBuilder sql = new StringBuilder("SELECT * FROM scheduler_job_archive WHERE 1=1");
    List<Object> params = new ArrayList<>();
    int idx = 0;
    if (targetClass != null) {
      sql.append(" AND target_class = ?");
      params.add(targetClass);
    }
    if (businessKey != null) {
      sql.append(" AND business_key = ?");
      params.add(businessKey);
    }
    if (from != null) {
      sql.append(" AND archived_at >= ?");
      params.add(Timestamp.from(from));
    }
    if (to != null) {
      sql.append(" AND archived_at <= ?");
      params.add(Timestamp.from(to));
    }
    sql.append(" ORDER BY archived_at DESC LIMIT ?");
    params.add(limit);

    var query = em.createNativeQuery(sql.toString(), ArchivedJobEntity.class);
    for (int i = 0; i < params.size(); i++) {
      query.setParameter(i + 1, params.get(i));
    }
    @SuppressWarnings("unchecked")
    List<ArchivedJobEntity> results = query.getResultList();
    return results;
  }

  @Override
  public int purgeArchivedJobs(Instant olderThan) {
    return em.createNativeQuery("DELETE FROM scheduler_job_archive WHERE archived_at < ?")
        .setParameter(1, Timestamp.from(olderThan))
        .executeUpdate();
  }

  // ──────────────────────────────────────────────
  // ExecutionStore
  // ──────────────────────────────────────────────

  @Override
  public JobExecutionEntity saveExecution(JobExecutionEntity execution) {
    if (execution.getId() == null) {
      em.persist(execution);
      return execution;
    }
    return em.merge(execution);
  }

  @Override
  public List<JobExecutionEntity> findExecutionsByJobId(long jobId) {
    @SuppressWarnings("unchecked")
    List<JobExecutionEntity> results =
        em.createNativeQuery(
                "SELECT * FROM scheduler_job_execution WHERE job_id = ? ORDER BY attempt ASC",
                JobExecutionEntity.class)
            .setParameter(1, jobId)
            .getResultList();
    return results;
  }

  @Override
  public Optional<JobExecutionEntity> findLatestExecution(long jobId) {
    @SuppressWarnings("unchecked")
    List<JobExecutionEntity> results =
        em.createNativeQuery(
                "SELECT * FROM scheduler_job_execution WHERE job_id = ? ORDER BY attempt DESC LIMIT 1",
                JobExecutionEntity.class)
            .setParameter(1, jobId)
            .getResultList();
    return results.isEmpty() ? Optional.empty() : Optional.of(results.get(0));
  }

  @Override
  public int countExecutionAttempts(long jobId) {
    return ((Number)
            em.createNativeQuery("SELECT COUNT(*) FROM scheduler_job_execution WHERE job_id = ?")
                .setParameter(1, jobId)
                .getSingleResult())
        .intValue();
  }

  // ──────────────────────────────────────────────
  // JobLogStore
  // ──────────────────────────────────────────────

  @Override
  public void appendLog(JobLogEntity logEntry) {
    em.persist(logEntry);
  }

  @Override
  public int purgeLogsOlderThan(Instant cutoff) {
    return em.createNativeQuery("DELETE FROM scheduler_job_log WHERE ts < ?")
        .setParameter(1, Timestamp.from(cutoff))
        .executeUpdate();
  }

  // ──────────────────────────────────────────────
  // TagStore
  // ──────────────────────────────────────────────

  @Override
  public void insertTags(long jobId, List<String> tags) {
    if (tags == null || tags.isEmpty()) {
      return;
    }
    for (String tag : tags) {
      em.createNativeQuery(
              "INSERT INTO scheduler_job_tag (job_id, tag) VALUES (?, ?) "
                  + "ON CONFLICT (job_id, tag) DO NOTHING")
          .setParameter(1, jobId)
          .setParameter(2, tag)
          .executeUpdate();
    }
  }

  @Override
  public int deleteTagsByJobId(long jobId) {
    return em.createNativeQuery("DELETE FROM scheduler_job_tag WHERE job_id = ?")
        .setParameter(1, jobId)
        .executeUpdate();
  }

  @Override
  public List<Long> findJobIdsByTag(String tag, int limit, int offset) {
    @SuppressWarnings("unchecked")
    List<Number> results =
        em.createNativeQuery(
                "SELECT job_id FROM scheduler_job_tag WHERE tag = ? "
                    + "ORDER BY job_id LIMIT ? OFFSET ?")
            .setParameter(1, tag)
            .setParameter(2, limit)
            .setParameter(3, offset)
            .getResultList();
    return results.stream().map(Number::longValue).toList();
  }

  // ──────────────────────────────────────────────
  // WorkflowConditionStore
  // ──────────────────────────────────────────────

  @Override
  public WorkflowConditionEntity saveCondition(WorkflowConditionEntity condition) {
    if (condition.getId() == null) {
      em.persist(condition);
      return condition;
    }
    return em.merge(condition);
  }

  @Override
  public WorkflowConditionEntity findConditionById(long id) {
    return em.find(WorkflowConditionEntity.class, id);
  }

  @Override
  public List<WorkflowConditionEntity> findConditionsByParentJobId(long parentJobId) {
    @SuppressWarnings("unchecked")
    List<WorkflowConditionEntity> results =
        em.createNativeQuery(
                "SELECT * FROM scheduler_workflow_condition WHERE parent_job_id = ? "
                    + "ORDER BY condition_priority ASC",
                WorkflowConditionEntity.class)
            .setParameter(1, parentJobId)
            .getResultList();
    return results;
  }

  @Override
  public List<WorkflowConditionEntity> findConditionsByChildJobId(long childJobId) {
    @SuppressWarnings("unchecked")
    List<WorkflowConditionEntity> results =
        em.createNativeQuery(
                "SELECT * FROM scheduler_workflow_condition WHERE child_job_id = ?",
                WorkflowConditionEntity.class)
            .setParameter(1, childJobId)
            .getResultList();
    return results;
  }

  @Override
  public List<WorkflowConditionEntity> findConditionsByType(
      long parentJobId, WorkflowCondition.ConditionType type) {
    @SuppressWarnings("unchecked")
    List<WorkflowConditionEntity> results =
        em.createNativeQuery(
                "SELECT * FROM scheduler_workflow_condition "
                    + "WHERE parent_job_id = ? AND condition_type = ?",
                WorkflowConditionEntity.class)
            .setParameter(1, parentJobId)
            .setParameter(2, type.name())
            .getResultList();
    return results;
  }

  @Override
  public void deleteConditionById(long id) {
    em.createNativeQuery("DELETE FROM scheduler_workflow_condition WHERE id = ?")
        .setParameter(1, id)
        .executeUpdate();
  }

  @Override
  public void deleteConditionsByParentJobId(long parentJobId) {
    em.createNativeQuery("DELETE FROM scheduler_workflow_condition WHERE parent_job_id = ?")
        .setParameter(1, parentJobId)
        .executeUpdate();
  }

  @Override
  public void deleteConditionsByChildJobId(long childJobId) {
    em.createNativeQuery("DELETE FROM scheduler_workflow_condition WHERE child_job_id = ?")
        .setParameter(1, childJobId)
        .executeUpdate();
  }

  @Override
  public long countConditionsByParentJobId(long parentJobId) {
    return countByNative(
        "SELECT COUNT(*) FROM scheduler_workflow_condition WHERE parent_job_id = ?", parentJobId);
  }

  // ──────────────────────────────────────────────
  // BatchMetricsStore
  // ──────────────────────────────────────────────

  @Override
  public BatchMetricsEntity saveBatchMetrics(BatchMetricsEntity metrics) {
    if (em.find(BatchMetricsEntity.class, metrics.getBatchId()) == null) {
      em.persist(metrics);
      return metrics;
    }
    return em.merge(metrics);
  }

  @Override
  public Optional<BatchMetricsEntity> findBatchMetrics(long batchId) {
    return Optional.ofNullable(em.find(BatchMetricsEntity.class, batchId));
  }

  @Override
  public void addChildExecutionTime(long batchId, long durationMs) {
    em.createNativeQuery(
            "UPDATE scheduler_batch_metrics SET "
                + "child_execution_ms = COALESCE(child_execution_ms, 0) + ?, "
                + "success_count = success_count + 1 "
                + "WHERE batch_id = ?")
        .setParameter(1, durationMs)
        .setParameter(2, batchId)
        .executeUpdate();
  }

  @Override
  public void finalizeBatchMetrics(long batchId) {
    em.createNativeQuery(
            "UPDATE scheduler_batch_metrics SET "
                + "completed_at = statement_timestamp(), "
                + "total_duration_ms = CASE WHEN started_at IS NOT NULL "
                + "  THEN EXTRACT(EPOCH FROM (statement_timestamp() - started_at))::bigint * 1000 "
                + "  ELSE NULL END, "
                + "overhead_ms = CASE WHEN started_at IS NOT NULL AND child_execution_ms IS NOT NULL "
                + "  THEN EXTRACT(EPOCH FROM (statement_timestamp() - started_at))::bigint * 1000 - child_execution_ms "
                + "  ELSE NULL END "
                + "WHERE batch_id = ?")
        .setParameter(1, batchId)
        .executeUpdate();
  }

  @Override
  public void updateBatchMetricsChildCount(long batchId, int childCount) {
    em.createNativeQuery("UPDATE scheduler_batch_metrics SET child_count = ? WHERE batch_id = ?")
        .setParameter(1, childCount)
        .setParameter(2, batchId)
        .executeUpdate();
  }

  // ──────────────────────────────────────────────
  // DlqAlertStore
  // ──────────────────────────────────────────────

  @Override
  public DlqAlertEntity saveDlqAlert(DlqAlertEntity alert) {
    if (alert.getId() == null) {
      em.persist(alert);
      return alert;
    }
    return em.merge(alert);
  }

  @Override
  public boolean existsRecentDlqAlert(long jobId, String errorHash, Instant cutoff) {
    long count =
        countByNative(
            "SELECT COUNT(*) FROM scheduler_dlq_alerts "
                + "WHERE job_id = ? AND error_hash = ? AND alert_sent_at >= ?",
            jobId,
            errorHash,
            Timestamp.from(cutoff));
    return count > 0;
  }

  // ──────────────────────────────────────────────
  // ResourcePermitStore
  // ──────────────────────────────────────────────

  @Override
  public boolean tryAcquirePermit(String resource, long jobId, String nodeId) {
    // Lock the resource limit row to serialize concurrent permit acquisitions
    Object[] limitRow;
    try {
      limitRow =
          (Object[])
              em.createNativeQuery(
                      "SELECT max_concurrent, retry_delay_ms FROM scheduler_resource_limit "
                          + "WHERE resource_name = ? FOR UPDATE")
                  .setParameter(1, resource)
                  .getSingleResult();
    } catch (NoResultException e) {
      return false;
    }

    int maxConcurrent = ((Number) limitRow[0]).intValue();
    long activeCount =
        countByNative(
            "SELECT COUNT(*) FROM scheduler_resource_permit WHERE resource_name = ?", resource);

    if (activeCount >= maxConcurrent) {
      return false;
    }

    ResourcePermitEntity permit = ResourcePermitEntity.create(resource, jobId, nodeId);
    em.persist(permit);
    return true;
  }

  @Override
  public void releasePermit(String resource, long jobId) {
    em.createNativeQuery(
            "DELETE FROM scheduler_resource_permit WHERE resource_name = ? AND job_id = ?")
        .setParameter(1, resource)
        .setParameter(2, jobId)
        .executeUpdate();
  }

  @Override
  public void releaseAllPermits(long jobId) {
    em.createNativeQuery("DELETE FROM scheduler_resource_permit WHERE job_id = ?")
        .setParameter(1, jobId)
        .executeUpdate();
  }

  @Override
  public int getPermitRetryDelay(String resource) {
    try {
      Object result =
          em.createNativeQuery(
                  "SELECT retry_delay_ms FROM scheduler_resource_limit WHERE resource_name = ?")
              .setParameter(1, resource)
              .getSingleResult();
      return ((Number) result).intValue();
    } catch (NoResultException e) {
      return 5000;
    }
  }

  @Override
  public void configureResource(
      String name, int maxConcurrent, int retryDelayMs, String description) {
    em.createNativeQuery(
            "INSERT INTO scheduler_resource_limit (resource_name, max_concurrent, retry_delay_ms, "
                + "description, created_at, updated_at) "
                + "VALUES (?, ?, ?, ?, statement_timestamp(), statement_timestamp()) "
                + "ON CONFLICT (resource_name) DO UPDATE SET "
                + "  max_concurrent = EXCLUDED.max_concurrent, "
                + "  retry_delay_ms = EXCLUDED.retry_delay_ms, "
                + "  description = EXCLUDED.description, "
                + "  updated_at = statement_timestamp()")
        .setParameter(1, name)
        .setParameter(2, maxConcurrent)
        .setParameter(3, retryDelayMs)
        .setParameter(4, description)
        .executeUpdate();
  }

  @Override
  public int cleanupOrphanedPermits(List<String> staleNodeIds) {
    if (staleNodeIds.isEmpty()) {
      return 0;
    }
    return em.createNativeQuery("DELETE FROM scheduler_resource_permit WHERE node_id IN (:nodeIds)")
        .setParameter("nodeIds", staleNodeIds)
        .executeUpdate();
  }

  // ──────────────────────────────────────────────
  // Private helpers
  // ──────────────────────────────────────────────

  private long countByNative(String sql, Object... params) {
    var query = em.createNativeQuery(sql);
    for (int i = 0; i < params.length; i++) {
      query.setParameter(i + 1, params[i]);
    }
    return ((Number) query.getSingleResult()).longValue();
  }

  private ArchivedJobEntity buildArchive(JobEntity job, String reason, String archivedBy) {
    ArchivedJobEntity a = new ArchivedJobEntity();
    a.setOriginalJobId(job.getId());
    a.setFinalStatus(job.getStatus());
    a.setJobType(job.getJobType());
    a.setPriority(job.getPriority());
    a.setTotalAttempts(job.getAttempts());
    a.setMaxRetries(job.getMaxRetries());
    a.setBackoffPolicy(job.getBackoffPolicy());
    a.setBackoffParamMs(job.getBackoffParamMs());
    a.setTimeoutSec(job.getTimeoutSec());
    a.setTargetClass(job.getTargetClass());
    a.setMethodName(job.getMethodName());
    a.setBusinessKey(job.getBusinessKey());
    a.setCronExpr(job.getCronExpr());
    a.setZoneId(job.getZoneId());
    a.setOriginalScheduledTime(job.getScheduledTime());
    a.setOriginalCreatedAt(job.getCreatedAt());
    a.setFirstExecutionTime(job.getExecutionStartTime());
    a.setCompletionTime(job.getExecutionEndTime());
    a.setTotalExecutionTimeMs(job.getExecutionDurationMs());
    a.setQueueWaitMs(job.getQueueWaitMs());
    a.setArchivedAt(Instant.now());
    a.setArchivedBy(archivedBy);
    a.setArchiveReason(reason);
    a.setJobResult(job.getJobResult());
    a.setResultType(job.getResultType());
    a.setFinalError(job.getLastError());
    if (job.getPayload() != null) {
      a.setPayloadSummary(job.getPayload().target() + "#" + job.getPayload().method());
    }
    a.setDependedOn(job.getDependsOn());
    a.setSupersededBy(job.getSupersededBy());
    if (job.getTags() != null && !job.getTags().isEmpty()) {
      a.setTags(String.join(",", job.getTags()));
    }
    return a;
  }

  private String payloadToJson(JobEntity job) {
    if (job.getPayload() == null) {
      return "{}";
    }
    try {
      return OBJECT_MAPPER.writeValueAsString(job.getPayload());
    } catch (Exception e) {
      log.log(Level.WARNING, "Failed to serialize payload", e);
      return "{}";
    }
  }

  private String paramsToJson(JobEntity job) {
    if (job.getParams() == null) {
      return null;
    }
    try {
      return OBJECT_MAPPER.writeValueAsString(job.getParams());
    } catch (Exception e) {
      log.log(Level.WARNING, "Failed to serialize params", e);
      return null;
    }
  }

  private static Instant toInstant(Object value) {
    if (value instanceof Instant) {
      return (Instant) value;
    }
    if (value instanceof Timestamp) {
      return ((Timestamp) value).toInstant();
    }
    if (value instanceof java.time.OffsetDateTime) {
      return ((java.time.OffsetDateTime) value).toInstant();
    }
    throw new IllegalArgumentException("Cannot convert " + value.getClass() + " to Instant");
  }

  private static JobPriority safeJobPriority(int ordinal) {
    JobPriority[] values = JobPriority.values();
    if (ordinal < 0 || ordinal >= values.length) {
      return JobPriority.NORMAL;
    }
    return values[ordinal];
  }

  private static int getPriorityBoostIntervalMinutes() {
    String raw = System.getenv("SCHEDULER_PRIORITY_BOOST_INTERVAL_MINUTES");
    if (raw == null || raw.isBlank()) {
      return 15;
    }
    try {
      return Math.max(0, Integer.parseInt(raw.trim()));
    } catch (NumberFormatException e) {
      return 15;
    }
  }

  private static String buildBoostOrderBy(String timeColumn, int boostInterval) {
    return boostInterval > 0
        ? "(priority + FLOOR(GREATEST(0, EXTRACT(EPOCH FROM (statement_timestamp() - "
            + timeColumn
            + "))) / (60.0 * ?2))) DESC, "
            + timeColumn
            + " ASC"
        : "priority DESC, " + timeColumn + " ASC";
  }

  private static String buildClaimUpdateSql(
      String typeFilter, String timeColumn, int boostInterval) {
    return "UPDATE scheduler_job SET status = 'RUNNING', picked_by = ?3, "
        + "picked_at = statement_timestamp(), updated_at = statement_timestamp(), "
        + "version = version + 1 "
        + "WHERE job_id IN ("
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
        + "  LIMIT ?1"
        + ")";
  }

  private static String buildClaimReadBackSql(
      String selectClause, String timeColumn, int boostInterval) {
    // Parameters: ?1 = nodeId. If boost > 0: ?2 = boostInterval, ?3 = limit. Else: ?2 = limit.
    String limitParam = boostInterval > 0 ? "?3" : "?2";
    return "SELECT "
        + selectClause
        + " FROM scheduler_job "
        + "WHERE picked_by = ?1 AND status = 'RUNNING' "
        + "AND picked_at >= statement_timestamp() - INTERVAL '5 seconds' "
        + "ORDER BY "
        + buildBoostOrderBy(timeColumn, boostInterval)
        + " LIMIT "
        + limitParam;
  }
}
