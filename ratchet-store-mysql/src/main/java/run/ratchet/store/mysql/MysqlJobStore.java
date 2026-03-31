package run.ratchet.store.mysql;

import com.fasterxml.jackson.core.JsonProcessingException;
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
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.NoResultException;
import jakarta.persistence.PersistenceContext;
import jakarta.persistence.TypedQuery;
import jakarta.transaction.Transactional;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * MySQL implementation of the {@link JobStore} SPI.
 *
 * <p>Uses JPA EntityManager for standard CRUD operations and native SQL for MySQL-specific
 * operations such as {@code FOR UPDATE SKIP LOCKED}, {@code ON DUPLICATE KEY UPDATE}, and user
 * variables for atomic counters.
 */
@ApplicationScoped
@Transactional
public class MysqlJobStore implements JobStore {

  private static final Logger log = Logger.getLogger(MysqlJobStore.class.getName());
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final String EXECUTABLE_JOB_TYPE_FILTER =
      "job_type IN ('SINGLE','BATCH_CHILD','CHAIN_STEP','WORKFLOW_BRANCH')";
  private static final String RECURRING_JOB_TYPE_FILTER = "job_type = 'RECURRING'";

  @PersistenceContext private EntityManager em;

  /** Checks the connection isolation level on first use and warns if not READ COMMITTED. */
  @jakarta.annotation.PostConstruct
  void checkIsolationLevel() {
    try {
      Object result =
          em.createNativeQuery("SELECT @@SESSION.transaction_isolation").getSingleResult();
      String isolation = result != null ? result.toString() : "unknown";
      if (!"READ-COMMITTED".equals(isolation)) {
        log.warning(
            "MySQL session isolation is '"
                + isolation
                + "' — Ratchet requires READ COMMITTED. "
                + "REPEATABLE READ causes InnoDB gap locks that block concurrent job enqueue "
                + "during claim queries. Set hibernate.connection.isolation=2 in persistence.xml "
                + "or transaction-isolation=TRANSACTION_READ_COMMITTED on the datasource.");
      }
    } catch (Exception e) {
      log.fine("Could not check isolation level: " + e.getMessage());
    }
  }

  // ── JobCrudStore ──────────────────────────────────────────────────────

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
    List<JobEntity> results =
        em.createNativeQuery(
                "SELECT * FROM scheduler_job WHERE job_id = :id FOR UPDATE", JobEntity.class)
            .setParameter("id", id)
            .getResultList();
    return results.isEmpty() ? Optional.empty() : Optional.of(results.get(0));
  }

  @Override
  public void delete(long id) {
    findById(id).ifPresent(em::remove);
  }

  @Override
  public JobStatus getJobStatus(long id) {
    List<JobStatus> results =
        em.createQuery("SELECT j.status FROM JobEntity j WHERE j.id = :id", JobStatus.class)
            .setParameter("id", id)
            .getResultList();
    return results.isEmpty() ? null : results.get(0);
  }

  @Override
  public List<JobEntity> findByIds(List<Long> ids) {
    if (ids.isEmpty()) {
      return List.of();
    }
    return em.createNativeQuery(
            "SELECT * FROM scheduler_job WHERE job_id IN (:ids)", JobEntity.class)
        .setParameter("ids", ids)
        .getResultList();
  }

  @Override
  public Optional<JobEntity> findActiveByBusinessKey(String businessKey) {
    List<JobEntity> results =
        em.createQuery(
                "SELECT j FROM JobEntity j WHERE j.businessKey = :bk "
                    + "AND j.status IN (run.ratchet.store.entity.JobStatus.PENDING, "
                    + "run.ratchet.store.entity.JobStatus.RUNNING, "
                    + "run.ratchet.store.entity.JobStatus.PAUSED)",
                JobEntity.class)
            .setParameter("bk", businessKey)
            .setMaxResults(1)
            .getResultList();
    return results.isEmpty() ? Optional.empty() : Optional.of(results.get(0));
  }

  @Override
  public Optional<JobEntity> findByIdempotencyKey(String idempotencyKey) {
    List<JobEntity> results =
        em.createQuery("SELECT j FROM JobEntity j WHERE j.idempotencyKey = :key", JobEntity.class)
            .setParameter("key", idempotencyKey)
            .setMaxResults(1)
            .getResultList();
    return results.isEmpty() ? Optional.empty() : Optional.of(results.get(0));
  }

  @Override
  public List<JobEntity> findDependants(long parentJobId) {
    return em.createQuery("SELECT j FROM JobEntity j WHERE j.dependsOn = :pid", JobEntity.class)
        .setParameter("pid", parentJobId)
        .getResultList();
  }

  @Override
  public Optional<Instant> findEarliestRecurringNextFire() {
    List<?> results =
        em.createNativeQuery(
                "SELECT MIN(next_fire) FROM scheduler_job "
                    + "WHERE job_type = 'RECURRING' AND status = 'PENDING' AND next_fire IS NOT NULL")
            .getResultList();
    if (results.isEmpty() || results.get(0) == null) {
      return Optional.empty();
    }
    Object val = results.get(0);
    if (val instanceof Timestamp ts) {
      return Optional.of(ts.toInstant());
    }
    return Optional.empty();
  }

  @Override
  public long countPendingJobs() {
    return countJobsByStatus(JobStatus.PENDING);
  }

  @Override
  public long countJobsByStatus(JobStatus status) {
    return em.createQuery("SELECT COUNT(j) FROM JobEntity j WHERE j.status = :s", Long.class)
        .setParameter("s", status)
        .getSingleResult();
  }

  @Override
  public long countActiveJobs(JobExecutionType jobType) {
    return em.createQuery(
            "SELECT COUNT(j) FROM JobEntity j WHERE j.jobType = :jt "
                + "AND j.status IN (run.ratchet.store.entity.JobStatus.PENDING, "
                + "run.ratchet.store.entity.JobStatus.RUNNING)",
            Long.class)
        .setParameter("jt", jobType)
        .getSingleResult();
  }

  @Override
  public long countActiveNodes() {
    return em.createQuery("SELECT COUNT(n) FROM NodeEntity n", Long.class).getSingleResult();
  }

  @Override
  public long countReadyJobs(Instant now) {
    return em.createQuery(
            "SELECT COUNT(j) FROM JobEntity j WHERE j.status = run.ratchet.store.entity.JobStatus.PENDING "
                + "AND j.scheduledTime <= :now",
            Long.class)
        .setParameter("now", now)
        .getSingleResult();
  }

  @Override
  public long countStuckJobs(Instant stuckThreshold) {
    return em.createQuery(
            "SELECT COUNT(j) FROM JobEntity j WHERE j.status = run.ratchet.store.entity.JobStatus.RUNNING "
                + "AND j.pickedAt < :threshold",
            Long.class)
        .setParameter("threshold", stuckThreshold)
        .getSingleResult();
  }

  @Override
  public long countLongRunningJobs(Instant threshold) {
    return em.createQuery(
            "SELECT COUNT(j) FROM JobEntity j WHERE j.status = run.ratchet.store.entity.JobStatus.RUNNING "
                + "AND j.executionStartTime < :threshold",
            Long.class)
        .setParameter("threshold", threshold)
        .getSingleResult();
  }

  @Override
  public long countPendingBatchChildren() {
    return em.createQuery(
            "SELECT COUNT(j) FROM JobEntity j WHERE j.jobType = run.ratchet.store.entity.JobExecutionType.BATCH_CHILD "
                + "AND j.status = run.ratchet.store.entity.JobStatus.PENDING",
            Long.class)
        .getSingleResult();
  }

  @Override
  public long countPendingJobsByPriority(JobPriority priority) {
    return em.createQuery(
            "SELECT COUNT(j) FROM JobEntity j WHERE j.priority = :p "
                + "AND j.status = run.ratchet.store.entity.JobStatus.PENDING",
            Long.class)
        .setParameter("p", priority)
        .getSingleResult();
  }

  @Override
  public long countPendingJobsByType(JobExecutionType jobType) {
    return em.createQuery(
            "SELECT COUNT(j) FROM JobEntity j WHERE j.jobType = :jt "
                + "AND j.status = run.ratchet.store.entity.JobStatus.PENDING",
            Long.class)
        .setParameter("jt", jobType)
        .getSingleResult();
  }

  @Override
  public long countJobsByStatusSince(JobStatus status, Instant since) {
    return em.createQuery(
            "SELECT COUNT(j) FROM JobEntity j WHERE j.status = :s AND j.updatedAt >= :since",
            Long.class)
        .setParameter("s", status)
        .setParameter("since", since)
        .getSingleResult();
  }

  @Override
  public long countJobsWithRetries() {
    return em.createQuery("SELECT COUNT(j) FROM JobEntity j WHERE j.attempts > 0", Long.class)
        .getSingleResult();
  }

  @Override
  public double getRetryRateStats(Instant since) {
    Object result =
        em.createNativeQuery(
                "SELECT COALESCE(AVG(CASE WHEN attempts > 0 THEN 1.0 ELSE 0.0 END), 0) "
                    + "FROM scheduler_job WHERE updated_at >= :since")
            .setParameter("since", Timestamp.from(since))
            .getSingleResult();
    return ((Number) result).doubleValue();
  }

  @Override
  public double getAverageProcessingTime(Instant since) {
    Object result =
        em.createNativeQuery(
                "SELECT COALESCE(AVG(execution_duration_ms), 0) FROM scheduler_job "
                    + "WHERE status = 'SUCCEEDED' AND execution_duration_ms IS NOT NULL "
                    + "AND updated_at >= :since")
            .setParameter("since", Timestamp.from(since))
            .getSingleResult();
    return ((Number) result).doubleValue();
  }

  @Override
  public double getAverageBatchSize(Instant since) {
    Object result =
        em.createNativeQuery(
                "SELECT COALESCE(AVG(b.total_items), 0) FROM scheduler_batch b "
                    + "JOIN scheduler_job j ON j.job_id = b.batch_id "
                    + "WHERE j.updated_at >= :since")
            .setParameter("since", Timestamp.from(since))
            .getSingleResult();
    return ((Number) result).doubleValue();
  }

  @Override
  public Optional<Instant> getOldestPendingJobTime() {
    List<?> results =
        em.createNativeQuery(
                "SELECT MIN(scheduled_time) FROM scheduler_job WHERE status = 'PENDING'")
            .getResultList();
    if (results.isEmpty() || results.get(0) == null) {
      return Optional.empty();
    }
    Object val = results.get(0);
    if (val instanceof Timestamp ts) {
      return Optional.of(ts.toInstant());
    }
    return Optional.empty();
  }

  @Override
  public long getQueueWaitTimePercentile(double percentile) {
    // MySQL does not support expressions in the OFFSET clause, so compute the offset in Java.
    Number countResult =
        (Number)
            em.createNativeQuery(
                    // language=MySQL
                    "SELECT COUNT(*) FROM scheduler_job WHERE queue_wait_ms IS NOT NULL AND status = 'SUCCEEDED'")
                .getSingleResult();
    long total = countResult.longValue();
    if (total == 0) {
      return 0L;
    }
    int offset = (int) Math.floor(percentile * total);
    Object result =
        em
            .createNativeQuery(
                // language=MySQL
                """
                SELECT COALESCE(queue_wait_ms, 0)
                FROM scheduler_job
                WHERE queue_wait_ms IS NOT NULL AND status = 'SUCCEEDED'
                ORDER BY queue_wait_ms ASC
                LIMIT 1 OFFSET ?1""")
            .setParameter(1, offset)
            .getResultList()
            .stream()
            .findFirst()
            .orElse(0L);
    return ((Number) result).longValue();
  }

  // ── JobClaimStore ─────────────────────────────────────────────────────

  @Override
  @SuppressWarnings("unchecked")
  public List<JobEntity> claimNextBatch(int limit, String nodeId) {
    // Dependency resolution is handled by the orchestration layer (PostExecutionHandler
    // schedules child jobs only after the parent completes), so the claim query only needs
    // to find PENDING jobs that are due. No self-joins needed — matching the original
    // nets4 JobClaimStrategy pattern.
    int boostInterval = getPriorityBoostIntervalMinutes();
    var query =
        em.createNativeQuery(
                buildClaimSql("*", EXECUTABLE_JOB_TYPE_FILTER, "scheduled_time", boostInterval),
                JobEntity.class)
            .setParameter("lim", limit);
    if (boostInterval > 0) {
      query.setParameter("boost", boostInterval);
    }

    @SuppressWarnings("unchecked")
    List<JobEntity> candidates = query.getResultList();

    if (candidates.isEmpty()) {
      return List.of();
    }

    Instant now = Instant.now();
    List<Long> ids = candidates.stream().map(JobEntity::getId).collect(Collectors.toList());
    em.createNativeQuery(
            "UPDATE scheduler_job SET status = 'RUNNING', picked_by = :node, "
                + "picked_at = NOW(3), updated_at = NOW(3) WHERE job_id IN (:ids)")
        .setParameter("node", nodeId)
        .setParameter("ids", ids)
        .executeUpdate();

    em.clear();
    candidates.forEach(
        job -> {
          job.setStatus(JobStatus.RUNNING);
          job.setPickedBy(nodeId);
          job.setPickedAt(now);
        });
    return candidates;
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<JobClaimDto> claimNextBatchOptimized(int limit, String nodeId) {
    int boostInterval = getPriorityBoostIntervalMinutes();
    var query =
        em.createNativeQuery(
                buildClaimSql(
                    """
                    job_id, status, job_type, priority, scheduled_time,
                    version, timeout_sec, picked_by, picked_at, business_key,
                    attempts, max_retries
                    """,
                    EXECUTABLE_JOB_TYPE_FILTER,
                    "scheduled_time",
                    boostInterval))
            .setParameter("lim", limit);
    if (boostInterval > 0) {
      query.setParameter("boost", boostInterval);
    }

    @SuppressWarnings("unchecked")
    List<Object[]> rows = query.getResultList();

    if (rows.isEmpty()) {
      return List.of();
    }

    List<Long> ids =
        rows.stream().map(r -> ((Number) r[0]).longValue()).collect(Collectors.toList());

    em.createNativeQuery(
            "UPDATE scheduler_job SET status = 'RUNNING', picked_by = :node, "
                + "picked_at = NOW(3), updated_at = NOW(3) WHERE job_id IN (:ids)")
        .setParameter("node", nodeId)
        .setParameter("ids", ids)
        .executeUpdate();

    Instant now = Instant.now();
    return rows.stream()
        .map(
            r ->
                new JobClaimDto(
                    ((Number) r[0]).longValue(),
                    JobStatus.RUNNING,
                    JobExecutionType.valueOf((String) r[2]),
                    safeJobPriority(((Number) r[3]).intValue()),
                    toInstant(r[4]),
                    ((Number) r[5]).intValue(),
                    ((Number) r[6]).intValue(),
                    nodeId,
                    now,
                    (String) r[9],
                    ((Number) r[10]).intValue(),
                    ((Number) r[11]).intValue()))
        .collect(Collectors.toList());
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<JobEntity> claimDueRecurring(int limit, String nodeId) {
    int boostInterval = getPriorityBoostIntervalMinutes();
    var query =
        em.createNativeQuery(
                buildClaimSql("*", RECURRING_JOB_TYPE_FILTER, "next_fire", boostInterval),
                JobEntity.class)
            .setParameter("lim", limit);
    if (boostInterval > 0) {
      query.setParameter("boost", boostInterval);
    }

    @SuppressWarnings("unchecked")
    List<JobEntity> candidates = query.getResultList();

    if (candidates.isEmpty()) {
      return List.of();
    }

    Instant now = Instant.now();
    List<Long> ids = candidates.stream().map(JobEntity::getId).collect(Collectors.toList());
    em.createNativeQuery(
            "UPDATE scheduler_job SET status = 'RUNNING', picked_by = :node, "
                + "picked_at = NOW(3), updated_at = NOW(3) WHERE job_id IN (:ids)")
        .setParameter("node", nodeId)
        .setParameter("ids", ids)
        .executeUpdate();

    em.clear();
    candidates.forEach(
        job -> {
          job.setStatus(JobStatus.RUNNING);
          job.setPickedBy(nodeId);
          job.setPickedAt(now);
        });
    return candidates;
  }

  // ── JobStatusStore ────────────────────────────────────────────────────

  @Override
  public void updateJobStatus(long id, JobStatus status, String errorMessage) {
    em.createNativeQuery(
            "UPDATE scheduler_job SET status = :status, last_error = :err, "
                + "updated_at = NOW(3) WHERE job_id = :id")
        .setParameter("status", status.name())
        .setParameter("err", errorMessage)
        .setParameter("id", id)
        .executeUpdate();
  }

  @Override
  public boolean compareAndSwapStatus(
      long id, JobStatus expected, JobStatus newStatus, String error) {
    int updated =
        em.createNativeQuery(
                "UPDATE scheduler_job SET status = :newS, last_error = :err, "
                    + "updated_at = NOW(3) WHERE job_id = :id AND status = :exp")
            .setParameter("newS", newStatus.name())
            .setParameter("err", error)
            .setParameter("id", id)
            .setParameter("exp", expected.name())
            .executeUpdate();
    return updated > 0;
  }

  @Override
  public int incrementRetryAttempt(long id) {
    int updated =
        em.createNativeQuery(
                "UPDATE scheduler_job SET attempts = attempts + 1, updated_at = NOW(3) "
                    + "WHERE job_id = :id AND status = 'RUNNING'")
            .setParameter("id", id)
            .executeUpdate();
    if (updated == 0) {
      return -1;
    }
    Object result =
        em.createNativeQuery("SELECT attempts FROM scheduler_job WHERE job_id = :id")
            .setParameter("id", id)
            .getSingleResult();
    return ((Number) result).intValue();
  }

  @Override
  public boolean tryPickUpJob(long id, String nodeId) {
    int updated =
        em.createNativeQuery(
                "UPDATE scheduler_job SET status = 'RUNNING', picked_by = :node, "
                    + "picked_at = NOW(3), updated_at = NOW(3) "
                    + "WHERE job_id = :id AND status = 'PENDING'")
            .setParameter("node", nodeId)
            .setParameter("id", id)
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
                    + "job_result = CAST(:result AS JSON), result_type = :rtype, "
                    + "execution_start_time = :start, execution_end_time = :end, "
                    + "execution_duration_ms = :dur, queue_wait_ms = :qwait, "
                    + "updated_at = NOW(3) "
                    + "WHERE job_id = :id AND status = 'RUNNING'")
            .setParameter("result", resultJson)
            .setParameter("rtype", resultType)
            .setParameter("start", start != null ? Timestamp.from(start) : null)
            .setParameter("end", end != null ? Timestamp.from(end) : null)
            .setParameter("dur", durationMs)
            .setParameter("qwait", queueWaitMs)
            .setParameter("id", id)
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
    boolean succeeded =
        markJobSucceeded(jobId, resultJson, resultType, start, end, durationMs, queueWaitMs);
    if (succeeded) {
      incrementCompletedAtomic(batchId);
    }
    return succeeded;
  }

  @Override
  public boolean scheduleJobRetry(long id, String error, Instant newScheduledTime, int attempts) {
    int updated =
        em.createNativeQuery(
                "UPDATE scheduler_job SET status = 'PENDING', last_error = :err, "
                    + "scheduled_time = :st, attempts = :att, picked_by = NULL, picked_at = NULL, "
                    + "updated_at = NOW(3) WHERE job_id = :id AND status IN ('RUNNING','FAILED')")
            .setParameter("err", error)
            .setParameter("st", Timestamp.from(newScheduledTime))
            .setParameter("att", attempts)
            .setParameter("id", id)
            .executeUpdate();
    return updated > 0;
  }

  @Override
  public boolean resetRunningJob(long id, String nodeId) {
    int updated =
        em.createNativeQuery(
                "UPDATE scheduler_job SET status = 'PENDING', picked_by = NULL, picked_at = NULL, "
                    + "updated_at = NOW(3) WHERE job_id = :id AND status = 'RUNNING' AND picked_by = :node")
            .setParameter("id", id)
            .setParameter("node", nodeId)
            .executeUpdate();
    return updated > 0;
  }

  @Override
  public int resetRunningJobs(String nodeId) {
    return em.createNativeQuery(
            "UPDATE scheduler_job SET status = 'PENDING', picked_by = NULL, picked_at = NULL, "
                + "updated_at = NOW(3) WHERE status = 'RUNNING' AND picked_by = :node")
        .setParameter("node", nodeId)
        .executeUpdate();
  }

  @Override
  public int cancelRecurringJobsByTag(String tag) {
    return em.createNativeQuery(
            "UPDATE scheduler_job j JOIN scheduler_job_tag t ON j.job_id = t.job_id "
                + "SET j.status = 'CANCELED', j.updated_at = NOW(3) "
                + "WHERE t.tag = :tag AND j.job_type = 'RECURRING' "
                + "AND j.status IN ('PENDING', 'RUNNING', 'PAUSED')")
        .setParameter("tag", tag)
        .executeUpdate();
  }

  @Override
  public int cancelRecurringJobByBusinessKey(String businessKey) {
    return em.createNativeQuery(
            "UPDATE scheduler_job SET status = 'CANCELED', updated_at = NOW(3) "
                + "WHERE business_key = :bk AND job_type = 'RECURRING' "
                + "AND status IN ('PENDING', 'RUNNING', 'PAUSED')")
        .setParameter("bk", businessKey)
        .executeUpdate();
  }

  @Override
  public int cancelOrphanedRecurringAnnotationJobs(
      Set<String> registeredIds, Instant nodeStartTime) {
    if (registeredIds.isEmpty()) {
      return 0;
    }
    return em.createNativeQuery(
            "UPDATE scheduler_job SET status = 'CANCELED', updated_at = NOW(3) "
                + "WHERE job_type = 'RECURRING' "
                + "AND status IN ('PENDING', 'RUNNING', 'PAUSED') "
                + "AND created_at < :nodeStart "
                + "AND business_key IS NOT NULL "
                + "AND business_key NOT IN (:ids)")
        .setParameter("nodeStart", Timestamp.from(nodeStartTime))
        .setParameter("ids", registeredIds)
        .executeUpdate();
  }

  @Override
  public boolean resetFailedToPending(long id) {
    int updated =
        em.createNativeQuery(
                "UPDATE scheduler_job SET status = 'PENDING', attempts = 0, "
                    + "last_error = NULL, scheduled_time = NOW(3), "
                    + "picked_by = NULL, picked_at = NULL, updated_at = NOW(3) "
                    + "WHERE job_id = :id AND status = 'FAILED'")
            .setParameter("id", id)
            .executeUpdate();
    return updated > 0;
  }

  @Override
  public boolean transitionToPaused(long id, JobStatus expected) {
    int updated =
        em.createNativeQuery(
                "UPDATE scheduler_job SET status = 'PAUSED', "
                    + "paused_from_status = :exp, updated_at = NOW(3) "
                    + "WHERE job_id = :id AND status = :exp")
            .setParameter("exp", expected.name())
            .setParameter("id", id)
            .executeUpdate();
    return updated > 0;
  }

  @Override
  public boolean transitionFromPaused(long id, JobStatus target) {
    int updated =
        em.createNativeQuery(
                "UPDATE scheduler_job SET status = :target, "
                    + "paused_from_status = NULL, updated_at = NOW(3) "
                    + "WHERE job_id = :id AND status = 'PAUSED'")
            .setParameter("target", target.name())
            .setParameter("id", id)
            .executeUpdate();
    return updated > 0;
  }

  @Override
  public JobStatus transitionFromPausedAtomic(long id) {
    List<?> results =
        em.createNativeQuery(
                "SELECT paused_from_status FROM scheduler_job "
                    + "WHERE job_id = :id AND status = 'PAUSED' FOR UPDATE")
            .setParameter("id", id)
            .getResultList();
    if (results.isEmpty()) {
      return null;
    }
    String pausedFrom = (String) results.get(0);
    JobStatus target = pausedFrom != null ? JobStatus.valueOf(pausedFrom) : JobStatus.PENDING;
    int updated =
        em.createNativeQuery(
                "UPDATE scheduler_job SET status = :target, "
                    + "paused_from_status = NULL, updated_at = NOW(3) "
                    + "WHERE job_id = :id AND status = 'PAUSED'")
            .setParameter("target", target.name())
            .setParameter("id", id)
            .executeUpdate();
    return updated > 0 ? target : null;
  }

  // ── JobBulkStore ──────────────────────────────────────────────────────

  @Override
  public void bulkInsert(List<JobEntity> jobs) {
    if (jobs.isEmpty()) {
      return;
    }
    em.unwrap(Connection.class);
    // Use JDBC batch insert for performance
    em.unwrap(EntityManagerFactory.class);

    Connection conn = em.unwrap(Connection.class);
    try {
      String sql =
          "INSERT INTO scheduler_job (job_id, status, paused_from_status, scheduled_time, "
              + "job_type, priority, attempts, max_retries, backoff_policy, backoff_param_ms, "
              + "timeout_sec, cron_expr, zone_id, next_fire, payload, params, idempotency_key, "
              + "business_key, resource_name, on_success_payload, on_failure_payload, "
              + "depends_on, superseded_by, picked_by, picked_at, "
              + "last_error, created_at, created_by, updated_at, execution_start_time, "
              + "execution_end_time, execution_duration_ms, queue_wait_ms, job_result, "
              + "result_type, version) "
              + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CAST(? AS JSON), "
              + "CAST(? AS JSON), ?, ?, ?, CAST(? AS JSON), CAST(? AS JSON), "
              + "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
              + "CAST(? AS JSON), ?, 0)";

      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        Instant now = Instant.now();
        for (JobEntity job : jobs) {
          int i = 1;
          ps.setLong(i++, job.getId());
          ps.setString(i++, (job.getStatus() != null ? job.getStatus() : JobStatus.PENDING).name());
          ps.setString(
              i++, job.getPausedFromStatus() != null ? job.getPausedFromStatus().name() : null);
          ps.setTimestamp(i++, Timestamp.from(job.getScheduledTime()));
          ps.setString(i++, job.getJobType().name());
          ps.setInt(i++, job.getPriority().ordinal());
          ps.setInt(i++, job.getAttempts());
          ps.setInt(i++, job.getMaxRetries());
          ps.setString(i++, job.getBackoffPolicy().name());
          ps.setInt(i++, job.getBackoffParamMs());
          ps.setInt(i++, job.getTimeoutSec());
          ps.setString(i++, job.getCronExpr());
          ps.setString(i++, job.getZoneId());
          ps.setTimestamp(
              i++, job.getNextFire() != null ? Timestamp.from(job.getNextFire()) : null);
          ps.setString(i++, payloadToJson(job));
          ps.setString(i++, paramsToJson(job));
          ps.setString(i++, job.getIdempotencyKey());
          ps.setString(i++, job.getBusinessKey());
          ps.setString(i++, job.getResourceName());
          ps.setString(i++, callbackPayloadToJson(job.getOnSuccessPayload()));
          ps.setString(i++, callbackPayloadToJson(job.getOnFailurePayload()));
          if (job.getDependsOn() != null) {
            ps.setLong(i++, job.getDependsOn());
          } else {
            ps.setNull(i++, Types.BIGINT);
          }
          if (job.getSupersededBy() != null) {
            ps.setLong(i++, job.getSupersededBy());
          } else {
            ps.setNull(i++, Types.BIGINT);
          }
          ps.setString(i++, job.getPickedBy());
          ps.setTimestamp(
              i++, job.getPickedAt() != null ? Timestamp.from(job.getPickedAt()) : null);
          ps.setString(i++, job.getLastError());
          ps.setTimestamp(i++, Timestamp.from(now));
          ps.setString(i++, job.getCreatedBy());
          ps.setTimestamp(i++, Timestamp.from(now));
          ps.setTimestamp(
              i++,
              job.getExecutionStartTime() != null
                  ? Timestamp.from(job.getExecutionStartTime())
                  : null);
          ps.setTimestamp(
              i++,
              job.getExecutionEndTime() != null ? Timestamp.from(job.getExecutionEndTime()) : null);
          if (job.getExecutionDurationMs() != null) {
            ps.setLong(i++, job.getExecutionDurationMs());
          } else {
            ps.setNull(i++, Types.BIGINT);
          }
          if (job.getQueueWaitMs() != null) {
            ps.setLong(i++, job.getQueueWaitMs());
          } else {
            ps.setNull(i++, Types.BIGINT);
          }
          ps.setString(i++, job.getJobResult());
          ps.setString(i++, job.getResultType());
          ps.addBatch();
        }
        ps.executeBatch();

        // Assign generated IDs back
        var keys = ps.getGeneratedKeys();
        int idx = 0;
        while (keys.next() && idx < jobs.size()) {
          jobs.get(idx++).setId(keys.getLong(1));
        }
      }
    } catch (SQLException e) {
      throw new RuntimeException("Bulk insert failed", e);
    }
    em.clear();
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
            "DELETE FROM scheduler_job WHERE status = 'FAILED' AND attempts >= max_retries "
                + "AND updated_at < :cutoff")
        .setParameter("cutoff", Timestamp.from(cutoff))
        .executeUpdate();
  }

  @Override
  public int resetOrphanJobs(Duration grace) {
    return em.createNativeQuery(
            "UPDATE scheduler_job SET status = 'PENDING', picked_by = NULL, picked_at = NULL, "
                + "updated_at = NOW(3) "
                + "WHERE status = 'RUNNING' AND picked_by NOT IN ("
                + "  SELECT node_id FROM scheduler_node "
                + "  WHERE TIMESTAMPDIFF(MINUTE, heartbeat_ts, NOW(3)) <= :graceMin"
                + ") AND TIMESTAMPDIFF(MINUTE, picked_at, NOW(3)) >= :graceMin")
        .setParameter("graceMin", grace.toMinutes())
        .executeUpdate();
  }

  // ── BatchStore ────────────────────────────────────────────────────────

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
    // Lock the row first to ensure atomicity of read + update
    Object[] locked =
        (Object[])
            em.createNativeQuery(
                    "SELECT completed_items, failed_items, total_items, progress_hook "
                        + "FROM scheduler_batch WHERE batch_id = :bid FOR UPDATE")
                .setParameter("bid", batchId)
                .getSingleResult();

    int newCompleted = ((Number) locked[0]).intValue() + 1;
    em.createNativeQuery("UPDATE scheduler_batch SET completed_items = :ci WHERE batch_id = :bid")
        .setParameter("ci", newCompleted)
        .setParameter("bid", batchId)
        .executeUpdate();

    return new BatchProgress(
        batchId,
        ((Number) locked[2]).intValue(),
        newCompleted,
        ((Number) locked[1]).intValue(),
        parseProgressHook(locked[3]));
  }

  @Override
  public BatchProgress incrementFailedAtomic(long batchId) {
    // Lock the row first to ensure atomicity of read + update
    Object[] locked =
        (Object[])
            em.createNativeQuery(
                    "SELECT completed_items, failed_items, total_items, progress_hook "
                        + "FROM scheduler_batch WHERE batch_id = :bid FOR UPDATE")
                .setParameter("bid", batchId)
                .getSingleResult();

    int newFailed = ((Number) locked[1]).intValue() + 1;
    em.createNativeQuery("UPDATE scheduler_batch SET failed_items = :fi WHERE batch_id = :bid")
        .setParameter("fi", newFailed)
        .setParameter("bid", batchId)
        .executeUpdate();

    return new BatchProgress(
        batchId,
        ((Number) locked[2]).intValue(),
        ((Number) locked[0]).intValue(),
        newFailed,
        parseProgressHook(locked[3]));
  }

  private JobPayload parseProgressHook(Object jsonValue) {
    if (jsonValue == null) {
      return null;
    }
    try {
      return OBJECT_MAPPER.readValue(jsonValue.toString(), JobPayload.class);
    } catch (JsonProcessingException e) {
      log.warning("Failed to parse progress_hook JSON: " + e.getMessage());
      return null;
    }
  }

  @Override
  public boolean markBatchCompleteIfReady(long batchId) {
    int updated =
        em.createNativeQuery(
                "UPDATE scheduler_batch SET completion_processed = 1 "
                    + "WHERE batch_id = :bid AND completion_processed = 0 "
                    + "AND (completed_items + failed_items) >= total_items")
            .setParameter("bid", batchId)
            .executeUpdate();
    return updated > 0;
  }

  @Override
  public List<Long> findRecoverableBatchIds(int limit) {
    @SuppressWarnings("unchecked")
    List<Number> results =
        em.createNativeQuery(
                "SELECT batch_id FROM scheduler_batch "
                    + "WHERE completion_processed = 0 "
                    + "AND (completed_items + failed_items) >= total_items "
                    + "LIMIT :lim")
            .setParameter("lim", limit)
            .getResultList();
    return results.stream().map(Number::longValue).toList();
  }

  @Override
  public boolean updateBatchTotalItems(long batchId, int totalItems) {
    int updated =
        em.createNativeQuery(
                "UPDATE scheduler_batch SET total_items = :total WHERE batch_id = :bid")
            .setParameter("total", totalItems)
            .setParameter("bid", batchId)
            .executeUpdate();
    return updated > 0;
  }

  // ── LockStore ─────────────────────────────────────────────────────────

  @Override
  public boolean tryLock(String name, Duration ttl, String nodeId) {
    em.createNativeQuery(
            "INSERT INTO scheduler_lock (lock_name, owner_node, locked_at, expires_at) "
                + "VALUES (:name, :node, NOW(3), DATE_ADD(NOW(3), INTERVAL :ttl SECOND)) "
                + "ON DUPLICATE KEY UPDATE "
                + "  owner_node = IF(expires_at < NOW(3), VALUES(owner_node), owner_node), "
                + "  locked_at = IF(expires_at < NOW(3), NOW(3), locked_at), "
                + "  expires_at = IF(expires_at < NOW(3), VALUES(expires_at), expires_at)")
        .setParameter("name", name)
        .setParameter("node", nodeId)
        .setParameter("ttl", ttl.toSeconds())
        .executeUpdate();

    Object owner =
        em.createNativeQuery("SELECT owner_node FROM scheduler_lock WHERE lock_name = :name")
            .setParameter("name", name)
            .getSingleResult();
    return nodeId.equals(owner);
  }

  @Override
  public void unlock(String name, String nodeId) {
    em.createNativeQuery(
            "DELETE FROM scheduler_lock WHERE lock_name = :name AND owner_node = :node")
        .setParameter("name", name)
        .setParameter("node", nodeId)
        .executeUpdate();
  }

  @Override
  public boolean renewLock(String name, Duration extension, String nodeId) {
    int updated =
        em.createNativeQuery(
                "UPDATE scheduler_lock SET expires_at = DATE_ADD(NOW(3), INTERVAL :ext SECOND) "
                    + "WHERE lock_name = :name AND owner_node = :node")
            .setParameter("ext", extension.toSeconds())
            .setParameter("name", name)
            .setParameter("node", nodeId)
            .executeUpdate();
    return updated > 0;
  }

  // ── NodeStore ─────────────────────────────────────────────────────────

  @Override
  public void upsertHeartbeat(String nodeId, Instant ts) {
    em.createNativeQuery(
            "INSERT INTO scheduler_node (node_id, heartbeat_ts, started_at) "
                + "VALUES (:id, :ts, :ts) "
                + "ON DUPLICATE KEY UPDATE heartbeat_ts = VALUES(heartbeat_ts)")
        .setParameter("id", nodeId)
        .setParameter("ts", Timestamp.from(ts))
        .executeUpdate();
  }

  @Override
  public Optional<NodeEntity> findNodeById(String nodeId) {
    return Optional.ofNullable(em.find(NodeEntity.class, nodeId));
  }

  @Override
  public List<NodeEntity> findInactiveNodesSince(Instant cutoff) {
    return em.createQuery(
            "SELECT n FROM NodeEntity n WHERE n.lastHeartbeat < :cutoff", NodeEntity.class)
        .setParameter("cutoff", cutoff)
        .getResultList();
  }

  @Override
  public int deleteInactiveNodesSince(Instant cutoff) {
    return em.createQuery("DELETE FROM NodeEntity n WHERE n.lastHeartbeat < :cutoff")
        .setParameter("cutoff", cutoff)
        .executeUpdate();
  }

  @Override
  public Instant getDatabaseTime() {
    Timestamp ts = (Timestamp) em.createNativeQuery("SELECT NOW(3)").getSingleResult();
    return ts.toInstant();
  }

  // ── ArchiveStore ──────────────────────────────────────────────────────

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
      em.persist(buildArchive(job, reason, archivedBy));
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
    return em.createQuery(
            "SELECT COUNT(j) FROM JobEntity j WHERE j.status IN ("
                + "run.ratchet.store.entity.JobStatus.SUCCEEDED, "
                + "run.ratchet.store.entity.JobStatus.FAILED, "
                + "run.ratchet.store.entity.JobStatus.CANCELED) "
                + "AND j.updatedAt < :cutoff",
            Long.class)
        .setParameter("cutoff", olderThan)
        .getSingleResult();
  }

  @Override
  public List<ArchivedJobEntity> findArchivedJobs(
      String targetClass, String businessKey, Instant from, Instant to, int limit) {
    StringBuilder jpql = new StringBuilder("SELECT a FROM ArchivedJobEntity a WHERE 1=1");
    if (targetClass != null) {
      jpql.append(" AND a.targetClass = :tc");
    }
    if (businessKey != null) {
      jpql.append(" AND a.businessKey = :bk");
    }
    if (from != null) {
      jpql.append(" AND a.archivedAt >= :from");
    }
    if (to != null) {
      jpql.append(" AND a.archivedAt <= :to");
    }
    jpql.append(" ORDER BY a.archivedAt DESC");

    TypedQuery<ArchivedJobEntity> query = em.createQuery(jpql.toString(), ArchivedJobEntity.class);
    if (targetClass != null) {
      query.setParameter("tc", targetClass);
    }
    if (businessKey != null) {
      query.setParameter("bk", businessKey);
    }
    if (from != null) {
      query.setParameter("from", from);
    }
    if (to != null) {
      query.setParameter("to", to);
    }
    return query.setMaxResults(limit).getResultList();
  }

  @Override
  public int purgeArchivedJobs(Instant olderThan) {
    return em.createQuery("DELETE FROM ArchivedJobEntity a WHERE a.archivedAt < :cutoff")
        .setParameter("cutoff", olderThan)
        .executeUpdate();
  }

  // ── ExecutionStore ────────────────────────────────────────────────────

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
    return em.createQuery(
            "SELECT e FROM JobExecutionEntity e WHERE e.jobId = :jid ORDER BY e.attempt ASC",
            JobExecutionEntity.class)
        .setParameter("jid", jobId)
        .getResultList();
  }

  @Override
  public Optional<JobExecutionEntity> findLatestExecution(long jobId) {
    List<JobExecutionEntity> results =
        em.createQuery(
                "SELECT e FROM JobExecutionEntity e WHERE e.jobId = :jid ORDER BY e.attempt DESC",
                JobExecutionEntity.class)
            .setParameter("jid", jobId)
            .setMaxResults(1)
            .getResultList();
    return results.isEmpty() ? Optional.empty() : Optional.of(results.get(0));
  }

  @Override
  public int countExecutionAttempts(long jobId) {
    return em.createQuery(
            "SELECT COUNT(e) FROM JobExecutionEntity e WHERE e.jobId = :jid", Long.class)
        .setParameter("jid", jobId)
        .getSingleResult()
        .intValue();
  }

  // ── JobLogStore ───────────────────────────────────────────────────────

  @Override
  public void appendLog(JobLogEntity log) {
    em.persist(log);
  }

  @Override
  public int purgeLogsOlderThan(Instant cutoff) {
    return em.createQuery("DELETE FROM JobLogEntity l WHERE l.ts < :cutoff")
        .setParameter("cutoff", cutoff)
        .executeUpdate();
  }

  // ── TagStore ──────────────────────────────────────────────────────────

  @Override
  public void insertTags(long jobId, List<String> tags) {
    if (tags == null || tags.isEmpty()) {
      return;
    }
    for (String tag : tags) {
      em.createNativeQuery("INSERT IGNORE INTO scheduler_job_tag (job_id, tag) VALUES (:jid, :tag)")
          .setParameter("jid", jobId)
          .setParameter("tag", tag)
          .executeUpdate();
    }
  }

  @Override
  public int deleteTagsByJobId(long jobId) {
    return em.createNativeQuery("DELETE FROM scheduler_job_tag WHERE job_id = :jid")
        .setParameter("jid", jobId)
        .executeUpdate();
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<Long> findJobIdsByTag(String tag, int limit, int offset) {
    List<?> rows =
        em.createNativeQuery(
                "SELECT job_id FROM scheduler_job_tag WHERE tag = :tag LIMIT :lim OFFSET :off")
            .setParameter("tag", tag)
            .setParameter("lim", limit)
            .setParameter("off", offset)
            .getResultList();
    return rows.stream().map(r -> ((Number) r).longValue()).collect(Collectors.toList());
  }

  // ── WorkflowConditionStore ────────────────────────────────────────────

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
    return em.createQuery(
            "SELECT c FROM WorkflowConditionEntity c WHERE c.parentJobId = :pid "
                + "ORDER BY c.conditionPriority ASC",
            WorkflowConditionEntity.class)
        .setParameter("pid", parentJobId)
        .getResultList();
  }

  @Override
  public List<WorkflowConditionEntity> findConditionsByChildJobId(long childJobId) {
    return em.createQuery(
            "SELECT c FROM WorkflowConditionEntity c WHERE c.childJobId = :cid",
            WorkflowConditionEntity.class)
        .setParameter("cid", childJobId)
        .getResultList();
  }

  @Override
  public List<WorkflowConditionEntity> findConditionsByType(
      long parentJobId, WorkflowCondition.ConditionType type) {
    return em.createQuery(
            "SELECT c FROM WorkflowConditionEntity c WHERE c.parentJobId = :pid "
                + "AND c.conditionType = :type ORDER BY c.conditionPriority ASC",
            WorkflowConditionEntity.class)
        .setParameter("pid", parentJobId)
        .setParameter("type", type)
        .getResultList();
  }

  @Override
  public void deleteConditionById(long id) {
    WorkflowConditionEntity entity = em.find(WorkflowConditionEntity.class, id);
    if (entity != null) {
      em.remove(entity);
    }
  }

  @Override
  public void deleteConditionsByParentJobId(long parentJobId) {
    em.createQuery("DELETE FROM WorkflowConditionEntity c WHERE c.parentJobId = :pid")
        .setParameter("pid", parentJobId)
        .executeUpdate();
  }

  @Override
  public void deleteConditionsByChildJobId(long childJobId) {
    em.createQuery("DELETE FROM WorkflowConditionEntity c WHERE c.childJobId = :cid")
        .setParameter("cid", childJobId)
        .executeUpdate();
  }

  @Override
  public long countConditionsByParentJobId(long parentJobId) {
    return em.createQuery(
            "SELECT COUNT(c) FROM WorkflowConditionEntity c WHERE c.parentJobId = :pid", Long.class)
        .setParameter("pid", parentJobId)
        .getSingleResult();
  }

  // ── BatchMetricsStore ─────────────────────────────────────────────────

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
            "UPDATE scheduler_batch_metrics "
                + "SET child_execution_ms = COALESCE(child_execution_ms, 0) + :dur, "
                + "success_count = success_count + 1 "
                + "WHERE batch_id = :bid")
        .setParameter("dur", durationMs)
        .setParameter("bid", batchId)
        .executeUpdate();
  }

  @Override
  public void finalizeBatchMetrics(long batchId) {
    em.createNativeQuery(
            "UPDATE scheduler_batch_metrics SET completed_at = NOW(3), "
                + "total_duration_ms = TIMESTAMPDIFF(MICROSECOND, started_at, NOW(3)) / 1000, "
                + "overhead_ms = COALESCE("
                + "  TIMESTAMPDIFF(MICROSECOND, started_at, NOW(3)) / 1000 - child_execution_ms, 0) "
                + "WHERE batch_id = :bid")
        .setParameter("bid", batchId)
        .executeUpdate();
  }

  @Override
  public void updateBatchMetricsChildCount(long batchId, int childCount) {
    em.createNativeQuery(
            "UPDATE scheduler_batch_metrics SET child_count = :cnt WHERE batch_id = :bid")
        .setParameter("cnt", childCount)
        .setParameter("bid", batchId)
        .executeUpdate();
  }

  // ── DlqAlertStore ─────────────────────────────────────────────────────

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
    Long count =
        em.createQuery(
                "SELECT COUNT(a) FROM DlqAlertEntity a "
                    + "WHERE a.jobId = :jid AND a.errorHash = :hash AND a.alertSentAt >= :cutoff",
                Long.class)
            .setParameter("jid", jobId)
            .setParameter("hash", errorHash)
            .setParameter("cutoff", cutoff)
            .getSingleResult();
    return count > 0;
  }

  // ── ResourcePermitStore ───────────────────────────────────────────────

  @Override
  public boolean tryAcquirePermit(String resource, long jobId, String nodeId) {
    // Lock the resource limit row to serialize concurrent permit acquisitions
    Object[] limits =
        (Object[])
            em
                .createNativeQuery(
                    "SELECT max_concurrent, "
                        + "(SELECT COUNT(*) FROM scheduler_resource_permit WHERE resource_name = :res) "
                        + "FROM scheduler_resource_limit WHERE resource_name = :res "
                        + "FOR UPDATE")
                .setParameter("res", resource)
                .getResultList()
                .stream()
                .findFirst()
                .orElse(null);

    if (limits == null) {
      return false;
    }

    int maxConcurrent = ((Number) limits[0]).intValue();
    int active = ((Number) limits[1]).intValue();

    if (active >= maxConcurrent) {
      return false;
    }

    ResourcePermitEntity permit = ResourcePermitEntity.create(resource, jobId, nodeId);
    em.persist(permit);
    return true;
  }

  @Override
  public void releasePermit(String resource, long jobId) {
    em.createNativeQuery(
            "DELETE FROM scheduler_resource_permit "
                + "WHERE resource_name = :res AND job_id = :jid")
        .setParameter("res", resource)
        .setParameter("jid", jobId)
        .executeUpdate();
  }

  @Override
  public void releaseAllPermits(long jobId) {
    em.createNativeQuery("DELETE FROM scheduler_resource_permit WHERE job_id = :jid")
        .setParameter("jid", jobId)
        .executeUpdate();
  }

  @Override
  public int getPermitRetryDelay(String resource) {
    try {
      return ((Number)
              em.createNativeQuery(
                      "SELECT retry_delay_ms FROM scheduler_resource_limit WHERE resource_name = :res")
                  .setParameter("res", resource)
                  .getSingleResult())
          .intValue();
    } catch (NoResultException e) {
      return 5000;
    }
  }

  @Override
  public void configureResource(
      String name, int maxConcurrent, int retryDelayMs, String description) {
    em.createNativeQuery(
            "INSERT INTO scheduler_resource_limit "
                + "(resource_name, max_concurrent, retry_delay_ms, description, created_at, updated_at) "
                + "VALUES (:name, :max, :delay, :desc, NOW(3), NOW(3)) "
                + "ON DUPLICATE KEY UPDATE "
                + "max_concurrent = VALUES(max_concurrent), "
                + "retry_delay_ms = VALUES(retry_delay_ms), "
                + "description = VALUES(description), "
                + "updated_at = NOW(3)")
        .setParameter("name", name)
        .setParameter("max", maxConcurrent)
        .setParameter("delay", retryDelayMs)
        .setParameter("desc", description)
        .executeUpdate();
  }

  @Override
  public int cleanupOrphanedPermits(List<String> staleNodeIds) {
    if (staleNodeIds.isEmpty()) {
      return 0;
    }
    return em.createNativeQuery("DELETE FROM scheduler_resource_permit WHERE node_id IN (:nodes)")
        .setParameter("nodes", staleNodeIds)
        .executeUpdate();
  }

  // ── Private helpers ───────────────────────────────────────────────────

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

  private Instant toInstant(Object val) {
    if (val == null) {
      return null;
    }
    if (val instanceof Timestamp ts) {
      return ts.toInstant();
    }
    if (val instanceof Instant inst) {
      return inst;
    }
    return null;
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

  private static String buildClaimSql(
      String selectClause, String typeFilter, String timeColumn, int boostInterval) {
    String orderBy =
        boostInterval > 0
            ? "(priority + FLOOR(GREATEST(0, TIMESTAMPDIFF(MINUTE, "
                + timeColumn
                + ", NOW(3))) / :boost)) DESC, "
                + timeColumn
                + " ASC"
            : "priority DESC, " + timeColumn + " ASC";
    return """
        SELECT %s FROM scheduler_job
        WHERE status = 'PENDING'
          AND %s <= NOW(3)
          AND %s
        ORDER BY %s
        LIMIT :lim
        FOR UPDATE SKIP LOCKED"""
        .formatted(selectClause, timeColumn, typeFilter, orderBy);
  }

  private String payloadToJson(JobEntity job) {
    if (job.getPayload() == null) {
      return "{}";
    }
    try {
      return OBJECT_MAPPER.writeValueAsString(job.getPayload());
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize payload", e);
    }
  }

  private String paramsToJson(JobEntity job) {
    if (job.getParams() == null) {
      return null;
    }
    try {
      return OBJECT_MAPPER.writeValueAsString(job.getParams());
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize params", e);
    }
  }

  private String callbackPayloadToJson(JobPayload payload) {
    if (payload == null) {
      return null;
    }
    try {
      return OBJECT_MAPPER.writeValueAsString(payload);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize callback payload", e);
    }
  }
}
