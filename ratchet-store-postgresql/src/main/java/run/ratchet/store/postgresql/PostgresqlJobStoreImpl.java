package run.ratchet.store.postgresql;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import run.ratchet.api.JobPriority;
import run.ratchet.api.RatchetOptions;
import run.ratchet.api.WorkflowCondition;
import run.ratchet.api.exception.RatchetTransientStoreException;
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
import run.ratchet.store.id.TsidFactory;
import run.ratchet.store.spi.RatchetEntityManagerProvider;
import run.ratchet.store.util.ArchiveHelper;
import run.ratchet.store.util.IsolationCheck;
import run.ratchet.store.util.ObjectMapperFactory;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.persistence.NoResultException;
import jakarta.persistence.Query;
import jakarta.transaction.Transactional;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.jboss.logging.Logger;

/**
 * PostgreSQL implementation of the {@link PostgresqlJobStore} API.
 *
 * <p>Uses JPA EntityManager for simple CRUD and native SQL for PostgreSQL-specific operations such
 * as {@code FOR UPDATE SKIP LOCKED}, {@code ON CONFLICT}, and {@code RETURNING} clauses.
 */
@ApplicationScoped
@Transactional
class PostgresqlJobStoreImpl implements PostgresqlJobStore {

  private static final Logger log = Logger.getLogger(PostgresqlJobStoreImpl.class);
  private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.get();
  private static final PostgresqlConstraintDetector CONSTRAINT_DETECTOR =
      new PostgresqlConstraintDetector();
  private static final String EXECUTABLE_JOB_TYPE_FILTER =
      "job_type IN ('SINGLE','BATCH_CHILD','CHAIN_STEP','WORKFLOW_BRANCH')";
  private static final String RECURRING_JOB_TYPE_FILTER = "job_type = 'RECURRING'";

  private final RatchetEntityManagerProvider entityManagerProvider;
  private final RatchetOptions options;
  private EntityManager em;

  private PostgresqlStoreContext ctx;
  private PostgresqlBusinessKeyReservations reservations;
  private PostgresqlTagOperations tags;
  private PostgresqlJobCrudOperations jobs;

  /** No-arg constructor required by CDI normal-scope proxying. Not for direct use. */
  protected PostgresqlJobStoreImpl() {
    this.entityManagerProvider = null;
    this.options = null;
  }

  @Inject
  PostgresqlJobStoreImpl(
      RatchetEntityManagerProvider entityManagerProvider, RatchetOptions options) {
    this.entityManagerProvider = entityManagerProvider;
    this.options = options;
  }

  private static Instant toInstant(Object value) {
    if (value instanceof Instant) {
      return (Instant) value;
    }
    if (value instanceof Timestamp) {
      return ((Timestamp) value).toInstant();
    }
    if (value instanceof OffsetDateTime) {
      return ((OffsetDateTime) value).toInstant();
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

  private static String buildBoostOrderBy(String timeColumn, int boostInterval) {
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
  private static String buildClaimReturningSql(
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

  @Override
  public List<JobEntity> claimNextBatch(int limit, String nodeId) {
    try {
      int boostInterval = options.store().priorityBoostIntervalMinutes();
      var claimQuery =
          em.createNativeQuery(
              buildClaimReturningSql(
                  EXECUTABLE_JOB_TYPE_FILTER, "scheduled_time", boostInterval, "j.*"),
              JobEntity.class);
      int parameter = 1;
      if (boostInterval > 0) {
        claimQuery.setParameter(parameter++, boostInterval);
      }
      claimQuery.setParameter(parameter++, limit);
      claimQuery.setParameter(parameter++, nodeId);
      @SuppressWarnings("unchecked")
      List<JobEntity> jobs = claimQuery.getResultList();
      return jobs;
    } catch (RuntimeException e) {
      throw translateTransientStoreException("claim jobs", e);
    }
  }

  @Override
  public List<JobClaimDto> claimNextBatchOptimized(
      JobExecutionType jobType, int limit, String nodeId) {
    if (limit <= 0 || !PostgresqlStoreContext.isPollerExecutable(jobType)) {
      return List.of();
    }

    try {
      int boostInterval = options.store().priorityBoostIntervalMinutes();
      String selectColumns =
          "j.job_id, j.status, j.job_type, j.priority, j.scheduled_time, j.version, "
              + "j.timeout_sec, j.picked_by, j.picked_at, j.business_key, j.attempts, j.max_retries";
      var claimQuery =
          em.createNativeQuery(
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
    } catch (RuntimeException e) {
      throw translateTransientStoreException("optimized claim", e);
    }
  }

  @Override
  public JobEntity save(JobEntity job) {
    return jobs.save(job);
  }

  @Override
  public Optional<JobEntity> findById(long id) {
    return jobs.findById(id);
  }

  @Override
  public Optional<JobEntity> findByIdLatest(long id) {
    return jobs.findByIdLatest(id);
  }

  @Override
  public void delete(long id) {
    jobs.delete(id);
  }

  @Override
  public JobStatus getJobStatus(long id) {
    return jobs.getJobStatus(id);
  }

  @Override
  public List<JobEntity> findByIds(List<Long> ids) {
    return jobs.findByIds(ids);
  }

  @Override
  public Optional<JobEntity> findActiveByBusinessKey(String businessKey) {
    return jobs.findActiveByBusinessKey(businessKey);
  }

  @Override
  public Optional<JobEntity> findByIdempotencyKey(String idempotencyKey) {
    return jobs.findByIdempotencyKey(idempotencyKey);
  }

  @Override
  public List<JobEntity> findDependants(long parentJobId) {
    return jobs.findDependants(parentJobId);
  }

  @Override
  public Optional<Instant> findEarliestRecurringNextFire() {
    return jobs.findEarliestRecurringNextFire();
  }

  @Override
  public long countPendingJobs() {
    return jobs.countPendingJobs();
  }

  @Override
  public long countJobsByStatus(JobStatus status) {
    return jobs.countJobsByStatus(status);
  }

  @Override
  public long countActiveJobs(JobExecutionType jobType) {
    return jobs.countActiveJobs(jobType);
  }

  @Override
  public long countActiveNodes() {
    return jobs.countActiveNodes();
  }

  @Override
  public long countReadyJobs(Instant now) {
    return jobs.countReadyJobs(now);
  }

  @Override
  public long countStuckJobs(Instant stuckThreshold) {
    return jobs.countStuckJobs(stuckThreshold);
  }

  @Override
  public long countLongRunningJobs(Instant threshold) {
    return jobs.countLongRunningJobs(threshold);
  }

  @Override
  public long countPendingBatchChildren() {
    return jobs.countPendingBatchChildren();
  }

  @Override
  public long countPendingJobsByPriority(JobPriority priority) {
    return jobs.countPendingJobsByPriority(priority);
  }

  @Override
  public long countPendingJobsByType(JobExecutionType jobType) {
    return jobs.countPendingJobsByType(jobType);
  }

  @Override
  public long countJobsByStatusSince(JobStatus status, Instant since) {
    return jobs.countJobsByStatusSince(status, since);
  }

  @Override
  public long countJobsWithRetries() {
    return jobs.countJobsWithRetries();
  }

  @Override
  public double getRetryRateStats(Instant since) {
    return jobs.getRetryRateStats(since);
  }

  @Override
  public double getAverageProcessingTime(Instant since) {
    return jobs.getAverageProcessingTime(since);
  }

  @Override
  public double getAverageBatchSize(Instant since) {
    return jobs.getAverageBatchSize(since);
  }

  @Override
  public Optional<Instant> getOldestPendingJobTime() {
    return jobs.getOldestPendingJobTime();
  }

  @Override
  public long getQueueWaitTimePercentile(double percentile) {
    return jobs.getQueueWaitTimePercentile(percentile);
  }

  @Override
  public List<JobEntity> claimDueRecurring(int limit, String nodeId) {
    try {
      int boostInterval = options.store().priorityBoostIntervalMinutes();
      var claimQuery =
          em.createNativeQuery(
              buildClaimReturningSql(RECURRING_JOB_TYPE_FILTER, "next_fire", boostInterval, "j.*"),
              JobEntity.class);
      int parameter = 1;
      if (boostInterval > 0) {
        claimQuery.setParameter(parameter++, boostInterval);
      }
      claimQuery.setParameter(parameter++, limit);
      claimQuery.setParameter(parameter++, nodeId);
      @SuppressWarnings("unchecked")
      List<JobEntity> jobs = claimQuery.getResultList();
      return jobs;
    } catch (RuntimeException e) {
      throw translateTransientStoreException("claim recurring jobs", e);
    }
  }

  @Override
  public Instant getDatabaseTime() {
    // The PostgreSQL JDBC driver returns statement_timestamp() as java.time.OffsetDateTime /
    // Instant in recent versions, and as java.sql.Timestamp in older ones. Accept either shape
    // rather than casting narrowly.
    Object ts = em.createNativeQuery("SELECT statement_timestamp()").getSingleResult();
    if (ts instanceof Instant i) {
      return i;
    }
    if (ts instanceof OffsetDateTime odt) {
      return odt.toInstant();
    }
    if (ts instanceof Timestamp t) {
      return t.toInstant();
    }
    throw new IllegalStateException(
        "Unexpected statement_timestamp() result type: "
            + (ts == null ? "null" : ts.getClass().getName()));
  }

  @Override
  public void updateJobStatus(long id, JobStatus status, String errorMessage) {
    int updated =
        em.createNativeQuery(
                "UPDATE scheduler_job SET status = ?, last_error = ?, "
                    + "updated_at = statement_timestamp() WHERE job_id = ?")
            .setParameter(1, status.name())
            .setParameter(2, errorMessage)
            .setParameter(3, id)
            .executeUpdate();
    if (updated > 0) {
      reservations.syncForJob(id, status);
    }
  }

  @Override
  public boolean compareAndSwapStatus(
      long id, JobStatus expected, JobStatus newStatus, String error) {
    try {
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
      if (updated > 0) {
        if (PostgresqlStoreContext.isTerminalStatus(newStatus)) {
          reservations.deleteReservationByOwner(id);
        } else if (PostgresqlStoreContext.isTerminalStatus(expected)) {
          reservations.syncForJob(id, newStatus);
        }
      }
      return updated > 0;
    } catch (RuntimeException e) {
      throw translateTransientStoreException("compare-and-swap status", e);
    }
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
    try {
      int updated =
          em.createNativeQuery(
                  "UPDATE scheduler_job SET status = 'SUCCEEDED', "
                      + "job_result = ?::jsonb, result_type = ?, "
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
      if (updated > 0) {
        reservations.deleteReservationByOwner(id);
      }
      return updated > 0;
    } catch (RuntimeException e) {
      throw translateTransientStoreException("mark job succeeded", e);
    }
  }

  @Override
  public boolean markJobSucceededMinimal(
      long id, Instant start, Instant end, Long durationMs, Long queueWaitMs) {
    try {
      int updated =
          em.createNativeQuery(
                  "UPDATE scheduler_job SET status = 'SUCCEEDED', "
                      + "execution_start_time = ?, execution_end_time = ?, "
                      + "execution_duration_ms = ?, queue_wait_ms = ?, "
                      + "last_error = NULL, updated_at = statement_timestamp() "
                      + "WHERE job_id = ? AND status = 'RUNNING'")
              .setParameter(1, start == null ? null : Timestamp.from(start))
              .setParameter(2, end == null ? null : Timestamp.from(end))
              .setParameter(3, durationMs)
              .setParameter(4, queueWaitMs)
              .setParameter(5, id)
              .executeUpdate();
      if (updated > 0) {
        reservations.deleteReservationByOwner(id);
      }
      return updated > 0;
    } catch (RuntimeException e) {
      throw translateTransientStoreException("mark job succeeded minimally", e);
    }
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
    List<?> updated =
        em.createNativeQuery(
                "UPDATE scheduler_job SET status = 'PENDING', "
                    + "scheduled_time = ?, attempts = ?, last_error = ?, "
                    + "picked_by = NULL, picked_at = NULL, "
                    + "updated_at = statement_timestamp() "
                    + "WHERE job_id = ? AND status IN ('RUNNING','FAILED') "
                    + "RETURNING job_id")
            .setParameter(1, Timestamp.from(newScheduledTime))
            .setParameter(2, attempts)
            .setParameter(3, error)
            .setParameter(4, id)
            .getResultList();
    if (updated.isEmpty()) {
      return false;
    }
    reservations.syncForJob(id, JobStatus.PENDING);
    return true;
  }

  @Override
  public boolean pauseRecurring(long id) {
    // Single-table PG schema: pause-recurring is a normal PENDING→PAUSED status flip on the
    // recurring row. CP3 will move recurring masters to scheduler_recurring_job.
    int updated =
        em.createNativeQuery(
                "UPDATE scheduler_job SET status = 'PAUSED', paused_from_status = 'PENDING', "
                    + "updated_at = statement_timestamp() "
                    + "WHERE job_id = ? AND job_type = 'RECURRING' AND status = 'PENDING'")
            .setParameter(1, id)
            .executeUpdate();
    return updated > 0;
  }

  @Override
  public boolean resumeRecurring(long id) {
    int updated =
        em.createNativeQuery(
                "UPDATE scheduler_job SET status = 'PENDING', paused_from_status = NULL, "
                    + "updated_at = statement_timestamp() "
                    + "WHERE job_id = ? AND job_type = 'RECURRING' AND status = 'PAUSED'")
            .setParameter(1, id)
            .executeUpdate();
    return updated > 0;
  }

  @Override
  public boolean markJobFailedTerminal(long id, String terminalError, int totalAttempts) {
    int updated =
        em.createNativeQuery(
                "UPDATE scheduler_job SET status = 'FAILED', last_error = ?, "
                    + "attempts = ?, picked_by = NULL, picked_at = NULL, "
                    + "updated_at = statement_timestamp() "
                    + "WHERE job_id = ? AND status = 'RUNNING'")
            .setParameter(1, terminalError)
            .setParameter(2, totalAttempts)
            .setParameter(3, id)
            .executeUpdate();
    if (updated > 0) {
      reservations.deleteReservationByOwner(id);
    }
    return updated > 0;
  }

  @Override
  public boolean cancelJob(long id) {
    int updated =
        em.createNativeQuery(
                "UPDATE scheduler_job SET status = 'CANCELED', "
                    + "picked_by = NULL, picked_at = NULL, "
                    + "updated_at = statement_timestamp() "
                    + "WHERE job_id = ? AND status IN ('PENDING','RUNNING','PAUSED')")
            .setParameter(1, id)
            .executeUpdate();
    if (updated > 0) {
      reservations.deleteReservationByOwner(id);
    }
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
    @SuppressWarnings("unchecked")
    List<Number> canceled =
        em.createNativeQuery(
                "UPDATE scheduler_job SET status = 'CANCELED', "
                    + "updated_at = statement_timestamp() "
                    + "WHERE job_id IN ("
                    + "  SELECT j.job_id FROM scheduler_job j "
                    + "  INNER JOIN scheduler_job_tag t ON j.job_id = t.job_id "
                    + "  WHERE t.tag = ? AND j.job_type = 'RECURRING' "
                    + "  AND j.status IN ('PENDING','RUNNING','PAUSED')"
                    + ") "
                    + "RETURNING job_id")
            .setParameter(1, tag)
            .getResultList();
    reservations.deleteReservationsByOwners(canceled);
    return canceled.size();
  }

  @Override
  public int cancelRecurringJobByBusinessKey(String businessKey) {
    @SuppressWarnings("unchecked")
    List<Number> canceled =
        em.createNativeQuery(
                "UPDATE scheduler_job SET status = 'CANCELED', "
                    + "updated_at = statement_timestamp() "
                    + "WHERE business_key = ? AND job_type = 'RECURRING' "
                    + "AND status IN ('PENDING','RUNNING','PAUSED') "
                    + "RETURNING job_id")
            .setParameter(1, businessKey)
            .getResultList();
    reservations.deleteReservationsByOwners(canceled);
    return canceled.size();
  }

  @Override
  public int cancelOrphanedRecurringAnnotationJobs(
      Set<String> registeredIds, Instant nodeStartTime) {
    if (registeredIds.isEmpty()) {
      return 0;
    }
    List<String> idsList = new ArrayList<>(registeredIds);
    String placeholders = String.join(",", Collections.nCopies(idsList.size(), "?"));
    Query query =
        em.createNativeQuery(
            "UPDATE scheduler_job SET status = 'CANCELED', "
                + "updated_at = statement_timestamp() "
                + "WHERE job_type = 'RECURRING' "
                + "AND status IN ('PENDING','RUNNING','PAUSED') "
                + "AND created_at < ? "
                + "AND business_key IS NOT NULL "
                + "AND business_key NOT IN ("
                + placeholders
                + ") "
                + "RETURNING job_id");
    int parameter = 1;
    query.setParameter(parameter++, Timestamp.from(nodeStartTime));
    for (String id : idsList) {
      query.setParameter(parameter++, id);
    }
    @SuppressWarnings("unchecked")
    List<Number> canceled = query.getResultList();
    reservations.deleteReservationsByOwners(canceled);
    return canceled.size();
  }

  @Override
  public boolean resetFailedToPending(long id) {
    List<?> updated =
        em.createNativeQuery(
                "UPDATE scheduler_job SET status = 'PENDING', attempts = 0, "
                    + "last_error = NULL, scheduled_time = statement_timestamp(), "
                    + "picked_by = NULL, picked_at = NULL, "
                    + "updated_at = statement_timestamp() "
                    + "WHERE job_id = ? AND status = 'FAILED' "
                    + "RETURNING job_id")
            .setParameter(1, id)
            .getResultList();
    if (updated.isEmpty()) {
      return false;
    }
    reservations.syncForJob(id, JobStatus.PENDING);
    return true;
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
    if (updated > 0 && PostgresqlStoreContext.isTerminalStatus(target)) {
      reservations.deleteReservationByOwner(id);
    }
    return updated > 0;
  }

  @Override
  public JobStatus transitionFromPausedAtomic(long id) {
    List<?> results =
        em.createNativeQuery(
                "UPDATE scheduler_job "
                    + "SET status = COALESCE(paused_from_status, 'PENDING'), "
                    + "paused_from_status = NULL, "
                    + "updated_at = statement_timestamp() "
                    + "WHERE job_id = ? AND status = 'PAUSED' "
                    + "RETURNING status")
            .setParameter(1, id)
            .getResultList();
    if (results.isEmpty()) {
      return null;
    }
    JobStatus status = JobStatus.valueOf((String) results.get(0));
    if (PostgresqlStoreContext.isTerminalStatus(status)) {
      reservations.deleteReservationByOwner(id);
    }
    return status;
  }

  @Override
  public void bulkInsert(List<JobEntity> jobsToInsert) {
    jobs.bulkInsert(jobsToInsert);
  }

  @Override
  public int deleteJobsByIds(List<Long> ids) {
    return jobs.deleteJobsByIds(ids);
  }

  @Override
  public int deleteDlqOlderThan(Instant cutoff) {
    return jobs.deleteDlqOlderThan(cutoff);
  }

  @Override
  public int resetOrphanJobs(Duration grace) {
    return jobs.resetOrphanJobs(grace);
  }

  @Override
  public int resetOrphanJobsForNode(String nodeId) {
    return jobs.resetOrphanJobsForNode(nodeId);
  }

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
                    + "WHERE scheduler_lock.expires_at < statement_timestamp() "
                    + "   OR scheduler_lock.owner_node = ?")
            .setParameter(1, name)
            .setParameter(2, nodeId)
            .setParameter(3, ttlSeconds)
            .setParameter(4, ttlSeconds)
            .setParameter(5, nodeId)
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
  public ArchivedJobEntity archiveJob(JobEntity job, String reason, String archivedBy) {
    // Re-fetch with tags hydrated before building the archive record. The incoming job may be
    // detached (e.g. when the caller obtained it from a prior transaction), in which case
    // buildArchive's tags access would throw LazyInitializationException. JPQL JOIN FETCH is
    // JPA-spec portable and hydrates the collection in a single query.
    JobEntity hydrated = jobs.hydrateForArchive(job);
    ArchivedJobEntity archive = buildArchive(hydrated, reason, archivedBy);
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
    return em.createQuery(ArchiveHelper.FIND_JOBS_FOR_ARCHIVING_JPQL, JobEntity.class)
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

  @Override
  public void insertTags(long jobId, List<String> tagsToInsert) {
    tags.insertTags(jobId, tagsToInsert);
  }

  @Override
  public int deleteTagsByJobId(long jobId) {
    return tags.deleteTagsByJobId(jobId);
  }

  @Override
  public List<Long> findJobIdsByTag(String tag, int limit, int offset) {
    return tags.findJobIdsByTag(tag, limit, offset);
  }

  @Override
  public Map<JobStatus, Long> countJobsByStatusForTag(String tag) {
    return tags.countJobsByStatusForTag(tag);
  }

  @Override
  public Map<String, Long> countJobsByParamForTag(String tag, String paramKey) {
    return tags.countJobsByParamForTag(tag, paramKey);
  }

  @Override
  public Map<String, Long> countJobsByExecutionNodeForTag(String tag) {
    return tags.countJobsByExecutionNodeForTag(tag);
  }

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

  @Override
  public BatchMetricsEntity saveBatchMetrics(BatchMetricsEntity metrics) {
    if (em.find(BatchMetricsEntity.class, metrics.getBatchId()) == null) {
      // JPA 3.2 @MapsId derived-identity contract: the relationship attribute supplies identity.
      // Hibernate relaxes this and accepts a scalar id alone, but EclipseLink rejects persist().
      // Resolve the reference explicitly so both providers see a valid derived id.
      if (metrics.getBatchJob() == null) {
        metrics.setBatchJob(em.getReference(JobEntity.class, metrics.getBatchId()));
      }
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
    String placeholders = String.join(",", Collections.nCopies(staleNodeIds.size(), "?"));
    Query query =
        em.createNativeQuery(
            "DELETE FROM scheduler_resource_permit WHERE node_id IN (" + placeholders + ")");
    int parameter = 1;
    for (String nodeId : staleNodeIds) {
      query.setParameter(parameter++, nodeId);
    }
    return query.executeUpdate();
  }

  /** Warns (or fails) if the connection isolation level is not READ COMMITTED. */
  @PostConstruct
  void checkIsolationLevel() {
    if (em == null) {
      em = entityManagerProvider.getEntityManager();
    }
    options.node().explicitTsidNodeId().ifPresent(TsidFactory::configureNodeId);
    IsolationCheck.verifyReadCommitted(
        em,
        "PostgreSQL",
        List.of("SHOW transaction_isolation"),
        "read committed",
        "SERIALIZABLE and REPEATABLE READ both surface SQLState 40001 serialization failures on"
            + " concurrent job claims, which Ratchet's claim loop does not retry. Set"
            + " default_transaction_isolation = 'read committed' in postgresql.conf or unset any"
            + " connection pool override (e.g. hibernate.connection.isolation=2).",
        options.store().isolationCheckMode());
    initDelegates();
  }

  private void initDelegates() {
    ctx = new PostgresqlStoreContext(em, options.store().priorityBoostIntervalMinutes());
    reservations = new PostgresqlBusinessKeyReservations(ctx);
    tags = new PostgresqlTagOperations(ctx);
    jobs = new PostgresqlJobCrudOperations(ctx, reservations, tags);
  }

  private JobPayload parseProgressHook(Object jsonValue) {
    if (jsonValue == null) {
      return null;
    }
    try {
      return OBJECT_MAPPER.readValue(jsonValue.toString(), JobPayload.class);
    } catch (JsonProcessingException e) {
      log.warnf("Bad progress_hook JSON: %s", e.getMessage());
      return null;
    }
  }

  private long countByNative(String sql, Object... params) {
    var query = em.createNativeQuery(sql);
    for (int i = 0; i < params.length; i++) {
      query.setParameter(i + 1, params[i]);
    }
    return ((Number) query.getSingleResult()).longValue();
  }

  private RuntimeException translateTransientStoreException(String operation, RuntimeException e) {
    if (CONSTRAINT_DETECTOR.isDeadlock(e) || CONSTRAINT_DETECTOR.isTransientConnectionFailure(e)) {
      return new RatchetTransientStoreException(
          "Transient PostgreSQL store concurrency failure during " + operation, e);
    }
    return e;
  }

  private ArchivedJobEntity buildArchive(JobEntity job, String reason, String archivedBy) {
    return ArchiveHelper.buildArchive(job, reason, archivedBy);
  }
}
