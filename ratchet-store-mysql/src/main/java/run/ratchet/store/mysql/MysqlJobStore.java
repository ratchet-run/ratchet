package run.ratchet.store.mysql;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import run.ratchet.api.JobPriority;
import run.ratchet.api.WorkflowCondition;
import run.ratchet.api.exception.RatchetTransientStoreException;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.store.converter.JobPayloadConverter;
import run.ratchet.store.converter.JsonMapConverter;
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
import run.ratchet.store.spi.JobStore;
import run.ratchet.store.util.ArchiveHelper;
import run.ratchet.store.util.IsolationCheck;
import run.ratchet.store.util.ObjectMapperFactory;
import run.ratchet.store.util.PriorityBoostConfig;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
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
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.jboss.logging.Logger;

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

  private static final Logger log = Logger.getLogger(MysqlJobStore.class);
  private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.get();
  private static final JobPayloadConverter JOB_PAYLOAD_CONVERTER = new JobPayloadConverter();
  private static final JsonMapConverter JSON_MAP_CONVERTER = new JsonMapConverter();
  private static final MysqlConstraintDetector CONSTRAINT_DETECTOR = new MysqlConstraintDetector();
  private static final String EXECUTABLE_JOB_TYPE_FILTER =
      "job_type IN ('SINGLE','BATCH_CHILD','CHAIN_STEP','WORKFLOW_BRANCH')";
  private static final String RECURRING_JOB_TYPE_FILTER = "job_type = 'RECURRING'";

  // ===========================================================================
  // CP1 hot/cold split: hydration projection + INSERT SQL contracts.
  //
  // HYDRATION_SELECT is a positional projection of (cold LEFT JOIN hot) used by every
  // composite-view read (findById, findByIdLatest, findByIds, findActiveByBusinessKey,
  // findByIdempotencyKey, findDependants). Column order matches the IDX_* indices below,
  // and the count is asserted in hydrateJobEntity().
  // ===========================================================================
  private static final String HYDRATION_SELECT =
      "c.job_id, c.job_type, c.priority, c.max_retries, c.backoff_policy, c.backoff_param_ms, "
          + "c.timeout_sec, c.cron_expr, c.zone_id, c.next_fire, c.payload, c.params, "
          + "c.target_class, c.method_name, c.idempotency_key, c.business_key, c.resource_name, "
          + "c.on_success_payload, c.on_failure_payload, c.depends_on, c.superseded_by, "
          + "c.created_at, c.created_by, c.terminal_status, c.terminal_error, c.total_attempts, "
          + "c.terminated_at, c.execution_start_time, c.execution_end_time, "
          + "c.execution_duration_ms, c.queue_wait_ms, c.job_result, c.result_type, c.rec_status, "
          + "q.status, q.scheduled_time, q.attempts, q.picked_by, q.picked_at, "
          + "q.paused_from_status, q.last_error, q.version, q.updated_at";

  private static final int HYDRATION_COL_COUNT = 43;

  // Cold positions
  private static final int IDX_JOB_ID = 0;
  private static final int IDX_JOB_TYPE = 1;
  private static final int IDX_PRIORITY = 2;
  private static final int IDX_MAX_RETRIES = 3;
  private static final int IDX_BACKOFF_POLICY = 4;
  private static final int IDX_BACKOFF_PARAM_MS = 5;
  private static final int IDX_TIMEOUT_SEC = 6;
  private static final int IDX_CRON_EXPR = 7;
  private static final int IDX_ZONE_ID = 8;
  private static final int IDX_NEXT_FIRE = 9;
  private static final int IDX_PAYLOAD = 10;
  private static final int IDX_PARAMS = 11;
  private static final int IDX_TARGET_CLASS = 12;
  private static final int IDX_METHOD_NAME = 13;
  private static final int IDX_IDEMPOTENCY_KEY = 14;
  private static final int IDX_BUSINESS_KEY = 15;
  private static final int IDX_RESOURCE_NAME = 16;
  private static final int IDX_ON_SUCCESS = 17;
  private static final int IDX_ON_FAILURE = 18;
  private static final int IDX_DEPENDS_ON = 19;
  private static final int IDX_SUPERSEDED_BY = 20;
  private static final int IDX_CREATED_AT = 21;
  private static final int IDX_CREATED_BY = 22;
  private static final int IDX_TERMINAL_STATUS = 23;
  private static final int IDX_TERMINAL_ERROR = 24;
  private static final int IDX_TOTAL_ATTEMPTS = 25;
  private static final int IDX_TERMINATED_AT = 26;
  private static final int IDX_EXEC_START = 27;
  private static final int IDX_EXEC_END = 28;
  private static final int IDX_EXEC_DURATION = 29;
  private static final int IDX_QUEUE_WAIT = 30;
  private static final int IDX_JOB_RESULT = 31;
  private static final int IDX_RESULT_TYPE = 32;
  private static final int IDX_REC_STATUS = 33;

  // Hot positions (LEFT JOIN — all may be null when no live row exists)
  private static final int IDX_Q_STATUS = 34;
  private static final int IDX_Q_SCHEDULED_TIME = 35;
  private static final int IDX_Q_ATTEMPTS = 36;
  private static final int IDX_Q_PICKED_BY = 37;
  private static final int IDX_Q_PICKED_AT = 38;
  private static final int IDX_Q_PAUSED = 39;
  private static final int IDX_Q_LAST_ERROR = 40;
  private static final int IDX_Q_VERSION = 41;
  // q.updated_at is included in the SELECT for completeness but currently not consumed
  // (composite views fall back to terminated_at / created_at instead).

  // Cold INSERT — 22 positional parameters; rec_status is 'P' for RECURRING, NULL otherwise.
  private static final String COLD_INSERT_SQL =
      "INSERT INTO scheduler_job ("
          + "job_id, job_type, priority, max_retries, backoff_policy, backoff_param_ms, "
          + "timeout_sec, cron_expr, zone_id, next_fire, payload, params, idempotency_key, "
          + "business_key, resource_name, on_success_payload, on_failure_payload, depends_on, "
          + "superseded_by, created_at, created_by, rec_status) "
          + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CAST(? AS JSON), CAST(? AS JSON), ?, ?, ?, "
          + "CAST(? AS JSON), CAST(? AS JSON), ?, ?, ?, ?, ?)";

  // Hot INSERT — 15 positional parameters (last is updated_at, bound from caller's `now`).
  private static final String HOT_INSERT_SQL =
      "INSERT INTO scheduler_job_queue ("
          + "job_id, status, job_type, priority, scheduled_time, business_key, timeout_sec, "
          + "max_retries, attempts, picked_by, picked_at, paused_from_status, last_error, "
          + "version, updated_at) "
          + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

  // bkres INSERT — 4 positional parameters.
  private static final String BKRES_INSERT_SQL =
      "INSERT INTO scheduler_business_key_reservation "
          + "(business_key, owner_job_id, owner_table, reserved_at) VALUES (?, ?, ?, ?)";

  private static final String OWNER_TABLE_QUEUE = "QUEUE";
  private static final String OWNER_TABLE_RECURRING = "RECURRING";

  @PersistenceContext private EntityManager em;
  @Inject private MetricsCollector metricsCollector;

  public MysqlJobStore() {}

  /** Test-only constructor: bypasses CDI to wire EM + metrics directly. */
  MysqlJobStore(EntityManager em, MetricsCollector metricsCollector) {
    this.em = em;
    this.metricsCollector = metricsCollector;
  }

  private static JobPriority safeJobPriority(int ordinal) {
    JobPriority[] values = JobPriority.values();
    if (ordinal < 0 || ordinal >= values.length) {
      return JobPriority.NORMAL;
    }
    return values[ordinal];
  }

  /**
   * Builds the executable claim SELECT against the hot queue table. Recurring claim has its own SQL
   * — see claimDueRecurring(). timeColumn is fixed at scheduled_time for executables; param is kept
   * for callers that may pass alternate ordering in future.
   */
  private static String buildClaimSql(
      String selectClause, String typeFilter, String timeColumn, int boostInterval) {
    String orderBy =
        boostInterval > 0
            ? "(priority + FLOOR(GREATEST(0, TIMESTAMPDIFF(MINUTE, "
                + timeColumn
                + ", NOW(3))) / :boost)) DESC, "
                + timeColumn
                + " ASC, job_id ASC"
            : "priority DESC, " + timeColumn + " ASC, job_id ASC";
    return """
        SELECT %s FROM scheduler_job_queue
        WHERE status = 'PENDING'
          AND %s <= NOW(3)
          AND %s
        ORDER BY %s
        LIMIT :lim
        FOR UPDATE SKIP LOCKED"""
        .formatted(selectClause, timeColumn, typeFilter, orderBy);
  }

  @Override
  public JobEntity save(JobEntity job) {
    if (job.getId() == null) {
      saveInsert(job);
    } else {
      saveColdUpdate(job);
    }
    return job;
  }

  /**
   * New-job INSERT path: cold + (hot if executable) + (bkres if business_key) in caller's tx.
   *
   * <p>Single-row inserts go through em.createNativeQuery rather than a JDBC Connection because
   * EntityManager.unwrap(Connection.class) is not portably supported across providers. The
   * createNativeQuery path runs within the active JTA transaction without requiring direct
   * Connection access.
   */
  private void saveInsert(JobEntity job) {
    assignTsidIfMissing(job);
    Instant now = Instant.now();
    Timestamp nowTs = Timestamp.from(now);
    if (job.getCreatedAt() == null) {
      job.setCreatedAt(now);
    }
    if (job.getUpdatedAt() == null) {
      job.setUpdatedAt(now);
    }

    boolean recurring = job.getJobType() == JobExecutionType.RECURRING;
    boolean hasBkey = job.getBusinessKey() != null;
    boolean bornTerminal = job.getStatus() != null && isTerminalStatus(job.getStatus());

    try {
      executeColdInsert(job, nowTs);
      if (bornTerminal) {
        // Test-fixture / migration callers may save() a job already in a terminal state. Skip
        // the hot insert and reservation; backfill terminal_status + terminated_at on cold so
        // findById/getJobStatus return the expected terminal value.
        executeColdTerminalBackfill(job, nowTs);
      } else {
        if (!recurring) {
          executeHotInsert(job, nowTs);
        }
        if (hasBkey) {
          String ownerTable = recurring ? OWNER_TABLE_RECURRING : OWNER_TABLE_QUEUE;
          insertReservation(job.getBusinessKey(), job.getId(), ownerTable);
        }
      }
    } catch (RuntimeException e) {
      if (CONSTRAINT_DETECTOR.isDuplicateBusinessKey(e)) {
        throw new RatchetTransientStoreException(
            "Active business key in use for job " + job.getId(), e);
      }
      throw e;
    }

    if (job.getTags() != null && !job.getTags().isEmpty()) {
      insertTags(job.getId(), job.getTags());
    }
  }

  private static boolean isTerminalStatus(JobStatus s) {
    return s == JobStatus.SUCCEEDED || s == JobStatus.FAILED || s == JobStatus.CANCELED;
  }

  /** Backfills cold terminal_status/terminated_at for born-terminal save() inserts. */
  private void executeColdTerminalBackfill(JobEntity job, Timestamp nowTs) {
    em.createNativeQuery(
            "UPDATE scheduler_job SET terminal_status = ?, terminal_error = ?, "
                + "total_attempts = ?, terminated_at = ? "
                + "WHERE job_id = ?")
        .setParameter(1, job.getStatus().name())
        .setParameter(2, job.getLastError())
        .setParameter(3, job.getAttempts())
        .setParameter(4, nowTs)
        .setParameter(5, job.getId())
        .executeUpdate();
  }

  /** Single-row cold INSERT via createNativeQuery (no JDBC Connection unwrap). */
  private void executeColdInsert(JobEntity job, Timestamp nowTs) {
    String recStatus = null;
    if (job.getJobType() == JobExecutionType.RECURRING) {
      JobStatus s = job.getStatus() != null ? job.getStatus() : JobStatus.PENDING;
      String r = recStatusForLiveStatus(s);
      recStatus = r != null ? r : "P";
    }
    em.createNativeQuery(COLD_INSERT_SQL)
        .setParameter(1, job.getId())
        .setParameter(2, job.getJobType().name())
        .setParameter(3, job.getPriority().ordinal())
        .setParameter(4, job.getMaxRetries())
        .setParameter(5, job.getBackoffPolicy().name())
        .setParameter(6, job.getBackoffParamMs())
        .setParameter(7, job.getTimeoutSec())
        .setParameter(8, job.getCronExpr())
        .setParameter(9, job.getZoneId())
        .setParameter(10, job.getNextFire() != null ? Timestamp.from(job.getNextFire()) : null)
        .setParameter(11, payloadToJson(job))
        .setParameter(12, paramsToJson(job))
        .setParameter(13, job.getIdempotencyKey())
        .setParameter(14, job.getBusinessKey())
        .setParameter(15, job.getResourceName())
        .setParameter(16, callbackPayloadToJson(job.getOnSuccessPayload()))
        .setParameter(17, callbackPayloadToJson(job.getOnFailurePayload()))
        .setParameter(18, job.getDependsOn())
        .setParameter(19, job.getSupersededBy())
        .setParameter(20, nowTs)
        .setParameter(21, job.getCreatedBy())
        .setParameter(22, recStatus)
        .executeUpdate();
  }

  /** Single-row hot INSERT via createNativeQuery (no JDBC Connection unwrap). */
  private void executeHotInsert(JobEntity job, Timestamp nowTs) {
    JobStatus s = job.getStatus() != null ? job.getStatus() : JobStatus.PENDING;
    Instant scheduled = job.getScheduledTime();
    em.createNativeQuery(HOT_INSERT_SQL)
        .setParameter(1, job.getId())
        .setParameter(2, s.name())
        .setParameter(3, job.getJobType().name())
        .setParameter(4, job.getPriority().ordinal())
        .setParameter(5, scheduled != null ? Timestamp.from(scheduled) : nowTs)
        .setParameter(6, job.getBusinessKey())
        .setParameter(7, job.getTimeoutSec())
        .setParameter(8, job.getMaxRetries())
        .setParameter(9, job.getAttempts())
        .setParameter(10, job.getPickedBy())
        .setParameter(11, job.getPickedAt() != null ? Timestamp.from(job.getPickedAt()) : null)
        .setParameter(
            12, job.getPausedFromStatus() != null ? job.getPausedFromStatus().name() : null)
        .setParameter(13, job.getLastError())
        .setParameter(14, job.getVersion() != null ? job.getVersion() : 0)
        .setParameter(15, nowTs)
        .executeUpdate();
  }

  /** Existing-job UPDATE path: cold-only metadata UPDATE; fail-fast on hot mutation. */
  private void saveColdUpdate(JobEntity job) {
    // Detect "scheduled_time-only on PENDING" — chain-unlock pattern. ChainScheduler.scheduleNext
    // and JobCascadeService resume-with-executeImmediately mutate scheduledTime on a PENDING
    // child entity then save(). Post-split, that pattern is the only legitimate hot-field
    // mutation through save(); route it as a narrow hot UPDATE rather than fail-fast.
    if (tryScheduledTimeOnlyHotUpdate(job)) {
      return;
    }
    guardAgainstHotMutation(job);

    em.createNativeQuery(
            "UPDATE scheduler_job SET "
                + "next_fire = ?, "
                + "params = CAST(? AS JSON), "
                + "on_success_payload = CAST(? AS JSON), "
                + "on_failure_payload = CAST(? AS JSON), "
                + "depends_on = ?, "
                + "superseded_by = ?, "
                + "resource_name = ? "
                + "WHERE job_id = ?")
        .setParameter(1, job.getNextFire() != null ? Timestamp.from(job.getNextFire()) : null)
        .setParameter(2, paramsToJson(job))
        .setParameter(3, callbackPayloadToJson(job.getOnSuccessPayload()))
        .setParameter(4, callbackPayloadToJson(job.getOnFailurePayload()))
        .setParameter(5, job.getDependsOn())
        .setParameter(6, job.getSupersededBy())
        .setParameter(7, job.getResourceName())
        .setParameter(8, job.getId())
        .executeUpdate();
  }

  /**
   * Returns true (and applies the hot UPDATE) when the only hot-field difference between incoming
   * and stored is scheduled_time, and the row is still PENDING. Supports chain-unlock and
   * resume-with-executeImmediately patterns without forcing callers to reach for an explicit SPI
   * method.
   */
  @SuppressWarnings("unchecked")
  private boolean tryScheduledTimeOnlyHotUpdate(JobEntity job) {
    long id = job.getId();
    List<Object[]> rows =
        em.createNativeQuery(
                "SELECT q.status, q.scheduled_time, q.attempts, q.picked_by, q.picked_at, "
                    + "q.paused_from_status, q.last_error, q.version "
                    + "FROM scheduler_job_queue q WHERE q.job_id = :id")
            .setParameter("id", id)
            .getResultList();
    if (rows.isEmpty()) {
      return false;
    }
    Object[] row = rows.get(0);
    if (!"PENDING".equals(row[0])) {
      return false;
    }
    Instant storedSched = toInstant(row[1]);
    Instant incomingSched = job.getScheduledTime();
    if (java.util.Objects.equals(storedSched, incomingSched)) {
      return false;
    }
    // All other hot fields must match (no other concurrent mutations expected).
    if (!java.util.Objects.equals(JobStatus.PENDING, job.getStatus())
        || !java.util.Objects.equals(((Number) row[2]).intValue(), job.getAttempts())
        || !java.util.Objects.equals(row[3], job.getPickedBy())
        || !java.util.Objects.equals(toInstant(row[4]), job.getPickedAt())
        || !java.util.Objects.equals(
            row[5] != null ? JobStatus.valueOf((String) row[5]) : null, job.getPausedFromStatus())
        || !java.util.Objects.equals(row[6], job.getLastError())
        || !java.util.Objects.equals(((Number) row[7]).intValue(), job.getVersion())) {
      return false;
    }
    em.createNativeQuery(
            "UPDATE scheduler_job_queue SET scheduled_time = ?, updated_at = NOW(3) "
                + "WHERE job_id = ? AND status = 'PENDING'")
        .setParameter(1, incomingSched != null ? Timestamp.from(incomingSched) : null)
        .setParameter(2, id)
        .executeUpdate();
    return true;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Optional<JobEntity> findById(long id) {
    List<Object[]> rows =
        em.createNativeQuery(
                "SELECT "
                    + HYDRATION_SELECT
                    + " FROM scheduler_job c "
                    + "LEFT JOIN scheduler_job_queue q ON q.job_id = c.job_id "
                    + "WHERE c.job_id = :id")
            .setParameter("id", id)
            .getResultList();
    if (rows.isEmpty()) {
      return Optional.empty();
    }
    JobEntity job = hydrateJobEntity(rows.get(0));
    hydrateTagsSingle(job);
    return Optional.of(job);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Optional<JobEntity> findByIdLatest(long id) {
    List<Object[]> rows =
        em.createNativeQuery(
                "SELECT "
                    + HYDRATION_SELECT
                    + " FROM scheduler_job c "
                    + "LEFT JOIN scheduler_job_queue q ON q.job_id = c.job_id "
                    + "WHERE c.job_id = :id FOR UPDATE")
            .setParameter("id", id)
            .getResultList();
    if (rows.isEmpty()) {
      return Optional.empty();
    }
    JobEntity job = hydrateJobEntity(rows.get(0));
    hydrateTagsSingle(job);
    return Optional.of(job);
  }

  @Override
  public void delete(long id) {
    // Cold DELETE; FK ON DELETE CASCADE drops the hot row. bkres has no FK so explicit delete.
    deleteReservationByOwner(id);
    em.createNativeQuery("DELETE FROM scheduler_job WHERE job_id = :id")
        .setParameter("id", id)
        .executeUpdate();
  }

  @Override
  @SuppressWarnings("unchecked")
  public JobStatus getJobStatus(long id) {
    List<Object[]> results =
        em.createNativeQuery(
                "SELECT q.status, c.rec_status, c.terminal_status "
                    + "FROM scheduler_job c "
                    + "LEFT JOIN scheduler_job_queue q ON q.job_id = c.job_id "
                    + "WHERE c.job_id = :id")
            .setParameter("id", id)
            .getResultList();
    if (results.isEmpty()) {
      return null;
    }
    Object[] row = results.get(0);
    String live = (String) row[0];
    if (live != null) {
      return JobStatus.valueOf(live);
    }
    JobStatus rec = recStatusDecode(stringOrNull(row[1]));
    if (rec != null) {
      return rec;
    }
    String terminal = (String) row[2];
    if (terminal != null) {
      return JobStatus.valueOf(terminal);
    }
    log.errorf("Job %d has no live, recurring, or terminal status — invariant violation", id);
    return null;
  }

  @SuppressWarnings("unchecked")
  @Override
  public List<JobEntity> findByIds(List<Long> ids) {
    if (ids.isEmpty()) {
      return List.of();
    }
    List<Object[]> rows =
        em.createNativeQuery(
                "SELECT "
                    + HYDRATION_SELECT
                    + " FROM scheduler_job c "
                    + "LEFT JOIN scheduler_job_queue q ON q.job_id = c.job_id "
                    + "WHERE c.job_id IN (:ids)")
            .setParameter("ids", ids)
            .getResultList();
    List<JobEntity> jobs = new ArrayList<>(rows.size());
    for (Object[] row : rows) {
      jobs.add(hydrateJobEntity(row));
    }
    hydrateTagsBatch(jobs);
    return jobs;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Optional<JobEntity> findActiveByBusinessKey(String businessKey) {
    // bkres is the authoritative active-uniqueness owner. JOIN through it to find the live job.
    List<Object[]> rows =
        em.createNativeQuery(
                "SELECT br.owner_table, "
                    + HYDRATION_SELECT
                    + " FROM scheduler_business_key_reservation br "
                    + "JOIN scheduler_job c ON c.job_id = br.owner_job_id "
                    + "LEFT JOIN scheduler_job_queue q ON q.job_id = c.job_id "
                    + "WHERE br.business_key = :bk LIMIT 1")
            .setParameter("bk", businessKey)
            .getResultList();
    if (rows.isEmpty()) {
      return Optional.empty();
    }
    Object[] full = rows.get(0);
    String ownerTable = (String) full[0];
    Object[] hydrationRow = new Object[HYDRATION_COL_COUNT];
    System.arraycopy(full, 1, hydrationRow, 0, HYDRATION_COL_COUNT);
    JobEntity job = hydrateJobEntity(hydrationRow);
    if (OWNER_TABLE_QUEUE.equals(ownerTable) && hydrationRow[IDX_Q_STATUS] == null) {
      log.errorf(
          "bkres invariant violation: business_key=%s claims QUEUE owner job=%d but no hot row",
          businessKey, job.getId());
      return Optional.empty();
    }
    hydrateTagsSingle(job);
    return Optional.of(job);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Optional<JobEntity> findByIdempotencyKey(String idempotencyKey) {
    List<Object[]> rows =
        em.createNativeQuery(
                "SELECT "
                    + HYDRATION_SELECT
                    + " FROM scheduler_job c "
                    + "LEFT JOIN scheduler_job_queue q ON q.job_id = c.job_id "
                    + "WHERE c.idempotency_key = :key LIMIT 1")
            .setParameter("key", idempotencyKey)
            .getResultList();
    if (rows.isEmpty()) {
      return Optional.empty();
    }
    JobEntity job = hydrateJobEntity(rows.get(0));
    hydrateTagsSingle(job);
    return Optional.of(job);
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<JobEntity> findDependants(long parentJobId) {
    List<Object[]> rows =
        em.createNativeQuery(
                "SELECT "
                    + HYDRATION_SELECT
                    + " FROM scheduler_job c "
                    + "LEFT JOIN scheduler_job_queue q ON q.job_id = c.job_id "
                    + "WHERE c.depends_on = :pid")
            .setParameter("pid", parentJobId)
            .getResultList();
    List<JobEntity> jobs = new ArrayList<>(rows.size());
    for (Object[] row : rows) {
      jobs.add(hydrateJobEntity(row));
    }
    hydrateTagsBatch(jobs);
    return jobs;
  }

  @Override
  public Optional<Instant> findEarliestRecurringNextFire() {
    List<?> results =
        em.createNativeQuery(
                "SELECT MIN(next_fire) FROM scheduler_job "
                    + "WHERE job_type = 'RECURRING' AND rec_status = 'P' "
                    + "AND next_fire IS NOT NULL")
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
    // Post hot/cold split: live status lives on scheduler_job_queue; terminal status lives on
    // scheduler_job.terminal_status. Dispatch by status kind.
    if (isLiveStatus(status)) {
      Object result =
          em.createNativeQuery("SELECT COUNT(*) FROM scheduler_job_queue WHERE status = :s")
              .setParameter("s", status.name())
              .getSingleResult();
      return ((Number) result).longValue();
    }
    Object result =
        em.createNativeQuery("SELECT COUNT(*) FROM scheduler_job WHERE terminal_status = :s")
            .setParameter("s", status.name())
            .getSingleResult();
    return ((Number) result).longValue();
  }

  @Override
  public long countActiveJobs(JobExecutionType jobType) {
    // Hot-only: PENDING + RUNNING by job_type. job_type is denormalized on hot.
    Object result =
        em.createNativeQuery(
                "SELECT COUNT(*) FROM scheduler_job_queue "
                    + "WHERE job_type = :jt AND status IN ('PENDING','RUNNING')")
            .setParameter("jt", jobType.name())
            .getSingleResult();
    return ((Number) result).longValue();
  }

  @Override
  public long countActiveNodes() {
    return em.createQuery("SELECT COUNT(n) FROM NodeEntity n", Long.class).getSingleResult();
  }

  @Override
  public long countReadyJobs(Instant now) {
    // Hot-only: PENDING due before now.
    Object result =
        em.createNativeQuery(
                "SELECT COUNT(*) FROM scheduler_job_queue "
                    + "WHERE status = 'PENDING' AND scheduled_time <= :now")
            .setParameter("now", Timestamp.from(now))
            .getSingleResult();
    return ((Number) result).longValue();
  }

  @Override
  public long countStuckJobs(Instant stuckThreshold) {
    // Hot-only: RUNNING with picked_at older than threshold.
    Object result =
        em.createNativeQuery(
                "SELECT COUNT(*) FROM scheduler_job_queue "
                    + "WHERE status = 'RUNNING' AND picked_at < :threshold")
            .setParameter("threshold", Timestamp.from(stuckThreshold))
            .getSingleResult();
    return ((Number) result).longValue();
  }

  @Override
  public long countLongRunningJobs(Instant threshold) {
    // Hot-only: post-split, execution_start_time on cold is only written at terminal transition,
    // so it's always NULL while RUNNING. picked_at is the closest live equivalent (set on claim
    // success) and is functionally equivalent for "running too long" detection.
    Object result =
        em.createNativeQuery(
                "SELECT COUNT(*) FROM scheduler_job_queue "
                    + "WHERE status = 'RUNNING' AND picked_at < :threshold")
            .setParameter("threshold", Timestamp.from(threshold))
            .getSingleResult();
    return ((Number) result).longValue();
  }

  @Override
  public long countPendingBatchChildren() {
    // Hot-only: PENDING BATCH_CHILD jobs.
    Object result =
        em.createNativeQuery(
                "SELECT COUNT(*) FROM scheduler_job_queue "
                    + "WHERE job_type = 'BATCH_CHILD' AND status = 'PENDING'")
            .getSingleResult();
    return ((Number) result).longValue();
  }

  @Override
  public long countPendingJobsByPriority(JobPriority priority) {
    // Hot-only: PENDING by priority. priority is denormalized on hot as TINYINT (ordinal).
    Object result =
        em.createNativeQuery(
                "SELECT COUNT(*) FROM scheduler_job_queue "
                    + "WHERE status = 'PENDING' AND priority = :p")
            .setParameter("p", priority.ordinal())
            .getSingleResult();
    return ((Number) result).longValue();
  }

  @Override
  public long countPendingJobsByType(JobExecutionType jobType) {
    // Hot-only: PENDING by job_type. job_type is denormalized on hot.
    Object result =
        em.createNativeQuery(
                "SELECT COUNT(*) FROM scheduler_job_queue "
                    + "WHERE status = 'PENDING' AND job_type = :jt")
            .setParameter("jt", jobType.name())
            .getSingleResult();
    return ((Number) result).longValue();
  }

  @Override
  public long countJobsByStatusSince(JobStatus status, Instant since) {
    // Dispatch by status kind. Live → hot.updated_at; terminal → cold.terminated_at (canonical
    // age column for terminal rows post hot/cold split).
    if (isLiveStatus(status)) {
      Object result =
          em.createNativeQuery(
                  "SELECT COUNT(*) FROM scheduler_job_queue "
                      + "WHERE status = :s AND updated_at >= :since")
              .setParameter("s", status.name())
              .setParameter("since", Timestamp.from(since))
              .getSingleResult();
      return ((Number) result).longValue();
    }
    Object result =
        em.createNativeQuery(
                "SELECT COUNT(*) FROM scheduler_job "
                    + "WHERE terminal_status = :s AND terminated_at >= :since")
            .setParameter("s", status.name())
            .setParameter("since", Timestamp.from(since))
            .getSingleResult();
    return ((Number) result).longValue();
  }

  @Override
  public long countJobsWithRetries() {
    // Live retry attempts on hot.attempts; terminal retry totals on cold.total_attempts. Hot rows
    // are deleted at terminal so the two sets are disjoint — sum is correct.
    Object result =
        em.createNativeQuery(
                "SELECT "
                    + "(SELECT COUNT(*) FROM scheduler_job_queue WHERE attempts > 0) "
                    + "+ (SELECT COUNT(*) FROM scheduler_job WHERE total_attempts > 0)")
            .getSingleResult();
    return ((Number) result).longValue();
  }

  @Override
  public double getRetryRateStats(Instant since) {
    // Recent rate = (jobs with retries) / (jobs total) over union of recently touched live (hot)
    // and recently terminated (cold) rows. Disjoint sets — sum counts directly.
    Object result =
        em.createNativeQuery(
                "SELECT COALESCE("
                    + "  ((SELECT COUNT(*) FROM scheduler_job_queue "
                    + "      WHERE attempts > 0 AND updated_at >= :since) "
                    + "   + (SELECT COUNT(*) FROM scheduler_job "
                    + "      WHERE total_attempts > 0 AND terminated_at >= :since)) "
                    + "  / NULLIF("
                    + "    ((SELECT COUNT(*) FROM scheduler_job_queue WHERE updated_at >= :since) "
                    + "     + (SELECT COUNT(*) FROM scheduler_job "
                    + "        WHERE terminated_at >= :since)), 0), 0)")
            .setParameter("since", Timestamp.from(since))
            .getSingleResult();
    return ((Number) result).doubleValue();
  }

  @Override
  public double getAverageProcessingTime(Instant since) {
    // Cold-only: SUCCEEDED is terminal; execution_duration_ms is on cold and written at terminal
    // transition; terminated_at is the canonical age column for terminal rows post-split.
    Object result =
        em.createNativeQuery(
                "SELECT COALESCE(AVG(execution_duration_ms), 0) FROM scheduler_job "
                    + "WHERE terminal_status = 'SUCCEEDED' AND execution_duration_ms IS NOT NULL "
                    + "AND terminated_at >= :since")
            .setParameter("since", Timestamp.from(since))
            .getSingleResult();
    return ((Number) result).doubleValue();
  }

  @Override
  public double getAverageBatchSize(Instant since) {
    // Batch parents may be live (hot row exists) or terminal (cold-only). Recency = whichever
    // canonical timestamp is available: hot.updated_at (live) or cold.terminated_at (terminal).
    Object result =
        em.createNativeQuery(
                "SELECT COALESCE(AVG(b.total_items), 0) FROM scheduler_batch b "
                    + "JOIN scheduler_job c ON c.job_id = b.batch_id "
                    + "LEFT JOIN scheduler_job_queue q ON q.job_id = c.job_id "
                    + "WHERE COALESCE(q.updated_at, c.terminated_at) >= :since")
            .setParameter("since", Timestamp.from(since))
            .getSingleResult();
    return ((Number) result).doubleValue();
  }

  @Override
  public Optional<Instant> getOldestPendingJobTime() {
    // Hot-only: scheduled_time + status live on scheduler_job_queue post-split.
    List<?> results =
        em.createNativeQuery(
                "SELECT MIN(scheduled_time) FROM scheduler_job_queue WHERE status = 'PENDING'")
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
    // Cold-only: queue_wait_ms is on cold and SUCCEEDED is terminal post-split. MySQL does not
    // support expressions in the OFFSET clause, so compute the offset in Java.
    Number countResult =
        (Number)
            em.createNativeQuery(
                    // language=MySQL
                    "SELECT COUNT(*) FROM scheduler_job "
                        + "WHERE queue_wait_ms IS NOT NULL AND terminal_status = 'SUCCEEDED'")
                .getSingleResult();
    long total = countResult.longValue();
    if (total == 0) {
      return 0L;
    }
    int offset = (int) Math.floor(percentile * total);
    @SuppressWarnings("unchecked")
    List<Object> percentileResults =
        em.createNativeQuery(
                // language=MySQL
                """
                SELECT COALESCE(queue_wait_ms, 0)
                FROM scheduler_job
                WHERE queue_wait_ms IS NOT NULL AND terminal_status = 'SUCCEEDED'
                ORDER BY queue_wait_ms ASC
                LIMIT 1 OFFSET ?1""")
            .setParameter(1, offset)
            .getResultList();
    Object result = percentileResults.stream().findFirst().orElse(0L);
    return ((Number) result).longValue();
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<JobEntity> claimNextBatch(int limit, String nodeId) {
    try {
      int boostInterval = PriorityBoostConfig.getPriorityBoostIntervalMinutes();
      var query =
          em.createNativeQuery(
                  buildClaimSql(
                      "job_id, status, job_type, priority, scheduled_time, "
                          + "version, timeout_sec, picked_by, picked_at, business_key, "
                          + "attempts, max_retries",
                      EXECUTABLE_JOB_TYPE_FILTER,
                      "scheduled_time",
                      boostInterval))
              .setParameter("lim", limit);
      if (boostInterval > 0) {
        query.setParameter("boost", boostInterval);
      }

      @SuppressWarnings("unchecked")
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
      // Hydrate the full composite view for each claimed id (callers may need cold metadata).
      return findByIds(claimedIds);
    } catch (RuntimeException e) {
      throw translateTransientStoreException("claim jobs", e);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<JobClaimDto> claimNextBatchOptimized(
      JobExecutionType jobType, int limit, String nodeId) {
    if (limit <= 0 || !isPollerExecutable(jobType)) {
      return List.of();
    }

    try {
      int boostInterval = PriorityBoostConfig.getPriorityBoostIntervalMinutes();
      var query =
          em.createNativeQuery(
                  buildClaimSql(
                      """
                      job_id, status, job_type, priority, scheduled_time,
                      version, timeout_sec, picked_by, picked_at, business_key,
                      attempts, max_retries
                      """,
                      "job_type = :jobType",
                      "scheduled_time",
                      boostInterval))
              .setParameter("jobType", jobType.name())
              .setParameter("lim", limit);
      if (boostInterval > 0) {
        query.setParameter("boost", boostInterval);
      }

      @SuppressWarnings("unchecked")
      List<Object[]> rows =
          timedStoreOperation(
              "claim_lookup", query::getResultList, result -> result.isEmpty() ? "empty" : "hit");

      if (rows.isEmpty()) {
        return List.of();
      }

      return claimOptimizedRows(rows, nodeId);
    } catch (RuntimeException e) {
      throw translateTransientStoreException("optimized claim", e);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<JobEntity> claimDueRecurring(int limit, String nodeId) {
    // CP1 shim: recurring masters live on cold with rec_status='P'. No state flip occurs at
    // claim — the caller (RecurringJobExecutor) holds the FOR UPDATE SKIP LOCKED row lock for
    // the lifetime of its @Transactional process() and writes only next_fire afterwards.
    try {
      List<Object[]> rows =
          em.createNativeQuery(
                  "SELECT job_id, next_fire, priority, business_key "
                      + "FROM scheduler_job "
                      + "WHERE job_type = 'RECURRING' "
                      + "  AND rec_status = 'P' "
                      + "  AND next_fire <= NOW(3) "
                      + "ORDER BY priority DESC, next_fire ASC, job_id ASC "
                      + "LIMIT :lim "
                      + "FOR UPDATE SKIP LOCKED")
              .setParameter("lim", limit)
              .getResultList();
      if (rows.isEmpty()) {
        return List.of();
      }
      List<Long> ids = new ArrayList<>(rows.size());
      for (Object[] row : rows) {
        ids.add(((Number) row[0]).longValue());
      }
      // Recurring masters need full hydration for the executor to spawn children. findByIds
      // returns by primary-key order; preserve the priority-DESC + next_fire-ASC + job_id-ASC
      // ordering from the claim SELECT so callers (and JobPriorityIT) see consistent ordering.
      return reorderById(findByIds(ids), ids);
    } catch (RuntimeException e) {
      throw translateTransientStoreException("claim recurring jobs", e);
    }
  }

  /** Reorders a list of JobEntity to match the order of the given id list. */
  private static List<JobEntity> reorderById(List<JobEntity> jobs, List<Long> orderedIds) {
    Map<Long, JobEntity> byId = new java.util.HashMap<>(jobs.size());
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

  @Override
  public void updateJobStatus(long id, JobStatus status, String errorMessage) {
    // Dispatch on requested target. Live → hot UPDATE; terminal → terminal pathway. The legacy
    // contract was a single setter; post-split it must split by target kind.
    timedStoreOperation(
        "update_status",
        () -> {
          if (isLiveStatus(status)) {
            return em.createNativeQuery(
                    "UPDATE scheduler_job_queue SET status = :status, last_error = :err, "
                        + "updated_at = NOW(3) WHERE job_id = :id")
                .setParameter("status", status.name())
                .setParameter("err", errorMessage)
                .setParameter("id", id)
                .executeUpdate();
          }
          if (status == JobStatus.CANCELED) {
            return cancelJob(id) ? 1 : 0;
          }
          if (status == JobStatus.FAILED) {
            // Caller didn't supply attempts; we can't reconstruct total_attempts here, so use 0.
            // Live callers should prefer markJobFailedTerminal directly.
            return markJobFailedTerminal(id, errorMessage, 0) ? 1 : 0;
          }
          if (status == JobStatus.SUCCEEDED) {
            return markJobSucceededMinimal(id, null, null, null, null) ? 1 : 0;
          }
          throw new IllegalArgumentException("Unsupported status target: " + status);
        },
        updated -> updated > 0 ? "updated" : "miss");
  }

  @Override
  public boolean compareAndSwapStatus(
      long id, JobStatus expected, JobStatus newStatus, String error) {
    // Dispatch on (expected, newStatus). All callers in RI today pass live `expected`. When
    // newStatus is terminal (CANCELED is the common case in DefaultJobSchedulerService.cancelJob),
    // route to the terminal pathway gated by the expected live status.
    return timedStoreOperation(
        "compare_and_swap_status",
        () -> {
          try {
            if (!isLiveStatus(expected)) {
              throw new IllegalArgumentException(
                  "compareAndSwapStatus expected must be a live status; got " + expected);
            }
            if (isLiveStatus(newStatus)) {
              return em.createNativeQuery(
                          "UPDATE scheduler_job_queue SET status = :newS, last_error = :err, "
                              + "updated_at = NOW(3) WHERE job_id = :id AND status = :exp")
                      .setParameter("newS", newStatus.name())
                      .setParameter("err", error)
                      .setParameter("id", id)
                      .setParameter("exp", expected.name())
                      .executeUpdate()
                  > 0;
            }
            if (newStatus == JobStatus.CANCELED) {
              // Gate: only proceed if a hot row in the expected status exists.
              int gateMatched =
                  em.createNativeQuery(
                                  "SELECT COUNT(*) FROM scheduler_job_queue "
                                      + "WHERE job_id = :id AND status = :exp")
                              .setParameter("id", id)
                              .setParameter("exp", expected.name())
                              .getSingleResult()
                          instanceof Number n
                      ? n.intValue()
                      : 0;
              return gateMatched > 0 && cancelJob(id);
            }
            if (newStatus == JobStatus.FAILED) {
              // Gate on RUNNING; FAILED transition only meaningful from RUNNING post-split.
              if (expected != JobStatus.RUNNING) {
                return false;
              }
              return markJobFailedTerminal(id, error, 0);
            }
            throw new IllegalArgumentException("Unsupported CAS target newStatus: " + newStatus);
          } catch (RuntimeException e) {
            throw translateTransientStoreException("compare-and-swap status", e);
          }
        },
        updated -> updated ? "updated" : "miss");
  }

  private static boolean isLiveStatus(JobStatus s) {
    return s == JobStatus.PENDING || s == JobStatus.RUNNING || s == JobStatus.PAUSED;
  }

  @Override
  public int incrementRetryAttempt(long id) {
    int updated =
        timedStoreOperation(
            "increment_retry_attempt",
            () ->
                em.createNativeQuery(
                        "UPDATE scheduler_job_queue SET attempts = attempts + 1, "
                            + "updated_at = NOW(3) "
                            + "WHERE job_id = :id AND status = 'RUNNING'")
                    .setParameter("id", id)
                    .executeUpdate(),
            count -> count > 0 ? "updated" : "miss");
    if (updated == 0) {
      return -1;
    }
    Object result =
        em.createNativeQuery("SELECT attempts FROM scheduler_job_queue WHERE job_id = :id")
            .setParameter("id", id)
            .getSingleResult();
    return ((Number) result).intValue();
  }

  @Override
  public boolean tryPickUpJob(long id, String nodeId) {
    return timedStoreOperation(
            "pickup_job",
            () ->
                em.createNativeQuery(
                        "UPDATE scheduler_job_queue SET status = 'RUNNING', picked_by = :node, "
                            + "picked_at = NOW(3), updated_at = NOW(3) "
                            + "WHERE job_id = :id AND status = 'PENDING'")
                    .setParameter("node", nodeId)
                    .setParameter("id", id)
                    .executeUpdate(),
            updated -> updated > 0 ? "updated" : "miss")
        > 0;
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
    return timedStoreOperation(
        "mark_succeeded",
        () -> {
          try {
            return doMarkTerminalSuccessWithResult(
                id, resultJson, resultType, start, end, durationMs, queueWaitMs);
          } catch (RuntimeException e) {
            throw translateTransientStoreException("mark job succeeded", e);
          }
        },
        updated -> updated ? "updated" : "miss");
  }

  @Override
  public boolean markJobSucceededMinimal(
      long id, Instant start, Instant end, Long durationMs, Long queueWaitMs) {
    return timedStoreOperation(
        "mark_succeeded_minimal",
        () -> {
          try {
            return doMarkTerminalSuccessMinimal(id, start, end, durationMs, queueWaitMs);
          } catch (RuntimeException e) {
            throw translateTransientStoreException("mark job succeeded minimally", e);
          }
        },
        updated -> updated ? "updated" : "miss");
  }

  /**
   * Terminal-success transition with a persisted result payload. Reads attempts directly from the
   * hot row via UPDATE ... JOIN, then drops the hot row and any business-key reservation in a
   * single multi-table DELETE.
   */
  private boolean doMarkTerminalSuccessWithResult(
      long id,
      String resultJson,
      String resultType,
      Instant start,
      Instant end,
      Long durationMs,
      Long queueWaitMs) {
    int coldUpdated =
        em.createNativeQuery(
                "UPDATE scheduler_job c "
                    + "JOIN scheduler_job_queue q ON q.job_id = c.job_id "
                    + "SET c.terminal_status = 'SUCCEEDED', "
                    + "c.job_result = CAST(:result AS JSON), c.result_type = :rtype, "
                    + "c.execution_start_time = :start, c.execution_end_time = :end, "
                    + "c.execution_duration_ms = :dur, c.queue_wait_ms = :qwait, "
                    + "c.total_attempts = q.attempts, c.terminated_at = NOW(3) "
                    + "WHERE c.job_id = :id AND c.terminal_status IS NULL "
                    + "AND q.status = 'RUNNING'")
            .setParameter("result", resultJson)
            .setParameter("rtype", resultType)
            .setParameter("start", start != null ? Timestamp.from(start) : null)
            .setParameter("end", end != null ? Timestamp.from(end) : null)
            .setParameter("dur", durationMs)
            .setParameter("qwait", queueWaitMs)
            .setParameter("id", id)
            .executeUpdate();
    if (coldUpdated == 0) {
      return false;
    }
    deleteHotRowAndReservationAfterSuccess(id);
    return true;
  }

  /**
   * Minimal terminal-success transition for empty/noop results. Uses the same hot-row JOIN to
   * capture attempts, but skips JSON/result columns entirely.
   */
  private boolean doMarkTerminalSuccessMinimal(
      long id, Instant start, Instant end, Long durationMs, Long queueWaitMs) {
    int coldUpdated =
        em.createNativeQuery(
                "UPDATE scheduler_job c "
                    + "JOIN scheduler_job_queue q ON q.job_id = c.job_id "
                    + "SET c.terminal_status = 'SUCCEEDED', "
                    + "c.execution_start_time = :start, c.execution_end_time = :end, "
                    + "c.execution_duration_ms = :dur, c.queue_wait_ms = :qwait, "
                    + "c.total_attempts = q.attempts, c.terminated_at = NOW(3) "
                    + "WHERE c.job_id = :id AND c.terminal_status IS NULL "
                    + "AND q.status = 'RUNNING'")
            .setParameter("start", start != null ? Timestamp.from(start) : null)
            .setParameter("end", end != null ? Timestamp.from(end) : null)
            .setParameter("dur", durationMs)
            .setParameter("qwait", queueWaitMs)
            .setParameter("id", id)
            .executeUpdate();
    if (coldUpdated == 0) {
      return false;
    }
    deleteHotRowAndReservationAfterSuccess(id);
    return true;
  }

  /**
   * Drops the RUNNING hot row and any business-key reservation after a successful cold-row terminal
   * transition. The preceding UPDATE ... JOIN ensures the hot row was the gating RUNNING row; if we
   * fail to remove it here, the transaction should roll back.
   */
  private void deleteHotRowAndReservationAfterSuccess(long id) {
    int deleted =
        em.createNativeQuery(
                "DELETE q, br FROM scheduler_job_queue q "
                    + "LEFT JOIN scheduler_business_key_reservation br "
                    + "ON br.owner_job_id = q.job_id "
                    + "WHERE q.job_id = :id AND q.status = 'RUNNING'")
            .setParameter("id", id)
            .executeUpdate();
    if (deleted == 0) {
      throw new IllegalStateException(
          "terminal success updated cold row but failed to remove hot row for job " + id);
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
    boolean succeeded =
        markJobSucceeded(jobId, resultJson, resultType, start, end, durationMs, queueWaitMs);
    if (succeeded) {
      incrementCompletedAtomic(batchId);
    }
    return succeeded;
  }

  @Override
  public boolean scheduleJobRetry(long id, String error, Instant newScheduledTime, int attempts) {
    // Hot-only: post-split FAILED has no hot row, so the WHERE clause drops 'FAILED'.
    return timedStoreOperation(
            "schedule_retry",
            () ->
                em.createNativeQuery(
                        "UPDATE scheduler_job_queue SET status = 'PENDING', last_error = :err, "
                            + "scheduled_time = :st, attempts = :att, picked_by = NULL, "
                            + "picked_at = NULL, updated_at = NOW(3) "
                            + "WHERE job_id = :id AND status = 'RUNNING'")
                    .setParameter("err", error)
                    .setParameter("st", Timestamp.from(newScheduledTime))
                    .setParameter("att", attempts)
                    .setParameter("id", id)
                    .executeUpdate(),
            updated -> updated > 0 ? "updated" : "miss")
        > 0;
  }

  @Override
  public boolean markJobFailedTerminal(long id, String terminalError, int totalAttempts) {
    // Real terminal: gate-DELETE the RUNNING hot row, UPDATE cold to FAILED with totals, drop
    // the bkres reservation. Single tx via @Transactional class annotation.
    int hotDeleted =
        em.createNativeQuery(
                "DELETE FROM scheduler_job_queue WHERE job_id = :id AND status = 'RUNNING'")
            .setParameter("id", id)
            .executeUpdate();
    if (hotDeleted == 0) {
      return false;
    }
    em.createNativeQuery(
            "UPDATE scheduler_job SET terminal_status = 'FAILED', terminal_error = :err, "
                + "total_attempts = :att, terminated_at = NOW(3), execution_end_time = NOW(3) "
                + "WHERE job_id = :id AND terminal_status IS NULL")
        .setParameter("err", terminalError)
        .setParameter("att", totalAttempts)
        .setParameter("id", id)
        .executeUpdate();
    deleteReservationByOwner(id);
    return true;
  }

  @Override
  public boolean cancelJob(long id) {
    // Dispatch on cold.job_type so callers don't need to know whether the target is recurring.
    @SuppressWarnings("unchecked")
    List<Object[]> rows =
        em.createNativeQuery(
                "SELECT job_type, terminal_status, rec_status FROM scheduler_job WHERE job_id = :id")
            .setParameter("id", id)
            .getResultList();
    if (rows.isEmpty()) {
      return false;
    }
    Object[] row = rows.get(0);
    String jobType = (String) row[0];
    String existingTerminal = (String) row[1];
    if (existingTerminal != null) {
      return false; // already terminal — idempotent no-op
    }
    if ("RECURRING".equals(jobType)) {
      // Recurring master cancel: clear rec_status, set terminal_status, drop bkres.
      // Hot row never exists for recurring masters post-split.
      int updated =
          em.createNativeQuery(
                  "UPDATE scheduler_job SET rec_status = NULL, terminal_status = 'CANCELED', "
                      + "terminated_at = NOW(3) "
                      + "WHERE job_id = :id AND job_type = 'RECURRING' "
                      + "AND rec_status IS NOT NULL AND terminal_status IS NULL")
              .setParameter("id", id)
              .executeUpdate();
      if (updated == 0) {
        return false;
      }
      deleteReservationByOwner(id);
      return true;
    }
    // Executable cancel: DELETE the live hot row regardless of live status, UPDATE cold to
    // CANCELED, drop bkres. If the hot row is gone (race with terminal), still allow the cold
    // UPDATE to fire — terminal_status IS NULL guard makes it a no-op when raced.
    em.createNativeQuery(
            "DELETE FROM scheduler_job_queue WHERE job_id = :id "
                + "AND status IN ('PENDING','RUNNING','PAUSED')")
        .setParameter("id", id)
        .executeUpdate();
    int coldUpdated =
        em.createNativeQuery(
                "UPDATE scheduler_job SET terminal_status = 'CANCELED', terminated_at = NOW(3) "
                    + "WHERE job_id = :id AND terminal_status IS NULL")
            .setParameter("id", id)
            .executeUpdate();
    deleteReservationByOwner(id);
    return coldUpdated > 0;
  }

  @Override
  public boolean resetRunningJob(long id, String nodeId) {
    return timedStoreOperation(
            "reset_running_job",
            () ->
                em.createNativeQuery(
                        "UPDATE scheduler_job_queue SET status = 'PENDING', picked_by = NULL, "
                            + "picked_at = NULL, updated_at = NOW(3) "
                            + "WHERE job_id = :id AND status = 'RUNNING' AND picked_by = :node")
                    .setParameter("id", id)
                    .setParameter("node", nodeId)
                    .executeUpdate(),
            updated -> updated > 0 ? "updated" : "miss")
        > 0;
  }

  @Override
  public int resetRunningJobs(String nodeId) {
    return timedStoreOperation(
        "reset_running_jobs",
        () ->
            em.createNativeQuery(
                    "UPDATE scheduler_job_queue SET status = 'PENDING', picked_by = NULL, "
                        + "picked_at = NULL, updated_at = NOW(3) "
                        + "WHERE status = 'RUNNING' AND picked_by = :node")
                .setParameter("node", nodeId)
                .executeUpdate(),
        updated -> updated > 0 ? "updated" : "miss");
  }

  @Override
  public int cancelRecurringJobsByTag(String tag) {
    @SuppressWarnings("unchecked")
    List<Number> ids =
        em.createNativeQuery(
                "SELECT j.job_id FROM scheduler_job j "
                    + "JOIN scheduler_job_tag t ON j.job_id = t.job_id "
                    + "WHERE t.tag = :tag AND j.job_type = 'RECURRING' "
                    + "AND j.rec_status IS NOT NULL AND j.terminal_status IS NULL")
            .setParameter("tag", tag)
            .getResultList();
    return cancelRecurringByIds(ids);
  }

  @Override
  public int cancelRecurringJobByBusinessKey(String businessKey) {
    @SuppressWarnings("unchecked")
    List<Number> ids =
        em.createNativeQuery(
                "SELECT job_id FROM scheduler_job "
                    + "WHERE business_key = :bk AND job_type = 'RECURRING' "
                    + "AND rec_status IS NOT NULL AND terminal_status IS NULL")
            .setParameter("bk", businessKey)
            .getResultList();
    return cancelRecurringByIds(ids);
  }

  @Override
  public int cancelOrphanedRecurringAnnotationJobs(
      Set<String> registeredIds, Instant nodeStartTime) {
    if (registeredIds.isEmpty()) {
      return 0;
    }
    @SuppressWarnings("unchecked")
    List<Number> ids =
        em.createNativeQuery(
                "SELECT job_id FROM scheduler_job WHERE job_type = 'RECURRING' "
                    + "AND rec_status IS NOT NULL AND terminal_status IS NULL "
                    + "AND created_at < :nodeStart AND business_key IS NOT NULL "
                    + "AND business_key NOT IN (:ids)")
            .setParameter("nodeStart", Timestamp.from(nodeStartTime))
            .setParameter("ids", registeredIds)
            .getResultList();
    return cancelRecurringByIds(ids);
  }

  /** Bulk recurring-cancel helper: cold UPDATE per id + bkres delete per id. */
  private int cancelRecurringByIds(List<Number> idRows) {
    if (idRows.isEmpty()) {
      return 0;
    }
    int total = 0;
    for (Number n : idRows) {
      long id = n.longValue();
      int updated =
          em.createNativeQuery(
                  "UPDATE scheduler_job SET rec_status = NULL, terminal_status = 'CANCELED', "
                      + "terminated_at = NOW(3) "
                      + "WHERE job_id = :id AND job_type = 'RECURRING' "
                      + "AND rec_status IS NOT NULL AND terminal_status IS NULL")
              .setParameter("id", id)
              .executeUpdate();
      if (updated > 0) {
        deleteReservationByOwner(id);
        total += updated;
      }
    }
    return total;
  }

  @Override
  public boolean resetFailedToPending(long id) {
    // (1) Lock cold and capture immutable shape fields needed for hot INSERT.
    @SuppressWarnings("unchecked")
    List<Object[]> rows =
        em.createNativeQuery(
                "SELECT terminal_status, job_type, priority, business_key, timeout_sec, max_retries "
                    + "FROM scheduler_job WHERE job_id = :id FOR UPDATE")
            .setParameter("id", id)
            .getResultList();
    if (rows.isEmpty()) {
      return false;
    }
    Object[] row = rows.get(0);
    String terminal = (String) row[0];
    if (!"FAILED".equals(terminal)) {
      return false;
    }
    String jobType = (String) row[1];
    int priority = ((Number) row[2]).intValue();
    String businessKey = (String) row[3];
    int timeoutSec = ((Number) row[4]).intValue();
    int maxRetries = ((Number) row[5]).intValue();

    // (2) Clear cold terminal fields.
    em.createNativeQuery(
            "UPDATE scheduler_job SET terminal_status = NULL, terminal_error = NULL, "
                + "job_result = NULL, result_type = NULL, "
                + "execution_start_time = NULL, execution_end_time = NULL, "
                + "execution_duration_ms = NULL, queue_wait_ms = NULL, "
                + "total_attempts = NULL, terminated_at = NULL "
                + "WHERE job_id = :id AND terminal_status = 'FAILED'")
        .setParameter("id", id)
        .executeUpdate();

    // (3) Re-insert the hot row.
    em.createNativeQuery(
            "INSERT INTO scheduler_job_queue "
                + "(job_id, status, job_type, priority, scheduled_time, business_key, "
                + "timeout_sec, max_retries, attempts, version, updated_at) "
                + "VALUES (:id, 'PENDING', :jt, :pr, NOW(3), :bk, :to, :mr, 0, 0, NOW(3))")
        .setParameter("id", id)
        .setParameter("jt", jobType)
        .setParameter("pr", priority)
        .setParameter("bk", businessKey)
        .setParameter("to", timeoutSec)
        .setParameter("mr", maxRetries)
        .executeUpdate();

    // (4) Re-insert bkres if needed; duplicate-bkey here means a live owner exists → rollback.
    if (businessKey != null) {
      try {
        insertReservation(businessKey, id, OWNER_TABLE_QUEUE);
      } catch (RuntimeException e) {
        if (CONSTRAINT_DETECTOR.isDuplicateBusinessKey(e)) {
          throw new RatchetTransientStoreException(
              "Cannot resurrect job " + id + ": business key already held", e);
        }
        throw e;
      }
    }
    return true;
  }

  @Override
  public boolean transitionToPaused(long id, JobStatus expected) {
    if (expected == JobStatus.PAUSED) {
      throw new IllegalArgumentException("transitionToPaused expects expected != PAUSED");
    }
    if (!isLiveStatus(expected)) {
      // Post hot/cold-split: terminal statuses (FAILED/SUCCEEDED/CANCELED) have no hot row, so
      // pause-of-terminal can't write the paused_from_status field anywhere. RI's pauseJob
      // chains transitionToPaused(PENDING) → transitionToPaused(FAILED); the FAILED arm returns
      // false here, indicating "pause-of-FAILED is not supported post-split" without throwing.
      log.debugf(
          "transitionToPaused(%d, %s) is a no-op post hot/cold-split — terminal jobs cannot be paused",
          id, expected);
      return false;
    }
    int updated =
        em.createNativeQuery(
                "UPDATE scheduler_job_queue SET status = 'PAUSED', "
                    + "paused_from_status = :exp, updated_at = NOW(3) "
                    + "WHERE job_id = :id AND status = :exp")
            .setParameter("exp", expected.name())
            .setParameter("id", id)
            .executeUpdate();
    return updated > 0;
  }

  @Override
  public boolean transitionFromPaused(long id, JobStatus target) {
    if (!isLiveStatus(target) || target == JobStatus.PAUSED) {
      throw new IllegalArgumentException(
          "transitionFromPaused expects a non-PAUSED live status; got " + target);
    }
    int updated =
        em.createNativeQuery(
                "UPDATE scheduler_job_queue SET status = :target, "
                    + "paused_from_status = NULL, updated_at = NOW(3) "
                    + "WHERE job_id = :id AND status = 'PAUSED'")
            .setParameter("target", target.name())
            .setParameter("id", id)
            .executeUpdate();
    return updated > 0;
  }

  @Override
  public boolean pauseRecurring(long id) {
    int updated =
        em.createNativeQuery(
                "UPDATE scheduler_job SET rec_status = 'A' "
                    + "WHERE job_id = :id AND job_type = 'RECURRING' "
                    + "AND rec_status = 'P' AND terminal_status IS NULL")
            .setParameter("id", id)
            .executeUpdate();
    return updated > 0;
  }

  @Override
  public boolean resumeRecurring(long id) {
    int updated =
        em.createNativeQuery(
                "UPDATE scheduler_job SET rec_status = 'P' "
                    + "WHERE job_id = :id AND job_type = 'RECURRING' "
                    + "AND rec_status = 'A' AND terminal_status IS NULL")
            .setParameter("id", id)
            .executeUpdate();
    return updated > 0;
  }

  @Override
  public JobStatus transitionFromPausedAtomic(long id) {
    List<?> results =
        em.createNativeQuery(
                "SELECT paused_from_status FROM scheduler_job_queue "
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
                "UPDATE scheduler_job_queue SET status = :target, "
                    + "paused_from_status = NULL, updated_at = NOW(3) "
                    + "WHERE job_id = :id AND status = 'PAUSED'")
            .setParameter("target", target.name())
            .setParameter("id", id)
            .executeUpdate();
    return updated > 0 ? target : null;
  }

  @Override
  public void bulkInsert(List<JobEntity> jobs) {
    if (jobs.isEmpty()) {
      return;
    }
    Connection conn = em.unwrap(Connection.class);
    Instant now = Instant.now();
    Timestamp nowTs = Timestamp.from(now);

    for (JobEntity job : jobs) {
      assignTsidIfMissing(job);
    }

    List<JobEntity> needsHot = new ArrayList<>(jobs.size());
    List<JobEntity> needsBkres = new ArrayList<>(jobs.size());
    for (JobEntity job : jobs) {
      if (job.getJobType() != JobExecutionType.RECURRING) {
        needsHot.add(job);
      }
      if (job.getBusinessKey() != null) {
        needsBkres.add(job);
      }
    }

    try {
      // 1. Cold batch — every job.
      try (PreparedStatement ps = conn.prepareStatement(COLD_INSERT_SQL)) {
        for (JobEntity job : jobs) {
          bindColdInsert(ps, job, nowTs);
          ps.addBatch();
        }
        ps.executeBatch();
      }

      // 2. Hot batch — executable jobs only.
      if (!needsHot.isEmpty()) {
        try (PreparedStatement ps = conn.prepareStatement(HOT_INSERT_SQL)) {
          for (JobEntity job : needsHot) {
            bindHotInsert(ps, job, nowTs);
            ps.addBatch();
          }
          ps.executeBatch();
        }
      }

      // 3. bkres batch — jobs that carry a business_key (executable or recurring).
      if (!needsBkres.isEmpty()) {
        try (PreparedStatement ps = conn.prepareStatement(BKRES_INSERT_SQL)) {
          for (JobEntity job : needsBkres) {
            bindBkresInsert(ps, job, nowTs);
            ps.addBatch();
          }
          ps.executeBatch();
        }
      }
    } catch (SQLException e) {
      throw new RuntimeException("Bulk insert failed", e);
    }
    em.clear();
  }

  // ---- bulkInsert bind helpers (PreparedStatement positional) ----

  private void bindColdInsert(PreparedStatement ps, JobEntity job, Timestamp nowTs)
      throws SQLException {
    int i = 1;
    ps.setLong(i++, job.getId());
    ps.setString(i++, job.getJobType().name());
    ps.setInt(i++, job.getPriority().ordinal());
    ps.setInt(i++, job.getMaxRetries());
    ps.setString(i++, job.getBackoffPolicy().name());
    ps.setInt(i++, job.getBackoffParamMs());
    ps.setInt(i++, job.getTimeoutSec());
    ps.setString(i++, job.getCronExpr());
    ps.setString(i++, job.getZoneId());
    if (job.getNextFire() != null) {
      ps.setTimestamp(i++, Timestamp.from(job.getNextFire()));
    } else {
      ps.setNull(i++, Types.TIMESTAMP);
    }
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
    ps.setTimestamp(i++, nowTs);
    ps.setString(i++, job.getCreatedBy());
    if (job.getJobType() == JobExecutionType.RECURRING) {
      JobStatus s = job.getStatus() != null ? job.getStatus() : JobStatus.PENDING;
      String rec = recStatusForLiveStatus(s);
      ps.setString(i, rec != null ? rec : "P");
    } else {
      ps.setNull(i, Types.CHAR);
    }
  }

  private void bindHotInsert(PreparedStatement ps, JobEntity job, Timestamp nowTs)
      throws SQLException {
    int i = 1;
    ps.setLong(i++, job.getId());
    JobStatus s = job.getStatus() != null ? job.getStatus() : JobStatus.PENDING;
    ps.setString(i++, s.name());
    ps.setString(i++, job.getJobType().name());
    ps.setInt(i++, job.getPriority().ordinal());
    Instant scheduled = job.getScheduledTime();
    ps.setTimestamp(i++, scheduled != null ? Timestamp.from(scheduled) : nowTs);
    ps.setString(i++, job.getBusinessKey());
    ps.setInt(i++, job.getTimeoutSec());
    ps.setInt(i++, job.getMaxRetries());
    ps.setInt(i++, job.getAttempts());
    ps.setString(i++, job.getPickedBy());
    if (job.getPickedAt() != null) {
      ps.setTimestamp(i++, Timestamp.from(job.getPickedAt()));
    } else {
      ps.setNull(i++, Types.TIMESTAMP);
    }
    ps.setString(i++, job.getPausedFromStatus() != null ? job.getPausedFromStatus().name() : null);
    ps.setString(i++, job.getLastError());
    ps.setInt(i++, job.getVersion() != null ? job.getVersion() : 0);
    ps.setTimestamp(i, nowTs);
  }

  private void bindBkresInsert(PreparedStatement ps, JobEntity job, Timestamp nowTs)
      throws SQLException {
    ps.setString(1, job.getBusinessKey());
    ps.setLong(2, job.getId());
    ps.setString(
        3,
        job.getJobType() == JobExecutionType.RECURRING ? OWNER_TABLE_RECURRING : OWNER_TABLE_QUEUE);
    ps.setTimestamp(4, nowTs);
  }

  @Override
  public int deleteJobsByIds(List<Long> ids) {
    if (ids.isEmpty()) {
      return 0;
    }
    // bkres has no FK; cold DELETE cascades to hot via FK ON DELETE CASCADE.
    em.createNativeQuery(
            "DELETE FROM scheduler_business_key_reservation WHERE owner_job_id IN (:ids)")
        .setParameter("ids", ids)
        .executeUpdate();
    return em.createNativeQuery("DELETE FROM scheduler_job WHERE job_id IN (:ids)")
        .setParameter("ids", ids)
        .executeUpdate();
  }

  @Override
  public int deleteDlqOlderThan(Instant cutoff) {
    // Post-split: DLQ-eligible rows live in cold with terminal_status='FAILED' and total_attempts
    // satisfied. terminated_at is the canonical age column. bkres rows for terminal jobs are
    // already dropped at terminal transition; the explicit cleanup below is belt-and-suspenders.
    @SuppressWarnings("unchecked")
    List<Number> idRows =
        em.createNativeQuery(
                "SELECT job_id FROM scheduler_job "
                    + "WHERE terminal_status = 'FAILED' AND total_attempts >= max_retries "
                    + "AND terminated_at < :cutoff")
            .setParameter("cutoff", Timestamp.from(cutoff))
            .getResultList();
    if (idRows.isEmpty()) {
      return 0;
    }
    List<Long> ids = new ArrayList<>(idRows.size());
    for (Number n : idRows) {
      ids.add(n.longValue());
    }
    em.createNativeQuery(
            "DELETE FROM scheduler_business_key_reservation WHERE owner_job_id IN (:ids)")
        .setParameter("ids", ids)
        .executeUpdate();
    return em.createNativeQuery("DELETE FROM scheduler_job WHERE job_id IN (:ids)")
        .setParameter("ids", ids)
        .executeUpdate();
  }

  @Override
  public int resetOrphanJobs(Duration grace) {
    // Use SECOND granularity — toMinutes() truncates sub-minute values
    return em.createNativeQuery(
            "UPDATE scheduler_job_queue SET status = 'PENDING', picked_by = NULL, "
                + "picked_at = NULL, updated_at = NOW(3) "
                + "WHERE status = 'RUNNING' AND picked_by NOT IN ("
                + "  SELECT node_id FROM scheduler_node "
                + "  WHERE TIMESTAMPDIFF(SECOND, heartbeat_ts, NOW(3)) <= :graceSec"
                + ") AND TIMESTAMPDIFF(SECOND, picked_at, NOW(3)) >= :graceSec")
        .setParameter("graceSec", grace.toSeconds())
        .executeUpdate();
  }

  @Override
  public int resetOrphanJobsForNode(String nodeId) {
    return em.createNativeQuery(
            "UPDATE scheduler_job_queue SET status = 'PENDING', picked_by = NULL, "
                + "picked_at = NULL, updated_at = NOW(3) "
                + "WHERE status = 'RUNNING' AND picked_by = :node")
        .setParameter("node", nodeId)
        .executeUpdate();
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
  @SuppressWarnings("unchecked")
  public List<JobEntity> findJobsForArchiving(Instant olderThan, int limit) {
    // Post hot/cold-split: terminal-state rows live in cold with terminal_status set and
    // terminated_at as the canonical age column. Native SQL via HYDRATION_SELECT bypasses the
    // legacy JPQL that referenced removed cold columns (status, updated_at).
    List<Object[]> rows =
        em.createNativeQuery(
                "SELECT "
                    + HYDRATION_SELECT
                    + " FROM scheduler_job c "
                    + "LEFT JOIN scheduler_job_queue q ON q.job_id = c.job_id "
                    + "WHERE c.terminal_status IS NOT NULL AND c.terminated_at < :cutoff "
                    + "ORDER BY c.terminated_at ASC "
                    + "LIMIT :lim")
            .setParameter("cutoff", Timestamp.from(olderThan))
            .setParameter("lim", limit)
            .getResultList();
    List<JobEntity> jobs = new ArrayList<>(rows.size());
    for (Object[] row : rows) {
      jobs.add(hydrateJobEntity(row));
    }
    hydrateTagsBatch(jobs);
    return jobs;
  }

  @Override
  public long countJobsForArchiving(Instant olderThan) {
    Object result =
        em.createNativeQuery(
                "SELECT COUNT(*) FROM scheduler_job "
                    + "WHERE terminal_status IS NOT NULL AND terminated_at < :cutoff")
            .setParameter("cutoff", Timestamp.from(olderThan))
            .getSingleResult();
    return ((Number) result).longValue();
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

  @Override
  @SuppressWarnings("unchecked")
  public Map<JobStatus, Long> countJobsByStatusForTag(String tag) {
    // UNION live (hot.status) + terminal (cold.terminal_status). Hot rows are deleted at
    // terminal so the two sets are disjoint per job_id.
    List<Object[]> rows =
        em.createNativeQuery(
                "SELECT s, SUM(c) FROM ("
                    + "  SELECT q.status AS s, COUNT(*) AS c FROM scheduler_job_queue q "
                    + "    JOIN scheduler_job_tag t ON t.job_id = q.job_id "
                    + "    WHERE t.tag = :tag GROUP BY q.status "
                    + "  UNION ALL "
                    + "  SELECT c.terminal_status AS s, COUNT(*) AS c FROM scheduler_job c "
                    + "    JOIN scheduler_job_tag t ON t.job_id = c.job_id "
                    + "    WHERE t.tag = :tag AND c.terminal_status IS NOT NULL "
                    + "    GROUP BY c.terminal_status"
                    + ") u GROUP BY s")
            .setParameter("tag", tag)
            .getResultList();
    Map<JobStatus, Long> counts = new EnumMap<>(JobStatus.class);
    for (Object[] row : rows) {
      counts.put(JobStatus.valueOf((String) row[0]), ((Number) row[1]).longValue());
    }
    return counts;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Map<String, Long> countJobsByParamForTag(String tag, String paramKey) {
    String jsonPath = toJsonFieldPath(paramKey);
    List<Object[]> rows =
        em.createNativeQuery(
                "SELECT JSON_UNQUOTE(JSON_EXTRACT(j.params, :jsonPath)) AS param_value, "
                    + "COUNT(*) FROM scheduler_job j "
                    + "JOIN scheduler_job_tag t ON j.job_id = t.job_id "
                    + "WHERE t.tag = :tag "
                    + "AND JSON_EXTRACT(j.params, :jsonPath) IS NOT NULL "
                    + "GROUP BY param_value ORDER BY param_value")
            .setParameter("tag", tag)
            .setParameter("jsonPath", jsonPath)
            .getResultList();
    return toStringCountMap(rows);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Map<String, Long> countJobsByExecutionNodeForTag(String tag) {
    // Pre-split picked_by survived terminal as "last node that touched the row." Post-split,
    // hot is deleted at terminal so picked_by is gone for terminal jobs. Substitute the latest
    // scheduler_job_execution.node_id for terminal rows; keep hot.picked_by for live rows.
    // Hot/cold are disjoint per job_id, so SUM across the union is correct.
    List<Object[]> rows =
        em.createNativeQuery(
                "SELECT node, SUM(c) FROM ("
                    + "  SELECT q.picked_by AS node, COUNT(*) AS c "
                    + "    FROM scheduler_job_queue q "
                    + "    JOIN scheduler_job_tag t ON t.job_id = q.job_id "
                    + "    WHERE t.tag = :tag AND q.picked_by IS NOT NULL AND q.picked_by <> '' "
                    + "    GROUP BY q.picked_by "
                    + "  UNION ALL "
                    + "  SELECT e.node_id AS node, COUNT(*) AS c "
                    + "    FROM scheduler_job c2 "
                    + "    JOIN scheduler_job_tag t ON t.job_id = c2.job_id "
                    + "    JOIN scheduler_job_execution e ON e.job_id = c2.job_id "
                    + "    WHERE t.tag = :tag AND c2.terminal_status IS NOT NULL "
                    + "      AND e.id = (SELECT MAX(e2.id) FROM scheduler_job_execution e2 "
                    + "                  WHERE e2.job_id = c2.job_id) "
                    + "      AND e.node_id IS NOT NULL AND e.node_id <> '' "
                    + "    GROUP BY e.node_id"
                    + ") u GROUP BY node ORDER BY node")
            .setParameter("tag", tag)
            .getResultList();
    return toStringCountMap(rows);
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

  @Override
  public boolean tryAcquirePermit(String resource, long jobId, String nodeId) {
    // Lock the resource limit row to serialize concurrent permit acquisitions
    @SuppressWarnings("unchecked")
    List<Object[]> permitResults =
        em.createNativeQuery(
                "SELECT max_concurrent, "
                    + "(SELECT COUNT(*) FROM scheduler_resource_permit WHERE resource_name = :res) "
                    + "FROM scheduler_resource_limit WHERE resource_name = :res "
                    + "FOR UPDATE")
            .setParameter("res", resource)
            .getResultList();
    Object[] limits = permitResults.stream().findFirst().orElse(null);

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

  /**
   * Checks the connection isolation level on first use and warns (or fails, depending on the {@code
   * ratchet.isolation-check} system property) if not READ COMMITTED.
   *
   * <p>Tries the MySQL 8.0+ system variable first, falling back to the MySQL 5.7 name if the
   * primary variable is unknown. The {@code @@tx_isolation} variable was deprecated in MySQL 5.7.20
   * and removed in MySQL 8.0; querying it on MySQL 8+ throws "Unknown system variable", which a
   * naive single-query check would treat as a detection failure.
   */
  @PostConstruct
  void checkIsolationLevel() {
    IsolationCheck.verifyReadCommitted(
        em,
        "MySQL",
        List.of("SELECT @@SESSION.transaction_isolation", "SELECT @@SESSION.tx_isolation"),
        "READ-COMMITTED",
        "REPEATABLE READ causes InnoDB gap locks that block concurrent job enqueue during claim"
            + " queries. Set hibernate.connection.isolation=2 in persistence.xml or"
            + " transaction-isolation=TRANSACTION_READ_COMMITTED on the datasource.");
  }

  // ===========================================================================
  // CP1 helpers — TSID assignment, bkres helpers, recurring shim helpers,
  // composite-view hydrator, hot-mutation guard.
  // ===========================================================================

  /** Assigns a TSID if the entity has no id yet. Replaces the JPA @PrePersist listener path. */
  private static void assignTsidIfMissing(JobEntity job) {
    if (job.getId() == null || job.getId() == 0L) {
      job.setId(TsidFactory.next());
    }
  }

  /** Inserts a bkres row in the caller's tx. No-op when businessKey is null. */
  private void insertReservation(String businessKey, long ownerJobId, String ownerTable) {
    if (businessKey == null) {
      return;
    }
    em.createNativeQuery(
            "INSERT INTO scheduler_business_key_reservation "
                + "(business_key, owner_job_id, owner_table, reserved_at) VALUES (?, ?, ?, NOW(3))")
        .setParameter(1, businessKey)
        .setParameter(2, ownerJobId)
        .setParameter(3, ownerTable)
        .executeUpdate();
  }

  /** Drops the bkres row owned by the given job_id (if any). Returns rows deleted. */
  @SuppressWarnings("UnusedReturnValue")
  private int deleteReservationByOwner(long ownerJobId) {
    return em.createNativeQuery(
            "DELETE FROM scheduler_business_key_reservation WHERE owner_job_id = ?")
        .setParameter(1, ownerJobId)
        .executeUpdate();
  }

  /** Encodes a recurring master's logical status into the rec_status shim character. */
  private static String recStatusForLiveStatus(JobStatus s) {
    if (s == JobStatus.PENDING) return "P";
    if (s == JobStatus.PAUSED) return "A";
    return null;
  }

  /** Decodes the rec_status shim character into a logical JobStatus. */
  private static JobStatus recStatusDecode(String c) {
    if ("P".equals(c)) return JobStatus.PENDING;
    if ("A".equals(c)) return JobStatus.PAUSED;
    return null;
  }

  /**
   * Hydrates a JobEntity from a (cold LEFT JOIN hot) projection row using HYDRATION_SELECT
   * positional contract. Status is resolved by 3-source COALESCE: live (q.status) wins, then the
   * rec_status shim, then cold.terminal_status. Tags are not populated here — callers fill them
   * with a follow-up SELECT.
   */
  private JobEntity hydrateJobEntity(Object[] row) {
    if (row == null) {
      return null;
    }
    if (row.length != HYDRATION_COL_COUNT) {
      throw new IllegalStateException(
          "Hydration projection length mismatch: expected "
              + HYDRATION_COL_COUNT
              + " columns, got "
              + row.length);
    }
    JobEntity j = new JobEntity();
    j.setId(((Number) row[IDX_JOB_ID]).longValue());
    j.setJobType(JobExecutionType.valueOf((String) row[IDX_JOB_TYPE]));
    j.setPriority(safeJobPriority(((Number) row[IDX_PRIORITY]).intValue()));
    j.setMaxRetries(((Number) row[IDX_MAX_RETRIES]).intValue());
    j.setBackoffPolicy(
        run.ratchet.api.BackoffPolicy.valueOf((String) row[IDX_BACKOFF_POLICY]));
    j.setBackoffParamMs(((Number) row[IDX_BACKOFF_PARAM_MS]).intValue());
    j.setTimeoutSec(((Number) row[IDX_TIMEOUT_SEC]).intValue());
    j.setCronExpr((String) row[IDX_CRON_EXPR]);
    j.setZoneId((String) row[IDX_ZONE_ID]);
    j.setNextFire(toInstant(row[IDX_NEXT_FIRE]));
    j.setPayload(JOB_PAYLOAD_CONVERTER.convertToEntityAttribute(stringOrNull(row[IDX_PAYLOAD])));
    j.setParams(JSON_MAP_CONVERTER.convertToEntityAttribute(stringOrNull(row[IDX_PARAMS])));
    j.setTargetClass((String) row[IDX_TARGET_CLASS]);
    j.setMethodName((String) row[IDX_METHOD_NAME]);
    j.setIdempotencyKey((String) row[IDX_IDEMPOTENCY_KEY]);
    j.setBusinessKey((String) row[IDX_BUSINESS_KEY]);
    j.setResourceName((String) row[IDX_RESOURCE_NAME]);
    j.setOnSuccessPayload(
        JOB_PAYLOAD_CONVERTER.convertToEntityAttribute(stringOrNull(row[IDX_ON_SUCCESS])));
    j.setOnFailurePayload(
        JOB_PAYLOAD_CONVERTER.convertToEntityAttribute(stringOrNull(row[IDX_ON_FAILURE])));
    j.setDependsOn(longOrNull(row[IDX_DEPENDS_ON]));
    j.setSupersededBy(longOrNull(row[IDX_SUPERSEDED_BY]));
    j.setCreatedAt(toInstant(row[IDX_CREATED_AT]));
    j.setCreatedBy((String) row[IDX_CREATED_BY]);

    String terminalStr = (String) row[IDX_TERMINAL_STATUS];
    JobStatus terminal = terminalStr != null ? JobStatus.valueOf(terminalStr) : null;
    j.setTerminalStatus(terminal);

    j.setExecutionStartTime(toInstant(row[IDX_EXEC_START]));
    j.setExecutionEndTime(toInstant(row[IDX_EXEC_END]));
    j.setExecutionDurationMs(longOrNull(row[IDX_EXEC_DURATION]));
    j.setQueueWaitMs(longOrNull(row[IDX_QUEUE_WAIT]));
    j.setJobResult(stringOrNull(row[IDX_JOB_RESULT]));
    j.setResultType((String) row[IDX_RESULT_TYPE]);

    String recStatus = stringOrNull(row[IDX_REC_STATUS]);

    String liveStr = (String) row[IDX_Q_STATUS];
    JobStatus live = liveStr != null ? JobStatus.valueOf(liveStr) : null;

    JobStatus resolved;
    if (live != null) {
      resolved = live;
    } else if (recStatus != null) {
      resolved = recStatusDecode(recStatus);
    } else if (terminal != null) {
      resolved = terminal;
    } else {
      log.errorf(
          "Job %d has no live, recurring, or terminal status — possible invariant violation",
          j.getId());
      resolved = null;
    }
    j.setStatus(resolved);

    if (live != null) {
      j.setScheduledTime(toInstant(row[IDX_Q_SCHEDULED_TIME]));
      j.setAttempts(((Number) row[IDX_Q_ATTEMPTS]).intValue());
      j.setPickedBy((String) row[IDX_Q_PICKED_BY]);
      j.setPickedAt(toInstant(row[IDX_Q_PICKED_AT]));
      String pausedFrom = (String) row[IDX_Q_PAUSED];
      j.setPausedFromStatus(pausedFrom != null ? JobStatus.valueOf(pausedFrom) : null);
      j.setLastError(stringOrNull(row[IDX_Q_LAST_ERROR]));
      j.setVersion(((Number) row[IDX_Q_VERSION]).intValue());
    } else if (recStatus != null) {
      // Recurring master: scheduled_time is the next fire time on cold.
      j.setScheduledTime(toInstant(row[IDX_NEXT_FIRE]));
      j.setAttempts(0);
      j.setVersion(0);
    } else {
      // Terminal-only row. Hot row is gone; scheduled_time has no live value. Fall back to
      // execution_start_time → created_at so callers like ArchiveHelper that read
      // job.getScheduledTime() into NOT NULL columns get a sensible value rather than null.
      Number ta = (Number) row[IDX_TOTAL_ATTEMPTS];
      j.setAttempts(ta != null ? ta.intValue() : 0);
      j.setLastError(stringOrNull(row[IDX_TERMINAL_ERROR]));
      j.setVersion(0);
      Instant fallbackSched = toInstant(row[IDX_EXEC_START]);
      if (fallbackSched == null) {
        fallbackSched = toInstant(row[IDX_CREATED_AT]);
      }
      j.setScheduledTime(fallbackSched);
    }

    Instant updatedAt = toInstant(row[IDX_TERMINATED_AT]);
    if (updatedAt == null) {
      updatedAt = j.getCreatedAt();
    }
    j.setUpdatedAt(updatedAt);

    return j;
  }

  /** Loads tags for a single job and assigns them to the entity. Non-hot-path follow-up SELECT. */
  @SuppressWarnings("unchecked")
  private void hydrateTagsSingle(JobEntity job) {
    if (job == null || job.getId() == null) return;
    List<String> tags =
        em.createNativeQuery("SELECT tag FROM scheduler_job_tag WHERE job_id = :id")
            .setParameter("id", job.getId())
            .getResultList();
    if (!tags.isEmpty()) {
      job.setTags(tags);
    }
  }

  /** Batch-loads tags for many jobs and assigns them by id. Single SELECT regardless of count. */
  @SuppressWarnings("unchecked")
  private void hydrateTagsBatch(List<JobEntity> jobs) {
    if (jobs.isEmpty()) return;
    List<Long> ids = new ArrayList<>(jobs.size());
    Map<Long, JobEntity> byId = new java.util.HashMap<>();
    for (JobEntity j : jobs) {
      if (j.getId() != null) {
        ids.add(j.getId());
        byId.put(j.getId(), j);
      }
    }
    if (ids.isEmpty()) return;
    List<Object[]> rows =
        em.createNativeQuery(
                "SELECT job_id, tag FROM scheduler_job_tag WHERE job_id IN (:ids) ORDER BY job_id")
            .setParameter("ids", ids)
            .getResultList();
    for (Object[] row : rows) {
      long jid = ((Number) row[0]).longValue();
      String tag = (String) row[1];
      JobEntity j = byId.get(jid);
      if (j == null) continue;
      List<String> tags = j.getTags();
      if (tags == null) {
        tags = new ArrayList<>();
        j.setTags(tags);
      }
      tags.add(tag);
    }
  }

  /**
   * Fail-fast guard for save() UPDATE path. Loads the current hot row + cold terminal/rec_status
   * and throws IllegalStateException if the incoming entity carries any hot-field mutation. Forces
   * callers to use explicit transition methods for live-state changes.
   */
  private void guardAgainstHotMutation(JobEntity incoming) {
    long id = incoming.getId();
    @SuppressWarnings("unchecked")
    List<Object[]> rows =
        em.createNativeQuery(
                "SELECT q.status, q.scheduled_time, q.attempts, q.picked_by, q.picked_at, "
                    + "q.paused_from_status, q.last_error, q.version, "
                    + "c.terminal_status, c.rec_status "
                    + "FROM scheduler_job c "
                    + "LEFT JOIN scheduler_job_queue q ON q.job_id = c.job_id "
                    + "WHERE c.job_id = :id")
            .setParameter("id", id)
            .getResultList();
    if (rows.isEmpty()) {
      throw new IllegalStateException("save() called on missing job id=" + id);
    }
    Object[] row = rows.get(0);
    String qStatus = (String) row[0];
    String terminal = (String) row[8];
    String recStatus = stringOrNull(row[9]);

    if (qStatus != null) {
      JobStatus storedStatus = JobStatus.valueOf(qStatus);
      checkHotField(id, "status", incoming.getStatus(), storedStatus);
      checkHotField(id, "scheduledTime", incoming.getScheduledTime(), toInstant(row[1]));
      Integer storedAttempts = ((Number) row[2]).intValue();
      checkHotField(id, "attempts", incoming.getAttempts(), storedAttempts);
      checkHotField(id, "pickedBy", incoming.getPickedBy(), row[3]);
      checkHotField(id, "pickedAt", incoming.getPickedAt(), toInstant(row[4]));
      String pausedFrom = (String) row[5];
      JobStatus storedPausedFrom = pausedFrom != null ? JobStatus.valueOf(pausedFrom) : null;
      checkHotField(id, "pausedFromStatus", incoming.getPausedFromStatus(), storedPausedFrom);
      checkHotField(id, "lastError", incoming.getLastError(), row[6]);
      Integer storedVersion = ((Number) row[7]).intValue();
      checkHotField(id, "version", incoming.getVersion(), storedVersion);
      return;
    }

    // No hot row: terminal or recurring master, or cold-only orphan.
    if (terminal != null) {
      // Terminal row: any incoming live status is a revival attempt.
      JobStatus incomingStatus = incoming.getStatus();
      if (incomingStatus != null && incomingStatus != JobStatus.valueOf(terminal)) {
        throw new IllegalStateException(
            "save() rejected: cannot mutate terminal job id="
                + id
                + " (terminal="
                + terminal
                + ", incoming.status="
                + incomingStatus
                + "). Use resetFailedToPending or markJobFailedTerminal.");
      }
      return;
    }

    if (recStatus != null) {
      JobStatus decoded = recStatusDecode(recStatus);
      JobStatus incomingStatus = incoming.getStatus();
      if (incomingStatus != null && incomingStatus != decoded) {
        throw new IllegalStateException(
            "save() rejected: recurring master id="
                + id
                + " status mutation requires explicit pause/resume API "
                + "(stored rec_status="
                + recStatus
                + ", incoming.status="
                + incomingStatus
                + ")");
      }
    }
  }

  private static void checkHotField(long jobId, String fieldName, Object incoming, Object stored) {
    if (java.util.Objects.equals(incoming, stored)) {
      return;
    }
    throw new IllegalStateException(
        "save() rejected: hot-field mutation detected for id="
            + jobId
            + " field="
            + fieldName
            + " incoming="
            + incoming
            + " stored="
            + stored
            + ". Use an explicit transition method.");
  }

  private static String stringOrNull(Object val) {
    if (val == null) return null;
    if (val instanceof String s) return s;
    return val.toString();
  }

  private static Long longOrNull(Object val) {
    if (val == null) return null;
    if (val instanceof Number n) return n.longValue();
    return null;
  }

  private static boolean isPollerExecutable(JobExecutionType jobType) {
    return jobType == JobExecutionType.SINGLE
        || jobType == JobExecutionType.BATCH_CHILD
        || jobType == JobExecutionType.CHAIN_STEP
        || jobType == JobExecutionType.WORKFLOW_BRANCH;
  }

  private List<JobClaimDto> claimOptimizedRows(List<Object[]> rows, String nodeId) {
    return timedStoreOperation(
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
                    safeJobPriority(((Number) row[3]).intValue()),
                    toInstant(row[4]),
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
      em.createNativeQuery(
              "UPDATE scheduler_job_queue SET status = 'RUNNING', picked_by = :node, "
                  + "picked_at = :pickedAt, updated_at = :updatedAt, version = version + 1 "
                  + "WHERE job_id IN (:ids) AND status = 'PENDING' ORDER BY job_id ASC")
          .setParameter("node", nodeId)
          .setParameter("pickedAt", nowTs)
          .setParameter("updatedAt", nowTs)
          .setParameter("ids", jobIds)
          .executeUpdate();

      @SuppressWarnings("unchecked")
      List<Number> claimedRows =
          em.createNativeQuery(
                  "SELECT job_id FROM scheduler_job_queue WHERE job_id IN (:ids) "
                      + "AND status = 'RUNNING' AND picked_by = :node AND picked_at = :pickedAt "
                      + "ORDER BY job_id ASC")
              .setParameter("ids", jobIds)
              .setParameter("node", nodeId)
              .setParameter("pickedAt", nowTs)
              .getResultList();

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
      throw translateTransientStoreException("claim jobs", e);
    }
  }

  private static Map<String, Long> toStringCountMap(List<Object[]> rows) {
    Map<String, Long> counts = new TreeMap<>();
    for (Object[] row : rows) {
      String key = (String) row[0];
      if (key == null || key.isBlank()) {
        continue;
      }
      counts.put(key, ((Number) row[1]).longValue());
    }
    return counts;
  }

  private static String toJsonFieldPath(String fieldName) {
    String escapedFieldName = fieldName.replace("\\", "\\\\").replace("\"", "\\\"");
    return "$.\"" + escapedFieldName + "\"";
  }

  private RuntimeException translateTransientStoreException(String operation, RuntimeException e) {
    if (CONSTRAINT_DETECTOR.isDeadlock(e) || CONSTRAINT_DETECTOR.isTransientConnectionFailure(e)) {
      return new RatchetTransientStoreException(
          "Transient MySQL store concurrency failure during " + operation, e);
    }
    return e;
  }

  private <T> T timedStoreOperation(
      String operation, Supplier<T> action, Function<T, String> outcomeFunction) {
    long startNanos = System.nanoTime();
    try {
      T result = action.get();
      recordStoreOperation(operation, outcomeFunction.apply(result), startNanos);
      return result;
    } catch (RatchetTransientStoreException e) {
      recordStoreOperation(operation, "transient_failure", startNanos);
      throw e;
    } catch (RuntimeException e) {
      recordStoreOperation(operation, "failure", startNanos);
      throw e;
    }
  }

  private void recordStoreOperation(String operation, String outcome, long startNanos) {
    metricsCollector.storeOperation("mysql", operation, outcome, System.nanoTime() - startNanos);
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

  private ArchivedJobEntity buildArchive(JobEntity job, String reason, String archivedBy) {
    return ArchiveHelper.buildArchive(job, reason, archivedBy);
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

  private String payloadToJson(JobEntity job) {
    return JOB_PAYLOAD_CONVERTER.convertToDatabaseColumn(job.getPayload());
  }

  private String paramsToJson(JobEntity job) {
    return JSON_MAP_CONVERTER.convertToDatabaseColumn(job.getParams());
  }

  private String callbackPayloadToJson(JobPayload payload) {
    return JOB_PAYLOAD_CONVERTER.convertToDatabaseColumn(payload);
  }
}
