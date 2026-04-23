package run.ratchet.store.mysql;

import run.ratchet.api.JobPriority;
import run.ratchet.api.exception.RatchetOptimisticLockException;
import run.ratchet.api.exception.RatchetTransientStoreException;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.entity.JobStatus;
import run.ratchet.store.id.TsidFactory;
import run.ratchet.store.spi.JobBulkStore;
import run.ratchet.store.spi.JobCrudStore;
import jakarta.persistence.Query;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.jboss.logging.Logger;

final class MysqlJobCrudOperations implements JobCrudStore, JobBulkStore {

  private static final Logger log = Logger.getLogger(MysqlJobCrudOperations.class);

  private static final String COLD_INSERT_SQL =
      "INSERT INTO scheduler_job ("
          + "job_id, job_type, priority, max_retries, backoff_policy, backoff_param_ms, "
          + "timeout_sec, cron_expr, zone_id, next_fire, payload, params, idempotency_key, "
          + "business_key, resource_name, on_success_payload, on_failure_payload, depends_on, "
          + "superseded_by, created_at, created_by, caller_principal, rec_status) "
          + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CAST(? AS JSON), CAST(? AS JSON), ?, ?, ?, "
          + "CAST(? AS JSON), CAST(? AS JSON), ?, ?, ?, ?, ?, ?)";

  private static final String HOT_INSERT_SQL =
      "INSERT INTO scheduler_job_queue ("
          + "job_id, status, job_type, priority, scheduled_time, business_key, timeout_sec, "
          + "max_retries, attempts, picked_by, picked_at, paused_from_status, last_error, "
          + "version, updated_at) "
          + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

  private final MysqlStoreContext ctx;
  private final MysqlJobRowMapper mapper;
  private final MysqlBusinessKeyReservations reservations;
  private final MysqlTagOperations tags;

  MysqlJobCrudOperations(
      MysqlStoreContext ctx,
      MysqlJobRowMapper mapper,
      MysqlBusinessKeyReservations reservations,
      MysqlTagOperations tags) {
    this.ctx = ctx;
    this.mapper = mapper;
    this.reservations = reservations;
    this.tags = tags;
  }

  private static void checkHotField(long jobId, String fieldName, Object incoming, Object stored) {
    if (Objects.equals(incoming, stored)) {
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

  private static void assignTsidIfMissing(JobEntity job) {
    if (job.getId() == null || job.getId() == 0L) {
      job.setId(TsidFactory.next());
    }
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

  @Override
  @SuppressWarnings("unchecked")
  public Optional<JobEntity> findById(long id) {
    List<Object[]> rows =
        ctx.em()
            .createNativeQuery(
                "SELECT "
                    + MysqlJobRowMapper.HYDRATION_SELECT
                    + " FROM scheduler_job c "
                    + "LEFT JOIN scheduler_job_queue q ON q.job_id = c.job_id "
                    + "WHERE c.job_id = ?")
            .setParameter(1, id)
            .getResultList();
    if (rows.isEmpty()) {
      return Optional.empty();
    }
    JobEntity job = mapper.hydrateJobEntity(rows.get(0));
    tags.hydrateTagsSingle(job);
    return Optional.of(job);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Optional<JobEntity> findByIdLatest(long id) {
    List<Object[]> rows =
        ctx.em()
            .createNativeQuery(
                "SELECT "
                    + MysqlJobRowMapper.HYDRATION_SELECT
                    + " FROM scheduler_job c "
                    + "LEFT JOIN scheduler_job_queue q ON q.job_id = c.job_id "
                    + "WHERE c.job_id = ?")
            .setParameter(1, id)
            .getResultList();
    if (rows.isEmpty()) {
      return Optional.empty();
    }
    JobEntity job = mapper.hydrateJobEntity(rows.get(0));
    tags.hydrateTagsSingle(job);
    return Optional.of(job);
  }

  @Override
  public void delete(long id) {
    reservations.deleteReservationByOwner(id);
    ctx.em()
        .createNativeQuery("DELETE FROM scheduler_job WHERE job_id = ?")
        .setParameter(1, id)
        .executeUpdate();
  }

  @Override
  @SuppressWarnings("unchecked")
  public JobStatus getJobStatus(long id) {
    List<Object[]> results =
        ctx.em()
            .createNativeQuery(
                "SELECT q.status, c.rec_status, c.terminal_status "
                    + "FROM scheduler_job c "
                    + "LEFT JOIN scheduler_job_queue q ON q.job_id = c.job_id "
                    + "WHERE c.job_id = ?")
            .setParameter(1, id)
            .getResultList();
    if (results.isEmpty()) {
      return null;
    }
    Object[] row = results.get(0);
    String live = (String) row[0];
    if (live != null) {
      return JobStatus.valueOf(live);
    }
    JobStatus rec = MysqlJobRowMapper.recStatusDecode(MysqlJobRowMapper.stringOrNull(row[1]));
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
    String placeholders = String.join(",", Collections.nCopies(ids.size(), "?"));
    Query idsQuery =
        ctx.em()
            .createNativeQuery(
                "SELECT "
                    + MysqlJobRowMapper.HYDRATION_SELECT
                    + " FROM scheduler_job c "
                    + "LEFT JOIN scheduler_job_queue q ON q.job_id = c.job_id "
                    + "WHERE c.job_id IN ("
                    + placeholders
                    + ")");
    int parameter = 1;
    for (Long id : ids) {
      idsQuery.setParameter(parameter++, id);
    }
    List<Object[]> rows = idsQuery.getResultList();
    List<JobEntity> jobs = new ArrayList<>(rows.size());
    for (Object[] row : rows) {
      jobs.add(mapper.hydrateJobEntity(row));
    }
    tags.hydrateTagsBatch(jobs);
    return jobs;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Optional<JobEntity> findActiveByBusinessKey(String businessKey) {
    List<Object[]> rows =
        ctx.em()
            .createNativeQuery(
                "SELECT br.owner_table, "
                    + MysqlJobRowMapper.HYDRATION_SELECT
                    + " FROM scheduler_business_key_reservation br "
                    + "JOIN scheduler_job c ON c.job_id = br.owner_job_id "
                    + "LEFT JOIN scheduler_job_queue q ON q.job_id = c.job_id "
                    + "WHERE br.business_key = ? LIMIT 1")
            .setParameter(1, businessKey)
            .getResultList();
    if (rows.isEmpty()) {
      return Optional.empty();
    }
    Object[] full = rows.get(0);
    String ownerTable = (String) full[0];
    Object[] hydrationRow = new Object[MysqlJobRowMapper.HYDRATION_COL_COUNT];
    System.arraycopy(full, 1, hydrationRow, 0, MysqlJobRowMapper.HYDRATION_COL_COUNT);
    JobEntity job = mapper.hydrateJobEntity(hydrationRow);
    if (MysqlBusinessKeyReservations.OWNER_TABLE_QUEUE.equals(ownerTable)
        && hydrationRow[MysqlJobRowMapper.IDX_Q_STATUS] == null) {
      log.errorf(
          "bkres invariant violation: business_key=%s claims QUEUE owner job=%d but no hot row",
          businessKey, job.getId());
      return Optional.empty();
    }
    tags.hydrateTagsSingle(job);
    return Optional.of(job);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Optional<JobEntity> findByIdempotencyKey(String idempotencyKey) {
    List<Object[]> rows =
        ctx.em()
            .createNativeQuery(
                "SELECT "
                    + MysqlJobRowMapper.HYDRATION_SELECT
                    + " FROM scheduler_job c "
                    + "LEFT JOIN scheduler_job_queue q ON q.job_id = c.job_id "
                    + "WHERE c.idempotency_key = ? LIMIT 1")
            .setParameter(1, idempotencyKey)
            .getResultList();
    if (rows.isEmpty()) {
      return Optional.empty();
    }
    JobEntity job = mapper.hydrateJobEntity(rows.get(0));
    tags.hydrateTagsSingle(job);
    return Optional.of(job);
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<JobEntity> findDependants(long parentJobId) {
    List<Object[]> rows =
        ctx.em()
            .createNativeQuery(
                "SELECT "
                    + MysqlJobRowMapper.HYDRATION_SELECT
                    + " FROM scheduler_job c "
                    + "LEFT JOIN scheduler_job_queue q ON q.job_id = c.job_id "
                    + "WHERE c.depends_on = ?")
            .setParameter(1, parentJobId)
            .getResultList();
    List<JobEntity> jobs = new ArrayList<>(rows.size());
    for (Object[] row : rows) {
      jobs.add(mapper.hydrateJobEntity(row));
    }
    tags.hydrateTagsBatch(jobs);
    return jobs;
  }

  @Override
  public Optional<Instant> findEarliestRecurringNextFire() {
    List<?> results =
        ctx.em()
            .createNativeQuery(
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
    if (MysqlJobRowMapper.isLiveStatus(status)) {
      Object result =
          ctx.em()
              .createNativeQuery("SELECT COUNT(*) FROM scheduler_job_queue WHERE status = ?")
              .setParameter(1, status.name())
              .getSingleResult();
      return ((Number) result).longValue();
    }
    Object result =
        ctx.em()
            .createNativeQuery("SELECT COUNT(*) FROM scheduler_job WHERE terminal_status = ?")
            .setParameter(1, status.name())
            .getSingleResult();
    return ((Number) result).longValue();
  }

  @Override
  public long countActiveJobs(JobExecutionType jobType) {
    Object result =
        ctx.em()
            .createNativeQuery(
                "SELECT COUNT(*) FROM scheduler_job_queue "
                    + "WHERE job_type = ? AND status IN ('PENDING','RUNNING')")
            .setParameter(1, jobType.name())
            .getSingleResult();
    return ((Number) result).longValue();
  }

  @Override
  public long countActiveNodes() {
    return ctx.em().createQuery("SELECT COUNT(n) FROM NodeEntity n", Long.class).getSingleResult();
  }

  @Override
  public long countReadyJobs(Instant now) {
    Object result =
        ctx.em()
            .createNativeQuery(
                "SELECT COUNT(*) FROM scheduler_job_queue "
                    + "WHERE status = 'PENDING' AND scheduled_time <= ?")
            .setParameter(1, Timestamp.from(now))
            .getSingleResult();
    return ((Number) result).longValue();
  }

  @Override
  public long countStuckJobs(Instant stuckThreshold) {
    Object result =
        ctx.em()
            .createNativeQuery(
                "SELECT COUNT(*) FROM scheduler_job_queue "
                    + "WHERE status = 'RUNNING' AND picked_at < ?")
            .setParameter(1, Timestamp.from(stuckThreshold))
            .getSingleResult();
    return ((Number) result).longValue();
  }

  @Override
  public long countLongRunningJobs(Instant threshold) {
    Object result =
        ctx.em()
            .createNativeQuery(
                "SELECT COUNT(*) FROM scheduler_job_queue "
                    + "WHERE status = 'RUNNING' AND picked_at < ?")
            .setParameter(1, Timestamp.from(threshold))
            .getSingleResult();
    return ((Number) result).longValue();
  }

  @Override
  public long countPendingBatchChildren() {
    Object result =
        ctx.em()
            .createNativeQuery(
                "SELECT COUNT(*) FROM scheduler_job_queue "
                    + "WHERE job_type = 'BATCH_CHILD' AND status = 'PENDING'")
            .getSingleResult();
    return ((Number) result).longValue();
  }

  @Override
  public long countPendingJobsByPriority(JobPriority priority) {
    Object result =
        ctx.em()
            .createNativeQuery(
                "SELECT COUNT(*) FROM scheduler_job_queue "
                    + "WHERE status = 'PENDING' AND priority = ?")
            .setParameter(1, priority.ordinal())
            .getSingleResult();
    return ((Number) result).longValue();
  }

  @Override
  public long countPendingJobsByType(JobExecutionType jobType) {
    Object result =
        ctx.em()
            .createNativeQuery(
                "SELECT COUNT(*) FROM scheduler_job_queue "
                    + "WHERE status = 'PENDING' AND job_type = ?")
            .setParameter(1, jobType.name())
            .getSingleResult();
    return ((Number) result).longValue();
  }

  @Override
  public long countJobsByStatusSince(JobStatus status, Instant since) {
    if (MysqlJobRowMapper.isLiveStatus(status)) {
      Object result =
          ctx.em()
              .createNativeQuery(
                  "SELECT COUNT(*) FROM scheduler_job_queue "
                      + "WHERE status = ? AND updated_at >= ?")
              .setParameter(1, status.name())
              .setParameter(2, Timestamp.from(since))
              .getSingleResult();
      return ((Number) result).longValue();
    }
    Object result =
        ctx.em()
            .createNativeQuery(
                "SELECT COUNT(*) FROM scheduler_job "
                    + "WHERE terminal_status = ? AND terminated_at >= ?")
            .setParameter(1, status.name())
            .setParameter(2, Timestamp.from(since))
            .getSingleResult();
    return ((Number) result).longValue();
  }

  @Override
  public long countJobsWithRetries() {
    Object result =
        ctx.em()
            .createNativeQuery(
                "SELECT "
                    + "(SELECT COUNT(*) FROM scheduler_job_queue WHERE attempts > 0) "
                    + "+ (SELECT COUNT(*) FROM scheduler_job WHERE total_attempts > 0)")
            .getSingleResult();
    return ((Number) result).longValue();
  }

  @Override
  public double getRetryRateStats(Instant since) {
    Timestamp sinceTs = Timestamp.from(since);
    Object result =
        ctx.em()
            .createNativeQuery(
                "SELECT COALESCE("
                    + "  ((SELECT COUNT(*) FROM scheduler_job_queue "
                    + "      WHERE attempts > 0 AND updated_at >= ?) "
                    + "   + (SELECT COUNT(*) FROM scheduler_job "
                    + "      WHERE total_attempts > 0 AND terminated_at >= ?)) "
                    + "  / NULLIF("
                    + "    ((SELECT COUNT(*) FROM scheduler_job_queue WHERE updated_at >= ?) "
                    + "     + (SELECT COUNT(*) FROM scheduler_job "
                    + "        WHERE terminated_at >= ?)), 0), 0)")
            .setParameter(1, sinceTs)
            .setParameter(2, sinceTs)
            .setParameter(3, sinceTs)
            .setParameter(4, sinceTs)
            .getSingleResult();
    return ((Number) result).doubleValue();
  }

  @Override
  public double getAverageProcessingTime(Instant since) {
    Object result =
        ctx.em()
            .createNativeQuery(
                "SELECT COALESCE(AVG(execution_duration_ms), 0) FROM scheduler_job "
                    + "WHERE terminal_status = 'SUCCEEDED' AND execution_duration_ms IS NOT NULL "
                    + "AND terminated_at >= ?")
            .setParameter(1, Timestamp.from(since))
            .getSingleResult();
    return ((Number) result).doubleValue();
  }

  @Override
  public double getAverageBatchSize(Instant since) {
    Object result =
        ctx.em()
            .createNativeQuery(
                "SELECT COALESCE(AVG(b.total_items), 0) FROM scheduler_batch b "
                    + "JOIN scheduler_job c ON c.job_id = b.batch_id "
                    + "LEFT JOIN scheduler_job_queue q ON q.job_id = c.job_id "
                    + "WHERE COALESCE(q.updated_at, c.terminated_at) >= ?")
            .setParameter(1, Timestamp.from(since))
            .getSingleResult();
    return ((Number) result).doubleValue();
  }

  @Override
  public Optional<Instant> getOldestPendingJobTime() {
    List<?> results =
        ctx.em()
            .createNativeQuery(
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
    Number countResult =
        (Number)
            ctx.em()
                .createNativeQuery(
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
        ctx.em()
            .createNativeQuery(
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
  public void bulkInsert(List<JobEntity> jobs) {
    if (jobs.isEmpty()) {
      return;
    }
    Instant now = Instant.now();
    Timestamp nowTs = Timestamp.from(now);

    for (JobEntity job : jobs) {
      assignTsidIfMissing(job);
    }

    for (JobEntity job : jobs) {
      Query q = ctx.em().createNativeQuery(COLD_INSERT_SQL);
      bindColdInsert(q, job, nowTs);
      q.executeUpdate();
    }

    for (JobEntity job : jobs) {
      if (job.getJobType() == JobExecutionType.RECURRING) {
        continue;
      }
      Query q = ctx.em().createNativeQuery(HOT_INSERT_SQL);
      bindHotInsert(q, job, nowTs);
      q.executeUpdate();
    }

    for (JobEntity job : jobs) {
      if (job.getBusinessKey() == null) {
        continue;
      }
      Query q = ctx.em().createNativeQuery(MysqlBusinessKeyReservations.BKRES_INSERT_SQL);
      reservations.bindInsert(q, job, nowTs);
      q.executeUpdate();
    }

    ctx.em().clear();
  }

  @Override
  public int deleteJobsByIds(List<Long> ids) {
    if (ids.isEmpty()) {
      return 0;
    }
    String placeholders = String.join(",", Collections.nCopies(ids.size(), "?"));
    Query bkresDelete =
        ctx.em()
            .createNativeQuery(
                "DELETE FROM scheduler_business_key_reservation WHERE owner_job_id IN ("
                    + placeholders
                    + ")");
    int parameter = 1;
    for (Long id : ids) {
      bkresDelete.setParameter(parameter++, id);
    }
    bkresDelete.executeUpdate();
    Query jobDelete =
        ctx.em()
            .createNativeQuery("DELETE FROM scheduler_job WHERE job_id IN (" + placeholders + ")");
    parameter = 1;
    for (Long id : ids) {
      jobDelete.setParameter(parameter++, id);
    }
    return jobDelete.executeUpdate();
  }

  @Override
  public int deleteDlqOlderThan(Instant cutoff) {
    @SuppressWarnings("unchecked")
    List<Number> idRows =
        ctx.em()
            .createNativeQuery(
                "SELECT job_id FROM scheduler_job "
                    + "WHERE terminal_status = 'FAILED' AND total_attempts >= max_retries "
                    + "AND terminated_at < ?")
            .setParameter(1, Timestamp.from(cutoff))
            .getResultList();
    if (idRows.isEmpty()) {
      return 0;
    }
    List<Long> ids = new ArrayList<>(idRows.size());
    for (Number n : idRows) {
      ids.add(n.longValue());
    }
    String placeholders = String.join(",", Collections.nCopies(ids.size(), "?"));
    Query bkresDelete =
        ctx.em()
            .createNativeQuery(
                "DELETE FROM scheduler_business_key_reservation WHERE owner_job_id IN ("
                    + placeholders
                    + ")");
    int parameter = 1;
    for (Long id : ids) {
      bkresDelete.setParameter(parameter++, id);
    }
    bkresDelete.executeUpdate();
    Query jobDelete =
        ctx.em()
            .createNativeQuery("DELETE FROM scheduler_job WHERE job_id IN (" + placeholders + ")");
    parameter = 1;
    for (Long id : ids) {
      jobDelete.setParameter(parameter++, id);
    }
    return jobDelete.executeUpdate();
  }

  @Override
  public int resetOrphanJobs(Duration grace) {
    long graceSec = grace.toSeconds();
    return ctx.em()
        .createNativeQuery(
            "UPDATE scheduler_job_queue SET status = 'PENDING', picked_by = NULL, "
                + "picked_at = NULL, updated_at = NOW(3) "
                + "WHERE status = 'RUNNING' AND picked_by NOT IN ("
                + "  SELECT node_id FROM scheduler_node "
                + "  WHERE TIMESTAMPDIFF(SECOND, heartbeat_ts, NOW(3)) <= ?"
                + ") AND TIMESTAMPDIFF(SECOND, picked_at, NOW(3)) >= ?")
        .setParameter(1, graceSec)
        .setParameter(2, graceSec)
        .executeUpdate();
  }

  @Override
  public int resetOrphanJobsForNode(String nodeId) {
    return ctx.em()
        .createNativeQuery(
            "UPDATE scheduler_job_queue SET status = 'PENDING', picked_by = NULL, "
                + "picked_at = NULL, updated_at = NOW(3) "
                + "WHERE status = 'RUNNING' AND picked_by = ?")
        .setParameter(1, nodeId)
        .executeUpdate();
  }

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
    boolean bornTerminal =
        job.getStatus() != null && MysqlJobRowMapper.isTerminalStatus(job.getStatus());

    try {
      executeColdInsert(job, nowTs);
      if (bornTerminal) {
        executeColdTerminalBackfill(job, nowTs);
      } else {
        if (!recurring) {
          executeHotInsert(job, nowTs);
        }
        if (hasBkey) {
          String ownerTable =
              recurring
                  ? MysqlBusinessKeyReservations.OWNER_TABLE_RECURRING
                  : MysqlBusinessKeyReservations.OWNER_TABLE_QUEUE;
          reservations.insertReservation(job.getBusinessKey(), job.getId(), ownerTable);
        }
      }
    } catch (RuntimeException e) {
      if (ctx.constraintDetector().isDuplicateBusinessKey(e)) {
        throw new RatchetTransientStoreException(
            "Active business key in use for job " + job.getId(), e);
      }
      throw e;
    }

    if (job.getTags() != null && !job.getTags().isEmpty()) {
      tags.insertTags(job.getId(), job.getTags());
    }
  }

  private void executeColdTerminalBackfill(JobEntity job, Timestamp nowTs) {
    ctx.em()
        .createNativeQuery(
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

  private void executeColdInsert(JobEntity job, Timestamp nowTs) {
    String recStatus = null;
    if (job.getJobType() == JobExecutionType.RECURRING) {
      JobStatus s = job.getStatus() != null ? job.getStatus() : JobStatus.PENDING;
      String r = MysqlJobRowMapper.recStatusForLiveStatus(s);
      recStatus = r != null ? r : "P";
    }
    ctx.em()
        .createNativeQuery(COLD_INSERT_SQL)
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
        .setParameter(11, mapper.payloadToJson(job))
        .setParameter(12, mapper.paramsToJson(job))
        .setParameter(13, job.getIdempotencyKey())
        .setParameter(14, job.getBusinessKey())
        .setParameter(15, job.getResourceName())
        .setParameter(16, mapper.callbackPayloadToJson(job.getOnSuccessPayload()))
        .setParameter(17, mapper.callbackPayloadToJson(job.getOnFailurePayload()))
        .setParameter(18, job.getDependsOn())
        .setParameter(19, job.getSupersededBy())
        .setParameter(20, nowTs)
        .setParameter(21, job.getCreatedBy())
        .setParameter(22, job.getCallerPrincipal())
        .setParameter(23, recStatus)
        .executeUpdate();
  }

  private void executeHotInsert(JobEntity job, Timestamp nowTs) {
    JobStatus s = job.getStatus() != null ? job.getStatus() : JobStatus.PENDING;
    Instant scheduled = job.getScheduledTime();
    ctx.em()
        .createNativeQuery(HOT_INSERT_SQL)
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

  private void saveColdUpdate(JobEntity job) {
    if (tryScheduledTimeOnlyHotUpdate(job)) {
      return;
    }
    if (tryHotMutationDispatch(job)) {
      return;
    }
    guardAgainstHotMutation(job);

    ctx.em()
        .createNativeQuery(
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
        .setParameter(2, mapper.paramsToJson(job))
        .setParameter(3, mapper.callbackPayloadToJson(job.getOnSuccessPayload()))
        .setParameter(4, mapper.callbackPayloadToJson(job.getOnFailurePayload()))
        .setParameter(5, job.getDependsOn())
        .setParameter(6, job.getSupersededBy())
        .setParameter(7, job.getResourceName())
        .setParameter(8, job.getId())
        .executeUpdate();
  }

  private void bindColdInsert(Query q, JobEntity job, Timestamp nowTs) {
    int i = 1;
    q.setParameter(i++, job.getId());
    q.setParameter(i++, job.getJobType().name());
    q.setParameter(i++, job.getPriority().ordinal());
    q.setParameter(i++, job.getMaxRetries());
    q.setParameter(i++, job.getBackoffPolicy().name());
    q.setParameter(i++, job.getBackoffParamMs());
    q.setParameter(i++, job.getTimeoutSec());
    q.setParameter(i++, job.getCronExpr());
    q.setParameter(i++, job.getZoneId());
    q.setParameter(i++, job.getNextFire() != null ? Timestamp.from(job.getNextFire()) : null);
    q.setParameter(i++, mapper.payloadToJson(job));
    q.setParameter(i++, mapper.paramsToJson(job));
    q.setParameter(i++, job.getIdempotencyKey());
    q.setParameter(i++, job.getBusinessKey());
    q.setParameter(i++, job.getResourceName());
    q.setParameter(i++, mapper.callbackPayloadToJson(job.getOnSuccessPayload()));
    q.setParameter(i++, mapper.callbackPayloadToJson(job.getOnFailurePayload()));
    q.setParameter(i++, job.getDependsOn());
    q.setParameter(i++, job.getSupersededBy());
    q.setParameter(i++, nowTs);
    q.setParameter(i++, job.getCreatedBy());
    q.setParameter(i++, job.getCallerPrincipal());
    String recStatus = null;
    if (job.getJobType() == JobExecutionType.RECURRING) {
      JobStatus s = job.getStatus() != null ? job.getStatus() : JobStatus.PENDING;
      String rec = MysqlJobRowMapper.recStatusForLiveStatus(s);
      recStatus = rec != null ? rec : "P";
    }
    q.setParameter(i, recStatus);
  }

  private void bindHotInsert(Query q, JobEntity job, Timestamp nowTs) {
    int i = 1;
    q.setParameter(i++, job.getId());
    JobStatus s = job.getStatus() != null ? job.getStatus() : JobStatus.PENDING;
    q.setParameter(i++, s.name());
    q.setParameter(i++, job.getJobType().name());
    q.setParameter(i++, job.getPriority().ordinal());
    Instant scheduled = job.getScheduledTime();
    q.setParameter(i++, scheduled != null ? Timestamp.from(scheduled) : nowTs);
    q.setParameter(i++, job.getBusinessKey());
    q.setParameter(i++, job.getTimeoutSec());
    q.setParameter(i++, job.getMaxRetries());
    q.setParameter(i++, job.getAttempts());
    q.setParameter(i++, job.getPickedBy());
    q.setParameter(i++, job.getPickedAt() != null ? Timestamp.from(job.getPickedAt()) : null);
    q.setParameter(
        i++, job.getPausedFromStatus() != null ? job.getPausedFromStatus().name() : null);
    q.setParameter(i++, job.getLastError());
    q.setParameter(i++, job.getVersion() != null ? job.getVersion() : 0);
    q.setParameter(i, nowTs);
  }

  @SuppressWarnings("unchecked")
  private boolean tryScheduledTimeOnlyHotUpdate(JobEntity job) {
    long id = job.getId();
    List<Object[]> rows =
        ctx.em()
            .createNativeQuery(
                "SELECT q.status, q.scheduled_time, q.attempts, q.picked_by, q.picked_at, "
                    + "q.paused_from_status, q.last_error, q.version "
                    + "FROM scheduler_job_queue q WHERE q.job_id = ?")
            .setParameter(1, id)
            .getResultList();
    if (rows.isEmpty()) {
      return false;
    }
    Object[] row = rows.get(0);
    if (!"PENDING".equals(row[0])) {
      return false;
    }
    Instant storedSched = MysqlJobRowMapper.toInstant(row[1]);
    Instant incomingSched = job.getScheduledTime();
    if (Objects.equals(storedSched, incomingSched)) {
      return false;
    }
    if (!Objects.equals(JobStatus.PENDING, job.getStatus())
        || !Objects.equals(((Number) row[2]).intValue(), job.getAttempts())
        || !Objects.equals(row[3], job.getPickedBy())
        || !Objects.equals(MysqlJobRowMapper.toInstant(row[4]), job.getPickedAt())
        || !Objects.equals(
            row[5] != null ? JobStatus.valueOf((String) row[5]) : null, job.getPausedFromStatus())
        || !Objects.equals(row[6], job.getLastError())
        || !Objects.equals(((Number) row[7]).intValue(), job.getVersion())) {
      return false;
    }
    ctx.em()
        .createNativeQuery(
            "UPDATE scheduler_job_queue SET scheduled_time = ?, updated_at = NOW(3) "
                + "WHERE job_id = ? AND status = 'PENDING'")
        .setParameter(1, incomingSched != null ? Timestamp.from(incomingSched) : null)
        .setParameter(2, id)
        .executeUpdate();
    return true;
  }

  private void updateHotLiveViaVersion(JobEntity incoming, int expectedVersion) {
    long id = incoming.getId();
    JobStatus status = incoming.getStatus() != null ? incoming.getStatus() : JobStatus.PENDING;
    int updated =
        ctx.em()
            .createNativeQuery(
                "UPDATE scheduler_job_queue SET "
                    + "status = ?, scheduled_time = ?, attempts = ?, picked_by = ?, picked_at = ?, "
                    + "paused_from_status = ?, last_error = ?, version = version + 1, "
                    + "updated_at = NOW(3) "
                    + "WHERE job_id = ? AND version = ?")
            .setParameter(1, status.name())
            .setParameter(
                2,
                incoming.getScheduledTime() != null
                    ? Timestamp.from(incoming.getScheduledTime())
                    : null)
            .setParameter(3, incoming.getAttempts())
            .setParameter(4, incoming.getPickedBy())
            .setParameter(
                5, incoming.getPickedAt() != null ? Timestamp.from(incoming.getPickedAt()) : null)
            .setParameter(
                6,
                incoming.getPausedFromStatus() != null
                    ? incoming.getPausedFromStatus().name()
                    : null)
            .setParameter(7, incoming.getLastError())
            .setParameter(8, id)
            .setParameter(9, expectedVersion)
            .executeUpdate();
    if (updated == 0) {
      throw new RatchetOptimisticLockException("Concurrent modification on job " + id);
    }
    incoming.setVersion(expectedVersion + 1);
  }

  private void terminalizeViaSave(JobEntity incoming, int expectedVersion) {
    long id = incoming.getId();
    int deleted =
        ctx.em()
            .createNativeQuery("DELETE FROM scheduler_job_queue WHERE job_id = ? AND version = ?")
            .setParameter(1, id)
            .setParameter(2, expectedVersion)
            .executeUpdate();
    if (deleted == 0) {
      throw new RatchetOptimisticLockException("Concurrent modification on job " + id);
    }
    ctx.em()
        .createNativeQuery(
            "UPDATE scheduler_job SET terminal_status = ?, "
                + "terminal_error = COALESCE(?, terminal_error), "
                + "terminated_at = NOW(3) "
                + "WHERE job_id = ? AND terminal_status IS NULL")
        .setParameter(1, incoming.getStatus().name())
        .setParameter(2, incoming.getLastError())
        .setParameter(3, id)
        .executeUpdate();
    reservations.deleteReservationByOwner(id);
    incoming.setVersion(expectedVersion + 1);
  }

  private void guardAgainstHotMutation(JobEntity incoming) {
    long id = incoming.getId();
    @SuppressWarnings("unchecked")
    List<Object[]> rows =
        ctx.em()
            .createNativeQuery(
                "SELECT q.status, q.scheduled_time, q.attempts, q.picked_by, q.picked_at, "
                    + "q.paused_from_status, q.last_error, q.version, "
                    + "c.terminal_status, c.rec_status "
                    + "FROM scheduler_job c "
                    + "LEFT JOIN scheduler_job_queue q ON q.job_id = c.job_id "
                    + "WHERE c.job_id = ?")
            .setParameter(1, id)
            .getResultList();
    if (rows.isEmpty()) {
      throw new IllegalStateException("save() called on missing job id=" + id);
    }
    Object[] row = rows.get(0);
    String qStatus = (String) row[0];
    String terminal = (String) row[8];
    String recStatus = MysqlJobRowMapper.stringOrNull(row[9]);

    if (qStatus != null) {
      JobStatus storedStatus = JobStatus.valueOf(qStatus);
      checkHotField(id, "status", incoming.getStatus(), storedStatus);
      checkHotField(
          id, "scheduledTime", incoming.getScheduledTime(), MysqlJobRowMapper.toInstant(row[1]));
      Integer storedAttempts = ((Number) row[2]).intValue();
      checkHotField(id, "attempts", incoming.getAttempts(), storedAttempts);
      checkHotField(id, "pickedBy", incoming.getPickedBy(), row[3]);
      checkHotField(id, "pickedAt", incoming.getPickedAt(), MysqlJobRowMapper.toInstant(row[4]));
      String pausedFrom = (String) row[5];
      JobStatus storedPausedFrom = pausedFrom != null ? JobStatus.valueOf(pausedFrom) : null;
      checkHotField(id, "pausedFromStatus", incoming.getPausedFromStatus(), storedPausedFrom);
      checkHotField(id, "lastError", incoming.getLastError(), row[6]);
      Integer storedVersion = ((Number) row[7]).intValue();
      checkHotField(id, "version", incoming.getVersion(), storedVersion);
      return;
    }

    if (terminal != null) {
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
      JobStatus decoded = MysqlJobRowMapper.recStatusDecode(recStatus);
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

  @SuppressWarnings("unchecked")
  private boolean tryHotMutationDispatch(JobEntity incoming) {
    long id = incoming.getId();
    List<Object[]> rows =
        ctx.em()
            .createNativeQuery(
                "SELECT q.status, q.scheduled_time, q.attempts, q.picked_by, q.picked_at, "
                    + "q.paused_from_status, q.last_error, q.version, "
                    + "c.terminal_status, c.rec_status "
                    + "FROM scheduler_job c "
                    + "LEFT JOIN scheduler_job_queue q ON q.job_id = c.job_id "
                    + "WHERE c.job_id = ?")
            .setParameter(1, id)
            .getResultList();
    if (rows.isEmpty()) {
      return false;
    }
    Object[] row = rows.get(0);
    String hotStatusStr = (String) row[0];
    String terminalStr = (String) row[8];
    String recStatus = MysqlJobRowMapper.stringOrNull(row[9]);
    if (terminalStr != null || recStatus != null || hotStatusStr == null) {
      return false;
    }

    JobStatus storedStatus = JobStatus.valueOf(hotStatusStr);
    Instant storedSched = MysqlJobRowMapper.toInstant(row[1]);
    int storedAttempts = ((Number) row[2]).intValue();
    Object storedPickedBy = row[3];
    Instant storedPickedAt = MysqlJobRowMapper.toInstant(row[4]);
    String storedPausedFromStr = (String) row[5];
    JobStatus storedPausedFrom =
        storedPausedFromStr != null ? JobStatus.valueOf(storedPausedFromStr) : null;
    Object storedLastError = row[6];
    int storedVersion = ((Number) row[7]).intValue();

    boolean statusDiffers = incoming.getStatus() != null && incoming.getStatus() != storedStatus;
    boolean anyHotFieldDiffers =
        statusDiffers
            || !Objects.equals(incoming.getScheduledTime(), storedSched)
            || incoming.getAttempts() != storedAttempts
            || !Objects.equals(incoming.getPickedBy(), storedPickedBy)
            || !Objects.equals(incoming.getPickedAt(), storedPickedAt)
            || !Objects.equals(incoming.getPausedFromStatus(), storedPausedFrom)
            || !Objects.equals(incoming.getLastError(), storedLastError);

    if (!anyHotFieldDiffers) {
      return false;
    }

    Integer incomingVersion = incoming.getVersion();
    if (incomingVersion != null && incomingVersion.intValue() != storedVersion) {
      throw new RatchetOptimisticLockException("Concurrent modification on job " + id);
    }

    if (statusDiffers && incoming.getStatus().isTerminal()) {
      terminalizeViaSave(incoming, storedVersion);
    } else {
      updateHotLiveViaVersion(incoming, storedVersion);
    }
    return true;
  }
}
