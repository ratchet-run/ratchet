package run.ratchet.store.mysql;

import run.ratchet.api.exception.RatchetOptimisticLockException;
import run.ratchet.api.exception.RatchetTransientStoreException;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.entity.JobStatus;
import run.ratchet.store.id.UuidV7Factory;
import jakarta.persistence.Query;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

final class MysqlJobWriteOperations {

  // language=MySQL
  private static final String COLD_INSERT_SQL =
      """
      INSERT INTO scheduler_job (
        job_id, job_type, priority, max_retries, backoff_policy, backoff_param_ms,
        timeout_sec, cron_expr, zone_id, next_fire, payload, params, idempotency_key,
        business_key, resource_name, on_success_payload, on_failure_payload, depends_on,
        superseded_by, created_at, created_by, caller_principal, rec_status)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CAST(? AS JSON), CAST(? AS JSON), ?, ?, ?,
              CAST(? AS JSON), CAST(? AS JSON), ?, ?, ?, ?, ?, ?)
      """;

  // language=MySQL
  private static final String HOT_INSERT_SQL =
      """
      INSERT INTO scheduler_job_queue (
        job_id, status, job_type, priority, scheduled_time, business_key, timeout_sec,
        max_retries, attempts, picked_by, picked_at, paused_from_status, last_error,
        version, updated_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      """;

  private final MysqlStoreContext ctx;
  private final MysqlJobRowMapper mapper;
  private final MysqlBusinessKeyReservations reservations;
  private final MysqlTagOperations tags;

  MysqlJobWriteOperations(
      MysqlStoreContext ctx,
      MysqlJobRowMapper mapper,
      MysqlBusinessKeyReservations reservations,
      MysqlTagOperations tags) {
    this.ctx = ctx;
    this.mapper = mapper;
    this.reservations = reservations;
    this.tags = tags;
  }

  private static void checkHotField(UUID jobId, String fieldName, Object incoming, Object stored) {
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
    if (job.getId() == null) {
      job.setId(UuidV7Factory.create());
    }
  }

  JobEntity save(JobEntity job) {
    if (job.getId() == null) {
      saveInsert(job);
    } else {
      saveColdUpdate(job);
    }
    return job;
  }

  void bulkInsert(List<JobEntity> jobs) {
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
    // language=MySQL
    String sql =
        """
        UPDATE scheduler_job
        SET terminal_status = ?, terminal_error = ?, total_attempts = ?, terminated_at = ?
        WHERE job_id = ?
        """;
    ctx.em()
        .createNativeQuery(sql)
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

    // language=MySQL
    String sql =
        """
        UPDATE scheduler_job
        SET next_fire = ?,
            params = CAST(? AS JSON),
            on_success_payload = CAST(? AS JSON),
            on_failure_payload = CAST(? AS JSON),
            depends_on = ?,
            superseded_by = ?,
            resource_name = ?
        WHERE job_id = ?
        """;
    ctx.em()
        .createNativeQuery(sql)
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
    UUID id = job.getId();
    // language=MySQL
    String selectSql =
        """
        SELECT q.status, q.scheduled_time, q.attempts, q.picked_by, q.picked_at,
               q.paused_from_status, q.last_error, q.version
        FROM scheduler_job_queue q
        WHERE q.job_id = ?
        """;
    List<Object[]> rows = ctx.em().createNativeQuery(selectSql).setParameter(1, id).getResultList();
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
    // language=MySQL
    String updateSql =
        """
        UPDATE scheduler_job_queue
        SET scheduled_time = ?, updated_at = NOW(3)
        WHERE job_id = ? AND status = 'PENDING'
        """;
    ctx.em()
        .createNativeQuery(updateSql)
        .setParameter(1, incomingSched != null ? Timestamp.from(incomingSched) : null)
        .setParameter(2, id)
        .executeUpdate();
    return true;
  }

  private void updateHotLiveViaVersion(JobEntity incoming, int expectedVersion) {
    UUID id = incoming.getId();
    JobStatus status = incoming.getStatus() != null ? incoming.getStatus() : JobStatus.PENDING;
    // language=MySQL
    String sql =
        """
        UPDATE scheduler_job_queue
        SET status = ?, scheduled_time = ?, attempts = ?, picked_by = ?, picked_at = ?,
            paused_from_status = ?, last_error = ?, version = version + 1,
            updated_at = NOW(3)
        WHERE job_id = ? AND version = ?
        """;
    int updated =
        ctx.em()
            .createNativeQuery(sql)
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
    UUID id = incoming.getId();
    int deleted =
        ctx.em()
            .createNativeQuery("DELETE FROM scheduler_job_queue WHERE job_id = ? AND version = ?")
            .setParameter(1, id)
            .setParameter(2, expectedVersion)
            .executeUpdate();
    if (deleted == 0) {
      throw new RatchetOptimisticLockException("Concurrent modification on job " + id);
    }
    // language=MySQL
    String updateSql =
        """
        UPDATE scheduler_job
        SET terminal_status = ?,
            terminal_error = COALESCE(?, terminal_error),
            terminated_at = NOW(3)
        WHERE job_id = ? AND terminal_status IS NULL
        """;
    ctx.em()
        .createNativeQuery(updateSql)
        .setParameter(1, incoming.getStatus().name())
        .setParameter(2, incoming.getLastError())
        .setParameter(3, id)
        .executeUpdate();
    reservations.deleteReservationByOwner(id);
    incoming.setVersion(expectedVersion + 1);
  }

  @SuppressWarnings("unchecked")
  private Object[] snapshotHotRow(UUID id) {
    // language=MySQL
    String sql =
        """
        SELECT q.status, q.scheduled_time, q.attempts, q.picked_by, q.picked_at,
               q.paused_from_status, q.last_error, q.version,
               c.terminal_status, c.rec_status
        FROM scheduler_job c
        LEFT JOIN scheduler_job_queue q ON q.job_id = c.job_id
        WHERE c.job_id = ?
        """;
    List<Object[]> rows = ctx.em().createNativeQuery(sql).setParameter(1, id).getResultList();
    return rows.isEmpty() ? null : rows.get(0);
  }

  private void guardAgainstHotMutation(JobEntity incoming) {
    UUID id = incoming.getId();
    Object[] row = snapshotHotRow(id);
    if (row == null) {
      throw new IllegalStateException("save() called on missing job id=" + id);
    }
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

  private boolean tryHotMutationDispatch(JobEntity incoming) {
    UUID id = incoming.getId();
    Object[] row = snapshotHotRow(id);
    if (row == null) {
      return false;
    }
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
