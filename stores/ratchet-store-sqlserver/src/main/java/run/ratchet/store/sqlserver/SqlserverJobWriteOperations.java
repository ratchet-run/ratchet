/*
 * Copyright 2026 Ratchet Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package run.ratchet.store.sqlserver;

import static run.ratchet.store.util.JobWriteSupport.assignIdIfMissing;
import static run.ratchet.store.util.JobWriteSupport.checkHotField;

import jakarta.persistence.Query;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import run.ratchet.api.JobStatus;
import run.ratchet.api.exception.DuplicateIdempotencyKeyException;
import run.ratchet.api.exception.RatchetOptimisticLockException;
import run.ratchet.api.exception.RatchetTransientStoreException;
import run.ratchet.spi.ProtectedSurface;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.sqlserver.converter.UuidByteArrayConverter;
import run.ratchet.store.util.JobEncryption;
import run.ratchet.store.util.JobWriteSupport;
import run.ratchet.store.util.RowValues;

final class SqlserverJobWriteOperations {

  // SQL Server caps a single statement at 2100 bound parameters. The widest insert here
  // (scheduler_job) binds 25 columns per row, so 80 rows * 25 = 2000 stays safely under the limit.
  private static final int MAX_BULK_INSERT_ROWS = 80;

  // language=SQL Server
  private static final String COLD_INSERT_PREFIX =
      """
      INSERT INTO scheduler_job (
        job_id, job_type, priority, max_retries, backoff_policy, backoff_param_ms,
        timeout_sec, cron_expr, zone_id, payload, params, idempotency_key,
        business_key, resource_name, on_success_payload, on_failure_payload, depends_on,
        superseded_by, created_at, caller_principal, trace_context, recurring_master_id,
        execution_target, encrypted_payload, encryption_key_id)
      VALUES
      """;

  // depends_on (17), superseded_by (18), recurring_master_id (22) are nullable BINARY(16) columns.
  // mssql-jdbc binds an untyped Java null as nvarchar, and SQL Server forbids the implicit
  // nvarchar -> binary conversion (it allowed nvarchar -> uniqueidentifier, which is why this only
  // surfaced after the BINARY(16) migration). CAST(? AS BINARY(16)) makes the conversion explicit
  // so
  // a null binds cleanly; a non-null byte[] (varbinary) casts to binary harmlessly.
  private static final String COLD_INSERT_VALUES =
      """
      (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
              ?, ?, CAST(? AS BINARY(16)), CAST(? AS BINARY(16)), ?, ?, ?, CAST(? AS BINARY(16)), ?, ?, ?)
      """;

  private static final String COLD_INSERT_SQL = COLD_INSERT_PREFIX + COLD_INSERT_VALUES;

  // language=SQL Server
  private static final String HOT_INSERT_PREFIX =
      """
      INSERT INTO scheduler_job_queue (
        job_id, status, job_type, priority, scheduled_time, business_key, timeout_sec,
        max_retries, attempts, picked_by, picked_at, paused_from_status, last_error,
        version, updated_at, signal_key, signal_timeout, signal_payload, signal_payload_type,
        signal_outcome, signal_rejection_reason, signal_delivered_at, signal_delivered_by,
        signal_delivery_id, execution_target)
      VALUES
      """;

  private static final String HOT_INSERT_VALUES =
      "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

  private static final String HOT_INSERT_SQL = HOT_INSERT_PREFIX + HOT_INSERT_VALUES;

  // language=SQL Server
  private static final String BKRES_INSERT_PREFIX =
      """
      INSERT INTO scheduler_business_key_reservation
        (business_key, owner_job_id, owner_table, reserved_at)
      VALUES
      """;

  private static final String BKRES_INSERT_VALUES = "(?, ?, ?, SYSUTCDATETIME())";

  private static final int IDX_HOT_STATUS = 0;
  private static final int IDX_HOT_SCHEDULED_TIME = 1;
  private static final int IDX_HOT_ATTEMPTS = 2;
  private static final int IDX_HOT_PICKED_BY = 3;
  private static final int IDX_HOT_PICKED_AT = 4;
  private static final int IDX_HOT_PAUSED_FROM_STATUS = 5;
  private static final int IDX_HOT_LAST_ERROR = 6;
  private static final int IDX_HOT_VERSION = 7;
  private static final int IDX_HOT_TERMINAL_STATUS = 8;

  private final SqlserverStoreContext ctx;
  private final SqlserverBusinessKeyReservations reservations;
  private final SqlserverTagOperations tags;

  SqlserverJobWriteOperations(
      SqlserverStoreContext ctx,
      SqlserverBusinessKeyReservations reservations,
      SqlserverTagOperations tags) {
    this.ctx = ctx;
    this.reservations = reservations;
    this.tags = tags;
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
      assignIdIfMissing(job);
      if (job.getCreatedAt() == null) {
        job.setCreatedAt(now);
      }
      if (job.getUpdatedAt() == null) {
        job.setUpdatedAt(now);
      }
    }

    try {
      executeColdBulkInsert(jobs, nowTs);
      executeHotBulkInsert(jobs, nowTs);
      executeTerminalBackfills(jobs, nowTs);
      executeBusinessKeyBulkInsert(jobs);
    } catch (RuntimeException e) {
      if (ctx.constraintDetector().isDuplicateBusinessKey(e)) {
        throw new RatchetTransientStoreException("Active business key in use", e);
      }
      throw e;
    } finally {
      detachInsertedJobs(jobs);
    }
  }

  void saveInsert(JobEntity job) {
    assignIdIfMissing(job);
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
        job.getStatus() != null && SqlserverJobRowMapper.isTerminalStatus(job.getStatus());

    try {
      executeColdInsert(job, nowTs);
      if (bornTerminal) {
        executeColdTerminalBackfill(job, nowTs);
        job.setTerminalStatus(job.getStatus());
      } else {
        if (!recurring) {
          executeHotInsert(job, nowTs);
        }
        if (hasBkey) {
          String ownerTable =
              recurring
                  ? SqlserverBusinessKeyReservations.OWNER_TABLE_RECURRING
                  : SqlserverBusinessKeyReservations.OWNER_TABLE_QUEUE;
          reservations.insertReservation(job.getBusinessKey(), job.getId(), ownerTable);
        }
      }
      if (job.getTags() != null && !job.getTags().isEmpty()) {
        tags.insertTags(job.getId(), job.getTags());
      }
    } catch (RuntimeException e) {
      if (ctx.constraintDetector().isDuplicateIdempotencyKey(e)) {
        throw new DuplicateIdempotencyKeyException(job.getIdempotencyKey(), e);
      }
      if (ctx.constraintDetector().isDuplicateBusinessKey(e)) {
        throw new RatchetTransientStoreException(
            "Active business key in use for job " + job.getId(), e);
      }
      throw e;
    }
  }

  private void executeColdTerminalBackfill(JobEntity job, Timestamp nowTs) {
    // language=SQL Server
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
        .setParameter(5, UuidByteArrayConverter.toBytes(job.getId()))
        .executeUpdate();
  }

  private void executeColdInsert(JobEntity job, Timestamp nowTs) {
    Query q = ctx.em().createNativeQuery(COLD_INSERT_SQL);
    bindColdInsert(q, job, nowTs);
    q.executeUpdate();
  }

  private void executeHotInsert(JobEntity job, Timestamp nowTs) {
    Query q = ctx.em().createNativeQuery(HOT_INSERT_SQL);
    bindHotInsert(q, job, nowTs);
    q.executeUpdate();
  }

  private void bindColdInsert(Query q, JobEntity job, Timestamp nowTs) {
    bindColdInsert(q, job, nowTs, 1);
  }

  private int bindColdInsert(Query q, JobEntity job, Timestamp nowTs, int i) {
    boolean active = JobEncryption.activeFor(job);
    String keyId = JobEncryption.keyId(active);
    q.setParameter(i++, UuidByteArrayConverter.toBytes(job.getId()));
    q.setParameter(i++, job.getJobType().name());
    q.setParameter(i++, job.getPriority().ordinal());
    q.setParameter(i++, job.getMaxRetries());
    q.setParameter(i++, job.getBackoffPolicy().name());
    q.setParameter(i++, job.getBackoffParamMs());
    q.setParameter(i++, job.getTimeoutSec());
    q.setParameter(i++, JobWriteSupport.coerceCronExpr(job.getCronExpr()));
    q.setParameter(i++, JobWriteSupport.coerceZoneId(job.getZoneId()));
    q.setParameter(i++, SqlserverJobRowMapper.payloadToJson(job, active));
    q.setParameter(i++, SqlserverJobRowMapper.paramsToJson(job, active));
    q.setParameter(i++, job.getIdempotencyKey());
    q.setParameter(i++, job.getBusinessKey());
    q.setParameter(i++, job.getResourceName());
    q.setParameter(
        i++,
        SqlserverJobRowMapper.callbackPayloadToJson(
            job, job.getOnSuccessPayload(), ProtectedSurface.ON_SUCCESS_PAYLOAD, active));
    q.setParameter(
        i++,
        SqlserverJobRowMapper.callbackPayloadToJson(
            job, job.getOnFailurePayload(), ProtectedSurface.ON_FAILURE_PAYLOAD, active));
    q.setParameter(i++, UuidByteArrayConverter.toBytes(job.getDependsOn()));
    q.setParameter(i++, UuidByteArrayConverter.toBytes(job.getSupersededBy()));
    q.setParameter(i++, job.getCreatedAt() != null ? Timestamp.from(job.getCreatedAt()) : nowTs);
    q.setParameter(i++, job.getCallerPrincipal());
    q.setParameter(i++, SqlserverJobRowMapper.traceContextToJson(job));
    q.setParameter(i++, UuidByteArrayConverter.toBytes(job.getRecurringMasterId()));
    q.setParameter(i++, job.getExecutionTarget());
    q.setParameter(i++, active);
    q.setParameter(i, keyId);
    return i + 1;
  }

  private void bindHotInsert(Query q, JobEntity job, Timestamp nowTs) {
    bindHotInsert(q, job, nowTs, 1);
  }

  private int bindHotInsert(Query q, JobEntity job, Timestamp nowTs, int i) {
    q.setParameter(i++, UuidByteArrayConverter.toBytes(job.getId()));
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
    q.setParameter(i++, nowTs);
    q.setParameter(i++, job.getSignalKey());
    q.setParameter(
        i++, job.getSignalTimeout() != null ? Timestamp.from(job.getSignalTimeout()) : null);
    q.setParameter(i++, job.getSignalPayload());
    q.setParameter(i++, job.getSignalPayloadType());
    q.setParameter(i++, job.getSignalOutcome());
    q.setParameter(i++, job.getSignalRejectionReason());
    q.setParameter(
        i++,
        job.getSignalDeliveredAt() != null ? Timestamp.from(job.getSignalDeliveredAt()) : null);
    q.setParameter(i++, job.getSignalDeliveredBy());
    q.setParameter(i++, job.getSignalDeliveryId());
    q.setParameter(i, job.getExecutionTarget());
    return i + 1;
  }

  private void executeColdBulkInsert(List<JobEntity> jobs, Timestamp nowTs) {
    for (int start = 0; start < jobs.size(); start += MAX_BULK_INSERT_ROWS) {
      List<JobEntity> chunk =
          jobs.subList(start, Math.min(start + MAX_BULK_INSERT_ROWS, jobs.size()));
      Query query =
          ctx.em()
              .createNativeQuery(
                  COLD_INSERT_PREFIX + repeatValues(COLD_INSERT_VALUES, chunk.size()));
      int parameter = 1;
      for (JobEntity job : chunk) {
        parameter = bindColdInsert(query, job, nowTs, parameter);
      }
      query.executeUpdate();
    }
  }

  private void executeHotBulkInsert(List<JobEntity> jobs, Timestamp nowTs) {
    List<JobEntity> hotJobs =
        jobs.stream()
            .filter(job -> job.getJobType() != JobExecutionType.RECURRING)
            .filter(
                job ->
                    job.getStatus() == null
                        || !SqlserverJobRowMapper.isTerminalStatus(job.getStatus()))
            .toList();
    for (int start = 0; start < hotJobs.size(); start += MAX_BULK_INSERT_ROWS) {
      List<JobEntity> chunk =
          hotJobs.subList(start, Math.min(start + MAX_BULK_INSERT_ROWS, hotJobs.size()));
      Query query =
          ctx.em()
              .createNativeQuery(HOT_INSERT_PREFIX + repeatValues(HOT_INSERT_VALUES, chunk.size()));
      int parameter = 1;
      for (JobEntity job : chunk) {
        parameter = bindHotInsert(query, job, nowTs, parameter);
      }
      query.executeUpdate();
    }
  }

  private void executeTerminalBackfills(List<JobEntity> jobs, Timestamp nowTs) {
    List<JobEntity> terminalJobs =
        jobs.stream()
            .filter(job -> job.getStatus() != null)
            .filter(job -> SqlserverJobRowMapper.isTerminalStatus(job.getStatus()))
            .toList();
    for (int start = 0; start < terminalJobs.size(); start += MAX_BULK_INSERT_ROWS) {
      List<JobEntity> chunk =
          terminalJobs.subList(start, Math.min(start + MAX_BULK_INSERT_ROWS, terminalJobs.size()));
      String values =
          String.join(
              ",",
              Collections.nCopies(
                  chunk.size(),
                  "(CAST(? AS BINARY(16)), CAST(? AS VARCHAR(16)), ?, ?, CAST(? AS DATETIME2(6)))"));
      // language=SQL Server
      String sql =
          """
          UPDATE scheduler_job AS c
          SET terminal_status = v.terminal_status,
              terminal_error = v.terminal_error,
              total_attempts = v.total_attempts,
              terminated_at = v.terminated_at
          FROM (VALUES %s) AS v(job_id, terminal_status, terminal_error, total_attempts, terminated_at)
          WHERE c.job_id = v.job_id
          """
              .formatted(values);
      Query query = ctx.em().createNativeQuery(sql);
      int parameter = 1;
      for (JobEntity job : chunk) {
        query.setParameter(parameter++, UuidByteArrayConverter.toBytes(job.getId()));
        query.setParameter(parameter++, job.getStatus().name());
        query.setParameter(parameter++, job.getLastError());
        query.setParameter(parameter++, job.getAttempts());
        query.setParameter(parameter++, nowTs);
        job.setTerminalStatus(job.getStatus());
      }
      query.executeUpdate();
    }
  }

  private void detachInsertedJobs(List<JobEntity> jobs) {
    for (JobEntity job : jobs) {
      if (ctx.em().contains(job)) {
        ctx.em().detach(job);
      }
    }
  }

  private void executeBusinessKeyBulkInsert(List<JobEntity> jobs) {
    List<JobEntity> keyedJobs =
        jobs.stream()
            .filter(job -> job.getBusinessKey() != null)
            .filter(
                job ->
                    job.getStatus() == null
                        || !SqlserverJobRowMapper.isTerminalStatus(job.getStatus()))
            .toList();
    for (int start = 0; start < keyedJobs.size(); start += MAX_BULK_INSERT_ROWS) {
      List<JobEntity> chunk =
          keyedJobs.subList(start, Math.min(start + MAX_BULK_INSERT_ROWS, keyedJobs.size()));
      Query query =
          ctx.em()
              .createNativeQuery(
                  BKRES_INSERT_PREFIX + repeatValues(BKRES_INSERT_VALUES, chunk.size()));
      int parameter = 1;
      for (JobEntity job : chunk) {
        parameter = reservations.bindInsert(query, job, parameter);
      }
      query.executeUpdate();
    }
  }

  private static String repeatValues(String values, int count) {
    return String.join(", ", Collections.nCopies(count, values));
  }

  private void saveColdUpdate(JobEntity job) {
    Object[] row = snapshotHotRow(job.getId());
    if (tryScheduledTimeOnlyHotUpdate(job, row)) {
      return;
    }
    if (tryHotMutationDispatch(job, row)) {
      return;
    }
    guardAgainstHotMutation(job, row);

    // language=SQL Server
    String sql =
        """
        UPDATE scheduler_job
        SET params = ?,
            on_success_payload = ?,
            on_failure_payload = ?,
            depends_on = CAST(? AS BINARY(16)),
            superseded_by = CAST(? AS BINARY(16)),
            resource_name = ?
        WHERE job_id = ?
        """;
    boolean active = JobEncryption.activeFor(job);
    ctx.em()
        .createNativeQuery(sql)
        .setParameter(1, SqlserverJobRowMapper.paramsToJson(job, active))
        .setParameter(
            2,
            SqlserverJobRowMapper.callbackPayloadToJson(
                job, job.getOnSuccessPayload(), ProtectedSurface.ON_SUCCESS_PAYLOAD, active))
        .setParameter(
            3,
            SqlserverJobRowMapper.callbackPayloadToJson(
                job, job.getOnFailurePayload(), ProtectedSurface.ON_FAILURE_PAYLOAD, active))
        .setParameter(4, UuidByteArrayConverter.toBytes(job.getDependsOn()))
        .setParameter(5, UuidByteArrayConverter.toBytes(job.getSupersededBy()))
        .setParameter(6, job.getResourceName())
        .setParameter(7, UuidByteArrayConverter.toBytes(job.getId()))
        .executeUpdate();
  }

  private boolean tryScheduledTimeOnlyHotUpdate(JobEntity job, Object[] row) {
    UUID id = job.getId();
    if (row == null) {
      return false;
    }
    String storedStatus = (String) row[IDX_HOT_STATUS];
    if (!"PENDING".equals(storedStatus) && !"WAITING".equals(storedStatus)) {
      return false;
    }
    Instant storedSched = RowValues.instantOrNull(row[IDX_HOT_SCHEDULED_TIME]);
    Instant incomingSched = job.getScheduledTime();
    if (Objects.equals(storedSched, incomingSched)) {
      return false;
    }
    if (!Objects.equals(JobStatus.valueOf(storedStatus), job.getStatus())
        || !Objects.equals(((Number) row[IDX_HOT_ATTEMPTS]).intValue(), job.getAttempts())
        || !Objects.equals(row[IDX_HOT_PICKED_BY], job.getPickedBy())
        || !Objects.equals(RowValues.instantOrNull(row[IDX_HOT_PICKED_AT]), job.getPickedAt())
        || !Objects.equals(
            row[IDX_HOT_PAUSED_FROM_STATUS] != null
                ? JobStatus.valueOf((String) row[IDX_HOT_PAUSED_FROM_STATUS])
                : null,
            job.getPausedFromStatus())
        || !Objects.equals(row[IDX_HOT_LAST_ERROR], job.getLastError())
        || !Objects.equals(((Number) row[IDX_HOT_VERSION]).intValue(), job.getVersion())) {
      return false;
    }
    // language=SQL Server
    String updateSql =
        """
        UPDATE scheduler_job_queue
        SET scheduled_time = ?, updated_at = SYSUTCDATETIME()
        WHERE job_id = ? AND status = ?
        """;
    ctx.em()
        .createNativeQuery(updateSql)
        .setParameter(1, incomingSched != null ? Timestamp.from(incomingSched) : null)
        .setParameter(2, UuidByteArrayConverter.toBytes(id))
        .setParameter(3, storedStatus)
        .executeUpdate();
    return true;
  }

  private void updateHotLiveViaVersion(JobEntity incoming, int expectedVersion) {
    UUID id = incoming.getId();
    JobStatus status = incoming.getStatus() != null ? incoming.getStatus() : JobStatus.PENDING;
    // language=SQL Server
    String sql =
        """
        UPDATE scheduler_job_queue
        SET status = ?, scheduled_time = ?, attempts = ?, picked_by = ?, picked_at = ?,
            paused_from_status = ?, last_error = ?, version = version + 1,
            updated_at = SYSUTCDATETIME()
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
            .setParameter(8, UuidByteArrayConverter.toBytes(id))
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
            .setParameter(1, UuidByteArrayConverter.toBytes(id))
            .setParameter(2, expectedVersion)
            .executeUpdate();
    if (deleted == 0) {
      throw new RatchetOptimisticLockException("Concurrent modification on job " + id);
    }
    // language=SQL Server
    String updateSql =
        """
        UPDATE scheduler_job
        SET terminal_status = ?,
            terminal_error = COALESCE(?, terminal_error),
            terminated_at = SYSUTCDATETIME()
        WHERE job_id = ? AND terminal_status IS NULL
        """;
    ctx.em()
        .createNativeQuery(updateSql)
        .setParameter(1, incoming.getStatus().name())
        .setParameter(2, incoming.getLastError())
        .setParameter(3, UuidByteArrayConverter.toBytes(id))
        .executeUpdate();
    reservations.deleteReservationByOwner(id);
    incoming.setVersion(expectedVersion + 1);
    incoming.setTerminalStatus(incoming.getStatus());
  }

  @SuppressWarnings("unchecked")
  private Object[] snapshotHotRow(UUID id) {
    // language=SQL Server
    String sql =
        """
        SELECT q.status, q.scheduled_time, q.attempts, q.picked_by, q.picked_at,
               q.paused_from_status, q.last_error, q.version,
               c.terminal_status
        FROM scheduler_job c
        LEFT JOIN scheduler_job_queue q ON q.job_id = c.job_id
        WHERE c.job_id = ?
        """;
    List<Object[]> rows =
        ctx.em()
            .createNativeQuery(sql)
            .setParameter(1, UuidByteArrayConverter.toBytes(id))
            .getResultList();
    return rows.isEmpty() ? null : rows.get(0);
  }

  private void guardAgainstHotMutation(JobEntity incoming, Object[] row) {
    UUID id = incoming.getId();
    if (row == null) {
      throw new IllegalStateException("save() called on missing job id=" + id);
    }
    String qStatus = (String) row[IDX_HOT_STATUS];
    String terminal = (String) row[IDX_HOT_TERMINAL_STATUS];

    if (qStatus != null) {
      JobStatus storedStatus = JobStatus.valueOf(qStatus);
      checkHotField(id, "status", incoming.getStatus(), storedStatus);
      checkHotField(
          id,
          "scheduledTime",
          incoming.getScheduledTime(),
          RowValues.instantOrNull(row[IDX_HOT_SCHEDULED_TIME]));
      Integer storedAttempts = ((Number) row[IDX_HOT_ATTEMPTS]).intValue();
      checkHotField(id, "attempts", incoming.getAttempts(), storedAttempts);
      checkHotField(id, "pickedBy", incoming.getPickedBy(), row[IDX_HOT_PICKED_BY]);
      checkHotField(
          id, "pickedAt", incoming.getPickedAt(), RowValues.instantOrNull(row[IDX_HOT_PICKED_AT]));
      String pausedFrom = (String) row[IDX_HOT_PAUSED_FROM_STATUS];
      JobStatus storedPausedFrom = pausedFrom != null ? JobStatus.valueOf(pausedFrom) : null;
      checkHotField(id, "pausedFromStatus", incoming.getPausedFromStatus(), storedPausedFrom);
      checkHotField(id, "lastError", incoming.getLastError(), row[IDX_HOT_LAST_ERROR]);
      Integer storedVersion = ((Number) row[IDX_HOT_VERSION]).intValue();
      checkHotField(id, "version", incoming.getVersion(), storedVersion);
      return;
    }

    if (terminal != null) {
      JobStatus storedTerminal = JobStatus.valueOf(terminal);
      if (incoming.getTerminalStatus() != storedTerminal) {
        throw new RatchetOptimisticLockException("Concurrent modification on job " + id);
      }
      JobStatus incomingStatus = incoming.getStatus();
      if (incomingStatus != null && incomingStatus != storedTerminal) {
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
  }

  private boolean tryHotMutationDispatch(JobEntity incoming, Object[] row) {
    UUID id = incoming.getId();
    if (row == null) {
      return false;
    }
    String hotStatusStr = (String) row[IDX_HOT_STATUS];
    String terminalStr = (String) row[IDX_HOT_TERMINAL_STATUS];
    if (terminalStr != null || hotStatusStr == null) {
      return false;
    }

    JobStatus storedStatus = JobStatus.valueOf(hotStatusStr);
    Instant storedSched = RowValues.instantOrNull(row[IDX_HOT_SCHEDULED_TIME]);
    int storedAttempts = ((Number) row[IDX_HOT_ATTEMPTS]).intValue();
    Object storedPickedBy = row[IDX_HOT_PICKED_BY];
    Instant storedPickedAt = RowValues.instantOrNull(row[IDX_HOT_PICKED_AT]);
    String storedPausedFromStr = (String) row[IDX_HOT_PAUSED_FROM_STATUS];
    JobStatus storedPausedFrom =
        storedPausedFromStr != null ? JobStatus.valueOf(storedPausedFromStr) : null;
    Object storedLastError = row[IDX_HOT_LAST_ERROR];
    int storedVersion = ((Number) row[IDX_HOT_VERSION]).intValue();

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
