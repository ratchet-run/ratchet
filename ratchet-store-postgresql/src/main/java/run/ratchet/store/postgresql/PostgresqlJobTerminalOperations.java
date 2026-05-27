package run.ratchet.store.postgresql;

import jakarta.persistence.NoResultException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import run.ratchet.api.JobStatus;
import run.ratchet.api.exception.RatchetTransientStoreException;

/*
 * Keep terminal transitions dialect-local until a shared helper can preserve each backend's
 * SQL shape explicitly. These methods mirror MySQL conceptually, but PostgreSQL binds UUIDs
 * natively and uses statement_timestamp(), jsonb casts, and UPDATE ... FROM semantics.
 */
final class PostgresqlJobTerminalOperations {

  private final PostgresqlStoreContext ctx;
  private final PostgresqlBusinessKeyReservations reservations;
  private final PostgresqlBatchOperations batches;

  PostgresqlJobTerminalOperations(
      PostgresqlStoreContext ctx,
      PostgresqlBusinessKeyReservations reservations,
      PostgresqlBatchOperations batches) {
    this.ctx = ctx;
    this.reservations = reservations;
    this.batches = batches;
  }

  void updateJobStatus(UUID id, JobStatus status, String errorMessage) {
    ctx.timedStoreOperation(
        "update_status",
        () -> {
          if (PostgresqlJobRowMapper.isLiveStatus(status)) {
            // language=PostgreSQL
            String sql =
                """
                UPDATE scheduler_job_queue
                SET status = ?, last_error = ?, updated_at = statement_timestamp()
                WHERE job_id = ?
                """;
            return ctx.em()
                .createNativeQuery(sql)
                .setParameter(1, status.name())
                .setParameter(2, errorMessage)
                .setParameter(3, id)
                .executeUpdate();
          }
          if (status == JobStatus.CANCELED) {
            return cancelJob(id) ? 1 : 0;
          }
          if (status == JobStatus.FAILED) {
            return markJobFailedTerminal(id, errorMessage, 0) ? 1 : 0;
          }
          if (status == JobStatus.SUCCEEDED) {
            return markJobSucceededMinimal(id, null, null, null, null) ? 1 : 0;
          }
          throw new IllegalArgumentException("Unsupported status target: " + status);
        },
        updated -> updated > 0 ? "updated" : "miss");
  }

  boolean compareAndSwapStatus(UUID id, JobStatus expected, JobStatus newStatus, String error) {
    return ctx.timedStoreOperation(
        "compare_and_swap_status",
        () -> {
          if (!PostgresqlJobRowMapper.isLiveStatus(expected)) {
            throw new IllegalArgumentException(
                "compareAndSwapStatus expected must be a live status; got " + expected);
          }
          if (PostgresqlJobRowMapper.isLiveStatus(newStatus)) {
            // language=PostgreSQL
            String sql =
                """
                UPDATE scheduler_job_queue
                SET status = ?, last_error = ?, updated_at = statement_timestamp()
                WHERE job_id = ? AND status = ?
                """;
            return ctx.em()
                    .createNativeQuery(sql)
                    .setParameter(1, newStatus.name())
                    .setParameter(2, error)
                    .setParameter(3, id)
                    .setParameter(4, expected.name())
                    .executeUpdate()
                > 0;
          }
          if (newStatus == JobStatus.CANCELED) {
            return lockExpectedQueueStatusForTerminalCas(id, expected) && cancelJob(id);
          }
          if (newStatus == JobStatus.FAILED) {
            if (expected != JobStatus.RUNNING && expected != JobStatus.WAITING) {
              return false;
            }
            return markJobFailedTerminalFromStatus(id, error, null, expected);
          }
          throw new IllegalArgumentException("Unsupported CAS target newStatus: " + newStatus);
        },
        updated -> updated ? "updated" : "miss");
  }

  int incrementRetryAttempt(UUID id) {
    // language=PostgreSQL
    String sql =
        """
        UPDATE scheduler_job_queue
        SET attempts = attempts + 1, updated_at = statement_timestamp()
        WHERE job_id = ? AND status IN ('RUNNING', 'WAITING')
        RETURNING attempts
        """;
    return ctx.timedStoreOperation(
        "increment_retry_attempt",
        () -> {
          try {
            Object result = ctx.em().createNativeQuery(sql).setParameter(1, id).getSingleResult();
            return ((Number) result).intValue();
          } catch (NoResultException e) {
            return -1;
          }
        },
        attempts -> attempts > 0 ? "updated" : "miss");
  }

  boolean markJobSucceeded(
      UUID id,
      String resultJson,
      String resultType,
      Instant start,
      Instant end,
      Long durationMs,
      Long queueWaitMs) {
    return ctx.timedStoreOperation(
        "mark_succeeded",
        () ->
            doMarkTerminalSuccessWithResult(
                id, resultJson, resultType, start, end, durationMs, queueWaitMs),
        updated -> updated ? "updated" : "miss");
  }

  boolean markJobSucceededMinimal(
      UUID id, Instant start, Instant end, Long durationMs, Long queueWaitMs) {
    return ctx.timedStoreOperation(
        "mark_succeeded_minimal",
        () -> doMarkTerminalSuccessMinimal(id, start, end, durationMs, queueWaitMs),
        updated -> updated ? "updated" : "miss");
  }

  boolean markJobSucceededAndUpdateBatch(
      UUID jobId,
      String resultJson,
      String resultType,
      Instant start,
      Instant end,
      Long durationMs,
      Long queueWaitMs,
      UUID batchId) {
    boolean succeeded =
        markJobSucceeded(jobId, resultJson, resultType, start, end, durationMs, queueWaitMs);
    if (succeeded) {
      try {
        batches.incrementCompletedAtomic(batchId);
      } catch (RuntimeException e) {
        throw ctx.translateTransientStoreException(
            "increment completed batch after job success", e);
      }
    }
    return succeeded;
  }

  boolean scheduleJobRetry(UUID id, String error, Instant newScheduledTime, int attempts) {
    // language=PostgreSQL
    String sql =
        """
        UPDATE scheduler_job_queue
        SET status = 'PENDING', last_error = ?, scheduled_time = ?, attempts = ?,
            picked_by = NULL, picked_at = NULL, updated_at = statement_timestamp()
        WHERE job_id = ? AND status IN ('RUNNING', 'WAITING')
        """;
    return ctx.timedStoreOperation(
            "schedule_retry",
            () ->
                ctx.em()
                    .createNativeQuery(sql)
                    .setParameter(1, error)
                    .setParameter(2, Timestamp.from(newScheduledTime))
                    .setParameter(3, attempts)
                    .setParameter(4, id)
                    .executeUpdate(),
            updated -> updated > 0 ? "updated" : "miss")
        > 0;
  }

  boolean markJobFailedTerminal(UUID id, String terminalError, int totalAttempts) {
    return ctx.timedStoreOperation(
        "mark_failed_terminal",
        () -> markJobFailedTerminalFromStatus(id, terminalError, totalAttempts, JobStatus.RUNNING),
        updated -> updated ? "updated" : "miss");
  }

  private boolean lockExpectedQueueStatusForTerminalCas(UUID id, JobStatus expected) {
    // The terminal cancel path deletes any live queue row after updating the cold row. Lock the
    // expected hot row first so this multi-statement path preserves CAS semantics inside the
    // method transaction: either this caller owns the expected status, or it reports a miss.
    // language=PostgreSQL
    String gateSql =
        """
        SELECT job_id
        FROM scheduler_job_queue
        WHERE job_id = ? AND status = ?
        FOR UPDATE
        """;
    @SuppressWarnings("unchecked")
    List<Object> rows =
        ctx.em()
            .createNativeQuery(gateSql)
            .setParameter(1, id)
            .setParameter(2, expected.name())
            .getResultList();
    return !rows.isEmpty();
  }

  boolean cancelJob(UUID id) {
    return ctx.timedStoreOperation(
        "cancel_job", () -> doCancelJob(id), updated -> updated ? "updated" : "miss");
  }

  private boolean doCancelJob(UUID id) {
    try {
      // language=PostgreSQL
      String selectSql =
          """
          SELECT terminal_status
          FROM scheduler_job
          WHERE job_id = ?
          FOR UPDATE
          """;
      @SuppressWarnings("unchecked")
      List<Object> rows = ctx.em().createNativeQuery(selectSql).setParameter(1, id).getResultList();
      if (rows.isEmpty()) {
        return false;
      }
      String existingTerminal = (String) rows.get(0);
      if (existingTerminal != null) {
        return false;
      }
      // Recurring masters live in scheduler_recurring_job — cancel-by-id of a recurring master
      // routes through RecurringJobStore.cancelRecurringAndArchive, not through here.
      // language=PostgreSQL
      String deleteHotSql =
          """
          DELETE FROM scheduler_job_queue
          WHERE job_id = ? AND status IN ('PENDING','RUNNING','PAUSED','WAITING')
          """;
      // language=PostgreSQL
      String updateColdSql =
          """
          UPDATE scheduler_job c
          SET terminal_status = 'CANCELED',
              terminated_at = statement_timestamp(),
              execution_start_time =
                  CASE WHEN q.status = 'RUNNING'
                       THEN COALESCE(c.execution_start_time, q.picked_at, statement_timestamp())
                       ELSE c.execution_start_time END,
              execution_end_time =
                  CASE WHEN q.status = 'RUNNING'
                       THEN statement_timestamp()
                       ELSE c.execution_end_time END,
              execution_duration_ms =
                  CASE WHEN q.status = 'RUNNING'
                       THEN GREATEST(
                           0,
                           FLOOR(
                               EXTRACT(
                                   EPOCH FROM statement_timestamp()
                                     - COALESCE(
                                         c.execution_start_time,
                                         q.picked_at,
                                         statement_timestamp()))
                               * 1000)::bigint)
                       ELSE c.execution_duration_ms END
          FROM scheduler_job_queue q
          WHERE c.job_id = ? AND q.job_id = c.job_id
            AND c.terminal_status IS NULL
            AND q.status IN ('PENDING','RUNNING','PAUSED','WAITING')
          """;
      int coldUpdated =
          ctx.em().createNativeQuery(updateColdSql).setParameter(1, id).executeUpdate();
      if (coldUpdated == 0) {
        return false;
      }
      int hotDeleted = ctx.em().createNativeQuery(deleteHotSql).setParameter(1, id).executeUpdate();
      if (hotDeleted == 0) {
        throw new IllegalStateException(
            "cancel updated cold row but did not remove hot row for job " + id);
      }
      reservations.deleteReservationByOwner(id);
      return coldUpdated > 0;
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("cancel job", e);
    }
  }

  boolean resetFailedToPending(UUID id) {
    try {
      // language=PostgreSQL
      String selectSql =
          """
          SELECT terminal_status, job_type, priority, business_key, timeout_sec, max_retries,
                 execution_target
          FROM scheduler_job
          WHERE job_id = ?
          FOR UPDATE
          """;
      @SuppressWarnings("unchecked")
      List<Object[]> rows =
          ctx.em().createNativeQuery(selectSql).setParameter(1, id).getResultList();
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
      String executionTarget = (String) row[6];

      // language=PostgreSQL
      String clearTerminalSql =
          """
          UPDATE scheduler_job
          SET terminal_status = NULL, terminal_error = NULL,
              job_result = NULL, result_type = NULL,
              execution_start_time = NULL, execution_end_time = NULL,
              execution_duration_ms = NULL, queue_wait_ms = NULL,
              total_attempts = NULL, terminated_at = NULL
          WHERE job_id = ? AND terminal_status = 'FAILED'
          """;
      ctx.em().createNativeQuery(clearTerminalSql).setParameter(1, id).executeUpdate();

      // language=PostgreSQL
      String insertHotSql =
          """
          INSERT INTO scheduler_job_queue
            (job_id, status, job_type, priority, scheduled_time, business_key,
             timeout_sec, max_retries, attempts, version, updated_at, execution_target)
          VALUES (?, 'PENDING', ?, ?, statement_timestamp(), ?, ?, ?, 0, 0,
                  statement_timestamp(), ?)
          """;
      ctx.em()
          .createNativeQuery(insertHotSql)
          .setParameter(1, id)
          .setParameter(2, jobType)
          .setParameter(3, priority)
          .setParameter(4, businessKey)
          .setParameter(5, timeoutSec)
          .setParameter(6, maxRetries)
          .setParameter(7, executionTarget)
          .executeUpdate();

      if (businessKey != null) {
        try {
          reservations.insertReservation(
              businessKey, id, PostgresqlBusinessKeyReservations.OWNER_TABLE_QUEUE);
        } catch (RuntimeException e) {
          if (ctx.constraintDetector().isDuplicateBusinessKey(e)) {
            throw new RatchetTransientStoreException(
                "Cannot resurrect job " + id + ": business key already held", e);
          }
          throw e;
        }
      }
      return true;
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("reset failed job to pending", e);
    }
  }

  private boolean markJobFailedTerminalFromStatus(
      UUID id, String terminalError, Integer totalAttempts, JobStatus expectedStatus) {
    String attemptsExpression = totalAttempts == null ? "q.attempts" : "?";
    // language=PostgreSQL
    String updateColdSql =
        """
        UPDATE scheduler_job c
        SET terminal_status = 'FAILED',
            terminal_error = ?,
            total_attempts = %s,
            terminated_at = statement_timestamp(),
            execution_start_time =
                CASE WHEN q.status = 'RUNNING'
                     THEN COALESCE(c.execution_start_time, q.picked_at, statement_timestamp())
                     ELSE c.execution_start_time END,
            execution_end_time =
                CASE WHEN q.status = 'RUNNING'
                     THEN statement_timestamp()
                     ELSE c.execution_end_time END,
            execution_duration_ms =
                CASE WHEN q.status = 'RUNNING'
                     THEN GREATEST(
                         0,
                         FLOOR(
                             EXTRACT(
                                 EPOCH FROM statement_timestamp()
                                   - COALESCE(
                                       c.execution_start_time,
                                       q.picked_at,
                                       statement_timestamp()))
                             * 1000)::bigint)
                     ELSE c.execution_duration_ms END
        FROM scheduler_job_queue q
        WHERE c.job_id = ? AND q.job_id = c.job_id
          AND c.terminal_status IS NULL AND q.status = ?
        """
            .formatted(attemptsExpression);
    var query = ctx.em().createNativeQuery(updateColdSql).setParameter(1, terminalError);
    int parameter = 2;
    if (totalAttempts != null) {
      query.setParameter(parameter++, totalAttempts);
    }
    int coldUpdated =
        query
            .setParameter(parameter++, id)
            .setParameter(parameter, expectedStatus.name())
            .executeUpdate();
    if (coldUpdated == 0) {
      return false;
    }
    // language=PostgreSQL
    String deleteHotSql = "DELETE FROM scheduler_job_queue WHERE job_id = ? AND status = ?";
    int hotDeleted =
        ctx.em()
            .createNativeQuery(deleteHotSql)
            .setParameter(1, id)
            .setParameter(2, expectedStatus.name())
            .executeUpdate();
    if (hotDeleted == 0) {
      throw new IllegalStateException(
          "terminal failure updated cold row but did not remove hot row for job " + id);
    }
    reservations.deleteReservationByOwner(id);
    return true;
  }

  private boolean doMarkTerminalSuccessWithResult(
      UUID id,
      String resultJson,
      String resultType,
      Instant start,
      Instant end,
      Long durationMs,
      Long queueWaitMs) {
    // language=PostgreSQL
    String sql =
        """
        UPDATE scheduler_job c
        SET terminal_status = 'SUCCEEDED',
            job_result = CAST(? AS jsonb), result_type = ?,
            execution_start_time = ?, execution_end_time = ?,
            execution_duration_ms = ?, queue_wait_ms = ?,
            total_attempts = q.attempts, terminated_at = statement_timestamp()
        FROM scheduler_job_queue q
        WHERE c.job_id = ? AND q.job_id = c.job_id
          AND c.terminal_status IS NULL AND q.status = 'RUNNING'
        """;
    int coldUpdated =
        ctx.em()
            .createNativeQuery(sql)
            .setParameter(1, resultJson)
            .setParameter(2, resultType)
            .setParameter(3, start != null ? Timestamp.from(start) : null)
            .setParameter(4, end != null ? Timestamp.from(end) : null)
            .setParameter(5, durationMs)
            .setParameter(6, queueWaitMs)
            .setParameter(7, id)
            .executeUpdate();
    if (coldUpdated == 0) {
      return false;
    }
    deleteHotRowAndReservationAfterSuccess(id);
    return true;
  }

  private boolean doMarkTerminalSuccessMinimal(
      UUID id, Instant start, Instant end, Long durationMs, Long queueWaitMs) {
    // language=PostgreSQL
    String sql =
        """
        UPDATE scheduler_job c
        SET terminal_status = 'SUCCEEDED',
            execution_start_time = ?, execution_end_time = ?,
            execution_duration_ms = ?, queue_wait_ms = ?,
            total_attempts = q.attempts, terminated_at = statement_timestamp()
        FROM scheduler_job_queue q
        WHERE c.job_id = ? AND q.job_id = c.job_id
          AND c.terminal_status IS NULL AND q.status = 'RUNNING'
        """;
    int coldUpdated =
        ctx.em()
            .createNativeQuery(sql)
            .setParameter(1, start != null ? Timestamp.from(start) : null)
            .setParameter(2, end != null ? Timestamp.from(end) : null)
            .setParameter(3, durationMs)
            .setParameter(4, queueWaitMs)
            .setParameter(5, id)
            .executeUpdate();
    if (coldUpdated == 0) {
      return false;
    }
    deleteHotRowAndReservationAfterSuccess(id);
    return true;
  }

  private void deleteHotRowAndReservationAfterSuccess(UUID id) {
    // language=PostgreSQL
    String sql = "DELETE FROM scheduler_job_queue WHERE job_id = ? AND status = 'RUNNING'";
    int deleted = ctx.em().createNativeQuery(sql).setParameter(1, id).executeUpdate();
    if (deleted == 0) {
      throw new IllegalStateException(
          "terminal success updated cold row but failed to remove hot row for job " + id);
    }
    reservations.deleteReservationByOwner(id);
  }
}
