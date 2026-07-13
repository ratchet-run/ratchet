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
package run.ratchet.store.oracle;

import jakarta.persistence.Query;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import run.ratchet.api.JobStatus;
import run.ratchet.api.exception.RatchetTransientStoreException;
import run.ratchet.store.oracle.converter.UuidRawConverter;

/*
 * Keep terminal transitions dialect-local until a shared helper can preserve each backend's
 * SQL shape explicitly. These methods mirror PostgreSQL conceptually, but Oracle binds UUIDs as
 * binary values, uses CAST(SYS_EXTRACT_UTC(SYSTIMESTAMP) AS TIMESTAMP), JSON casts, and multi-table deletes differently.
 *
 * Oracle also keeps timing labels on terminal operations because these paths do more than one SQL
 * statement for hot/cold row moves and business-key cleanup. The labels make slow terminal
 * transitions distinguishable from ordinary write latency in store metrics.
 */
final class OracleJobTerminalOperations {

  private final OracleStoreContext ctx;
  private final OracleBusinessKeyReservations reservations;
  private final OracleBatchOperations batches;

  OracleJobTerminalOperations(
      OracleStoreContext ctx,
      OracleBusinessKeyReservations reservations,
      OracleBatchOperations batches) {
    this.ctx = ctx;
    this.reservations = reservations;
    this.batches = batches;
  }

  void updateJobStatus(UUID id, JobStatus status, String errorMessage) {
    ctx.timedStoreOperation(
        "update_status",
        () -> {
          try {
            if (OracleJobRowMapper.isLiveStatus(status)) {
              // language=Oracle
              String sql =
                  """
                  UPDATE scheduler_job_queue
                  SET status = ?, last_error = ?, updated_at = CAST(SYS_EXTRACT_UTC(SYSTIMESTAMP) AS TIMESTAMP)
                  WHERE job_id = ?
                  """;
              return ctx.em()
                  .createNativeQuery(sql)
                  .setParameter(1, status.name())
                  .setParameter(2, errorMessage)
                  .setParameter(3, UuidRawConverter.toBytes(id))
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
          } catch (RuntimeException e) {
            throw ctx.translateTransientStoreException("update status", e);
          }
        },
        updated -> updated > 0 ? "updated" : "miss");
  }

  boolean compareAndSwapStatus(UUID id, JobStatus expected, JobStatus newStatus, String error) {
    return ctx.timedStoreOperation(
        "compare_and_swap_status",
        () -> {
          try {
            if (!OracleJobRowMapper.isLiveStatus(expected)) {
              throw new IllegalArgumentException(
                  "compareAndSwapStatus expected must be a live status; got " + expected);
            }
            if (OracleJobRowMapper.isLiveStatus(newStatus)) {
              // language=Oracle
              String casSql =
                  """
                  UPDATE scheduler_job_queue
                  SET status = ?, last_error = ?, updated_at = CAST(SYS_EXTRACT_UTC(SYSTIMESTAMP) AS TIMESTAMP)
                  WHERE job_id = ? AND status = ?
                  """;
              return ctx.em()
                      .createNativeQuery(casSql)
                      .setParameter(1, newStatus.name())
                      .setParameter(2, error)
                      .setParameter(3, UuidRawConverter.toBytes(id))
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
          } catch (RuntimeException e) {
            throw ctx.translateTransientStoreException("compare-and-swap status", e);
          }
        },
        updated -> updated ? "updated" : "miss");
  }

  int incrementRetryAttempt(UUID id) {
    try {
      // language=Oracle
      String updateSql =
          """
          UPDATE scheduler_job_queue
          SET attempts = attempts + 1, updated_at = CAST(SYS_EXTRACT_UTC(SYSTIMESTAMP) AS TIMESTAMP)
          WHERE job_id = ? AND status IN ('RUNNING', 'WAITING')
          """;
      int updated =
          ctx.timedStoreOperation(
              "increment_retry_attempt",
              () -> {
                try {
                  return ctx.em()
                      .createNativeQuery(updateSql)
                      .setParameter(1, UuidRawConverter.toBytes(id))
                      .executeUpdate();
                } catch (RuntimeException e) {
                  throw ctx.translateTransientStoreException("increment retry attempt", e);
                }
              },
              count -> count > 0 ? "updated" : "miss");
      if (updated == 0) {
        return -1;
      }
      // Oracle has no LAST_INSERT_ID; read the just-incremented value back within the same
      // REQUIRED transaction, which sees its own uncommitted write.
      // language=Oracle
      String selectSql = "SELECT attempts FROM scheduler_job_queue WHERE job_id = ?";
      Object result =
          ctx.em()
              .createNativeQuery(selectSql)
              .setParameter(1, UuidRawConverter.toBytes(id))
              .getSingleResult();
      return ((Number) result).intValue();
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("increment retry attempt", e);
    }
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
        () -> {
          try {
            return doMarkTerminalSuccessWithResult(
                id, resultJson, resultType, start, end, durationMs, queueWaitMs);
          } catch (RuntimeException e) {
            throw ctx.translateTransientStoreException("mark job succeeded", e);
          }
        },
        updated -> updated ? "updated" : "miss");
  }

  boolean markJobSucceededMinimal(
      UUID id, Instant start, Instant end, Long durationMs, Long queueWaitMs) {
    return ctx.timedStoreOperation(
        "mark_succeeded_minimal",
        () -> {
          try {
            return doMarkTerminalSuccessMinimal(id, start, end, durationMs, queueWaitMs);
          } catch (RuntimeException e) {
            throw ctx.translateTransientStoreException("mark job succeeded minimally", e);
          }
        },
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
    try {
      boolean succeeded =
          markJobSucceeded(jobId, resultJson, resultType, start, end, durationMs, queueWaitMs);
      if (succeeded) {
        batches.incrementCompletedAtomic(batchId);
      }
      return succeeded;
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("mark job succeeded and update batch", e);
    }
  }

  boolean scheduleJobRetry(UUID id, String error, Instant newScheduledTime, int attempts) {
    // language=Oracle
    String sql =
        """
        UPDATE scheduler_job_queue
        SET status = 'PENDING', last_error = ?, scheduled_time = ?, attempts = ?,
            picked_by = NULL, picked_at = NULL, updated_at = CAST(SYS_EXTRACT_UTC(SYSTIMESTAMP) AS TIMESTAMP)
        WHERE job_id = ? AND status IN ('RUNNING', 'WAITING')
        """;
    return ctx.timedStoreOperation(
            "schedule_retry",
            () -> {
              try {
                return ctx.em()
                    .createNativeQuery(sql)
                    .setParameter(1, error)
                    .setParameter(2, Timestamp.from(newScheduledTime))
                    .setParameter(3, attempts)
                    .setParameter(4, UuidRawConverter.toBytes(id))
                    .executeUpdate();
              } catch (RuntimeException e) {
                throw ctx.translateTransientStoreException("schedule job retry", e);
              }
            },
            updated -> updated > 0 ? "updated" : "miss")
        > 0;
  }

  boolean markJobFailedTerminal(UUID id, String terminalError, int totalAttempts) {
    return ctx.timedStoreOperation(
        "mark_failed_terminal",
        () -> {
          try {
            return markJobFailedTerminalFromStatus(
                id, terminalError, totalAttempts, JobStatus.RUNNING);
          } catch (RuntimeException e) {
            throw ctx.translateTransientStoreException("mark job failed terminal", e);
          }
        },
        updated -> updated ? "updated" : "miss");
  }

  private boolean lockExpectedQueueStatusForTerminalCas(UUID id, JobStatus expected) {
    // The terminal cancel path deletes any live queue row after updating the cold row. Lock the
    // expected hot row first so this multi-statement path preserves CAS semantics inside the
    // method transaction: either this caller owns the expected status, or it reports a miss.
    // language=Oracle
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
            .setParameter(1, UuidRawConverter.toBytes(id))
            .setParameter(2, expected.name())
            .getResultList();
    return !rows.isEmpty();
  }

  boolean cancelJob(UUID id) {
    return ctx.timedStoreOperation(
        "cancel_job",
        () -> {
          try {
            return doCancelJob(id);
          } catch (RuntimeException e) {
            throw ctx.translateTransientStoreException("cancel job", e);
          }
        },
        updated -> updated ? "updated" : "miss");
  }

  // UTC wall-clock "now", session-time-zone independent (see store dialect notes).
  private static final String NOW_UTC = "CAST(SYS_EXTRACT_UTC(SYSTIMESTAMP) AS TIMESTAMP)";

  /**
   * Elapsed whole milliseconds between two TIMESTAMP expressions. Oracle has no TIMESTAMPDIFF, so
   * the interval is decomposed with EXTRACT; TRUNC reproduces MySQL's integer {@code DIV 1000}.
   */
  private static String msBetween(String fromExpr, String toExpr) {
    String d = "(" + toExpr + " - " + fromExpr + ")";
    return "TRUNC(EXTRACT(DAY FROM "
        + d
        + ") * 86400000 + EXTRACT(HOUR FROM "
        + d
        + ") * 3600000 + EXTRACT(MINUTE FROM "
        + d
        + ") * 60000 + EXTRACT(SECOND FROM "
        + d
        + ") * 1000)";
  }

  /**
   * The execution-timing SET fragment shared by cancel and terminal-failure. Oracle has no
   * UPDATE..JOIN, so the queue-derived timing is computed in a correlated subquery against the
   * single matching {@code scheduler_job_queue} row. Only set when the job is RUNNING; otherwise
   * the cold values are preserved.
   */
  private static String runningTimingTuple() {
    String startExpr = "COALESCE(c.execution_start_time, q.picked_at, " + NOW_UTC + ")";
    return "(c.execution_start_time, c.execution_end_time, c.execution_duration_ms) = ("
        + " SELECT CASE WHEN q.status = 'RUNNING' THEN "
        + startExpr
        + " ELSE c.execution_start_time END,"
        + " CASE WHEN q.status = 'RUNNING' THEN "
        + NOW_UTC
        + " ELSE c.execution_end_time END,"
        + " CASE WHEN q.status = 'RUNNING' THEN GREATEST(0, "
        + msBetween(startExpr, NOW_UTC)
        + ") ELSE c.execution_duration_ms END"
        + " FROM scheduler_job_queue q WHERE q.job_id = c.job_id)";
  }

  private boolean doCancelJob(UUID id) {
    // language=Oracle
    String cancelNonRecurringSql =
        "UPDATE scheduler_job c SET c.terminal_status = 'CANCELED', c.terminated_at = "
            + NOW_UTC
            + ", "
            + runningTimingTuple()
            + " WHERE c.job_id = ? AND c.job_type <> 'RECURRING' AND c.terminal_status IS NULL"
            + " AND EXISTS (SELECT 1 FROM scheduler_job_queue q WHERE q.job_id = c.job_id"
            + " AND q.status IN ('PENDING','RUNNING','PAUSED','WAITING'))";
    int nonRecurringUpdated =
        ctx.em()
            .createNativeQuery(cancelNonRecurringSql)
            .setParameter(1, UuidRawConverter.toBytes(id))
            .executeUpdate();
    if (nonRecurringUpdated > 0) {
      // language=Oracle
      String deleteHotSql =
          """
          DELETE FROM scheduler_job_queue
          WHERE job_id = ? AND status IN ('PENDING','RUNNING','PAUSED','WAITING')
          """;
      int hotDeleted =
          ctx.em()
              .createNativeQuery(deleteHotSql)
              .setParameter(1, UuidRawConverter.toBytes(id))
              .executeUpdate();
      if (hotDeleted == 0) {
        throw new IllegalStateException(
            "cancel updated cold row but did not remove hot row for job " + id);
      }
      reservations.deleteReservationByOwner(id);
      return true;
    }

    // Recurring masters live in scheduler_recurring_job, not scheduler_job, so a job missing
    // from the hot queue with no terminal row is either a terminal-only survivor (no-op) or a
    // missing id. Either way, the cancel is a no-op here; recurring cancel routes through
    // RecurringJobStore.cancelRecurringAndArchive.
    return false;
  }

  boolean resetFailedToPending(UUID id) {
    try {
      return doResetFailedToPending(id);
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("reset failed to pending", e);
    }
  }

  private boolean doResetFailedToPending(UUID id) {
    // language=Oracle
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
        ctx.em()
            .createNativeQuery(selectSql)
            .setParameter(1, UuidRawConverter.toBytes(id))
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
    String executionTarget = (String) row[6];

    // language=Oracle
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
    ctx.em()
        .createNativeQuery(clearTerminalSql)
        .setParameter(1, UuidRawConverter.toBytes(id))
        .executeUpdate();

    // language=Oracle
    String insertHotSql =
        """
        INSERT INTO scheduler_job_queue
          (job_id, status, job_type, priority, scheduled_time, business_key,
           timeout_sec, max_retries, attempts, version, updated_at, execution_target)
        VALUES (?, 'PENDING', ?, ?, CAST(SYS_EXTRACT_UTC(SYSTIMESTAMP) AS TIMESTAMP), ?, ?, ?, 0, 0, CAST(SYS_EXTRACT_UTC(SYSTIMESTAMP) AS TIMESTAMP), ?)
        """;
    ctx.em()
        .createNativeQuery(insertHotSql)
        .setParameter(1, UuidRawConverter.toBytes(id))
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
            businessKey, id, OracleBusinessKeyReservations.OWNER_TABLE_QUEUE);
      } catch (RuntimeException e) {
        if (ctx.constraintDetector().isDuplicateBusinessKey(e)) {
          throw new RatchetTransientStoreException(
              "Cannot resurrect job " + id + ": business key already held", e);
        }
        throw e;
      }
    }
    return true;
  }

  int resetFailedToPending(List<UUID> ids) {
    if (ids.isEmpty()) {
      return 0;
    }
    try {
      String placeholders = "?,".repeat(ids.size() - 1) + "?";
      // language=Oracle
      String clearTerminalSql =
          """
          UPDATE scheduler_job
          SET terminal_status = NULL, terminal_error = NULL,
              job_result = NULL, result_type = NULL,
              execution_start_time = NULL, execution_end_time = NULL,
              execution_duration_ms = NULL, queue_wait_ms = NULL,
              total_attempts = NULL, terminated_at = NULL
          WHERE terminal_status = 'FAILED' AND job_id IN (%s)
          """
              .formatted(placeholders);
      int cleared = bindIds(clearTerminalSql, ids).executeUpdate();
      requireCompleteBulkSelection(ids, cleared);

      // language=Oracle
      String insertHotSql =
          """
          INSERT INTO scheduler_job_queue
            (job_id, status, job_type, priority, scheduled_time, business_key,
             timeout_sec, max_retries, attempts, version, updated_at, execution_target)
          SELECT job_id, 'PENDING', job_type, priority,
                 CAST(SYS_EXTRACT_UTC(SYSTIMESTAMP) AS TIMESTAMP), business_key,
                 timeout_sec, max_retries, 0, 0,
                 CAST(SYS_EXTRACT_UTC(SYSTIMESTAMP) AS TIMESTAMP), execution_target
          FROM scheduler_job
          WHERE job_id IN (%s)
          """
              .formatted(placeholders);
      int inserted = bindIds(insertHotSql, ids).executeUpdate();
      if (inserted != ids.size()) {
        throw new IllegalStateException(
            "Bulk retry inserted " + inserted + " queue rows for " + ids.size() + " jobs");
      }

      // language=Oracle
      String reserveSql =
          """
          INSERT INTO scheduler_business_key_reservation
            (business_key, owner_job_id, owner_table, reserved_at)
          SELECT business_key, job_id, 'QUEUE',
                 CAST(SYS_EXTRACT_UTC(SYSTIMESTAMP) AS TIMESTAMP)
          FROM scheduler_job
          WHERE business_key IS NOT NULL AND job_id IN (%s)
          """
              .formatted(placeholders);
      bindIds(reserveSql, ids).executeUpdate();
      return ids.size();
    } catch (RuntimeException e) {
      if (ctx.constraintDetector().isDuplicateBusinessKey(e)) {
        throw new RatchetTransientStoreException(
            "Cannot bulk-resurrect jobs: business key already held", e);
      }
      throw ctx.translateTransientStoreException("bulk reset failed jobs to pending", e);
    }
  }

  private Query bindIds(String sql, List<UUID> ids) {
    Query query = ctx.em().createNativeQuery(sql);
    for (int i = 0; i < ids.size(); i++) {
      query.setParameter(i + 1, UuidRawConverter.toBytes(ids.get(i)));
    }
    return query;
  }

  private static void requireCompleteBulkSelection(List<UUID> ids, int cleared) {
    if (cleared != ids.size()) {
      throw new RatchetTransientStoreException(
          "Bulk retry selection changed concurrently: selected "
              + ids.size()
              + " jobs but reset "
              + cleared);
    }
  }

  private boolean markJobFailedTerminalFromStatus(
      UUID id, String terminalError, Integer totalAttempts, JobStatus expectedStatus) {
    // total_attempts: either a caller-supplied count (bind) or the queue row's attempts (subquery).
    String attemptsExpression =
        totalAttempts == null
            ? "(SELECT q.attempts FROM scheduler_job_queue q WHERE q.job_id = c.job_id)"
            : "?";
    // language=Oracle
    String updateColdSql =
        "UPDATE scheduler_job c SET c.terminal_status = 'FAILED', c.terminal_error = ?,"
            + " c.total_attempts = "
            + attemptsExpression
            + ", c.terminated_at = "
            + NOW_UTC
            + ", "
            + runningTimingTuple()
            + " WHERE c.job_id = ? AND c.terminal_status IS NULL"
            + " AND EXISTS (SELECT 1 FROM scheduler_job_queue q WHERE q.job_id = c.job_id"
            + " AND q.status = ?)";
    var query = ctx.em().createNativeQuery(updateColdSql).setParameter(1, terminalError);
    int parameter = 2;
    if (totalAttempts != null) {
      query.setParameter(parameter++, totalAttempts);
    }
    int coldUpdated =
        query
            .setParameter(parameter++, UuidRawConverter.toBytes(id))
            .setParameter(parameter, expectedStatus.name())
            .executeUpdate();
    if (coldUpdated == 0) {
      return false;
    }
    // language=Oracle
    String deleteHotSql = "DELETE FROM scheduler_job_queue WHERE job_id = ? AND status = ?";
    int hotDeleted =
        ctx.em()
            .createNativeQuery(deleteHotSql)
            .setParameter(1, UuidRawConverter.toBytes(id))
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
    // language=Oracle
    String sql =
        "UPDATE scheduler_job c SET c.terminal_status = 'SUCCEEDED', c.job_result = ?,"
            + " c.result_type = ?, c.execution_start_time = ?, c.execution_end_time = ?,"
            + " c.execution_duration_ms = ?, c.queue_wait_ms = ?,"
            + " c.total_attempts ="
            + " (SELECT q.attempts FROM scheduler_job_queue q WHERE q.job_id = c.job_id),"
            + " c.terminated_at = "
            + NOW_UTC
            + " WHERE c.job_id = ? AND c.terminal_status IS NULL"
            + " AND EXISTS (SELECT 1 FROM scheduler_job_queue q WHERE q.job_id = c.job_id"
            + " AND q.status = 'RUNNING')";
    int coldUpdated =
        ctx.em()
            .createNativeQuery(sql)
            .setParameter(1, resultJson)
            .setParameter(2, resultType)
            .setParameter(3, start != null ? Timestamp.from(start) : null)
            .setParameter(4, end != null ? Timestamp.from(end) : null)
            .setParameter(5, durationMs)
            .setParameter(6, queueWaitMs)
            .setParameter(7, UuidRawConverter.toBytes(id))
            .executeUpdate();
    if (coldUpdated == 0) {
      return false;
    }
    deleteHotRowAndReservationAfterSuccess(id);
    return true;
  }

  private boolean doMarkTerminalSuccessMinimal(
      UUID id, Instant start, Instant end, Long durationMs, Long queueWaitMs) {
    // language=Oracle
    String sql =
        "UPDATE scheduler_job c SET c.terminal_status = 'SUCCEEDED',"
            + " c.execution_start_time = ?, c.execution_end_time = ?,"
            + " c.execution_duration_ms = ?, c.queue_wait_ms = ?,"
            + " c.total_attempts ="
            + " (SELECT q.attempts FROM scheduler_job_queue q WHERE q.job_id = c.job_id),"
            + " c.terminated_at = "
            + NOW_UTC
            + " WHERE c.job_id = ? AND c.terminal_status IS NULL"
            + " AND EXISTS (SELECT 1 FROM scheduler_job_queue q WHERE q.job_id = c.job_id"
            + " AND q.status = 'RUNNING')";
    int coldUpdated =
        ctx.em()
            .createNativeQuery(sql)
            .setParameter(1, start != null ? Timestamp.from(start) : null)
            .setParameter(2, end != null ? Timestamp.from(end) : null)
            .setParameter(3, durationMs)
            .setParameter(4, queueWaitMs)
            .setParameter(5, UuidRawConverter.toBytes(id))
            .executeUpdate();
    if (coldUpdated == 0) {
      return false;
    }
    deleteHotRowAndReservationAfterSuccess(id);
    return true;
  }

  private void deleteHotRowAndReservationAfterSuccess(UUID id) {
    // Oracle has no multi-table DELETE; remove the hot row (its count decides success) then the
    // job's business-key reservation, mirroring the cancel / terminal-failure cleanup order.
    // language=Oracle
    String deleteHotSql = "DELETE FROM scheduler_job_queue WHERE job_id = ? AND status = 'RUNNING'";
    int deleted =
        ctx.em()
            .createNativeQuery(deleteHotSql)
            .setParameter(1, UuidRawConverter.toBytes(id))
            .executeUpdate();
    if (deleted == 0) {
      throw new IllegalStateException(
          "terminal success updated cold row but failed to remove hot row for job " + id);
    }
    reservations.deleteReservationByOwner(id);
  }
}
