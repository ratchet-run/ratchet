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
package run.ratchet.store.mysql;

import jakarta.persistence.Query;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import run.ratchet.api.JobStatus;
import run.ratchet.api.exception.RatchetTransientStoreException;
import run.ratchet.store.mysql.converter.UuidByteArrayConverter;

/*
 * Keep terminal transitions dialect-local until a shared helper can preserve each backend's
 * SQL shape explicitly. These methods mirror PostgreSQL conceptually, but MySQL binds UUIDs as
 * binary values, uses NOW(3), JSON casts, and multi-table deletes differently.
 *
 * MySQL also keeps timing labels on terminal operations because these paths do more than one SQL
 * statement for hot/cold row moves and business-key cleanup. The labels make slow terminal
 * transitions distinguishable from ordinary write latency in store metrics.
 */
final class MysqlJobTerminalOperations {

  private final MysqlStoreContext ctx;
  private final MysqlBusinessKeyReservations reservations;
  private final MysqlBatchOperations batches;

  MysqlJobTerminalOperations(
      MysqlStoreContext ctx,
      MysqlBusinessKeyReservations reservations,
      MysqlBatchOperations batches) {
    this.ctx = ctx;
    this.reservations = reservations;
    this.batches = batches;
  }

  void updateJobStatus(UUID id, JobStatus status, String errorMessage) {
    ctx.timedStoreOperation(
        "update_status",
        () -> {
          try {
            if (MysqlJobRowMapper.isLiveStatus(status)) {
              // language=MySQL
              String sql =
                  """
                  UPDATE scheduler_job_queue
                  SET status = ?, last_error = ?, updated_at = NOW(3)
                  WHERE job_id = ?
                  """;
              return ctx.em()
                  .createNativeQuery(sql)
                  .setParameter(1, status.name())
                  .setParameter(2, errorMessage)
                  .setParameter(3, UuidByteArrayConverter.toBytes(id))
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
            if (!MysqlJobRowMapper.isLiveStatus(expected)) {
              throw new IllegalArgumentException(
                  "compareAndSwapStatus expected must be a live status; got " + expected);
            }
            if (MysqlJobRowMapper.isLiveStatus(newStatus)) {
              // language=MySQL
              String casSql =
                  """
                  UPDATE scheduler_job_queue
                  SET status = ?, last_error = ?, updated_at = NOW(3)
                  WHERE job_id = ? AND status = ?
                  """;
              return ctx.em()
                      .createNativeQuery(casSql)
                      .setParameter(1, newStatus.name())
                      .setParameter(2, error)
                      .setParameter(3, UuidByteArrayConverter.toBytes(id))
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
      // language=MySQL
      String updateSql =
          """
          UPDATE scheduler_job_queue
          SET attempts = LAST_INSERT_ID(attempts + 1), updated_at = NOW(3)
          WHERE job_id = ? AND status IN ('RUNNING', 'WAITING')
          """;
      int updated =
          ctx.timedStoreOperation(
              "increment_retry_attempt",
              () -> {
                try {
                  return ctx.em()
                      .createNativeQuery(updateSql)
                      .setParameter(1, UuidByteArrayConverter.toBytes(id))
                      .executeUpdate();
                } catch (RuntimeException e) {
                  throw ctx.translateTransientStoreException("increment retry attempt", e);
                }
              },
              count -> count > 0 ? "updated" : "miss");
      if (updated == 0) {
        return -1;
      }
      // LAST_INSERT_ID() is connection-local; the enclosing REQUIRED store transaction pins both
      // native statements to the same MySQL connection.
      // language=MySQL
      String selectSql = "SELECT LAST_INSERT_ID()";
      Object result = ctx.em().createNativeQuery(selectSql).getSingleResult();
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
    // language=MySQL
    String sql =
        """
        UPDATE scheduler_job_queue
        SET status = 'PENDING', last_error = ?, scheduled_time = ?, attempts = ?,
            picked_by = NULL, picked_at = NULL, updated_at = NOW(3)
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
                    .setParameter(4, UuidByteArrayConverter.toBytes(id))
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
    // language=MySQL
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
            .setParameter(1, UuidByteArrayConverter.toBytes(id))
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

  private boolean doCancelJob(UUID id) {
    // language=MySQL
    String cancelNonRecurringSql =
        """
        UPDATE scheduler_job c
        JOIN scheduler_job_queue q ON q.job_id = c.job_id
        SET c.terminal_status = 'CANCELED',
            c.terminated_at = NOW(3),
            c.execution_start_time =
                CASE WHEN q.status = 'RUNNING'
                     THEN COALESCE(c.execution_start_time, q.picked_at, NOW(3))
                     ELSE c.execution_start_time END,
            c.execution_end_time =
                CASE WHEN q.status = 'RUNNING' THEN NOW(3) ELSE c.execution_end_time END,
            c.execution_duration_ms =
                CASE WHEN q.status = 'RUNNING'
                     THEN GREATEST(
                         0,
                         TIMESTAMPDIFF(
                             MICROSECOND,
                             COALESCE(c.execution_start_time, q.picked_at, NOW(3)),
                             NOW(3)) DIV 1000)
                     ELSE c.execution_duration_ms END
        WHERE c.job_id = ? AND c.job_type <> 'RECURRING' AND c.terminal_status IS NULL
          AND q.status IN ('PENDING','RUNNING','PAUSED','WAITING')
        """;
    int nonRecurringUpdated =
        ctx.em()
            .createNativeQuery(cancelNonRecurringSql)
            .setParameter(1, UuidByteArrayConverter.toBytes(id))
            .executeUpdate();
    if (nonRecurringUpdated > 0) {
      // language=MySQL
      String deleteHotSql =
          """
          DELETE FROM scheduler_job_queue
          WHERE job_id = ? AND status IN ('PENDING','RUNNING','PAUSED','WAITING')
          """;
      int hotDeleted =
          ctx.em()
              .createNativeQuery(deleteHotSql)
              .setParameter(1, UuidByteArrayConverter.toBytes(id))
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
    // language=MySQL
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
            .setParameter(1, UuidByteArrayConverter.toBytes(id))
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

    // language=MySQL
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
        .setParameter(1, UuidByteArrayConverter.toBytes(id))
        .executeUpdate();

    // language=MySQL
    String insertHotSql =
        """
        INSERT INTO scheduler_job_queue
          (job_id, status, job_type, priority, scheduled_time, business_key,
           timeout_sec, max_retries, attempts, version, updated_at, execution_target)
        VALUES (?, 'PENDING', ?, ?, NOW(3), ?, ?, ?, 0, 0, NOW(3), ?)
        """;
    ctx.em()
        .createNativeQuery(insertHotSql)
        .setParameter(1, UuidByteArrayConverter.toBytes(id))
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
            businessKey, id, MysqlBusinessKeyReservations.OWNER_TABLE_QUEUE);
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
      // language=MySQL
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

      // language=MySQL
      String insertHotSql =
          """
          INSERT INTO scheduler_job_queue
            (job_id, status, job_type, priority, scheduled_time, business_key,
             timeout_sec, max_retries, attempts, version, updated_at, execution_target)
          SELECT job_id, 'PENDING', job_type, priority, NOW(3), business_key,
                 timeout_sec, max_retries, 0, 0, NOW(3), execution_target
          FROM scheduler_job
          WHERE job_id IN (%s)
          """
              .formatted(placeholders);
      int inserted = bindIds(insertHotSql, ids).executeUpdate();
      if (inserted != ids.size()) {
        throw new IllegalStateException(
            "Bulk retry inserted " + inserted + " queue rows for " + ids.size() + " jobs");
      }

      // language=MySQL
      String reserveSql =
          """
          INSERT INTO scheduler_business_key_reservation
            (business_key, owner_job_id, owner_table, reserved_at)
          SELECT business_key, job_id, 'QUEUE', NOW(3)
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
      query.setParameter(i + 1, UuidByteArrayConverter.toBytes(ids.get(i)));
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
    String attemptsExpression = totalAttempts == null ? "q.attempts" : "?";
    // language=MySQL
    String updateColdSql =
        """
        UPDATE scheduler_job c
        JOIN scheduler_job_queue q ON q.job_id = c.job_id
        SET c.terminal_status = 'FAILED',
            c.terminal_error = ?,
            c.total_attempts = %s,
            c.terminated_at = NOW(3),
            c.execution_start_time =
                CASE WHEN q.status = 'RUNNING'
                     THEN COALESCE(c.execution_start_time, q.picked_at, NOW(3))
                     ELSE c.execution_start_time END,
            c.execution_end_time =
                CASE WHEN q.status = 'RUNNING' THEN NOW(3) ELSE c.execution_end_time END,
            c.execution_duration_ms =
                CASE WHEN q.status = 'RUNNING'
                     THEN GREATEST(
                         0,
                         TIMESTAMPDIFF(
                             MICROSECOND,
                             COALESCE(c.execution_start_time, q.picked_at, NOW(3)),
                             NOW(3)) DIV 1000)
                     ELSE c.execution_duration_ms END
        WHERE c.job_id = ? AND c.terminal_status IS NULL AND q.status = ?
        """
            .formatted(attemptsExpression);
    var query = ctx.em().createNativeQuery(updateColdSql).setParameter(1, terminalError);
    int parameter = 2;
    if (totalAttempts != null) {
      query.setParameter(parameter++, totalAttempts);
    }
    int coldUpdated =
        query
            .setParameter(parameter++, UuidByteArrayConverter.toBytes(id))
            .setParameter(parameter, expectedStatus.name())
            .executeUpdate();
    if (coldUpdated == 0) {
      return false;
    }
    // language=MySQL
    String deleteHotSql = "DELETE FROM scheduler_job_queue WHERE job_id = ? AND status = ?";
    int hotDeleted =
        ctx.em()
            .createNativeQuery(deleteHotSql)
            .setParameter(1, UuidByteArrayConverter.toBytes(id))
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
    // language=MySQL
    String sql =
        """
        UPDATE scheduler_job c
        JOIN scheduler_job_queue q ON q.job_id = c.job_id
        SET c.terminal_status = 'SUCCEEDED',
            c.job_result = CAST(? AS JSON), c.result_type = ?,
            c.execution_start_time = ?, c.execution_end_time = ?,
            c.execution_duration_ms = ?, c.queue_wait_ms = ?,
            c.total_attempts = q.attempts, c.terminated_at = NOW(3)
        WHERE c.job_id = ? AND c.terminal_status IS NULL
          AND q.status = 'RUNNING'
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
            .setParameter(7, UuidByteArrayConverter.toBytes(id))
            .executeUpdate();
    if (coldUpdated == 0) {
      return false;
    }
    deleteHotRowAndReservationAfterSuccess(id);
    return true;
  }

  private boolean doMarkTerminalSuccessMinimal(
      UUID id, Instant start, Instant end, Long durationMs, Long queueWaitMs) {
    // language=MySQL
    String sql =
        """
        UPDATE scheduler_job c
        JOIN scheduler_job_queue q ON q.job_id = c.job_id
        SET c.terminal_status = 'SUCCEEDED',
            c.execution_start_time = ?, c.execution_end_time = ?,
            c.execution_duration_ms = ?, c.queue_wait_ms = ?,
            c.total_attempts = q.attempts, c.terminated_at = NOW(3)
        WHERE c.job_id = ? AND c.terminal_status IS NULL
          AND q.status = 'RUNNING'
        """;
    int coldUpdated =
        ctx.em()
            .createNativeQuery(sql)
            .setParameter(1, start != null ? Timestamp.from(start) : null)
            .setParameter(2, end != null ? Timestamp.from(end) : null)
            .setParameter(3, durationMs)
            .setParameter(4, queueWaitMs)
            .setParameter(5, UuidByteArrayConverter.toBytes(id))
            .executeUpdate();
    if (coldUpdated == 0) {
      return false;
    }
    deleteHotRowAndReservationAfterSuccess(id);
    return true;
  }

  private void deleteHotRowAndReservationAfterSuccess(UUID id) {
    // language=MySQL
    String sql =
        """
        DELETE q, br FROM scheduler_job_queue q
        LEFT JOIN scheduler_business_key_reservation br
          ON br.owner_job_id = q.job_id
        WHERE q.job_id = ? AND q.status = 'RUNNING'
        """;
    int deleted =
        ctx.em()
            .createNativeQuery(sql)
            .setParameter(1, UuidByteArrayConverter.toBytes(id))
            .executeUpdate();
    if (deleted == 0) {
      throw new IllegalStateException(
          "terminal success updated cold row but failed to remove hot row for job " + id);
    }
  }
}
