package run.ratchet.store.mysql;

import run.ratchet.api.exception.RatchetTransientStoreException;
import run.ratchet.store.entity.JobStatus;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;

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

  void updateJobStatus(long id, JobStatus status, String errorMessage) {
    ctx.timedStoreOperation(
        "update_status",
        () -> {
          if (MysqlJobRowMapper.isLiveStatus(status)) {
            return ctx.em()
                .createNativeQuery(
                    "UPDATE scheduler_job_queue SET status = ?, last_error = ?, "
                        + "updated_at = NOW(3) WHERE job_id = ?")
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

  boolean compareAndSwapStatus(long id, JobStatus expected, JobStatus newStatus, String error) {
    return ctx.timedStoreOperation(
        "compare_and_swap_status",
        () -> {
          try {
            if (!MysqlJobRowMapper.isLiveStatus(expected)) {
              throw new IllegalArgumentException(
                  "compareAndSwapStatus expected must be a live status; got " + expected);
            }
            if (MysqlJobRowMapper.isLiveStatus(newStatus)) {
              return ctx.em()
                      .createNativeQuery(
                          "UPDATE scheduler_job_queue SET status = ?, last_error = ?, "
                              + "updated_at = NOW(3) WHERE job_id = ? AND status = ?")
                      .setParameter(1, newStatus.name())
                      .setParameter(2, error)
                      .setParameter(3, id)
                      .setParameter(4, expected.name())
                      .executeUpdate()
                  > 0;
            }
            if (newStatus == JobStatus.CANCELED) {
              int gateMatched =
                  ctx.em()
                              .createNativeQuery(
                                  "SELECT COUNT(*) FROM scheduler_job_queue "
                                      + "WHERE job_id = ? AND status = ?")
                              .setParameter(1, id)
                              .setParameter(2, expected.name())
                              .getSingleResult()
                          instanceof Number n
                      ? n.intValue()
                      : 0;
              return gateMatched > 0 && cancelJob(id);
            }
            if (newStatus == JobStatus.FAILED) {
              if (expected != JobStatus.RUNNING) {
                return false;
              }
              return markJobFailedTerminal(id, error, 0);
            }
            throw new IllegalArgumentException("Unsupported CAS target newStatus: " + newStatus);
          } catch (RuntimeException e) {
            throw ctx.translateTransientStoreException("compare-and-swap status", e);
          }
        },
        updated -> updated ? "updated" : "miss");
  }

  int incrementRetryAttempt(long id) {
    int updated =
        ctx.timedStoreOperation(
            "increment_retry_attempt",
            () ->
                ctx.em()
                    .createNativeQuery(
                        "UPDATE scheduler_job_queue SET attempts = attempts + 1, "
                            + "updated_at = NOW(3) "
                            + "WHERE job_id = ? AND status = 'RUNNING'")
                    .setParameter(1, id)
                    .executeUpdate(),
            count -> count > 0 ? "updated" : "miss");
    if (updated == 0) {
      return -1;
    }
    Object result =
        ctx.em()
            .createNativeQuery("SELECT attempts FROM scheduler_job_queue WHERE job_id = ?")
            .setParameter(1, id)
            .getSingleResult();
    return ((Number) result).intValue();
  }

  boolean markJobSucceeded(
      long id,
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
      long id, Instant start, Instant end, Long durationMs, Long queueWaitMs) {
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
      batches.incrementCompletedAtomic(batchId);
    }
    return succeeded;
  }

  boolean scheduleJobRetry(long id, String error, Instant newScheduledTime, int attempts) {
    return ctx.timedStoreOperation(
            "schedule_retry",
            () ->
                ctx.em()
                    .createNativeQuery(
                        "UPDATE scheduler_job_queue SET status = 'PENDING', last_error = ?, "
                            + "scheduled_time = ?, attempts = ?, picked_by = NULL, "
                            + "picked_at = NULL, updated_at = NOW(3) "
                            + "WHERE job_id = ? AND status = 'RUNNING'")
                    .setParameter(1, error)
                    .setParameter(2, Timestamp.from(newScheduledTime))
                    .setParameter(3, attempts)
                    .setParameter(4, id)
                    .executeUpdate(),
            updated -> updated > 0 ? "updated" : "miss")
        > 0;
  }

  boolean markJobFailedTerminal(long id, String terminalError, int totalAttempts) {
    int hotDeleted =
        ctx.em()
            .createNativeQuery(
                "DELETE FROM scheduler_job_queue WHERE job_id = ? AND status = 'RUNNING'")
            .setParameter(1, id)
            .executeUpdate();
    if (hotDeleted == 0) {
      return false;
    }
    ctx.em()
        .createNativeQuery(
            "UPDATE scheduler_job SET terminal_status = 'FAILED', terminal_error = ?, "
                + "total_attempts = ?, terminated_at = NOW(3), execution_end_time = NOW(3) "
                + "WHERE job_id = ? AND terminal_status IS NULL")
        .setParameter(1, terminalError)
        .setParameter(2, totalAttempts)
        .setParameter(3, id)
        .executeUpdate();
    reservations.deleteReservationByOwner(id);
    return true;
  }

  boolean cancelJob(long id) {
    @SuppressWarnings("unchecked")
    List<Object[]> rows =
        ctx.em()
            .createNativeQuery(
                "SELECT job_type, terminal_status, rec_status FROM scheduler_job WHERE job_id = ?")
            .setParameter(1, id)
            .getResultList();
    if (rows.isEmpty()) {
      return false;
    }
    Object[] row = rows.get(0);
    String jobType = (String) row[0];
    String existingTerminal = (String) row[1];
    if (existingTerminal != null) {
      return false;
    }
    if ("RECURRING".equals(jobType)) {
      int updated =
          ctx.em()
              .createNativeQuery(
                  "UPDATE scheduler_job SET rec_status = NULL, terminal_status = 'CANCELED', "
                      + "terminated_at = NOW(3) "
                      + "WHERE job_id = ? AND job_type = 'RECURRING' "
                      + "AND rec_status IS NOT NULL AND terminal_status IS NULL")
              .setParameter(1, id)
              .executeUpdate();
      if (updated == 0) {
        return false;
      }
      reservations.deleteReservationByOwner(id);
      return true;
    }
    ctx.em()
        .createNativeQuery(
            "DELETE FROM scheduler_job_queue WHERE job_id = ? "
                + "AND status IN ('PENDING','RUNNING','PAUSED')")
        .setParameter(1, id)
        .executeUpdate();
    int coldUpdated =
        ctx.em()
            .createNativeQuery(
                "UPDATE scheduler_job SET terminal_status = 'CANCELED', terminated_at = NOW(3) "
                    + "WHERE job_id = ? AND terminal_status IS NULL")
            .setParameter(1, id)
            .executeUpdate();
    reservations.deleteReservationByOwner(id);
    return coldUpdated > 0;
  }

  boolean resetFailedToPending(long id) {
    @SuppressWarnings("unchecked")
    List<Object[]> rows =
        ctx.em()
            .createNativeQuery(
                "SELECT terminal_status, job_type, priority, business_key, timeout_sec, max_retries "
                    + "FROM scheduler_job WHERE job_id = ? FOR UPDATE")
            .setParameter(1, id)
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

    ctx.em()
        .createNativeQuery(
            "UPDATE scheduler_job SET terminal_status = NULL, terminal_error = NULL, "
                + "job_result = NULL, result_type = NULL, "
                + "execution_start_time = NULL, execution_end_time = NULL, "
                + "execution_duration_ms = NULL, queue_wait_ms = NULL, "
                + "total_attempts = NULL, terminated_at = NULL "
                + "WHERE job_id = ? AND terminal_status = 'FAILED'")
        .setParameter(1, id)
        .executeUpdate();

    ctx.em()
        .createNativeQuery(
            "INSERT INTO scheduler_job_queue "
                + "(job_id, status, job_type, priority, scheduled_time, business_key, "
                + "timeout_sec, max_retries, attempts, version, updated_at) "
                + "VALUES (?, 'PENDING', ?, ?, NOW(3), ?, ?, ?, 0, 0, NOW(3))")
        .setParameter(1, id)
        .setParameter(2, jobType)
        .setParameter(3, priority)
        .setParameter(4, businessKey)
        .setParameter(5, timeoutSec)
        .setParameter(6, maxRetries)
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

  private boolean doMarkTerminalSuccessWithResult(
      long id,
      String resultJson,
      String resultType,
      Instant start,
      Instant end,
      Long durationMs,
      Long queueWaitMs) {
    int coldUpdated =
        ctx.em()
            .createNativeQuery(
                "UPDATE scheduler_job c "
                    + "JOIN scheduler_job_queue q ON q.job_id = c.job_id "
                    + "SET c.terminal_status = 'SUCCEEDED', "
                    + "c.job_result = CAST(? AS JSON), c.result_type = ?, "
                    + "c.execution_start_time = ?, c.execution_end_time = ?, "
                    + "c.execution_duration_ms = ?, c.queue_wait_ms = ?, "
                    + "c.total_attempts = q.attempts, c.terminated_at = NOW(3) "
                    + "WHERE c.job_id = ? AND c.terminal_status IS NULL "
                    + "AND q.status = 'RUNNING'")
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
      long id, Instant start, Instant end, Long durationMs, Long queueWaitMs) {
    int coldUpdated =
        ctx.em()
            .createNativeQuery(
                "UPDATE scheduler_job c "
                    + "JOIN scheduler_job_queue q ON q.job_id = c.job_id "
                    + "SET c.terminal_status = 'SUCCEEDED', "
                    + "c.execution_start_time = ?, c.execution_end_time = ?, "
                    + "c.execution_duration_ms = ?, c.queue_wait_ms = ?, "
                    + "c.total_attempts = q.attempts, c.terminated_at = NOW(3) "
                    + "WHERE c.job_id = ? AND c.terminal_status IS NULL "
                    + "AND q.status = 'RUNNING'")
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

  private void deleteHotRowAndReservationAfterSuccess(long id) {
    int deleted =
        ctx.em()
            .createNativeQuery(
                "DELETE q, br FROM scheduler_job_queue q "
                    + "LEFT JOIN scheduler_business_key_reservation br "
                    + "ON br.owner_job_id = q.job_id "
                    + "WHERE q.job_id = ? AND q.status = 'RUNNING'")
            .setParameter(1, id)
            .executeUpdate();
    if (deleted == 0) {
      throw new IllegalStateException(
          "terminal success updated cold row but failed to remove hot row for job " + id);
    }
  }
}
