package run.ratchet.store.mysql;

import run.ratchet.api.exception.RatchetTransientStoreException;
import run.ratchet.store.entity.JobStatus;
import run.ratchet.store.spi.JobBatchStatusStore;
import run.ratchet.store.spi.JobPauseStore;
import run.ratchet.store.spi.JobRetryStore;
import run.ratchet.store.spi.JobTerminalStore;
import jakarta.persistence.Query;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.jboss.logging.Logger;

final class MysqlJobLifecycleOperations
    implements JobBatchStatusStore, JobRetryStore, JobTerminalStore, JobPauseStore {

  private static final Logger log = Logger.getLogger(MysqlJobLifecycleOperations.class);

  private final MysqlStoreContext ctx;
  private final MysqlBusinessKeyReservations reservations;
  private final MysqlBatchOperations batches;

  MysqlJobLifecycleOperations(
      MysqlStoreContext ctx,
      MysqlBusinessKeyReservations reservations,
      MysqlBatchOperations batches) {
    this.ctx = ctx;
    this.reservations = reservations;
    this.batches = batches;
  }

  @Override
  public void updateJobStatus(long id, JobStatus status, String errorMessage) {
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

  @Override
  public boolean compareAndSwapStatus(
      long id, JobStatus expected, JobStatus newStatus, String error) {
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

  @Override
  public int incrementRetryAttempt(long id) {
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

  @Override
  public boolean tryPickUpJob(long id, String nodeId) {
    return ctx.timedStoreOperation(
            "pickup_job",
            () ->
                ctx.em()
                    .createNativeQuery(
                        "UPDATE scheduler_job_queue SET status = 'RUNNING', picked_by = ?, "
                            + "picked_at = NOW(3), updated_at = NOW(3) "
                            + "WHERE job_id = ? AND status = 'PENDING'")
                    .setParameter(1, nodeId)
                    .setParameter(2, id)
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

  @Override
  public boolean markJobSucceededMinimal(
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
      batches.incrementCompletedAtomic(batchId);
    }
    return succeeded;
  }

  @Override
  public boolean scheduleJobRetry(long id, String error, Instant newScheduledTime, int attempts) {
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

  @Override
  public boolean markJobFailedTerminal(long id, String terminalError, int totalAttempts) {
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

  @Override
  public boolean cancelJob(long id) {
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

  @Override
  public boolean resetRunningJob(long id, String nodeId) {
    return ctx.timedStoreOperation(
            "reset_running_job",
            () ->
                ctx.em()
                    .createNativeQuery(
                        "UPDATE scheduler_job_queue SET status = 'PENDING', picked_by = NULL, "
                            + "picked_at = NULL, updated_at = NOW(3) "
                            + "WHERE job_id = ? AND status = 'RUNNING' AND picked_by = ?")
                    .setParameter(1, id)
                    .setParameter(2, nodeId)
                    .executeUpdate(),
            updated -> updated > 0 ? "updated" : "miss")
        > 0;
  }

  @Override
  public int resetRunningJobs(String nodeId) {
    return ctx.timedStoreOperation(
        "reset_running_jobs",
        () ->
            ctx.em()
                .createNativeQuery(
                    "UPDATE scheduler_job_queue SET status = 'PENDING', picked_by = NULL, "
                        + "picked_at = NULL, updated_at = NOW(3) "
                        + "WHERE status = 'RUNNING' AND picked_by = ?")
                .setParameter(1, nodeId)
                .executeUpdate(),
        updated -> updated > 0 ? "updated" : "miss");
  }

  @Override
  public int cancelRecurringJobsByTag(String tag) {
    @SuppressWarnings("unchecked")
    List<Number> ids =
        ctx.em()
            .createNativeQuery(
                "SELECT j.job_id FROM scheduler_job j "
                    + "JOIN scheduler_job_tag t ON j.job_id = t.job_id "
                    + "WHERE t.tag = ? AND j.job_type = 'RECURRING' "
                    + "AND j.rec_status IS NOT NULL AND j.terminal_status IS NULL")
            .setParameter(1, tag)
            .getResultList();
    return cancelRecurringByIds(ids);
  }

  @Override
  public int cancelRecurringJobByBusinessKey(String businessKey) {
    @SuppressWarnings("unchecked")
    List<Number> ids =
        ctx.em()
            .createNativeQuery(
                "SELECT job_id FROM scheduler_job "
                    + "WHERE business_key = ? AND job_type = 'RECURRING' "
                    + "AND rec_status IS NOT NULL AND terminal_status IS NULL")
            .setParameter(1, businessKey)
            .getResultList();
    return cancelRecurringByIds(ids);
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
        ctx.em()
            .createNativeQuery(
                "SELECT job_id FROM scheduler_job WHERE job_type = 'RECURRING' "
                    + "AND rec_status IS NOT NULL AND terminal_status IS NULL "
                    + "AND created_at < ? AND business_key IS NOT NULL "
                    + "AND business_key NOT IN ("
                    + placeholders
                    + ")");
    int parameter = 1;
    query.setParameter(parameter++, Timestamp.from(nodeStartTime));
    for (String registeredId : idsList) {
      query.setParameter(parameter++, registeredId);
    }
    @SuppressWarnings("unchecked")
    List<Number> ids = query.getResultList();
    return cancelRecurringByIds(ids);
  }

  private int cancelRecurringByIds(List<Number> idRows) {
    if (idRows.isEmpty()) {
      return 0;
    }
    int total = 0;
    for (Number n : idRows) {
      long id = n.longValue();
      int updated =
          ctx.em()
              .createNativeQuery(
                  "UPDATE scheduler_job SET rec_status = NULL, terminal_status = 'CANCELED', "
                      + "terminated_at = NOW(3) "
                      + "WHERE job_id = ? AND job_type = 'RECURRING' "
                      + "AND rec_status IS NOT NULL AND terminal_status IS NULL")
              .setParameter(1, id)
              .executeUpdate();
      if (updated > 0) {
        reservations.deleteReservationByOwner(id);
        total += updated;
      }
    }
    return total;
  }

  @Override
  public boolean resetFailedToPending(long id) {
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

  @Override
  public boolean transitionToPaused(long id, JobStatus expected) {
    if (expected == JobStatus.PAUSED) {
      throw new IllegalArgumentException("transitionToPaused expects expected != PAUSED");
    }
    if (!MysqlJobRowMapper.isLiveStatus(expected)) {
      log.debugf(
          "transitionToPaused(%d, %s) is a no-op post hot/cold-split — terminal jobs cannot be paused",
          id, expected);
      return false;
    }
    int updated =
        ctx.em()
            .createNativeQuery(
                "UPDATE scheduler_job_queue SET status = 'PAUSED', "
                    + "paused_from_status = ?, updated_at = NOW(3) "
                    + "WHERE job_id = ? AND status = ?")
            .setParameter(1, expected.name())
            .setParameter(2, id)
            .setParameter(3, expected.name())
            .executeUpdate();
    return updated > 0;
  }

  @Override
  public boolean transitionFromPaused(long id, JobStatus target) {
    if (!MysqlJobRowMapper.isLiveStatus(target) || target == JobStatus.PAUSED) {
      throw new IllegalArgumentException(
          "transitionFromPaused expects a non-PAUSED live status; got " + target);
    }
    int updated =
        ctx.em()
            .createNativeQuery(
                "UPDATE scheduler_job_queue SET status = ?, "
                    + "paused_from_status = NULL, updated_at = NOW(3) "
                    + "WHERE job_id = ? AND status = 'PAUSED'")
            .setParameter(1, target.name())
            .setParameter(2, id)
            .executeUpdate();
    return updated > 0;
  }

  @Override
  public boolean pauseRecurring(long id) {
    int updated =
        ctx.em()
            .createNativeQuery(
                "UPDATE scheduler_job SET rec_status = 'A' "
                    + "WHERE job_id = ? AND job_type = 'RECURRING' "
                    + "AND rec_status = 'P' AND terminal_status IS NULL")
            .setParameter(1, id)
            .executeUpdate();
    return updated > 0;
  }

  @Override
  public boolean resumeRecurring(long id) {
    int updated =
        ctx.em()
            .createNativeQuery(
                "UPDATE scheduler_job SET rec_status = 'P' "
                    + "WHERE job_id = ? AND job_type = 'RECURRING' "
                    + "AND rec_status = 'A' AND terminal_status IS NULL")
            .setParameter(1, id)
            .executeUpdate();
    return updated > 0;
  }

  @Override
  public JobStatus transitionFromPausedAtomic(long id) {
    List<?> results =
        ctx.em()
            .createNativeQuery(
                "SELECT paused_from_status FROM scheduler_job_queue "
                    + "WHERE job_id = ? AND status = 'PAUSED' FOR UPDATE")
            .setParameter(1, id)
            .getResultList();
    if (results.isEmpty()) {
      return null;
    }
    String pausedFrom = (String) results.get(0);
    JobStatus target = pausedFrom != null ? JobStatus.valueOf(pausedFrom) : JobStatus.PENDING;
    int updated =
        ctx.em()
            .createNativeQuery(
                "UPDATE scheduler_job_queue SET status = ?, "
                    + "paused_from_status = NULL, updated_at = NOW(3) "
                    + "WHERE job_id = ? AND status = 'PAUSED'")
            .setParameter(1, target.name())
            .setParameter(2, id)
            .executeUpdate();
    return updated > 0 ? target : null;
  }
}
