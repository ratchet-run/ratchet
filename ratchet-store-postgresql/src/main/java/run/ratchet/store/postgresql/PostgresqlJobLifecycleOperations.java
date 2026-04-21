package run.ratchet.store.postgresql;

import run.ratchet.store.entity.JobStatus;
import run.ratchet.store.spi.JobPauseStore;
import run.ratchet.store.spi.JobRetryStore;
import run.ratchet.store.spi.JobStatusStore;
import run.ratchet.store.spi.JobTerminalStore;
import jakarta.persistence.Query;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

final class PostgresqlJobLifecycleOperations
    implements JobStatusStore, JobTerminalStore, JobRetryStore, JobPauseStore {

  private final PostgresqlStoreContext ctx;
  private final PostgresqlBusinessKeyReservations reservations;
  private final PostgresqlBatchOperations batches;

  PostgresqlJobLifecycleOperations(
      PostgresqlStoreContext ctx,
      PostgresqlBusinessKeyReservations reservations,
      PostgresqlBatchOperations batches) {
    this.ctx = ctx;
    this.reservations = reservations;
    this.batches = batches;
  }

  @Override
  public void updateJobStatus(long id, JobStatus status, String errorMessage) {
    int updated =
        ctx.em()
            .createNativeQuery(
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
          ctx.em()
              .createNativeQuery(
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
      throw ctx.translateTransientStoreException("compare-and-swap status", e);
    }
  }

  @Override
  public int incrementRetryAttempt(long id) {
    List<?> results =
        ctx.em()
            .createNativeQuery(
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
        ctx.em()
            .createNativeQuery(
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
          ctx.em()
              .createNativeQuery(
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
      throw ctx.translateTransientStoreException("mark job succeeded", e);
    }
  }

  @Override
  public boolean markJobSucceededMinimal(
      long id, Instant start, Instant end, Long durationMs, Long queueWaitMs) {
    try {
      int updated =
          ctx.em()
              .createNativeQuery(
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
      throw ctx.translateTransientStoreException("mark job succeeded minimally", e);
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
      batches.incrementCompletedAtomic(batchId);
    }
    return jobUpdated;
  }

  @Override
  public boolean markJobFailedTerminal(long id, String terminalError, int totalAttempts) {
    int updated =
        ctx.em()
            .createNativeQuery(
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
        ctx.em()
            .createNativeQuery(
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
  public boolean scheduleJobRetry(long id, String error, Instant newScheduledTime, int attempts) {
    List<?> updated =
        ctx.em()
            .createNativeQuery(
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
  public boolean resetFailedToPending(long id) {
    List<?> updated =
        ctx.em()
            .createNativeQuery(
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
  public boolean resetRunningJob(long id, String nodeId) {
    int updated =
        ctx.em()
            .createNativeQuery(
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
    return ctx.em()
        .createNativeQuery(
            "UPDATE scheduler_job SET status = 'PENDING', "
                + "picked_by = NULL, picked_at = NULL, "
                + "updated_at = statement_timestamp() "
                + "WHERE status = 'RUNNING' AND picked_by = ?")
        .setParameter(1, nodeId)
        .executeUpdate();
  }

  @Override
  @SuppressWarnings("unchecked")
  public int cancelRecurringJobsByTag(String tag) {
    List<Number> canceled =
        ctx.em()
            .createNativeQuery(
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
  @SuppressWarnings("unchecked")
  public int cancelRecurringJobByBusinessKey(String businessKey) {
    List<Number> canceled =
        ctx.em()
            .createNativeQuery(
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
        ctx.em()
            .createNativeQuery(
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
  public boolean transitionToPaused(long id, JobStatus expected) {
    int updated =
        ctx.em()
            .createNativeQuery(
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
        ctx.em()
            .createNativeQuery(
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
        ctx.em()
            .createNativeQuery(
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
  public boolean pauseRecurring(long id) {
    int updated =
        ctx.em()
            .createNativeQuery(
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
        ctx.em()
            .createNativeQuery(
                "UPDATE scheduler_job SET status = 'PENDING', paused_from_status = NULL, "
                    + "updated_at = statement_timestamp() "
                    + "WHERE job_id = ? AND job_type = 'RECURRING' AND status = 'PAUSED'")
            .setParameter(1, id)
            .executeUpdate();
    return updated > 0;
  }
}
