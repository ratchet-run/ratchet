package run.ratchet.store.postgresql;

import run.ratchet.api.JobPriority;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.entity.JobStatus;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

final class PostgresqlJobCountOperations {

  private final PostgresqlStoreContext ctx;

  PostgresqlJobCountOperations(PostgresqlStoreContext ctx) {
    this.ctx = ctx;
  }

  long countPendingJobs() {
    return countJobsByStatus(JobStatus.PENDING);
  }

  long countJobsByStatus(JobStatus status) {
    if (PostgresqlJobRowMapper.isLiveStatus(status)) {
      return ctx.countByNative(
          "SELECT COUNT(*) FROM scheduler_job_queue WHERE status = ?", status.name());
    }
    return ctx.countByNative(
        "SELECT COUNT(*) FROM scheduler_job WHERE terminal_status = ?", status.name());
  }

  long countActiveJobs(JobExecutionType jobType) {
    return ctx.countByNative(
        "SELECT COUNT(*) FROM scheduler_job_queue "
            + "WHERE job_type = ? AND status IN ('PENDING','RUNNING')",
        jobType.name());
  }

  long countActiveNodes() {
    return ctx.countByNative("SELECT COUNT(*) FROM scheduler_node");
  }

  long countReadyJobs(Instant now) {
    return ctx.countByNative(
        "SELECT COUNT(*) FROM scheduler_job_queue "
            + "WHERE status = 'PENDING' AND scheduled_time <= ?",
        Timestamp.from(now));
  }

  long countStuckJobs(Instant stuckThreshold) {
    return ctx.countByNative(
        "SELECT COUNT(*) FROM scheduler_job_queue " + "WHERE status = 'RUNNING' AND picked_at < ?",
        Timestamp.from(stuckThreshold));
  }

  long countLongRunningJobs(Instant threshold) {
    return ctx.countByNative(
        "SELECT COUNT(*) FROM scheduler_job_queue " + "WHERE status = 'RUNNING' AND picked_at < ?",
        Timestamp.from(threshold));
  }

  long countPendingBatchChildren() {
    return ctx.countByNative(
        "SELECT COUNT(*) FROM scheduler_job_queue "
            + "WHERE job_type = 'BATCH_CHILD' AND status = 'PENDING'");
  }

  long countPendingJobsByPriority(JobPriority priority) {
    return ctx.countByNative(
        "SELECT COUNT(*) FROM scheduler_job_queue " + "WHERE status = 'PENDING' AND priority = ?",
        priority.ordinal());
  }

  long countPendingJobsByType(JobExecutionType jobType) {
    return ctx.countByNative(
        "SELECT COUNT(*) FROM scheduler_job_queue " + "WHERE status = 'PENDING' AND job_type = ?",
        jobType.name());
  }

  long countJobsByStatusSince(JobStatus status, Instant since) {
    if (PostgresqlJobRowMapper.isLiveStatus(status)) {
      return ctx.countByNative(
          "SELECT COUNT(*) FROM scheduler_job_queue WHERE status = ? AND updated_at >= ?",
          status.name(),
          Timestamp.from(since));
    }
    return ctx.countByNative(
        "SELECT COUNT(*) FROM scheduler_job WHERE terminal_status = ? AND terminated_at >= ?",
        status.name(),
        Timestamp.from(since));
  }

  long countJobsWithRetries() {
    return ctx.countByNative(
        "SELECT "
            + "(SELECT COUNT(*) FROM scheduler_job_queue WHERE attempts > 0) "
            + "+ (SELECT COUNT(*) FROM scheduler_job WHERE total_attempts > 0)");
  }

  double getRetryRateStats(Instant since) {
    Timestamp sinceTs = Timestamp.from(since);
    Object result =
        ctx.em()
            .createNativeQuery(
                "SELECT COALESCE("
                    + "  CAST("
                    + "    ((SELECT COUNT(*) FROM scheduler_job_queue "
                    + "        WHERE attempts > 0 AND updated_at >= ?) "
                    + "     + (SELECT COUNT(*) FROM scheduler_job "
                    + "        WHERE total_attempts > 0 AND terminated_at >= ?))"
                    + "    AS DOUBLE PRECISION) "
                    + "  / NULLIF("
                    + "    ((SELECT COUNT(*) FROM scheduler_job_queue WHERE updated_at >= ?) "
                    + "     + (SELECT COUNT(*) FROM scheduler_job "
                    + "        WHERE terminated_at >= ?)), 0), 0)")
            .setParameter(1, sinceTs)
            .setParameter(2, sinceTs)
            .setParameter(3, sinceTs)
            .setParameter(4, sinceTs)
            .getSingleResult();
    return result == null ? 0.0 : ((Number) result).doubleValue();
  }

  double getAverageProcessingTime(Instant since) {
    Object result =
        ctx.em()
            .createNativeQuery(
                "SELECT COALESCE(AVG(execution_duration_ms), 0) FROM scheduler_job "
                    + "WHERE terminal_status = 'SUCCEEDED' AND execution_duration_ms IS NOT NULL "
                    + "AND terminated_at >= ?")
            .setParameter(1, Timestamp.from(since))
            .getSingleResult();
    return result == null ? 0.0 : ((Number) result).doubleValue();
  }

  double getAverageBatchSize(Instant since) {
    Object result =
        ctx.em()
            .createNativeQuery(
                "SELECT COALESCE(AVG(b.total_items), 0) FROM scheduler_batch b "
                    + "JOIN scheduler_job c ON c.job_id = b.batch_id "
                    + "LEFT JOIN scheduler_job_queue q ON q.job_id = c.job_id "
                    + "WHERE COALESCE(q.updated_at, c.terminated_at) >= ?")
            .setParameter(1, Timestamp.from(since))
            .getSingleResult();
    return result == null ? 0.0 : ((Number) result).doubleValue();
  }

  @SuppressWarnings("unchecked")
  Optional<Instant> getOldestPendingJobTime() {
    List<Object> results =
        ctx.em()
            .createNativeQuery(
                "SELECT MIN(scheduled_time) FROM scheduler_job_queue WHERE status = 'PENDING'")
            .getResultList();
    if (results.isEmpty() || results.get(0) == null) {
      return Optional.empty();
    }
    return Optional.ofNullable(PostgresqlJobRowMapper.toInstant(results.get(0)));
  }

  long getQueueWaitTimePercentile(double percentile) {
    Object result =
        ctx.em()
            .createNativeQuery(
                "SELECT COALESCE(PERCENTILE_CONT(?) WITHIN GROUP (ORDER BY queue_wait_ms), 0) "
                    + "FROM scheduler_job WHERE queue_wait_ms IS NOT NULL "
                    + "AND terminal_status = 'SUCCEEDED'")
            .setParameter(1, percentile)
            .getSingleResult();
    return result == null ? 0L : ((Number) result).longValue();
  }
}
