package run.ratchet.store.postgresql;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobStatus;
import run.ratchet.store.entity.JobExecutionType;

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
      // language=PostgreSQL
      String sql = "SELECT COUNT(*) FROM scheduler_job_queue WHERE status = ?";
      return ctx.countByNative(sql, status.name());
    }
    // language=PostgreSQL
    String sql = "SELECT COUNT(*) FROM scheduler_job WHERE terminal_status = ?";
    return ctx.countByNative(sql, status.name());
  }

  long countActiveJobs(JobExecutionType jobType) {
    // language=PostgreSQL
    String sql =
        """
        SELECT COUNT(*) FROM scheduler_job_queue
        WHERE job_type = ? AND status IN ('PENDING','RUNNING')
        """;
    return ctx.countByNative(sql, jobType.name());
  }

  long countActiveNodes() {
    // language=PostgreSQL
    String sql = "SELECT COUNT(*) FROM scheduler_node";
    return ctx.countByNative(sql);
  }

  long countReadyJobs(Instant now) {
    // language=PostgreSQL
    String sql =
        """
        SELECT COUNT(*) FROM scheduler_job_queue
        WHERE status = 'PENDING' AND scheduled_time <= ?
        """;
    return ctx.countByNative(sql, Timestamp.from(now));
  }

  long countStuckJobs(Instant stuckThreshold) {
    return countRunningJobsPickedBefore(stuckThreshold);
  }

  long countLongRunningJobs(Instant threshold) {
    return countRunningJobsPickedBefore(threshold);
  }

  private long countRunningJobsPickedBefore(Instant threshold) {
    // language=PostgreSQL
    String sql =
        """
        SELECT COUNT(*) FROM scheduler_job_queue
        WHERE status = 'RUNNING' AND picked_at < ?
        """;
    return ctx.countByNative(sql, Timestamp.from(threshold));
  }

  long countPendingBatchChildren() {
    // language=PostgreSQL
    String sql =
        """
        SELECT COUNT(*) FROM scheduler_job_queue
        WHERE job_type = 'BATCH_CHILD' AND status = 'PENDING'
        """;
    return ctx.countByNative(sql);
  }

  long countPendingJobsByPriority(JobPriority priority) {
    // language=PostgreSQL
    String sql =
        """
        SELECT COUNT(*) FROM scheduler_job_queue
        WHERE status = 'PENDING' AND priority = ?
        """;
    return ctx.countByNative(sql, priority.ordinal());
  }

  long countPendingJobsByType(JobExecutionType jobType) {
    // language=PostgreSQL
    String sql =
        """
        SELECT COUNT(*) FROM scheduler_job_queue
        WHERE status = 'PENDING' AND job_type = ?
        """;
    return ctx.countByNative(sql, jobType.name());
  }

  long countJobsByStatusSince(JobStatus status, Instant since) {
    if (PostgresqlJobRowMapper.isLiveStatus(status)) {
      // language=PostgreSQL
      String sql = "SELECT COUNT(*) FROM scheduler_job_queue WHERE status = ? AND updated_at >= ?";
      return ctx.countByNative(sql, status.name(), Timestamp.from(since));
    }
    // language=PostgreSQL
    String sql =
        "SELECT COUNT(*) FROM scheduler_job WHERE terminal_status = ? AND terminated_at >= ?";
    return ctx.countByNative(sql, status.name(), Timestamp.from(since));
  }

  long countJobsWithRetries() {
    // language=PostgreSQL
    String sql =
        """
        SELECT
          (SELECT COUNT(*) FROM scheduler_job_queue WHERE attempts > 0)
          + (SELECT COUNT(*) FROM scheduler_job WHERE total_attempts > 0)
        """;
    return ctx.countByNative(sql);
  }

  double getRetryRateStats(Instant since) {
    Timestamp sinceTs = Timestamp.from(since);
    // language=PostgreSQL
    String sql =
        """
        SELECT COALESCE(
          CAST(
            ((SELECT COUNT(*) FROM scheduler_job_queue
                WHERE attempts > 0 AND updated_at >= ?)
             + (SELECT COUNT(*) FROM scheduler_job
                WHERE total_attempts > 0 AND terminated_at >= ?))
            AS DOUBLE PRECISION)
          / NULLIF(
            ((SELECT COUNT(*) FROM scheduler_job_queue WHERE updated_at >= ?)
             + (SELECT COUNT(*) FROM scheduler_job
                WHERE terminated_at >= ?)), 0), 0)
        """;
    Object result =
        ctx.em()
            .createNativeQuery(sql)
            .setParameter(1, sinceTs)
            .setParameter(2, sinceTs)
            .setParameter(3, sinceTs)
            .setParameter(4, sinceTs)
            .getSingleResult();
    return result == null ? 0.0 : ((Number) result).doubleValue();
  }

  double getAverageProcessingTime(Instant since) {
    // language=PostgreSQL
    String sql =
        """
        SELECT COALESCE(AVG(execution_duration_ms), 0) FROM scheduler_job
        WHERE terminal_status = 'SUCCEEDED' AND execution_duration_ms IS NOT NULL
          AND terminated_at >= ?
        """;
    Object result =
        ctx.em().createNativeQuery(sql).setParameter(1, Timestamp.from(since)).getSingleResult();
    return result == null ? 0.0 : ((Number) result).doubleValue();
  }

  double getAverageBatchSize(Instant since) {
    // language=PostgreSQL
    String sql =
        """
        SELECT COALESCE(AVG(b.total_items), 0) FROM scheduler_batch b
        JOIN scheduler_job c ON c.job_id = b.batch_id
        LEFT JOIN scheduler_job_queue q ON q.job_id = c.job_id
        WHERE COALESCE(q.updated_at, c.terminated_at) >= ?
        """;
    Object result =
        ctx.em().createNativeQuery(sql).setParameter(1, Timestamp.from(since)).getSingleResult();
    return result == null ? 0.0 : ((Number) result).doubleValue();
  }

  @SuppressWarnings("unchecked")
  Optional<Instant> getOldestPendingJobTime() {
    // language=PostgreSQL
    String sql = "SELECT MIN(scheduled_time) FROM scheduler_job_queue WHERE status = 'PENDING'";
    List<Object> results = ctx.em().createNativeQuery(sql).getResultList();
    if (results.isEmpty() || results.get(0) == null) {
      return Optional.empty();
    }
    return Optional.ofNullable(PostgresqlJobRowMapper.toInstant(results.get(0)));
  }

  long getQueueWaitTimePercentile(double percentile) {
    // language=PostgreSQL
    String sql =
        """
        SELECT COALESCE(PERCENTILE_CONT(?) WITHIN GROUP (ORDER BY queue_wait_ms), 0)
        FROM scheduler_job WHERE queue_wait_ms IS NOT NULL
          AND terminal_status = 'SUCCEEDED'
        """;
    Object result = ctx.em().createNativeQuery(sql).setParameter(1, percentile).getSingleResult();
    return result == null ? 0L : ((Number) result).longValue();
  }
}
