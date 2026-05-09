package run.ratchet.store.mysql;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobStatus;
import run.ratchet.store.entity.JobExecutionType;

final class MysqlJobCountOperations {

  private final MysqlStoreContext ctx;

  MysqlJobCountOperations(MysqlStoreContext ctx) {
    this.ctx = ctx;
  }

  long countPendingJobs() {
    return countJobsByStatus(JobStatus.PENDING);
  }

  long countJobsByStatus(JobStatus status) {
    if (MysqlJobRowMapper.isLiveStatus(status)) {
      // language=MySQL
      String sql = "SELECT COUNT(*) FROM scheduler_job_queue WHERE status = ?";
      return ctx.countByNative(sql, status.name());
    }
    // language=MySQL
    String sql = "SELECT COUNT(*) FROM scheduler_job WHERE terminal_status = ?";
    return ctx.countByNative(sql, status.name());
  }

  long countActiveJobs(JobExecutionType jobType) {
    // language=MySQL
    String sql =
        """
        SELECT COUNT(*) FROM scheduler_job_queue
        WHERE job_type = ? AND status IN ('PENDING','RUNNING')
        """;
    return ctx.countByNative(sql, jobType.name());
  }

  long countActiveNodes() {
    // language=MySQL
    String sql = "SELECT COUNT(*) FROM scheduler_node";
    return ctx.countByNative(sql);
  }

  long countReadyJobs(Instant now) {
    // language=MySQL
    String sql =
        """
        SELECT COUNT(*) FROM scheduler_job_queue
        WHERE status = 'PENDING' AND scheduled_time <= ?
        """;
    return ctx.countByNative(sql, Timestamp.from(now));
  }

  long countStuckJobs(Instant stuckThreshold) {
    // language=MySQL
    String sql =
        """
        SELECT COUNT(*) FROM scheduler_job_queue
        WHERE status = 'RUNNING' AND picked_at < ?
        """;
    return ctx.countByNative(sql, Timestamp.from(stuckThreshold));
  }

  long countLongRunningJobs(Instant threshold) {
    // language=MySQL
    String sql =
        """
        SELECT COUNT(*) FROM scheduler_job_queue
        WHERE status = 'RUNNING' AND picked_at < ?
        """;
    return ctx.countByNative(sql, Timestamp.from(threshold));
  }

  long countPendingBatchChildren() {
    // language=MySQL
    String sql =
        """
        SELECT COUNT(*) FROM scheduler_job_queue
        WHERE job_type = 'BATCH_CHILD' AND status = 'PENDING'
        """;
    return ctx.countByNative(sql);
  }

  long countPendingJobsByPriority(JobPriority priority) {
    // language=MySQL
    String sql =
        """
        SELECT COUNT(*) FROM scheduler_job_queue
        WHERE status = 'PENDING' AND priority = ?
        """;
    return ctx.countByNative(sql, priority.ordinal());
  }

  long countPendingJobsByType(JobExecutionType jobType) {
    // language=MySQL
    String sql =
        """
        SELECT COUNT(*) FROM scheduler_job_queue
        WHERE status = 'PENDING' AND job_type = ?
        """;
    return ctx.countByNative(sql, jobType.name());
  }

  long countJobsByStatusSince(JobStatus status, Instant since) {
    if (MysqlJobRowMapper.isLiveStatus(status)) {
      // language=MySQL
      String sql =
          """
          SELECT COUNT(*) FROM scheduler_job_queue
          WHERE status = ? AND updated_at >= ?
          """;
      return ctx.countByNative(sql, status.name(), Timestamp.from(since));
    }
    // language=MySQL
    String sql =
        """
        SELECT COUNT(*) FROM scheduler_job
        WHERE terminal_status = ? AND terminated_at >= ?
        """;
    return ctx.countByNative(sql, status.name(), Timestamp.from(since));
  }

  long countJobsWithRetries() {
    // language=MySQL
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
    // language=MySQL
    String sql =
        """
        SELECT COALESCE(
          ((SELECT COUNT(*) FROM scheduler_job_queue
              WHERE attempts > 0 AND updated_at >= ?)
           + (SELECT COUNT(*) FROM scheduler_job
              WHERE total_attempts > 0 AND terminated_at >= ?))
          / NULLIF(
            ((SELECT COUNT(*) FROM scheduler_job_queue WHERE updated_at >= ?)
             + (SELECT COUNT(*) FROM scheduler_job
                WHERE terminated_at >= ?)), 0), 0)
        """;
    return ctx.doubleByNativeOrZero(sql, sinceTs, sinceTs, sinceTs, sinceTs);
  }

  double getAverageProcessingTime(Instant since) {
    // language=MySQL
    String sql =
        """
        SELECT COALESCE(AVG(execution_duration_ms), 0) FROM scheduler_job
        WHERE terminal_status = 'SUCCEEDED' AND execution_duration_ms IS NOT NULL
          AND terminated_at >= ?
        """;
    return ctx.doubleByNativeOrZero(sql, Timestamp.from(since));
  }

  double getAverageBatchSize(Instant since) {
    // language=MySQL
    String sql =
        """
        SELECT COALESCE(AVG(b.total_items), 0) FROM scheduler_batch b
        JOIN scheduler_job c ON c.job_id = b.batch_id
        LEFT JOIN scheduler_job_queue q ON q.job_id = c.job_id
        WHERE COALESCE(q.updated_at, c.terminated_at) >= ?
        """;
    return ctx.doubleByNativeOrZero(sql, Timestamp.from(since));
  }

  Optional<Instant> getOldestPendingJobTime() {
    // language=MySQL
    String sql = "SELECT MIN(scheduled_time) FROM scheduler_job_queue WHERE status = 'PENDING'";
    List<?> results = ctx.em().createNativeQuery(sql).getResultList();
    if (results.isEmpty() || results.get(0) == null) {
      return Optional.empty();
    }
    Object val = results.get(0);
    if (val instanceof Timestamp ts) {
      return Optional.of(ts.toInstant());
    }
    return Optional.empty();
  }

  long getQueueWaitTimePercentile(double percentile) {
    // language=MySQL
    String countSql =
        """
        SELECT COUNT(*) FROM scheduler_job
        WHERE queue_wait_ms IS NOT NULL AND terminal_status = 'SUCCEEDED'
        """;
    Number countResult = (Number) ctx.em().createNativeQuery(countSql).getSingleResult();
    long total = countResult.longValue();
    if (total == 0) {
      return 0L;
    }
    int offset = (int) Math.floor(percentile * total);
    // language=MySQL
    String percentileSql =
        """
        SELECT COALESCE(queue_wait_ms, 0)
        FROM scheduler_job
        WHERE queue_wait_ms IS NOT NULL AND terminal_status = 'SUCCEEDED'
        ORDER BY queue_wait_ms ASC
        LIMIT 1 OFFSET ?1
        """;
    @SuppressWarnings("unchecked")
    List<Object> percentileResults =
        ctx.em().createNativeQuery(percentileSql).setParameter(1, offset).getResultList();
    Object result = percentileResults.stream().findFirst().orElse(0L);
    return ((Number) result).longValue();
  }
}
