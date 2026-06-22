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

import java.sql.Timestamp;
import java.time.Instant;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobStatus;
import run.ratchet.store.entity.JobExecutionType;

final class OracleJobCountOperations {

  private final OracleStoreContext ctx;

  OracleJobCountOperations(OracleStoreContext ctx) {
    this.ctx = ctx;
  }

  long countPendingJobs() {
    return countJobsByStatus(JobStatus.PENDING);
  }

  long countJobsByStatus(JobStatus status) {
    if (OracleJobRowMapper.isLiveStatus(status)) {
      // language=Oracle
      String sql = "SELECT COUNT(*) FROM scheduler_job_queue WHERE status = ?";
      return ctx.countByNative(sql, status.name());
    }
    // language=Oracle
    String sql = "SELECT COUNT(*) FROM scheduler_job WHERE terminal_status = ?";
    return ctx.countByNative(sql, status.name());
  }

  @SuppressWarnings("unchecked")
  Map<JobStatus, Long> countJobsByStatuses() {
    // language=Oracle
    String sql =
        """
        SELECT status, COUNT(*) FROM scheduler_job_queue
        GROUP BY status
        UNION ALL
        SELECT terminal_status, COUNT(*) FROM scheduler_job
        WHERE terminal_status IS NOT NULL
        GROUP BY terminal_status
        """;
    List<Object[]> rows = ctx.em().createNativeQuery(sql).getResultList();
    Map<JobStatus, Long> counts = new EnumMap<>(JobStatus.class);
    for (Object[] row : rows) {
      counts.merge(JobStatus.valueOf((String) row[0]), ((Number) row[1]).longValue(), Long::sum);
    }
    return counts;
  }

  long countActiveJobs(JobExecutionType jobType) {
    // language=Oracle
    String sql =
        """
        SELECT COUNT(*) FROM scheduler_job_queue
        WHERE job_type = ? AND status IN ('PENDING','RUNNING')
        """;
    return ctx.countByNative(sql, jobType.name());
  }

  long countActiveNodes() {
    // language=Oracle
    String sql = "SELECT COUNT(*) FROM scheduler_node";
    return ctx.countByNative(sql);
  }

  long countReadyJobs(Instant now) {
    // language=Oracle
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
    // language=Oracle
    String sql =
        """
        SELECT COUNT(*) FROM scheduler_job_queue
        WHERE status = 'RUNNING' AND picked_at < ?
        """;
    return ctx.countByNative(sql, OracleTimestamps.microTimestamp(threshold));
  }

  long countPendingBatchChildren() {
    // language=Oracle
    String sql =
        """
        SELECT COUNT(*) FROM scheduler_job_queue
        WHERE job_type = 'BATCH_CHILD' AND status = 'PENDING'
        """;
    return ctx.countByNative(sql);
  }

  long countPendingJobsByPriority(JobPriority priority) {
    // language=Oracle
    String sql =
        """
        SELECT COUNT(*) FROM scheduler_job_queue
        WHERE status = 'PENDING' AND priority = ?
        """;
    return ctx.countByNative(sql, priority.ordinal());
  }

  @SuppressWarnings("unchecked")
  Map<JobPriority, Long> countPendingJobsByPriorities() {
    // language=Oracle
    String sql =
        """
        SELECT priority, COUNT(*)
        FROM scheduler_job_queue
        WHERE status = 'PENDING'
        GROUP BY priority
        """;
    List<Object[]> rows = ctx.em().createNativeQuery(sql).getResultList();
    Map<JobPriority, Long> counts = new EnumMap<>(JobPriority.class);
    JobPriority[] values = JobPriority.values();
    for (Object[] row : rows) {
      int ordinal = ((Number) row[0]).intValue();
      if (ordinal >= 0 && ordinal < values.length) {
        counts.put(values[ordinal], ((Number) row[1]).longValue());
      }
    }
    return counts;
  }

  long countPendingJobsByType(JobExecutionType jobType) {
    // language=Oracle
    String sql =
        """
        SELECT COUNT(*) FROM scheduler_job_queue
        WHERE status = 'PENDING' AND job_type = ?
        """;
    return ctx.countByNative(sql, jobType.name());
  }

  @SuppressWarnings("unchecked")
  Map<JobExecutionType, Long> countPendingJobsByTypes() {
    // language=Oracle
    String sql =
        """
        SELECT job_type, COUNT(*)
        FROM scheduler_job_queue
        WHERE status = 'PENDING'
        GROUP BY job_type
        """;
    List<Object[]> rows = ctx.em().createNativeQuery(sql).getResultList();
    Map<JobExecutionType, Long> counts = new EnumMap<>(JobExecutionType.class);
    for (Object[] row : rows) {
      counts.put(JobExecutionType.valueOf((String) row[0]), ((Number) row[1]).longValue());
    }
    return counts;
  }

  long countJobsByStatusSince(JobStatus status, Instant since) {
    if (OracleJobRowMapper.isLiveStatus(status)) {
      // language=Oracle
      String sql =
          """
          SELECT COUNT(*) FROM scheduler_job_queue
          WHERE status = ? AND updated_at >= ?
          """;
      return ctx.countByNative(sql, status.name(), OracleTimestamps.microTimestamp(since));
    }
    // language=Oracle
    String sql =
        """
        SELECT COUNT(*) FROM scheduler_job
        WHERE terminal_status = ? AND terminated_at >= ?
        """;
    return ctx.countByNative(sql, status.name(), OracleTimestamps.microTimestamp(since));
  }

  long countJobsWithRetries() {
    // language=Oracle
    String sql =
        """
        SELECT
          (SELECT COUNT(*) FROM scheduler_job_queue WHERE attempts > 0)
          + (SELECT COUNT(*) FROM scheduler_job WHERE total_attempts > 0)
        """;
    return ctx.countByNative(sql);
  }

  double getRetryRateStats(Instant since) {
    Timestamp sinceTs = OracleTimestamps.microTimestamp(since);
    // language=Oracle
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
    // language=Oracle
    String sql =
        """
        SELECT COALESCE(AVG(execution_duration_ms), 0) FROM scheduler_job
        WHERE terminal_status = 'SUCCEEDED' AND execution_duration_ms IS NOT NULL
          AND terminated_at >= ?
        """;
    return ctx.doubleByNativeOrZero(sql, OracleTimestamps.microTimestamp(since));
  }

  double getAverageBatchSize(Instant since) {
    // language=Oracle
    String sql =
        """
        SELECT COALESCE(AVG(b.total_items), 0) FROM scheduler_batch b
        JOIN scheduler_job c ON c.job_id = b.batch_id
        LEFT JOIN scheduler_job_queue q ON q.job_id = c.job_id
        WHERE COALESCE(q.updated_at, c.terminated_at) >= ?
        """;
    return ctx.doubleByNativeOrZero(sql, OracleTimestamps.microTimestamp(since));
  }

  Optional<Instant> getOldestPendingJobTime() {
    // language=Oracle
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
    if (Double.isNaN(percentile) || percentile < 0.0 || percentile > 1.0) {
      throw new IllegalArgumentException("percentile must be in [0.0, 1.0]: " + percentile);
    }
    // language=Oracle
    String percentileSql =
        """
        SELECT queue_wait_ms
        FROM (
          SELECT queue_wait_ms, CUME_DIST() OVER (ORDER BY queue_wait_ms ASC) AS percentile_rank
          FROM scheduler_job
          WHERE queue_wait_ms IS NOT NULL AND terminal_status = 'SUCCEEDED'
        ) ranked
        WHERE percentile_rank >= ?
        ORDER BY queue_wait_ms ASC
        FETCH FIRST 1 ROWS ONLY
        """;
    @SuppressWarnings("unchecked")
    List<Object> percentileResults =
        ctx.em().createNativeQuery(percentileSql).setParameter(1, percentile).getResultList();
    Object result = percentileResults.stream().findFirst().orElse(0L);
    return ((Number) result).longValue();
  }
}
