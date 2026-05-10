package run.ratchet.api;

import java.time.Instant;
import java.util.Map;

/**
 * Point-in-time snapshot of queue health metrics for monitoring dashboards.
 *
 * <p>All counts reflect the store state at the moment the snapshot was taken. The snapshot is not
 * transactionally consistent across all fields — counts are best-effort reads.
 *
 * @param pendingCount jobs waiting to execute
 * @param runningCount jobs currently executing
 * @param failedCount jobs in terminal FAILED state
 * @param succeededCount jobs in terminal SUCCEEDED state
 * @param canceledCount jobs in terminal CANCELED state
 * @param pausedCount jobs currently paused
 * @param stuckCount running jobs whose pickup timestamp is suspiciously old
 * @param readyCount pending jobs whose scheduled time is &lt;= now
 * @param retryRate fraction of recently updated jobs that have retried at least once (0.0–1.0)
 * @param avgProcessingTimeMs average execution duration for recently succeeded jobs, in ms
 * @param p95QueueWaitMs 95th-percentile queue wait time for recently succeeded jobs, in ms
 * @param oldestPendingJobTime scheduled time of the oldest pending job, or null if no pending jobs
 * @param pendingByType pending job count keyed by public job type
 * @param pendingByPriority pending job count keyed by priority
 */
public record QueueHealthSnapshot(
    long pendingCount,
    long runningCount,
    long failedCount,
    long succeededCount,
    long canceledCount,
    long pausedCount,
    long stuckCount,
    long readyCount,
    double retryRate,
    double avgProcessingTimeMs,
    long p95QueueWaitMs,
    @Nullable Instant oldestPendingJobTime,
    Map<JobType, Long> pendingByType,
    Map<JobPriority, Long> pendingByPriority) {
  public QueueHealthSnapshot {
    pendingByType = pendingByType == null ? null : Map.copyOf(pendingByType);
    pendingByPriority = pendingByPriority == null ? null : Map.copyOf(pendingByPriority);
  }
}
