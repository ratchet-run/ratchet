package run.ratchet.ri.core;

import java.util.UUID;
import run.ratchet.store.entity.JobExecutionType;

/**
 * Outcome of checking drain, rate-limit, and permit gates before job submission. A CLEAR result
 * means a permit was acquired and must be released through execution or explicit release.
 */
public record GateCheckResult(GateStatus status, String reason) {

  public static GateCheckResult clear() {
    return new GateCheckResult(GateStatus.CLEAR, null);
  }

  public static GateCheckResult draining(UUID jobId) {
    return new GateCheckResult(
        GateStatus.DRAINING, "Node draining - returning job " + jobId + " to PENDING");
  }

  public static GateCheckResult noPermits(JobExecutionType jobType, UUID jobId) {
    return new GateCheckResult(
        GateStatus.NO_PERMITS,
        String.format(
            "Executor for %s saturated - returning job %s to PENDING for other nodes",
            jobType, jobId));
  }

  public static GateCheckResult rateLimited(
      JobExecutionType jobType, UUID jobId, int currentCount, int limit) {
    return new GateCheckResult(
        GateStatus.RATE_LIMITED,
        String.format(
            "Rate limit exceeded for %s (current: %d/min, limit: %d/min) - "
                + "returning job %s to PENDING",
            jobType, currentCount, limit, jobId));
  }

  public boolean isClear() {
    return status == GateStatus.CLEAR;
  }

  public boolean isBlocked() {
    return status != GateStatus.CLEAR;
  }

  public enum GateStatus {
    CLEAR,
    DRAINING,
    RATE_LIMITED,
    NO_PERMITS
  }
}
