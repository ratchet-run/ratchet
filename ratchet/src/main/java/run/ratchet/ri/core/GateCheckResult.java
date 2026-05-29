package run.ratchet.ri.core;

import java.util.UUID;
import run.ratchet.store.entity.JobExecutionType;

/**
 * Outcome of checking drain, rate-limit, and permit gates before job submission. A CLEAR result
 * means a permit was acquired and must be released through execution or explicit release.
 */
record GateCheckResult(GateStatus status, String reason) {

  static GateCheckResult clear() {
    return new GateCheckResult(GateStatus.CLEAR, null);
  }

  static GateCheckResult draining(UUID jobId) {
    return new GateCheckResult(
        GateStatus.DRAINING, "Node draining - returning job " + jobId + " to PENDING");
  }

  static GateCheckResult noPermits(JobExecutionType jobType, UUID jobId) {
    return new GateCheckResult(
        GateStatus.NO_PERMITS,
        String.format(
            "Executor for %s saturated - returning job %s to PENDING for other nodes",
            jobType, jobId));
  }

  static GateCheckResult rateLimited(
      JobExecutionType jobType, UUID jobId, int currentCount, int limit) {
    return new GateCheckResult(
        GateStatus.RATE_LIMITED,
        String.format(
            "Rate limit exceeded for %s (current: %d/min, limit: %d/min) - "
                + "returning job %s to PENDING",
            jobType, currentCount, limit, jobId));
  }

  boolean isClear() {
    return status == GateStatus.CLEAR;
  }

  boolean isBlocked() {
    return status != GateStatus.CLEAR;
  }

  enum GateStatus {
    CLEAR,
    DRAINING,
    RATE_LIMITED,
    NO_PERMITS
  }
}
