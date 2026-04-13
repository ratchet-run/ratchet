package run.ratchet.ri.core;

import run.ratchet.store.entity.JobExecutionType;

/**
 * Outcome of checking drain, rate-limit, and permit gates before job submission. A CLEAR result
 * means a permit was acquired and must be released through execution or explicit release.
 */
public record GateCheckResult(GateStatus status, String reason) {

  /** All gates passed; a permit has been acquired. */
  public static GateCheckResult clear() {
    return new GateCheckResult(GateStatus.CLEAR, null);
  }

  /** Node is draining (graceful shutdown). */
  public static GateCheckResult draining(Long jobId) {
    return new GateCheckResult(
        GateStatus.DRAINING, "Node draining - returning job " + jobId + " to PENDING");
  }

  /** Thread pool for the given job type is at capacity. */
  public static GateCheckResult noPermits(JobExecutionType jobType, Long jobId) {
    return new GateCheckResult(
        GateStatus.NO_PERMITS,
        String.format(
            "Executor for %s saturated - returning job %d to PENDING for other nodes",
            jobType, jobId));
  }

  /** Per-minute rate limit exceeded for this job type. */
  public static GateCheckResult rateLimited(
      JobExecutionType jobType, Long jobId, int currentCount, int limit) {
    return new GateCheckResult(
        GateStatus.RATE_LIMITED,
        String.format(
            "Rate limit exceeded for %s (current: %d/min, limit: %d/min) - "
                + "returning job %d to PENDING",
            jobType, currentCount, limit, jobId));
  }

  /** True if all gates passed. */
  public boolean isClear() {
    return status == GateStatus.CLEAR;
  }

  public boolean isBlocked() {
    return status != GateStatus.CLEAR;
  }

  /** Possible gate check outcomes. */
  public enum GateStatus {
    /** All gates passed; permit acquired. */
    CLEAR,
    /** Node is draining (graceful shutdown). */
    DRAINING,
    /** Per-minute rate limit exceeded. */
    RATE_LIMITED,
    /** Thread pool at capacity. */
    NO_PERMITS
  }
}
