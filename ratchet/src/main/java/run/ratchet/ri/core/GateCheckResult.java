package run.ratchet.ri.core;

import run.ratchet.api.JobType;

/**
 * Represents the result of checking all submission gates before job execution.
 *
 * <p>The job scheduler implements multiple "gates" that a job must pass before being submitted to a
 * thread pool for execution. This record encapsulates the outcome of those gate checks along with a
 * human-readable explanation when a gate blocks execution.
 *
 * <p>Gates are checked in the following order:
 *
 * <ol>
 *   <li><b>Drain Gate:</b> Ensures the node is not in drain mode (graceful shutdown)
 *   <li><b>Rate Limit Gate:</b> Ensures the job type hasn't exceeded its per-minute limit
 *   <li><b>Permit Gate:</b> Ensures thread pool capacity is available for the job type
 * </ol>
 *
 * <p><b>Important Resource Management:</b> When status is {@link GateStatus#CLEAR}, a permit has
 * been acquired from the {@link ThreadPoolManager}. This permit MUST be released either:
 *
 * <ul>
 *   <li>Automatically through successful job execution completion
 *   <li>Explicitly via permit release if execution is aborted before starting
 * </ul>
 *
 * Failure to release permits will cause thread pool starvation.
 *
 * <p>Usage pattern:
 *
 * <pre>{@code
 * GateCheckResult gate = gateChecker.check(job);
 * if (gate.isClear()) {
 *     try {
 *         executor.submit(job);
 *     } catch (Exception e) {
 *         permitManager.release(job.getJobType()); // Release on failure
 *     }
 * } else {
 *     log.debug("Gate blocked: " + gate.reason());
 *     returnJobToQueue(job);
 * }
 * }</pre>
 *
 * <p>Thread Safety: This record is immutable and thread-safe.
 *
 * @param status the {@link GateStatus} indicating which gate blocked (or CLEAR if all passed)
 * @param reason human-readable explanation of why the gate blocked, or null if CLEAR
 * @see DrainController for drain mode management
 * @see ThreadPoolManager for permit management
 */
public record GateCheckResult(GateStatus status, String reason) {

  /**
   * Creates a successful gate check result indicating all gates passed.
   *
   * <p>This factory method should only be called after successfully acquiring an executor permit.
   * The caller is responsible for ensuring permit release.
   *
   * @return a new GateCheckResult with CLEAR status and no reason message
   */
  public static GateCheckResult clear() {
    return new GateCheckResult(GateStatus.CLEAR, null);
  }

  /**
   * Creates a result indicating the node is in drain mode.
   *
   * <p>Drain mode is activated during graceful shutdown or maintenance windows. Jobs blocked by
   * this gate should be returned to PENDING status so they can be picked up by other nodes in the
   * cluster.
   *
   * @param jobId the database ID of the job being rejected; used in the reason message for logging
   *     and debugging
   * @return a new GateCheckResult with DRAINING status
   */
  public static GateCheckResult draining(Long jobId) {
    return new GateCheckResult(
        GateStatus.DRAINING, "Node draining - returning job " + jobId + " to PENDING");
  }

  /**
   * Creates a result indicating no executor permits are available.
   *
   * <p>Each job type has a dedicated thread pool with a maximum capacity. When all threads are busy
   * and no permits are available, jobs are blocked at this gate. Blocked jobs should be returned to
   * PENDING status so they can be picked up by nodes with available capacity.
   *
   * <p>This is a normal condition under high load and enables work-stealing across cluster nodes
   * for better load distribution.
   *
   * @param jobType the {@link JobType} with no available executor capacity
   * @param jobId the database ID of the job being rejected
   * @return a new GateCheckResult with NO_PERMITS status
   */
  public static GateCheckResult noPermits(JobType jobType, Long jobId) {
    return new GateCheckResult(
        GateStatus.NO_PERMITS,
        String.format(
            "Executor for %s saturated - returning job %d to PENDING for other nodes",
            jobType, jobId));
  }

  /**
   * Creates a result indicating the job type's rate limit has been exceeded.
   *
   * <p>Rate limiting prevents any single job type from monopolizing cluster resources. Jobs blocked
   * by this gate should be returned to PENDING status with a slight delay before retry, or
   * redirected to other nodes.
   *
   * @param jobType the {@link JobType} that exceeded its rate limit
   * @param jobId the database ID of the job being rejected
   * @param currentCount the current number of jobs processed in the time window
   * @param limit the configured maximum jobs per minute for this type
   * @return a new GateCheckResult with RATE_LIMITED status and detailed reason
   */
  public static GateCheckResult rateLimited(
      JobType jobType, Long jobId, int currentCount, int limit) {
    return new GateCheckResult(
        GateStatus.RATE_LIMITED,
        String.format(
            "Rate limit exceeded for %s (current: %d/min, limit: %d/min) - "
                + "returning job %d to PENDING",
            jobType, currentCount, limit, jobId));
  }

  /**
   * Checks whether all gates passed and the job can proceed to execution.
   *
   * <p>When this returns {@code true}:
   *
   * <ul>
   *   <li>The node is not draining
   *   <li>Rate limits have not been exceeded
   *   <li>A thread pool permit has been successfully acquired
   * </ul>
   *
   * <p><b>Important:</b> A {@code true} result means a permit has been allocated and must be
   * properly released through job execution or explicit release.
   *
   * @return {@code true} if all gates passed and the job can execute, {@code false} if any gate
   *     blocked execution
   */
  public boolean isClear() {
    return status == GateStatus.CLEAR;
  }

  /**
   * Enumeration of possible gate check outcomes.
   *
   * <p>Each status represents either successful passage through all gates (CLEAR) or the specific
   * gate that blocked job execution. The status can be used to determine appropriate handling:
   *
   * <ul>
   *   <li>CLEAR: Proceed with execution
   *   <li>DRAINING: Wait for drain to complete or redirect to another node
   *   <li>RATE_LIMITED: Apply backoff or redirect to another node
   *   <li>NO_PERMITS: Redirect to another node with available capacity
   * </ul>
   */
  public enum GateStatus {
    /**
     * All gates passed and an executor permit has been acquired. The job is ready for immediate
     * execution.
     */
    CLEAR,

    /**
     * The node is in drain mode and not accepting new jobs. Typically occurs during graceful
     * shutdown or maintenance.
     */
    DRAINING,

    /**
     * The job type has exceeded its configured rate limit. Too many jobs of this type have been
     * processed in the current time window.
     */
    RATE_LIMITED,

    /**
     * No executor permits are available for this job type. The thread pool for this job type is at
     * capacity.
     */
    NO_PERMITS
  }
}
