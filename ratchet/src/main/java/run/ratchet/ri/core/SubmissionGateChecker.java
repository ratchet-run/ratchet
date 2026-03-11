package run.ratchet.ri.core;

import run.ratchet.api.JobType;
import run.ratchet.store.dto.JobClaimDto;
import run.ratchet.store.entity.JobEntity;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/**
 * Validates pre-flight conditions before job submission.
 *
 * <p>Checks are performed in order:
 *
 * <ol>
 *   <li>Node draining status (first attempts only)
 *   <li>Rate limit for job type
 *   <li>Executor permit availability
 * </ol>
 *
 * <p>When {@link GateCheckResult#isClear()} returns true, a permit has been acquired from the
 * {@link ThreadPoolManager}. The caller is responsible for ensuring the permit is released, either
 * through job execution or explicit release on failure.
 */
@ApplicationScoped
public class SubmissionGateChecker {

  /**
   * Controller for managing node drain state during graceful shutdown.
   *
   * <p>This dependency is checked first during gate validation to prevent new jobs from starting
   * execution when the node is preparing to shut down.
   */
  private final DrainController drainController;

  /**
   * Rate limiter for controlling job submission frequency per job type.
   *
   * <p>This limits how many jobs of a particular type can be submitted within a time window,
   * preventing burst overload of the executor pools.
   */
  private final JobTypeRateLimiter rateLimiter;

  /**
   * Thread pool manager for acquiring execution permits.
   *
   * <p>When the gate check passes, a permit is acquired from this manager. The caller must ensure
   * the permit is released on completion or failure.
   */
  private final ThreadPoolManager threadPoolManager;

  // Required by CDI proxy
  protected SubmissionGateChecker() {
    this.drainController = null;
    this.rateLimiter = null;
    this.threadPoolManager = null;
  }

  @Inject
  public SubmissionGateChecker(
      DrainController drainController,
      JobTypeRateLimiter rateLimiter,
      ThreadPoolManager threadPoolManager) {
    this.drainController = drainController;
    this.rateLimiter = rateLimiter;
    this.threadPoolManager = threadPoolManager;
  }

  /**
   * Checks all gates for the given job.
   *
   * <p>Gate checks are performed in order of resource scarcity:
   *
   * <ol>
   *   <li>Node draining status (first attempts only) - instant check
   *   <li>Executor permit availability - scarce, bounded resource
   *   <li>Rate limit for job type - resets every 60 seconds
   * </ol>
   *
   * <p>The permit is acquired BEFORE checking rate limits to prevent counter drift: if rate
   * limiting fails after permit acquisition, the permit is released immediately. This ensures the
   * rate limiter only counts jobs that actually proceed to execution.
   *
   * @param job the job to check
   * @param isFirstAttempt true if this is the initial submission, false if from retry buffer
   * @return the gate check result indicating whether submission can proceed
   */
  public GateCheckResult check(JobEntity job, boolean isFirstAttempt) {
    return checkInternal(job.getJobType(), job.getId(), isFirstAttempt);
  }

  /**
   * Checks all gates for the given job claim DTO.
   *
   * <p>This optimized method accepts a lightweight {@link JobClaimDto} instead of the full {@link
   * JobEntity}. Since gate checking only requires jobType and jobId, this avoids the need to load
   * the full entity until execution time.
   *
   * @param claim the job claim DTO to check
   * @param isFirstAttempt true if this is the initial submission, false if from retry buffer
   * @return the gate check result indicating whether submission can proceed
   */
  public GateCheckResult check(JobClaimDto claim, boolean isFirstAttempt) {
    return checkInternal(claim.jobType(), claim.id(), isFirstAttempt);
  }

  /** Internal gate check implementation used by both entity and DTO methods. */
  private GateCheckResult checkInternal(JobType jobType, Long jobId, boolean isFirstAttempt) {
    if (isFirstAttempt && drainController.isDraining()) {
      return GateCheckResult.draining(jobId);
    }

    // Acquire permit FIRST (scarce resource), then check rate limit
    // This prevents rate limiter counter drift when permits are unavailable
    if (!threadPoolManager.tryAcquirePermit(jobType)) {
      return GateCheckResult.noPermits(jobType, jobId);
    }

    // Rate limit check AFTER permit acquisition
    // If rate limited, release the permit immediately to prevent resource leak
    if (!rateLimiter.tryAcquire(jobType)) {
      threadPoolManager.releasePermit(jobType);
      return GateCheckResult.rateLimited(
          jobType, jobId, rateLimiter.getCurrentCount(jobType), rateLimiter.getRateLimit(jobType));
    }

    return GateCheckResult.clear();
  }
}
