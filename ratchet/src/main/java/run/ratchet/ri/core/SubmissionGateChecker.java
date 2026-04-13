package run.ratchet.ri.core;

import run.ratchet.store.dto.JobClaimDto;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/**
 * Pre-flight gate checker: drain → permit → rate-limit. A CLEAR result holds an acquired permit.
 */
@ApplicationScoped
public class SubmissionGateChecker {

  private final DrainController drainController;
  private final JobTypeRateLimiter rateLimiter;
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
   * Checks all gates for the given job. On success a permit has been acquired and the caller must
   * release it.
   */
  public GateCheckResult check(JobEntity job, boolean isFirstAttempt) {
    return checkInternal(job.getJobType(), job.getId(), isFirstAttempt);
  }

  /** DTO overload of {@link #check(JobEntity, boolean)}. */
  public GateCheckResult check(JobClaimDto claim, boolean isFirstAttempt) {
    return checkInternal(claim.jobType(), claim.id(), isFirstAttempt);
  }

  /** Internal gate check implementation used by both entity and DTO methods. */
  private GateCheckResult checkInternal(
      JobExecutionType jobType, Long jobId, boolean isFirstAttempt) {
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
