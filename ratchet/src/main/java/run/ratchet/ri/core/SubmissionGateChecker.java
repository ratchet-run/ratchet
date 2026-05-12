package run.ratchet.ri.core;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.UUID;
import run.ratchet.store.dto.JobClaimDto;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;

/**
 * Pre-flight gate checker: drain → permit → rate-limit.
 *
 * <p>A {@code CLEAR} result means a thread-pool permit was acquired and must be handed to either
 * {@link JobExecutorService} for normal release on runner completion or {@link
 * SubmissionFailureHandler} for release on rejection/failure before execution starts.
 */
@ApplicationScoped
public class SubmissionGateChecker {

  private final DrainController drainController;
  private final JobTypeRateLimiter rateLimiter;
  private final ThreadPoolManager threadPoolManager;

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
   * Checks all gates for the given job. On success, ownership of one permit transfers to caller.
   *
   * <p>Retry submissions intentionally bypass drain mode because they already represent claimed
   * work owned by this node. The permit and rate-limit gates are separate best-effort checks; a
   * rate-limited job may hold a permit briefly before this method releases it.
   */
  public GateCheckResult check(JobEntity job, boolean isFirstAttempt) {
    return checkInternal(job.getJobType(), job.getId(), isFirstAttempt);
  }

  public GateCheckResult check(JobClaimDto claim, boolean isFirstAttempt) {
    return checkInternal(claim.jobType(), claim.id(), isFirstAttempt);
  }

  private GateCheckResult checkInternal(
      JobExecutionType jobType, UUID jobId, boolean isFirstAttempt) {
    if (isFirstAttempt && drainController.isDraining()) {
      return GateCheckResult.draining(jobId);
    }

    if (!threadPoolManager.tryAcquirePermit(jobType)) {
      return GateCheckResult.noPermits(jobType, jobId);
    }

    if (!rateLimiter.tryAcquire(jobType)) {
      threadPoolManager.releasePermit(jobType);
      return GateCheckResult.rateLimited(
          jobType, jobId, rateLimiter.getCurrentCount(jobType), rateLimiter.getRateLimit(jobType));
    }

    return GateCheckResult.clear();
  }
}
