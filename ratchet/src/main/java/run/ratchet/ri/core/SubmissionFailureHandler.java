package run.ratchet.ri.core;

import run.ratchet.store.dto.JobClaimDto;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.entity.JobStatus;
import run.ratchet.store.spi.JobCrudStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

/**
 * Handles job submission failures by either buffering for retry or resetting to PENDING. Covers
 * gate failures, executor rejections, and unexpected exceptions.
 */
@ApplicationScoped
public class SubmissionFailureHandler {

  private static final Logger log = Logger.getLogger(SubmissionFailureHandler.class);

  private final JobCrudStore jobCrudStore;
  private final JobStateManager jobStateManager;
  private final RetryBufferManager retryBufferManager;
  private final ThreadPoolManager threadPoolManager;

  // Required by CDI proxy
  protected SubmissionFailureHandler() {
    this.jobCrudStore = null;
    this.jobStateManager = null;
    this.retryBufferManager = null;
    this.threadPoolManager = null;
  }

  @Inject
  public SubmissionFailureHandler(
      JobCrudStore jobCrudStore,
      JobStateManager jobStateManager,
      RetryBufferManager retryBufferManager,
      ThreadPoolManager threadPoolManager) {
    this.jobCrudStore = jobCrudStore;
    this.jobStateManager = jobStateManager;
    this.retryBufferManager = retryBufferManager;
    this.threadPoolManager = threadPoolManager;
  }

  /**
   * Handles a gate check failure (draining, rate limited, no permits).
   *
   * <p>No permit was acquired, so no permit release is performed.
   *
   * @param job the job that failed the gate check
   * @param result the gate check result with failure details
   * @param isFirstAttempt true if initial submission, false if from retry buffer
   */
  public void handleGateFailure(JobEntity job, GateCheckResult result, boolean isFirstAttempt) {
    if (isFirstAttempt) {
      resetToPendingOrBuffer(job);
      log.info(result.reason());
    } else {
      if (!retryBufferManager.offer(job)) {
        resetToPendingOrBuffer(job);
        if (result.status() == GateCheckResult.GateStatus.NO_PERMITS) {
          log.warn(
              String.format(
                  "Buffer for %s is full - returning job %d to PENDING",
                  job.getJobType(), job.getId()));
        }
      }
    }
  }

  /**
   * Handles a gate check failure for a DTO-based submission.
   *
   * <p>Loads the full entity only when needed for retry buffer operations. This optimizes the
   * common case where reset-to-pending succeeds without needing the full entity.
   *
   * @param claim the job claim DTO that failed the gate check
   * @param result the gate check result with failure details
   */
  public void handleGateFailure(JobClaimDto claim, GateCheckResult result) {
    // For first attempts (DTO path is always first attempt from Poller), try reset first
    if (jobStateManager.resetJobToPending(claim.id())) {
      log.info(result.reason());
      return;
    }

    // Reset failed - load full entity for buffer operations
    JobEntity job = loadJobForBuffer(claim.id());
    if (job != null) {
      retryBufferManager.forceOffer(job);
    }
    log.info(result.reason());
  }

  /**
   * Handles execution rejection (executor refused the task after permit was acquired).
   *
   * <p>Releases the acquired permit before recovery.
   *
   * @param job the rejected job
   * @param jobType the job type (for permit release)
   * @param isFirstAttempt true if initial submission, false if from retry buffer
   */
  public void handleRejection(JobEntity job, JobExecutionType jobType, boolean isFirstAttempt) {
    threadPoolManager.releasePermit(jobType);

    if (isFirstAttempt) {
      resetToPendingOrBuffer(job);
      log.warn(
          String.format(
              "Executor for %s rejected job %d - returned to PENDING", jobType, job.getId()));
    } else {
      if (retryBufferManager.offer(job)) {
        log.warn(
            String.format(
                "Executor for %s rejected buffered job %d - re-buffering", jobType, job.getId()));
      } else {
        resetToPendingOrBuffer(job);
        log.warn(
            String.format(
                "Buffer for %s is full - returning rejected job %d to PENDING",
                jobType, job.getId()));
      }
    }
  }

  /**
   * Handles execution rejection for a DTO-based submission.
   *
   * <p>Releases the acquired permit and attempts reset-to-pending first to avoid loading the full
   * entity.
   *
   * @param claim the rejected job claim DTO
   * @param jobType the job type (for permit release)
   */
  public void handleRejection(JobClaimDto claim, JobExecutionType jobType) {
    threadPoolManager.releasePermit(jobType);

    // Try reset first (DTO path is always first attempt from Poller)
    if (jobStateManager.resetJobToPending(claim.id())) {
      log.warn(
          String.format(
              "Executor for %s rejected job %d - returned to PENDING", jobType, claim.id()));
      return;
    }

    // Reset failed - load full entity for buffer operations
    JobEntity job = loadJobForBuffer(claim.id());
    if (job != null) {
      retryBufferManager.forceOffer(job);
    }
    log.warn(
        String.format("Executor for %s rejected job %d - buffered for retry", jobType, claim.id()));
  }

  /**
   * Handles unexpected exception during submission setup (after permit was acquired).
   *
   * <p>Releases the acquired permit before recovery.
   *
   * @param job the job that failed
   * @param jobType the job type (for permit release)
   * @param isFirstAttempt true if initial submission, false if from retry buffer
   * @param exception the exception that occurred
   */
  public void handleUnexpectedException(
      JobEntity job, JobExecutionType jobType, boolean isFirstAttempt, Exception exception) {
    threadPoolManager.releasePermit(jobType);
    log.errorf(exception, "Unexpected exception submitting job %s - permit released", job.getId());

    if (isFirstAttempt || !retryBufferManager.offer(job)) {
      resetToPendingOrBuffer(job);
    }
  }

  /**
   * Handles unexpected exception for a DTO-based submission.
   *
   * <p>Releases the acquired permit and attempts reset-to-pending first to avoid loading the full
   * entity.
   *
   * @param claim the job claim DTO that failed
   * @param jobType the job type (for permit release)
   * @param exception the exception that occurred
   */
  public void handleUnexpectedException(
      JobClaimDto claim, JobExecutionType jobType, Exception exception) {
    threadPoolManager.releasePermit(jobType);
    log.errorf(exception, "Unexpected exception submitting job %s - permit released", claim.id());

    // Try reset first (DTO path is always first attempt from Poller)
    if (jobStateManager.resetJobToPending(claim.id())) {
      return;
    }

    // Reset failed - load full entity for buffer operations
    JobEntity job = loadJobForBuffer(claim.id());
    if (job != null) {
      retryBufferManager.forceOffer(job);
    }
  }

  /** Resets to PENDING, falling back to force-buffer if the DB update fails. */
  private void resetToPendingOrBuffer(JobEntity job) {
    if (!jobStateManager.resetJobToPending(job)) {
      // forceOffer() returns false and logs error if hard cap is reached
      retryBufferManager.forceOffer(job);
    }
  }

  private JobEntity loadJobForBuffer(Long jobId) {
    return jobCrudStore
        .findByIdLatest(jobId)
        .filter(
            job -> {
              // Verify job is still RUNNING - if status changed, another node handled it
              if (job.getStatus() != JobStatus.RUNNING) {
                log.infof(
                    "Job %s status changed to %s - skipping buffer, another node may have handled it",
                    jobId, job.getStatus());
                return false;
              }
              return true;
            })
        .orElseGet(
            () -> {
              log.warnf("Job %s not found when loading for buffer - may have been deleted", jobId);
              return null;
            });
  }
}
