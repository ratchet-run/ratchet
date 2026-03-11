package run.ratchet.ri.core;

import run.ratchet.api.JobType;
import run.ratchet.store.dto.JobClaimDto;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobStatus;
import run.ratchet.store.spi.JobCrudStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Handles job submission failures by either buffering for retry or resetting to pending.
 *
 * <p>This service centralizes the failure recovery logic that was previously scattered throughout
 * the submission flow. It handles three types of failures:
 *
 * <ul>
 *   <li><b>Gate failures</b>: Pre-flight check failed (draining, rate limited, no permits). No
 *       permit was acquired, so no permit release needed.
 *   <li><b>Rejections</b>: Executor rejected the task after permit was acquired. Permit must be
 *       released.
 *   <li><b>Unexpected exceptions</b>: Error during submission setup after permit was acquired.
 *       Permit must be released.
 * </ul>
 *
 * <p>Recovery strategy depends on whether this is a first submission attempt or a buffered retry:
 *
 * <ul>
 *   <li><b>First attempt</b>: Reset job to PENDING so other nodes can pick it up
 *   <li><b>Buffered attempt</b>: Try to re-buffer; if buffer full, reset to PENDING
 * </ul>
 */
@ApplicationScoped
public class SubmissionFailureHandler {

  private static final Logger log = Logger.getLogger(SubmissionFailureHandler.class.getName());

  /**
   * Store for loading full job entities when failure handling requires them.
   *
   * <p>When using DTO-based submission, failure handling needs the full entity for retry buffer
   * operations. This store provides lazy loading of the entity only when needed.
   */
  private final JobCrudStore jobCrudStore;

  /**
   * Manager for job state transitions in the database.
   *
   * <p>Used to reset failed jobs to PENDING status, allowing them to be picked up by any node in
   * the cluster for retry.
   */
  private final JobStateManager jobStateManager;

  /**
   * Manager for the in-memory retry buffer.
   *
   * <p>Provides temporary storage for jobs that cannot be immediately submitted or reset to
   * PENDING. Jobs in the buffer will be retried on the next polling interval.
   */
  private final RetryBufferManager retryBufferManager;

  /**
   * Thread pool manager for permit release operations.
   *
   * <p>When a job fails after acquiring a permit (rejection or exception), the permit must be
   * explicitly released to prevent resource leakage.
   */
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
          log.warning(
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
  public void handleRejection(JobEntity job, JobType jobType, boolean isFirstAttempt) {
    threadPoolManager.releasePermit(jobType);

    if (isFirstAttempt) {
      resetToPendingOrBuffer(job);
      log.warning(
          String.format(
              "Executor for %s rejected job %d - returned to PENDING", jobType, job.getId()));
    } else {
      if (retryBufferManager.offer(job)) {
        log.warning(
            String.format(
                "Executor for %s rejected buffered job %d - re-buffering", jobType, job.getId()));
      } else {
        resetToPendingOrBuffer(job);
        log.warning(
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
  public void handleRejection(JobClaimDto claim, JobType jobType) {
    threadPoolManager.releasePermit(jobType);

    // Try reset first (DTO path is always first attempt from Poller)
    if (jobStateManager.resetJobToPending(claim.id())) {
      log.warning(
          String.format(
              "Executor for %s rejected job %d - returned to PENDING", jobType, claim.id()));
      return;
    }

    // Reset failed - load full entity for buffer operations
    JobEntity job = loadJobForBuffer(claim.id());
    if (job != null) {
      retryBufferManager.forceOffer(job);
    }
    log.warning(
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
      JobEntity job, JobType jobType, boolean isFirstAttempt, Exception exception) {
    threadPoolManager.releasePermit(jobType);
    log.log(
        Level.SEVERE,
        "Unexpected exception submitting job " + job.getId() + " - permit released",
        exception);

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
  public void handleUnexpectedException(JobClaimDto claim, JobType jobType, Exception exception) {
    threadPoolManager.releasePermit(jobType);
    log.log(
        Level.SEVERE,
        "Unexpected exception submitting job " + claim.id() + " - permit released",
        exception);

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

  /**
   * Attempts to reset a job to PENDING status for retry, with fallback to forced buffering.
   *
   * <p>This method implements the core recovery mechanism. It first attempts to reset the job to
   * PENDING status in the database, allowing any node in the cluster to pick it up. If the database
   * update fails (e.g., due to concurrent modification), the job is force-offered to the retry
   * buffer as a last resort.
   *
   * <p>The force offer bypasses normal buffer capacity checks but still respects a hard cap to
   * prevent unbounded memory growth. If the hard cap is reached (indicating sustained system
   * failure), the job will be dropped with a critical error log.
   *
   * @param job the job to recover
   */
  private void resetToPendingOrBuffer(JobEntity job) {
    if (!jobStateManager.resetJobToPending(job)) {
      // forceOffer() returns false and logs error if hard cap is reached
      retryBufferManager.forceOffer(job);
    }
  }

  /**
   * Loads a job entity for retry buffer operations with pessimistic locking.
   *
   * <p>This method is called only when reset-to-pending fails and the job needs to be buffered.
   * Uses pessimistic write lock to prevent race conditions where another node might claim the job
   * between the reset failure and entity loading.
   *
   * <p>Returns null if:
   *
   * <ul>
   *   <li>Job cannot be found (may have been deleted concurrently)
   *   <li>Job is no longer RUNNING (was claimed by another node or status changed)
   * </ul>
   *
   * @param jobId the ID of the job to load
   * @return the job entity if found and still RUNNING, or null otherwise
   */
  private JobEntity loadJobForBuffer(Long jobId) {
    return jobCrudStore
        .findByIdForUpdate(jobId)
        .filter(
            job -> {
              // Verify job is still RUNNING - if status changed, another node handled it
              if (job.getStatus() != JobStatus.RUNNING) {
                log.info(
                    "Job "
                        + jobId
                        + " status changed to "
                        + job.getStatus()
                        + " - skipping buffer, another node may have handled it");
                return false;
              }
              return true;
            })
        .orElseGet(
            () -> {
              log.warning(
                  "Job " + jobId + " not found when loading for buffer - may have been deleted");
              return null;
            });
  }
}
