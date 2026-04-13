package run.ratchet.ri.core;

import run.ratchet.store.dto.JobClaimDto;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.entity.JobStatus;
import run.ratchet.store.spi.JobCrudStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

/** Handles submission failures by buffering for retry or resetting to PENDING. */
@ApplicationScoped
public class SubmissionFailureHandler {

  private static final Logger log = Logger.getLogger(SubmissionFailureHandler.class);

  private final JobCrudStore jobCrudStore;
  private final JobStateManager jobStateManager;
  private final RetryBufferManager retryBufferManager;
  private final ThreadPoolManager threadPoolManager;

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

  public void handleGateFailure(JobClaimDto claim, GateCheckResult result) {
    if (jobStateManager.resetJobToPending(claim.id())) {
      log.info(result.reason());
      return;
    }

    JobEntity job = loadJobForBuffer(claim.id());
    if (job != null) {
      retryBufferManager.forceOffer(job);
    }
    log.info(result.reason());
  }

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

  public void handleRejection(JobClaimDto claim, JobExecutionType jobType) {
    threadPoolManager.releasePermit(jobType);

    if (jobStateManager.resetJobToPending(claim.id())) {
      log.warn(
          String.format(
              "Executor for %s rejected job %d - returned to PENDING", jobType, claim.id()));
      return;
    }

    JobEntity job = loadJobForBuffer(claim.id());
    if (job != null) {
      retryBufferManager.forceOffer(job);
    }
    log.warn(
        String.format("Executor for %s rejected job %d - buffered for retry", jobType, claim.id()));
  }

  public void handleUnexpectedException(
      JobEntity job, JobExecutionType jobType, boolean isFirstAttempt, Exception exception) {
    threadPoolManager.releasePermit(jobType);
    log.errorf(exception, "Unexpected exception submitting job %s - permit released", job.getId());

    if (isFirstAttempt || !retryBufferManager.offer(job)) {
      resetToPendingOrBuffer(job);
    }
  }

  public void handleUnexpectedException(
      JobClaimDto claim, JobExecutionType jobType, Exception exception) {
    threadPoolManager.releasePermit(jobType);
    log.errorf(exception, "Unexpected exception submitting job %s - permit released", claim.id());

    if (jobStateManager.resetJobToPending(claim.id())) {
      return;
    }

    JobEntity job = loadJobForBuffer(claim.id());
    if (job != null) {
      retryBufferManager.forceOffer(job);
    }
  }

  private void resetToPendingOrBuffer(JobEntity job) {
    if (!jobStateManager.resetJobToPending(job)) {
      retryBufferManager.forceOffer(job);
    }
  }

  private JobEntity loadJobForBuffer(Long jobId) {
    return jobCrudStore
        .findByIdLatest(jobId)
        .filter(
            job -> {
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
