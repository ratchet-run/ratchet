package run.ratchet.ri.core;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.store.dto.JobClaimDto;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;

/** Handles submission failures by buffering for retry or resetting to PENDING. */
@ApplicationScoped
public class SubmissionFailureHandler {

  private static final Logger log = Logger.getLogger(SubmissionFailureHandler.class);

  private final JobStateManager jobStateManager;
  private final RetryBufferManager retryBufferManager;
  private final ThreadPoolManager threadPoolManager;
  private final PollerScheduler pollerScheduler;
  private final MetricsCollector metricsCollector;

  protected SubmissionFailureHandler() {
    this.jobStateManager = null;
    this.retryBufferManager = null;
    this.threadPoolManager = null;
    this.pollerScheduler = null;
    this.metricsCollector = null;
  }

  @Inject
  public SubmissionFailureHandler(
      JobStateManager jobStateManager,
      RetryBufferManager retryBufferManager,
      ThreadPoolManager threadPoolManager,
      PollerScheduler pollerScheduler,
      MetricsCollector metricsCollector) {
    this.jobStateManager = jobStateManager;
    this.retryBufferManager = retryBufferManager;
    this.threadPoolManager = threadPoolManager;
    this.pollerScheduler = pollerScheduler;
    this.metricsCollector = metricsCollector;
  }

  public void handleGateFailure(JobEntity job, GateCheckResult result, boolean isFirstAttempt) {
    recordGateRejected(job.getJobType(), result);
    if (isFirstAttempt) {
      resetToPendingOrBuffer(job);
      log.info(result.reason());
    } else {
      if (!retryBufferManager.offer(job)) {
        resetToPendingOrBuffer(job);
        if (result.status() == GateCheckResult.GateStatus.NO_PERMITS) {
          log.warn(
              String.format(
                  "Buffer for %s is full - returning job %s to PENDING",
                  job.getJobType(), job.getId()));
        }
      }
    }
  }

  public void handleGateFailure(JobClaimDto claim, GateCheckResult result) {
    recordGateRejected(claim.jobType(), result);
    if (bufferClaim(claim)) {
      log.info(result.reason());
      return;
    }
    jobStateManager.resetJobToPending(claim.id());
    log.info(result.reason());
  }

  public void handleRejection(JobEntity job, JobExecutionType jobType, boolean isFirstAttempt) {
    releaseSubmissionPermit(jobType);

    if (isFirstAttempt) {
      resetToPendingOrBuffer(job);
      log.warn(
          String.format(
              "Executor for %s rejected job %s - returned to PENDING", jobType, job.getId()));
    } else {
      if (retryBufferManager.offer(job)) {
        log.warn(
            String.format(
                "Executor for %s rejected buffered job %s - re-buffering", jobType, job.getId()));
      } else {
        resetToPendingOrBuffer(job);
        log.warn(
            String.format(
                "Buffer for %s is full - returning rejected job %s to PENDING",
                jobType, job.getId()));
      }
    }
  }

  public void handleRejection(JobClaimDto claim, JobExecutionType jobType) {
    releaseSubmissionPermit(jobType);

    if (bufferClaim(claim)) {
      log.warn(
          String.format("Executor for %s rejected job %s - buffered locally", jobType, claim.id()));
      return;
    }
    if (jobStateManager.resetJobToPending(claim.id())) {
      log.warn(
          String.format(
              "Executor for %s rejected job %s - returned to PENDING", jobType, claim.id()));
      return;
    }
    log.warn(
        String.format(
            "Executor for %s rejected job %s - neither buffered nor reset cleanly",
            jobType, claim.id()));
  }

  public void handleUnexpectedException(
      JobEntity job, JobExecutionType jobType, boolean isFirstAttempt, Exception exception) {
    releaseSubmissionPermit(jobType);
    log.errorf(exception, "Unexpected exception submitting job %s - permit released", job.getId());

    if (isFirstAttempt || !retryBufferManager.offer(job)) {
      resetToPendingOrBuffer(job);
    }
  }

  public void handleUnexpectedException(
      JobClaimDto claim, JobExecutionType jobType, Exception exception) {
    releaseSubmissionPermit(jobType);
    log.errorf(exception, "Unexpected exception submitting job %s - permit released", claim.id());

    if (bufferClaim(claim)) {
      return;
    }
    jobStateManager.resetJobToPending(claim.id());
  }

  private void resetToPendingOrBuffer(JobEntity job) {
    if (!jobStateManager.resetJobToPending(job)) {
      retryBufferManager.forceOffer(job);
    }
  }

  private void releaseSubmissionPermit(JobExecutionType jobType) {
    threadPoolManager.releasePermit(jobType);
    pollerScheduler.wakeup();
  }

  private void recordGateRejected(JobExecutionType jobType, GateCheckResult result) {
    if (metricsCollector != null && result != null && result.isBlocked()) {
      metricsCollector.gateRejected(jobType.name(), result.status().name());
    }
  }

  private boolean bufferClaim(JobClaimDto claim) {
    return retryBufferManager.offer(claim);
  }
}
