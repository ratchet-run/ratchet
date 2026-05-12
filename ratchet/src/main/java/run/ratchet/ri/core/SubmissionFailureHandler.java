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
      ResetOutcome outcome = resetToPendingOrBuffer(job);
      logFirstAttemptGateFailure(job, result, outcome);
    } else {
      if (!retryBufferManager.offer(job)) {
        resetToPendingOrBuffer(job);
        if (result.status() == GateCheckResult.GateStatus.NO_PERMITS) {
          log.warnf(
              "Buffer for %s is full - returning job %s to PENDING", job.getJobType(), job.getId());
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
      log.warnf("Executor for %s rejected job %s - returned to PENDING", jobType, job.getId());
    } else {
      if (retryBufferManager.offer(job)) {
        log.warnf("Executor for %s rejected buffered job %s - re-buffering", jobType, job.getId());
      } else {
        resetToPendingOrBuffer(job);
        log.warnf(
            "Buffer for %s is full - returning rejected job %s to PENDING", jobType, job.getId());
      }
    }
  }

  public void handleRejection(JobClaimDto claim, JobExecutionType jobType) {
    releaseSubmissionPermit(jobType);

    if (bufferClaim(claim)) {
      log.warnf("Executor for %s rejected job %s - buffered locally", jobType, claim.id());
      return;
    }
    if (jobStateManager.resetJobToPending(claim.id())) {
      log.warnf("Executor for %s rejected job %s - returned to PENDING", jobType, claim.id());
      return;
    }
    log.warnf(
        "Executor for %s rejected job %s - neither buffered nor reset cleanly",
        jobType, claim.id());
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

  private ResetOutcome resetToPendingOrBuffer(JobEntity job) {
    if (jobStateManager.resetJobToPending(job)) {
      return ResetOutcome.RESET_TO_PENDING;
    }
    boolean buffered = retryBufferManager.forceOffer(job);
    if (buffered) {
      return ResetOutcome.BUFFERED;
    }
    log.warnf("Job %s was neither reset to PENDING nor buffered", job.getId());
    return ResetOutcome.NOT_RECOVERED;
  }

  private void logFirstAttemptGateFailure(
      JobEntity job, GateCheckResult result, ResetOutcome outcome) {
    switch (outcome) {
      case RESET_TO_PENDING ->
          log.infof("%s - returned job %s to PENDING", result.reason(), job.getId());
      case BUFFERED ->
          log.infof(
              "%s - buffered job %s because reset to PENDING did not apply",
              result.reason(), job.getId());
      case NOT_RECOVERED ->
          log.warnf("%s - job %s was neither reset nor buffered", result.reason(), job.getId());
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

  private enum ResetOutcome {
    RESET_TO_PENDING,
    BUFFERED,
    NOT_RECOVERED
  }
}
