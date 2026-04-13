package run.ratchet.ri.core;

import run.ratchet.store.dto.JobClaimDto;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/** Orchestrates job submission: checks gates, executes, handles failures. */
@ApplicationScoped
public class JobSubmissionService {

  private final SubmissionGateChecker gateChecker;
  private final JobExecutorService executorService;
  private final SubmissionFailureHandler failureHandler;

  protected JobSubmissionService() {
    this.gateChecker = null;
    this.executorService = null;
    this.failureHandler = null;
  }

  @Inject
  public JobSubmissionService(
      SubmissionGateChecker gateChecker,
      JobExecutorService executorService,
      SubmissionFailureHandler failureHandler) {
    this.gateChecker = gateChecker;
    this.executorService = executorService;
    this.failureHandler = failureHandler;
  }

  public void submit(JobEntity job) {
    trySubmit(job, true);
  }

  /** Accepts a lightweight {@link JobClaimDto}; full entity loading is deferred until execution. */
  public void submit(JobClaimDto claim) {
    trySubmit(claim);
  }

  void submitBuffered(JobEntity job) {
    trySubmit(job, false);
  }

  private void trySubmit(JobEntity job, boolean isFirstAttempt) {
    JobExecutionType jobType = job.getJobType();

    GateCheckResult gateResult = gateChecker.check(job, isFirstAttempt);

    if (gateResult.isBlocked()) {
      failureHandler.handleGateFailure(job, gateResult, isFirstAttempt);
      return;
    }

    try {
      ExecutionResult execResult = executorService.execute(job);

      if (execResult.isRejected()) {
        failureHandler.handleRejection(job, jobType, isFirstAttempt);
      }
    } catch (Exception e) {
      failureHandler.handleUnexpectedException(job, jobType, isFirstAttempt, e);
    }
  }

  private void trySubmit(JobClaimDto claim) {
    JobExecutionType jobType = claim.jobType();

    GateCheckResult gateResult = gateChecker.check(claim, true);

    if (gateResult.isBlocked()) {
      failureHandler.handleGateFailure(claim, gateResult);
      return;
    }

    try {
      ExecutionResult execResult = executorService.execute(claim);

      if (execResult.isRejected()) {
        failureHandler.handleRejection(claim, jobType);
      }
    } catch (Exception e) {
      failureHandler.handleUnexpectedException(claim, jobType, e);
    }
  }
}
