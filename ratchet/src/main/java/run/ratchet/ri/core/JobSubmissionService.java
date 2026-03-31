package run.ratchet.ri.core;

import run.ratchet.store.dto.JobClaimDto;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/**
 * Orchestrates job submission by coordinating gate checks, execution, and failure handling.
 *
 * <p>This service delegates to specialized components:
 *
 * <ul>
 *   <li>{@link SubmissionGateChecker}: Validates pre-flight conditions
 *   <li>{@link JobExecutorService}: Handles actual job execution
 *   <li>{@link SubmissionFailureHandler}: Manages recovery when submission fails
 * </ul>
 *
 * <p>The submission flow:
 *
 * <ol>
 *   <li>Check all gates - if any fail, handle the failure and return
 *   <li>Execute the job - if rejected, handle the rejection
 *   <li>Any unexpected exception is handled by the failure handler
 * </ol>
 */
@ApplicationScoped
public class JobSubmissionService {

  /** Gate checker for validating pre-flight conditions before job submission. */
  private final SubmissionGateChecker gateChecker;

  /** Executor service that handles the actual job execution. */
  private final JobExecutorService executorService;

  /** Handler for various submission failure scenarios. */
  private final SubmissionFailureHandler failureHandler;

  // Required by CDI proxy
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

  /**
   * Submits a job for execution.
   *
   * @param job the job to submit
   */
  public void submit(JobEntity job) {
    trySubmit(job, true);
  }

  /**
   * Submits a job claim for execution.
   *
   * <p>This optimized method accepts a lightweight {@link JobClaimDto} instead of the full {@link
   * JobEntity}. The gate check and executor dispatch use only DTO fields (jobType, id), deferring
   * full entity loading until actual job execution.
   *
   * @param claim the job claim DTO to submit
   */
  public void submit(JobClaimDto claim) {
    trySubmit(claim);
  }

  /**
   * Submits a buffered job for execution (from retry buffer).
   *
   * @param job the job to submit
   */
  void submitBuffered(JobEntity job) {
    trySubmit(job, false);
  }

  /**
   * Attempts to submit a job for execution.
   *
   * @param job the job entity to submit for execution
   * @param isFirstAttempt true if this is the initial submission attempt
   */
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

  /**
   * Attempts to submit a job claim for execution using only DTO data.
   *
   * @param claim the job claim DTO to submit
   */
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
