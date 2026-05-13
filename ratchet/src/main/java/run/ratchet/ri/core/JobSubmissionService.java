package run.ratchet.ri.core;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import run.ratchet.store.dto.JobClaimDto;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;

/**
 * Orchestrates job submission: checks gates, executes, handles failures.
 *
 * <p>A clear gate check transfers a thread-pool permit to {@link JobExecutorService}. The executor
 * releases that permit in the job runner's {@code finally} block; rejection and pre-execution
 * failure paths release it through {@link SubmissionFailureHandler}.
 */
@ApplicationScoped
public class JobSubmissionService {

  private final SubmissionGateChecker gateChecker;
  private final JobExecutorService executorService;
  private final SubmissionFailureHandler failureHandler;
  private final SubmissionOperations<JobEntity> entitySubmissionOperations =
      new SubmissionOperations<>() {
        @Override
        public JobExecutionType jobType(JobEntity job) {
          return job.getJobType();
        }

        @Override
        public GateCheckResult checkGate(JobEntity job, boolean isFirstAttempt) {
          return gateChecker.check(job, isFirstAttempt);
        }

        @Override
        public ExecutionResult execute(JobEntity job) {
          return executorService.execute(job);
        }

        @Override
        public void handleGateFailure(
            JobEntity job, GateCheckResult gateResult, boolean isFirstAttempt) {
          failureHandler.handleGateFailure(job, gateResult, isFirstAttempt);
        }

        @Override
        public void handleRejection(
            JobEntity job, JobExecutionType jobType, boolean isFirstAttempt) {
          failureHandler.handleRejection(job, jobType, isFirstAttempt);
        }

        @Override
        public void handleUnexpectedException(
            JobEntity job, JobExecutionType jobType, boolean isFirstAttempt, Exception exception) {
          failureHandler.handleUnexpectedException(job, jobType, isFirstAttempt, exception);
        }
      };
  private final SubmissionOperations<JobClaimDto> claimSubmissionOperations =
      new SubmissionOperations<>() {
        @Override
        public JobExecutionType jobType(JobClaimDto claim) {
          return claim.jobType();
        }

        @Override
        public GateCheckResult checkGate(JobClaimDto claim, boolean isFirstAttempt) {
          return gateChecker.check(claim, isFirstAttempt);
        }

        @Override
        public ExecutionResult execute(JobClaimDto claim) {
          return executorService.execute(claim);
        }

        @Override
        public void handleGateFailure(
            JobClaimDto claim, GateCheckResult gateResult, boolean isFirstAttempt) {
          failureHandler.handleGateFailure(claim, gateResult);
        }

        @Override
        public void handleRejection(
            JobClaimDto claim, JobExecutionType jobType, boolean isFirstAttempt) {
          failureHandler.handleRejection(claim, jobType);
        }

        @Override
        public void handleUnexpectedException(
            JobClaimDto claim,
            JobExecutionType jobType,
            boolean isFirstAttempt,
            Exception exception) {
          failureHandler.handleUnexpectedException(claim, jobType, exception);
        }
      };

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
    trySubmit(job, true, entitySubmissionOperations);
  }

  /** Accepts a lightweight {@link JobClaimDto}; full entity loading is deferred until execution. */
  public void submit(JobClaimDto claim) {
    trySubmit(claim, true, claimSubmissionOperations);
  }

  void submitBuffered(JobEntity job) {
    trySubmit(job, false, entitySubmissionOperations);
  }

  void submitBuffered(JobClaimDto claim) {
    // Buffered claims represent already-claimed work owned by this node; they must bypass the
    // drain-mode gate the same way submitBuffered(JobEntity) does.
    trySubmit(claim, false, claimSubmissionOperations);
  }

  private <T> void trySubmit(
      T submission, boolean isFirstAttempt, SubmissionOperations<T> operations) {
    JobExecutionType jobType = operations.jobType(submission);

    GateCheckResult gateResult = operations.checkGate(submission, isFirstAttempt);

    if (gateResult.isBlocked()) {
      operations.handleGateFailure(submission, gateResult, isFirstAttempt);
      return;
    }

    try {
      ExecutionResult execResult = operations.execute(submission);

      if (execResult.isRejected()) {
        operations.handleRejection(submission, jobType, isFirstAttempt);
      }
    } catch (Exception e) {
      operations.handleUnexpectedException(submission, jobType, isFirstAttempt, e);
    }
  }

  private interface SubmissionOperations<T> {
    JobExecutionType jobType(T submission);

    GateCheckResult checkGate(T submission, boolean isFirstAttempt);

    ExecutionResult execute(T submission);

    void handleGateFailure(T submission, GateCheckResult gateResult, boolean isFirstAttempt);

    void handleRejection(T submission, JobExecutionType jobType, boolean isFirstAttempt);

    void handleUnexpectedException(
        T submission, JobExecutionType jobType, boolean isFirstAttempt, Exception exception);
  }
}
