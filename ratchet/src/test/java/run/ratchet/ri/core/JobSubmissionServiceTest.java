package run.ratchet.ri.core;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import run.ratchet.api.JobStatus;
import run.ratchet.store.dto.JobClaimDto;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;

@ExtendWith(MockitoExtension.class)
class JobSubmissionServiceTest {

  @Mock private SubmissionGateChecker gateChecker;
  @Mock private JobExecutorService executorService;
  @Mock private SubmissionFailureHandler failureHandler;

  private JobSubmissionService service;

  @BeforeEach
  void setUp() {
    service = new JobSubmissionService(gateChecker, executorService, failureHandler);
  }

  @Test
  void submit_entityChecksFirstAttemptGateAndExecutes() {
    JobEntity job = singleJob();
    when(gateChecker.check(job, true)).thenReturn(GateCheckResult.clear());
    when(executorService.execute(job)).thenReturn(ExecutionResult.success(completedFuture()));

    service.submit(job);

    verify(gateChecker).check(job, true);
    verify(executorService).execute(job);
    verify(failureHandler, never()).handleGateFailure(job, GateCheckResult.clear(), true);
  }

  @Test
  void submitBuffered_entityChecksRetryGateAndPreservesRetryFailureHandling() {
    JobEntity job = singleJob();
    GateCheckResult gateResult = GateCheckResult.noPermits(JobExecutionType.SINGLE, job.getId());
    when(gateChecker.check(job, false)).thenReturn(gateResult);

    service.submitBuffered(job);

    verify(gateChecker).check(job, false);
    verify(failureHandler).handleGateFailure(job, gateResult, false);
    verify(executorService, never()).execute(job);
  }

  @Test
  void submitBuffered_claimStillChecksFirstAttemptGate() {
    JobClaimDto claim = singleClaim();
    GateCheckResult gateResult = GateCheckResult.noPermits(JobExecutionType.SINGLE, claim.id());
    when(gateChecker.check(claim, true)).thenReturn(gateResult);

    service.submitBuffered(claim);

    verify(gateChecker).check(claim, true);
    verify(failureHandler).handleGateFailure(claim, gateResult);
    verify(executorService, never()).execute(claim);
  }

  private static JobEntity singleJob() {
    JobEntity job = new JobEntity();
    job.setId(UUID.randomUUID());
    job.setJobType(JobExecutionType.SINGLE);
    return job;
  }

  private static JobClaimDto singleClaim() {
    return new JobClaimDto(
        UUID.randomUUID(),
        JobStatus.RUNNING,
        JobExecutionType.SINGLE,
        null,
        null,
        0,
        30,
        "node-1",
        null,
        null,
        0,
        0);
  }

  private static CompletableFuture<Void> completedFuture() {
    return CompletableFuture.completedFuture(null);
  }
}
