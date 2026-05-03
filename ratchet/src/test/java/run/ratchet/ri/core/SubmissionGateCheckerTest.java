package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import run.ratchet.store.dto.JobClaimDto;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.entity.JobStatus;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SubmissionGateCheckerTest {

  @Mock private DrainController drainController;
  @Mock private JobTypeRateLimiter rateLimiter;
  @Mock private ThreadPoolManager threadPoolManager;

  private SubmissionGateChecker gateChecker;

  @BeforeEach
  void setUp() {
    gateChecker = new SubmissionGateChecker(drainController, rateLimiter, threadPoolManager);
  }

  @Test
  void check_draining_firstAttempt_returnsDraining() {
    when(drainController.isDraining()).thenReturn(true);

    GateCheckResult result = gateChecker.check(singleJob(), true);

    assertEquals(GateCheckResult.GateStatus.DRAINING, result.status());
    verify(threadPoolManager, never()).tryAcquirePermit(any());
    verify(rateLimiter, never()).tryAcquire(any());
  }

  @Test
  void check_draining_notFirstAttempt_skipsDrainGate() {
    // isFirstAttempt=false short-circuits the drain condition before isDraining() is called,
    // so no isDraining stub is needed. Permit and rate checks still run normally.
    when(threadPoolManager.tryAcquirePermit(JobExecutionType.SINGLE)).thenReturn(true);
    when(rateLimiter.tryAcquire(JobExecutionType.SINGLE)).thenReturn(true);

    GateCheckResult result = gateChecker.check(singleJob(), false);

    assertTrue(result.isClear(), "retry should bypass the drain gate");
  }

  @Test
  void check_noPermits_returnsNoPermits() {
    when(drainController.isDraining()).thenReturn(false);
    when(threadPoolManager.tryAcquirePermit(JobExecutionType.SINGLE)).thenReturn(false);

    GateCheckResult result = gateChecker.check(singleJob(), true);

    assertEquals(GateCheckResult.GateStatus.NO_PERMITS, result.status());
    verify(rateLimiter, never()).tryAcquire(any());
  }

  @Test
  void check_rateLimited_releasesPermitBeforeReturning() {
    // This is the critical invariant: if rate-limited, the permit acquired from threadPoolManager
    // must be released to prevent permanent Semaphore exhaustion.
    when(drainController.isDraining()).thenReturn(false);
    when(threadPoolManager.tryAcquirePermit(JobExecutionType.SINGLE)).thenReturn(true);
    when(rateLimiter.tryAcquire(JobExecutionType.SINGLE)).thenReturn(false);
    when(rateLimiter.getCurrentCount(JobExecutionType.SINGLE)).thenReturn(5);
    when(rateLimiter.getRateLimit(JobExecutionType.SINGLE)).thenReturn(5);

    GateCheckResult result = gateChecker.check(singleJob(), true);

    assertEquals(GateCheckResult.GateStatus.RATE_LIMITED, result.status());
    verify(threadPoolManager).releasePermit(JobExecutionType.SINGLE);
  }

  @Test
  void check_allClear_returnsClear() {
    when(drainController.isDraining()).thenReturn(false);
    when(threadPoolManager.tryAcquirePermit(JobExecutionType.SINGLE)).thenReturn(true);
    when(rateLimiter.tryAcquire(JobExecutionType.SINGLE)).thenReturn(true);

    GateCheckResult result = gateChecker.check(singleJob(), true);

    assertTrue(result.isClear());
    verify(threadPoolManager, never()).releasePermit(any());
  }

  @Test
  void check_claimVariant_rateLimited_releasesPermit() {
    when(drainController.isDraining()).thenReturn(false);
    when(threadPoolManager.tryAcquirePermit(JobExecutionType.BATCH_CHILD)).thenReturn(true);
    when(rateLimiter.tryAcquire(JobExecutionType.BATCH_CHILD)).thenReturn(false);
    when(rateLimiter.getCurrentCount(JobExecutionType.BATCH_CHILD)).thenReturn(3);
    when(rateLimiter.getRateLimit(JobExecutionType.BATCH_CHILD)).thenReturn(3);

    JobClaimDto claim = batchChildClaim();
    GateCheckResult result = gateChecker.check(claim, true);

    assertEquals(GateCheckResult.GateStatus.RATE_LIMITED, result.status());
    verify(threadPoolManager).releasePermit(JobExecutionType.BATCH_CHILD);
  }

  @Test
  void check_claimVariant_allClear_doesNotRelease() {
    when(drainController.isDraining()).thenReturn(false);
    when(threadPoolManager.tryAcquirePermit(JobExecutionType.BATCH_CHILD)).thenReturn(true);
    when(rateLimiter.tryAcquire(JobExecutionType.BATCH_CHILD)).thenReturn(true);

    GateCheckResult result = gateChecker.check(batchChildClaim(), true);

    assertTrue(result.isClear());
    verify(threadPoolManager, never()).releasePermit(any());
  }

  private static JobEntity singleJob() {
    JobEntity job = new JobEntity();
    job.setId(UUID.randomUUID());
    job.setJobType(JobExecutionType.SINGLE);
    return job;
  }

  private static JobClaimDto batchChildClaim() {
    return new JobClaimDto(
        UUID.randomUUID(),
        JobStatus.RUNNING,
        JobExecutionType.BATCH_CHILD,
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
}
