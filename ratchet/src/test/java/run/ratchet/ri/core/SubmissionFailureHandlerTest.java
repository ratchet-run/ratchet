package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import run.ratchet.api.JobStatus;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.store.dto.JobClaimDto;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.spi.JobBatchStatusStore;

@ExtendWith(MockitoExtension.class)
class SubmissionFailureHandlerTest {

  @Mock private JobStateManager jobStateManager;
  @Mock private RetryBufferManager retryBufferManager;
  @Mock private ThreadPoolManager threadPoolManager;
  @Mock private PollerScheduler pollerScheduler;
  @Mock private MetricsCollector metricsCollector;
  @Mock private JobBatchStatusStore jobBatchStatusStore;
  @Mock private NodeIdentityProvider nodeIdentityProvider;

  private SubmissionFailureHandler handler;

  @BeforeEach
  void setUp() {
    handler =
        new SubmissionFailureHandler(
            jobStateManager,
            retryBufferManager,
            threadPoolManager,
            pollerScheduler,
            metricsCollector);
  }

  @Test
  void handleGateFailure_jobRecordsGateMetric() {
    JobEntity job = runningSingleJob(42L);
    when(jobStateManager.resetJobToPending(job)).thenReturn(true);

    handler.handleGateFailure(
        job, GateCheckResult.noPermits(JobExecutionType.SINGLE, job.getId()), true);

    verify(metricsCollector).gateRejected(JobExecutionType.SINGLE.name(), "NO_PERMITS");
    verify(jobStateManager).resetJobToPending(job);
    verify(retryBufferManager, never()).forceOffer(job);
  }

  @Test
  void handleGateFailure_bufferedRetryDoesNotResetJob() {
    JobEntity job = runningSingleJob(50L);
    when(retryBufferManager.offer(job)).thenReturn(true);

    handler.handleGateFailure(
        job, GateCheckResult.noPermits(JobExecutionType.SINGLE, job.getId()), false);

    verify(metricsCollector).gateRejected(JobExecutionType.SINGLE.name(), "NO_PERMITS");
    verify(retryBufferManager).offer(job);
    verify(jobStateManager, never()).resetJobToPending(job);
    verify(retryBufferManager, never()).forceOffer(job);
  }

  @Test
  void handleGateFailure_unblockedOrMissingMetricsCollectorDoesNotRecordMetric() {
    JobEntity job = runningSingleJob(51L);
    SubmissionFailureHandler handlerWithoutMetrics =
        new SubmissionFailureHandler(
            jobStateManager, retryBufferManager, threadPoolManager, pollerScheduler, null);
    when(jobStateManager.resetJobToPending(job)).thenReturn(true);

    handler.handleGateFailure(job, GateCheckResult.clear(), true);
    handlerWithoutMetrics.handleGateFailure(
        job, GateCheckResult.noPermits(JobExecutionType.SINGLE, job.getId()), true);

    verifyNoInteractions(metricsCollector);
  }

  @Test
  void handleGateFailure_claimRecordsGateMetric() {
    UUID claimJobId = new UUID(0L, 42L);
    JobClaimDto claim =
        new JobClaimDto(
            claimJobId,
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
    when(jobStateManager.resetJobToPending(claim.id())).thenReturn(true);

    handler.handleGateFailure(
        claim, GateCheckResult.rateLimited(JobExecutionType.BATCH_CHILD, claimJobId, 10, 5));

    verify(metricsCollector).gateRejected(JobExecutionType.BATCH_CHILD.name(), "RATE_LIMITED");
    verify(retryBufferManager).offer(claim);
    verify(jobStateManager).resetJobToPending(claim.id());
  }

  @Test
  void handleGateFailure_jobResetUpdatesEntityState() {
    JobEntity job = runningSingleJob(46L);
    SubmissionFailureHandler realStateHandler = handlerWithRealStateManager();
    when(nodeIdentityProvider.getNodeId()).thenReturn("node-1");
    when(jobBatchStatusStore.resetRunningJob(job.getId(), "node-1")).thenReturn(true);

    realStateHandler.handleGateFailure(
        job, GateCheckResult.noPermits(JobExecutionType.SINGLE, job.getId()), true);

    assertSame(JobStatus.PENDING, job.getStatus());
    assertNull(job.getPickedBy());
    assertNull(job.getPickedAt());
    verify(jobBatchStatusStore).resetRunningJob(job.getId(), "node-1");
    verify(retryBufferManager, never()).forceOffer(job);
  }

  @Test
  void handleGateFailure_bufferFullRetryResetsUuidJobWithoutThrowing() {
    JobEntity job = runningSingleJob(43L);
    when(retryBufferManager.offer(job)).thenReturn(false);
    when(jobStateManager.resetJobToPending(job)).thenReturn(true);

    assertDoesNotThrow(
        () ->
            handler.handleGateFailure(
                job, GateCheckResult.noPermits(JobExecutionType.SINGLE, job.getId()), false));
  }

  @Test
  void handleGateFailure_firstAttemptHandlesResetAndForceBufferFailure() {
    JobEntity job = runningSingleJob(54L);
    when(jobStateManager.resetJobToPending(job)).thenReturn(false);
    when(retryBufferManager.forceOffer(job)).thenReturn(false);

    handler.handleGateFailure(
        job, GateCheckResult.noPermits(JobExecutionType.SINGLE, job.getId()), true);

    verify(jobStateManager).resetJobToPending(job);
    verify(retryBufferManager).forceOffer(job);
  }

  @Test
  void handleRejection_firstAttemptReleasesPermitWakesPollerAndResets() {
    JobEntity job = runningSingleJob(44L);
    when(jobStateManager.resetJobToPending(job)).thenReturn(true);

    handler.handleRejection(job, JobExecutionType.SINGLE, true);

    verify(threadPoolManager).releasePermit(JobExecutionType.SINGLE);
    verify(pollerScheduler).wakeup();
    verify(jobStateManager).resetJobToPending(job);
    verify(retryBufferManager, never()).offer(job);
  }

  @Test
  void handleRejection_bufferedRetryRebuffersWithoutReset() {
    JobEntity job = runningSingleJob(52L);
    when(retryBufferManager.offer(job)).thenReturn(true);

    handler.handleRejection(job, JobExecutionType.SINGLE, false);

    verify(threadPoolManager).releasePermit(JobExecutionType.SINGLE);
    verify(pollerScheduler).wakeup();
    verify(retryBufferManager).offer(job);
    verify(jobStateManager, never()).resetJobToPending(job);
  }

  @Test
  void handleRejection_bufferFullRetryResetsJob() {
    JobEntity job = runningSingleJob(53L);
    when(retryBufferManager.offer(job)).thenReturn(false);
    when(jobStateManager.resetJobToPending(job)).thenReturn(true);

    handler.handleRejection(job, JobExecutionType.SINGLE, false);

    verify(threadPoolManager).releasePermit(JobExecutionType.SINGLE);
    verify(pollerScheduler).wakeup();
    verify(retryBufferManager).offer(job);
    verify(jobStateManager).resetJobToPending(job);
  }

  @Test
  void handleRejection_claimRetryPathsDoNotThrow() {
    UUID claimJobId = new UUID(0L, 45L);
    JobClaimDto claim =
        new JobClaimDto(
            claimJobId,
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
    when(retryBufferManager.offer(claim)).thenReturn(true, false, false);
    when(jobStateManager.resetJobToPending(claimJobId)).thenReturn(true, false);

    assertDoesNotThrow(() -> handler.handleRejection(claim, JobExecutionType.SINGLE));
    assertDoesNotThrow(() -> handler.handleRejection(claim, JobExecutionType.SINGLE));
    assertDoesNotThrow(() -> handler.handleRejection(claim, JobExecutionType.SINGLE));
  }

  @Test
  void handleUnexpectedException_firstAttemptJobReleasesPermitWakesPollerAndResetsEntityState() {
    JobEntity job = runningSingleJob(47L);
    SubmissionFailureHandler realStateHandler = handlerWithRealStateManager();
    when(nodeIdentityProvider.getNodeId()).thenReturn("node-1");
    when(jobBatchStatusStore.resetRunningJob(job.getId(), "node-1")).thenReturn(true);

    realStateHandler.handleUnexpectedException(
        job, JobExecutionType.SINGLE, true, new IllegalStateException("boom"));

    assertSame(JobStatus.PENDING, job.getStatus());
    assertNull(job.getPickedBy());
    assertNull(job.getPickedAt());
    verify(threadPoolManager).releasePermit(JobExecutionType.SINGLE);
    verify(pollerScheduler).wakeup();
    verify(jobBatchStatusStore).resetRunningJob(job.getId(), "node-1");
    verify(retryBufferManager, never()).offer(job);
    verify(retryBufferManager, never()).forceOffer(job);
  }

  @Test
  void handleUnexpectedException_bufferedJobRebuffersWithoutPersistentReset() {
    JobEntity job = runningSingleJob(48L);
    SubmissionFailureHandler realStateHandler = handlerWithRealStateManager();
    when(retryBufferManager.offer(job)).thenReturn(true);

    realStateHandler.handleUnexpectedException(
        job, JobExecutionType.SINGLE, false, new IllegalStateException("boom"));

    assertSame(JobStatus.RUNNING, job.getStatus());
    verify(threadPoolManager).releasePermit(JobExecutionType.SINGLE);
    verify(pollerScheduler).wakeup();
    verify(retryBufferManager).offer(job);
    verifyNoInteractions(jobBatchStatusStore, nodeIdentityProvider);
  }

  @Test
  void handleUnexpectedException_claimResetsPersistentClaimWhenBufferIsFull() {
    UUID claimJobId = new UUID(0L, 49L);
    JobClaimDto claim =
        new JobClaimDto(
            claimJobId,
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
    SubmissionFailureHandler realStateHandler = handlerWithRealStateManager();
    when(retryBufferManager.offer(claim)).thenReturn(false);
    when(nodeIdentityProvider.getNodeId()).thenReturn("node-1");
    when(jobBatchStatusStore.resetRunningJob(claimJobId, "node-1")).thenReturn(true);

    realStateHandler.handleUnexpectedException(
        claim, JobExecutionType.BATCH_CHILD, new IllegalStateException("boom"));

    verify(threadPoolManager).releasePermit(JobExecutionType.BATCH_CHILD);
    verify(pollerScheduler).wakeup();
    verify(retryBufferManager).offer(claim);
    verify(jobBatchStatusStore).resetRunningJob(claimJobId, "node-1");
  }

  private SubmissionFailureHandler handlerWithRealStateManager() {
    return new SubmissionFailureHandler(
        new JobStateManager(jobBatchStatusStore, nodeIdentityProvider),
        retryBufferManager,
        threadPoolManager,
        pollerScheduler,
        metricsCollector);
  }

  private static JobEntity runningSingleJob(long leastSignificantBits) {
    JobEntity job = new JobEntity();
    job.setId(new UUID(0L, leastSignificantBits));
    job.setJobType(JobExecutionType.SINGLE);
    job.setStatus(JobStatus.RUNNING);
    job.setPickedBy("node-1");
    job.setPickedAt(Instant.parse("2026-05-07T00:00:00Z"));
    return job;
  }
}
