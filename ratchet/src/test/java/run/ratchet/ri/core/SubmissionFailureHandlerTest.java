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
    UUID jobId = new UUID(0L, 42L);
    JobEntity job = new JobEntity();
    job.setId(jobId);
    job.setJobType(JobExecutionType.SINGLE);
    job.setStatus(JobStatus.RUNNING);
    job.setPickedBy("node-1");
    job.setPickedAt(Instant.parse("2026-05-07T00:00:00Z"));
    when(jobStateManager.resetJobToPending(job)).thenReturn(true);

    handler.handleGateFailure(job, GateCheckResult.noPermits(JobExecutionType.SINGLE, jobId), true);

    verify(metricsCollector).gateRejected(JobExecutionType.SINGLE.name(), "NO_PERMITS");
    verify(jobStateManager).resetJobToPending(job);
    verify(retryBufferManager, never()).forceOffer(job);
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
    UUID jobId = new UUID(0L, 46L);
    JobEntity job = new JobEntity();
    job.setId(jobId);
    job.setJobType(JobExecutionType.SINGLE);
    job.setStatus(JobStatus.RUNNING);
    job.setPickedBy("node-1");
    job.setPickedAt(Instant.parse("2026-05-07T00:00:00Z"));
    SubmissionFailureHandler realStateHandler = handlerWithRealStateManager();
    when(nodeIdentityProvider.getNodeId()).thenReturn("node-1");
    when(jobBatchStatusStore.resetRunningJob(jobId, "node-1")).thenReturn(true);

    realStateHandler.handleGateFailure(
        job, GateCheckResult.noPermits(JobExecutionType.SINGLE, jobId), true);

    assertSame(JobStatus.PENDING, job.getStatus());
    assertNull(job.getPickedBy());
    assertNull(job.getPickedAt());
    verify(jobBatchStatusStore).resetRunningJob(jobId, "node-1");
    verify(retryBufferManager, never()).forceOffer(job);
  }

  @Test
  void handleGateFailure_uuidJobIdFormatsSafely() {
    UUID jobId = new UUID(0L, 43L);
    JobEntity job = new JobEntity();
    job.setId(jobId);
    job.setJobType(JobExecutionType.SINGLE);
    when(retryBufferManager.offer(job)).thenReturn(false);
    when(jobStateManager.resetJobToPending(job)).thenReturn(true);

    assertDoesNotThrow(
        () ->
            handler.handleGateFailure(
                job, GateCheckResult.noPermits(JobExecutionType.SINGLE, jobId), false));
  }

  @Test
  void handleRejection_uuidJobIdFormatsSafely() {
    UUID jobId = new UUID(0L, 44L);
    JobEntity job = new JobEntity();
    job.setId(jobId);
    job.setJobType(JobExecutionType.SINGLE);
    when(jobStateManager.resetJobToPending(job)).thenReturn(true);
    when(retryBufferManager.offer(job)).thenReturn(true, false);

    assertDoesNotThrow(() -> handler.handleRejection(job, JobExecutionType.SINGLE, true));
    assertDoesNotThrow(() -> handler.handleRejection(job, JobExecutionType.SINGLE, false));
    assertDoesNotThrow(() -> handler.handleRejection(job, JobExecutionType.SINGLE, false));
  }

  @Test
  void handleRejection_uuidClaimIdFormatsSafely() {
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
    UUID jobId = new UUID(0L, 47L);
    JobEntity job = new JobEntity();
    job.setId(jobId);
    job.setJobType(JobExecutionType.SINGLE);
    job.setStatus(JobStatus.RUNNING);
    job.setPickedBy("node-1");
    job.setPickedAt(Instant.parse("2026-05-07T00:00:00Z"));
    SubmissionFailureHandler realStateHandler = handlerWithRealStateManager();
    when(nodeIdentityProvider.getNodeId()).thenReturn("node-1");
    when(jobBatchStatusStore.resetRunningJob(jobId, "node-1")).thenReturn(true);

    realStateHandler.handleUnexpectedException(
        job, JobExecutionType.SINGLE, true, new IllegalStateException("boom"));

    assertSame(JobStatus.PENDING, job.getStatus());
    assertNull(job.getPickedBy());
    assertNull(job.getPickedAt());
    verify(threadPoolManager).releasePermit(JobExecutionType.SINGLE);
    verify(pollerScheduler).wakeup();
    verify(jobBatchStatusStore).resetRunningJob(jobId, "node-1");
    verify(retryBufferManager, never()).offer(job);
    verify(retryBufferManager, never()).forceOffer(job);
  }

  @Test
  void handleUnexpectedException_bufferedJobRebuffersWithoutPersistentReset() {
    UUID jobId = new UUID(0L, 48L);
    JobEntity job = new JobEntity();
    job.setId(jobId);
    job.setJobType(JobExecutionType.SINGLE);
    job.setStatus(JobStatus.RUNNING);
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
}
