/*
 * Copyright 2026 Ratchet Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
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
import run.ratchet.api.ExecutorTargets;
import run.ratchet.api.JobStatus;
import run.ratchet.ri.core.internal.PoolRegistry;
import run.ratchet.ri.core.internal.ThreadPoolManager;
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
  @Mock private PoolRegistry poolRegistry;
  @Mock private ThreadPoolManager pool;
  @Mock private PollerScheduler pollerScheduler;
  @Mock private MetricsCollector metricsCollector;
  @Mock private JobBatchStatusStore jobBatchStatusStore;
  @Mock private NodeIdentityProvider nodeIdentityProvider;

  private SubmissionFailureHandler handler;

  @BeforeEach
  void setUp() {
    handler =
        new SubmissionFailureHandler(
            jobStateManager, retryBufferManager, poolRegistry, pollerScheduler, metricsCollector);
    lenient().when(poolRegistry.pool(ExecutorTargets.PLATFORM)).thenReturn(pool);
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
            jobStateManager, retryBufferManager, poolRegistry, pollerScheduler, null);
    when(jobStateManager.resetJobToPending(job)).thenReturn(true);

    handler.handleGateFailure(job, GateCheckResult.clear(ExecutorTargets.PLATFORM), true);
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
            0,
            null);
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

    handler.handleRejection(job, JobExecutionType.SINGLE, "platform", true);

    verify(pool).releasePermit(JobExecutionType.SINGLE);
    verify(pollerScheduler).wakeup();
    verify(jobStateManager).resetJobToPending(job);
    verify(retryBufferManager, never()).offer(job);
  }

  @Test
  void handleRejection_bufferedRetryRebuffersWithoutReset() {
    JobEntity job = runningSingleJob(52L);
    when(retryBufferManager.offer(job)).thenReturn(true);

    handler.handleRejection(job, JobExecutionType.SINGLE, "platform", false);

    verify(pool).releasePermit(JobExecutionType.SINGLE);
    verify(pollerScheduler).wakeup();
    verify(retryBufferManager).offer(job);
    verify(jobStateManager, never()).resetJobToPending(job);
  }

  @Test
  void handleRejection_bufferFullRetryResetsJob() {
    JobEntity job = runningSingleJob(53L);
    when(retryBufferManager.offer(job)).thenReturn(false);
    when(jobStateManager.resetJobToPending(job)).thenReturn(true);

    handler.handleRejection(job, JobExecutionType.SINGLE, "platform", false);

    verify(pool).releasePermit(JobExecutionType.SINGLE);
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
            0,
            null);
    when(retryBufferManager.offer(claim)).thenReturn(true, false, false);
    when(jobStateManager.resetJobToPending(claimJobId)).thenReturn(true, false);

    assertDoesNotThrow(() -> handler.handleRejection(claim, JobExecutionType.SINGLE, "platform"));
    assertDoesNotThrow(() -> handler.handleRejection(claim, JobExecutionType.SINGLE, "platform"));
    assertDoesNotThrow(() -> handler.handleRejection(claim, JobExecutionType.SINGLE, "platform"));

    // Every rejection releases the permit and wakes the poller; the claim is offered to the retry
    // buffer each time, and only the two calls where the buffer refused fall through to a state
    // reset. Pinning the counts proves which recovery branch ran instead of merely "did not throw".
    verify(pool, times(3)).releasePermit(JobExecutionType.SINGLE);
    verify(pollerScheduler, times(3)).wakeup();
    verify(retryBufferManager, times(3)).offer(claim);
    verify(jobStateManager, times(2)).resetJobToPending(claimJobId);
  }

  @Test
  void handleUnexpectedException_firstAttemptJobReleasesPermitWakesPollerAndResetsEntityState() {
    JobEntity job = runningSingleJob(47L);
    SubmissionFailureHandler realStateHandler = handlerWithRealStateManager();
    when(nodeIdentityProvider.getNodeId()).thenReturn("node-1");
    when(jobBatchStatusStore.resetRunningJob(job.getId(), "node-1")).thenReturn(true);

    realStateHandler.handleUnexpectedException(
        job, JobExecutionType.SINGLE, "platform", true, new IllegalStateException("boom"));

    assertSame(JobStatus.PENDING, job.getStatus());
    assertNull(job.getPickedBy());
    assertNull(job.getPickedAt());
    verify(pool).releasePermit(JobExecutionType.SINGLE);
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
        job, JobExecutionType.SINGLE, "platform", false, new IllegalStateException("boom"));

    assertSame(JobStatus.RUNNING, job.getStatus());
    verify(pool).releasePermit(JobExecutionType.SINGLE);
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
            0,
            null);
    SubmissionFailureHandler realStateHandler = handlerWithRealStateManager();
    when(retryBufferManager.offer(claim)).thenReturn(false);
    when(nodeIdentityProvider.getNodeId()).thenReturn("node-1");
    when(jobBatchStatusStore.resetRunningJob(claimJobId, "node-1")).thenReturn(true);

    realStateHandler.handleUnexpectedException(
        claim, JobExecutionType.BATCH_CHILD, "platform", new IllegalStateException("boom"));

    verify(pool).releasePermit(JobExecutionType.BATCH_CHILD);
    verify(pollerScheduler).wakeup();
    verify(retryBufferManager).offer(claim);
    verify(jobBatchStatusStore).resetRunningJob(claimJobId, "node-1");
  }

  private SubmissionFailureHandler handlerWithRealStateManager() {
    return new SubmissionFailureHandler(
        new JobStateManager(jobBatchStatusStore, nodeIdentityProvider),
        retryBufferManager,
        poolRegistry,
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
