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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import run.ratchet.api.JobFilter;
import run.ratchet.api.JobStatus;
import run.ratchet.api.event.JobsBulkRetriedEvent;
import run.ratchet.ri.core.internal.InternalEventPublisher;
import run.ratchet.ri.core.internal.JobWakeupService;
import run.ratchet.spi.AfterCommitRegistrar;
import run.ratchet.store.entity.BatchEntity;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.spi.BatchStore;
import run.ratchet.store.spi.JobBatchStatusStore;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.JobPauseStore;
import run.ratchet.store.spi.JobRetryStore;

class BatchControlTest {
  private final JobCrudStore jobs = mock(JobCrudStore.class);
  private final JobBatchStatusStore statuses = mock(JobBatchStatusStore.class);
  private final JobPauseStore pauses = mock(JobPauseStore.class);
  private final JobRetryStore retries = mock(JobRetryStore.class);
  private final BatchStore batches = mock(BatchStore.class);
  private final BatchService batchService = mock(BatchService.class);
  private final InternalEventPublisher events = mock(InternalEventPublisher.class);
  private final JobWakeupService wakeup = mock(JobWakeupService.class);
  private final List<Runnable> afterCommit = new ArrayList<>();
  private final JobEntity parent = new JobEntity();
  private DefaultJobSchedulerService scheduler;

  @BeforeEach
  void setup() {
    parent.setId(UUID.randomUUID());
    parent.setJobType(JobExecutionType.BATCH_PARENT);
    parent.setStatus(JobStatus.PENDING);
    when(jobs.findById(parent.getId())).thenReturn(Optional.of(parent));
    AfterCommitRegistrar registrar =
        action -> {
          afterCommit.add(action);
          return AfterCommitRegistrar.Result.REGISTERED;
        };
    scheduler = schedulerWith(batches, registrar);
  }

  private DefaultJobSchedulerService schedulerWith(
      BatchStore batchStore, AfterCommitRegistrar registrar) {
    return new DefaultJobSchedulerService(
        events,
        statuses,
        pauses,
        retries,
        null,
        jobs,
        batchStore,
        null,
        null,
        null,
        wakeup,
        mock(RecurringScheduler.class),
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        Clock.systemUTC(),
        null,
        registrar,
        batchService);
  }

  @Test
  void cancelOnlyCascadesAfterSuccessfulParentTransition() {
    assertFalse(scheduler.cancelJob(parent.getId()));
    verifyNoInteractions(batchService, batches);
    clearInvocations(statuses);
    when(statuses.compareAndSwapStatus(parent.getId(), JobStatus.PENDING, JobStatus.CANCELED, null))
        .thenReturn(true);
    assertTrue(scheduler.cancelJob(parent.getId()));
    var order = inOrder(statuses, batchService);
    order
        .verify(statuses)
        .compareAndSwapStatus(parent.getId(), JobStatus.PENDING, JobStatus.CANCELED, null);
    order.verify(batchService).cancelBatch(parent);
  }

  @Test
  void cancelParentSweepsAfterCommitAndToleratesSweepFailure() {
    when(statuses.compareAndSwapStatus(parent.getId(), JobStatus.PENDING, JobStatus.CANCELED, null))
        .thenReturn(true);
    when(batchService.sweepCancelledBatch(parent.getId()))
        .thenThrow(new IllegalStateException("retry later"));

    assertTrue(scheduler.cancelJob(parent.getId()));
    verify(batchService, never()).sweepCancelledBatch(any());
    afterCommit.forEach(Runnable::run);
    verify(batchService).sweepCancelledBatch(parent.getId());
  }

  @Test
  void ordinaryJobsNeverCascade() {
    parent.setJobType(JobExecutionType.SINGLE);
    when(statuses.compareAndSwapStatus(parent.getId(), JobStatus.PENDING, JobStatus.CANCELED, null))
        .thenReturn(true);
    when(pauses.transitionToPaused(parent.getId(), JobStatus.PENDING)).thenReturn(true);
    when(pauses.transitionFromPausedAtomic(parent.getId())).thenReturn(JobStatus.PENDING);
    assertTrue(scheduler.cancelJob(parent.getId()));
    assertTrue(scheduler.pauseJob(parent.getId()));
    assertTrue(scheduler.resumeJob(parent.getId()));
    verifyNoInteractions(batches, batchService);
  }

  @Test
  void pauseOnlyCascadesAfterSuccessfulTransition() {
    assertFalse(scheduler.pauseJob(parent.getId()));
    verifyNoInteractions(batches);
    when(pauses.transitionToPaused(parent.getId(), JobStatus.PENDING)).thenReturn(true);
    assertTrue(scheduler.pauseJob(parent.getId()));
    var order = inOrder(pauses, batches);
    order.verify(pauses, times(2)).transitionToPaused(parent.getId(), JobStatus.PENDING);
    order.verify(batches).pauseBatchChildren(parent.getId());
  }

  @Test
  void resumeChecksCompletionAfterCommitAndWakesForResumedChildren() {
    assertFalse(scheduler.resumeJob(parent.getId()));
    verifyNoInteractions(batchService);
    when(pauses.transitionFromPausedAtomic(parent.getId())).thenReturn(JobStatus.PENDING);
    when(batches.resumeBatchChildren(parent.getId())).thenReturn(3);
    assertTrue(scheduler.resumeJob(parent.getId()));
    verifyNoInteractions(batchService, wakeup);
    afterCommit.forEach(Runnable::run);
    verify(batchService).completeIfFinished(parent.getId());
    verify(wakeup).notify(any(), eq(true), eq(null));
  }

  @Test
  void resumeWakesWhenCompletionMakesCallbacksAvailableWithoutResumingChildren() {
    when(pauses.transitionFromPausedAtomic(parent.getId())).thenReturn(JobStatus.PENDING);
    when(batchService.completeIfFinished(parent.getId())).thenReturn(true);
    assertTrue(scheduler.resumeJob(parent.getId()));
    verifyNoInteractions(batchService);
    afterCommit.forEach(Runnable::run);
    verify(wakeup).notify(any(), eq(true), eq(null));
  }

  @Test
  void resumeDefersCompletionErrorsAndStillWakesResumedChildren() {
    when(pauses.transitionFromPausedAtomic(parent.getId())).thenReturn(JobStatus.PENDING);
    when(batches.resumeBatchChildren(parent.getId())).thenReturn(2);
    when(batchService.completeIfFinished(parent.getId()))
        .thenThrow(new IllegalStateException("retry later"));
    assertTrue(scheduler.resumeJob(parent.getId()));
    afterCommit.forEach(Runnable::run);
    verify(wakeup).notify(any(), eq(true), eq(null));
  }

  @Test
  void pausedParentFinishesInterruptedPause() {
    parent.setStatus(JobStatus.PAUSED);
    when(batches.pauseBatchChildren(parent.getId())).thenReturn(2);
    assertTrue(scheduler.pauseJob(parent.getId()));
    verify(batches).pauseBatchChildren(parent.getId());
    verifyNoInteractions(pauses);
  }

  @Test
  void pendingParentFinishesInterruptedResume() {
    BatchEntity batch = new BatchEntity();
    batch.setCompletionProcessed(false);
    when(batches.findBatchById(parent.getId())).thenReturn(Optional.of(batch));
    when(batches.resumeBatchChildren(parent.getId())).thenReturn(2, 0);
    assertTrue(scheduler.resumeJob(parent.getId()));
    verifyNoInteractions(batchService, wakeup, pauses);
    afterCommit.forEach(Runnable::run);
    verify(batchService).completeIfFinished(parent.getId());
    verify(wakeup).notify(any(), eq(true), eq(null));
    assertFalse(scheduler.resumeJob(parent.getId()));
  }

  @Test
  void closedPendingBatchDoesNotResumeChildren() {
    BatchEntity batch = new BatchEntity();
    batch.setCompletionProcessed(true);
    when(batches.findBatchById(parent.getId())).thenReturn(Optional.of(batch));
    assertFalse(scheduler.resumeJob(parent.getId()));
    verify(batches, never()).resumeBatchChildren(parent.getId());
  }

  @Test
  void coreOnlyStoreCanPauseAndResumeWithoutBatchCapability() {
    scheduler = schedulerWith(null, action -> AfterCommitRegistrar.Result.REGISTERED);
    when(pauses.transitionToPaused(parent.getId(), JobStatus.PENDING)).thenReturn(true);
    assertTrue(scheduler.pauseJob(parent.getId()));
    parent.setStatus(JobStatus.PAUSED);
    assertTrue(scheduler.pauseJob(parent.getId()));
    when(pauses.transitionFromPausedAtomic(parent.getId())).thenReturn(JobStatus.PENDING);
    assertTrue(scheduler.resumeJob(parent.getId()));
    verifyNoInteractions(batches, batchService);
  }

  @Test
  void pendingOrdinaryJobCannotResume() {
    parent.setJobType(JobExecutionType.SINGLE);
    assertFalse(scheduler.resumeJob(parent.getId()));
    verifyNoInteractions(batches);
  }

  @Test
  void retryParentLoopsUntilDrainedAndPublishesOneBulkEventPerChunk() {
    BatchEntity batch = new BatchEntity();
    batch.setCompletionProcessed(false);
    when(batches.findBatchById(parent.getId())).thenReturn(Optional.of(batch));
    when(retries.resetFailedToPending(any(JobFilter.class), eq(1000))).thenReturn(1000, 7, 0);
    assertTrue(scheduler.retryJob(parent.getId()));
    verify(retries, times(3)).resetFailedToPending(any(JobFilter.class), eq(1000));
    verify(retries, never()).resetFailedToPending(parent.getId());
    afterCommit.forEach(Runnable::run);
    ArgumentCaptor<JobsBulkRetriedEvent> event =
        ArgumentCaptor.forClass(JobsBulkRetriedEvent.class);
    verify(events, times(2)).publish(event.capture());
    assertEquals(
        List.of(1000, 7),
        event.getAllValues().stream().map(JobsBulkRetriedEvent::getCount).toList());
    assertEquals(parent.getId(), event.getValue().getFilter().parentJobId());
  }

  @Test
  void retryParentReturnsFalseWhenNothingWasResetOrBatchIsClosed() {
    BatchEntity batch = new BatchEntity();
    batch.setCompletionProcessed(false);
    when(batches.findBatchById(parent.getId())).thenReturn(Optional.of(batch));
    assertFalse(scheduler.retryJob(parent.getId()));
    batch.setCompletionProcessed(true);
    assertFalse(scheduler.retryJob(parent.getId()));
    verify(retries, times(1)).resetFailedToPending(any(JobFilter.class), anyInt());
    verifyNoInteractions(events, wakeup);
  }
}
