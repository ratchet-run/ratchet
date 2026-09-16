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
package run.ratchet.ri.core.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import jakarta.transaction.Transactional;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import run.ratchet.ri.core.BatchService;
import run.ratchet.ri.core.PollerScheduler;
import run.ratchet.ri.core.internal.PostExecutionHandler.TerminalTimeoutTransition;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;

@ExtendWith(MockitoExtension.class)
class PostExecutionHandlerTest {

  @Mock private run.ratchet.store.spi.JobTerminalStore jobTerminalStore;
  @Mock private BatchService batchService;
  @Mock private WorkflowScheduler workflowScheduler;
  @Mock private DeadLetterService deadLetterService;
  @Mock private PollerScheduler pollerScheduler;
  @Mock private Supplier<Optional<TerminalTimeoutTransition>> timeoutTransition;

  private PostExecutionHandler handler;

  private static JobEntity job(JobExecutionType jobType) {
    JobEntity job = new JobEntity();
    job.setId(UUID.randomUUID());
    job.setJobType(jobType);
    return job;
  }

  @BeforeEach
  void setUp() {
    handler =
        new PostExecutionHandler(
            batchService, workflowScheduler, deadLetterService, pollerScheduler, jobTerminalStore);
  }

  @Test
  void successUsesOneAtomicPlanAndDoesNotRepeatBatchCounterUpdates() {
    JobEntity child = job(JobExecutionType.BATCH_CHILD);
    UUID batchId = UUID.randomUUID();
    child.setDependsOn(batchId);
    run.ratchet.store.dto.BatchProgress progress =
        new run.ratchet.store.dto.BatchProgress(batchId, 2, 1, 0, null);
    when(jobTerminalStore.commitCompletion(any()))
        .thenReturn(new run.ratchet.store.dto.JobCompletionResult(true, progress));
    java.time.Instant now = java.time.Instant.parse("2026-09-15T00:00:00Z");
    assertTrue(handler.completeSuccess(child, "42", "java.lang.Integer", now, now, 5L, 2L));
    org.mockito.ArgumentCaptor<run.ratchet.store.dto.JobCompletionPlan> plan =
        org.mockito.ArgumentCaptor.forClass(run.ratchet.store.dto.JobCompletionPlan.class);
    verify(jobTerminalStore).commitCompletion(plan.capture());
    assertEquals(batchId, plan.getValue().batchId());
    assertEquals(run.ratchet.api.JobStatus.SUCCEEDED, plan.getValue().terminalStatus());
    assertEquals("42", plan.getValue().resultJson());
    InOrder order = inOrder(jobTerminalStore, workflowScheduler, batchService);
    order.verify(jobTerminalStore).commitCompletion(any());
    org.mockito.ArgumentCaptor<run.ratchet.api.event.JobCompletedEvent> completed =
        org.mockito.ArgumentCaptor.forClass(run.ratchet.api.event.JobCompletedEvent.class);
    order.verify(workflowScheduler).publishTerminalEvent(completed.capture());
    order.verify(workflowScheduler).publishCompletion(any());
    order.verify(batchService).afterChildCompletion(progress);
    assertEquals(now, completed.getValue().getTimestamp());
    verify(batchService).afterChildCompletion(progress);
    verify(batchService, never()).markChildSucceeded(any());
  }

  @Test
  void lostSuccessRacePublishesNothing() {
    JobEntity child = job(JobExecutionType.BATCH_CHILD);
    when(jobTerminalStore.commitCompletion(any()))
        .thenReturn(run.ratchet.store.dto.JobCompletionResult.notCommitted());
    java.time.Instant now = java.time.Instant.now();
    assertFalse(handler.completeSuccess(child, null, null, now, now, 0, 0));
    verifyNoInteractions(workflowScheduler, batchService, deadLetterService, pollerScheduler);
  }

  @Test
  void staleSuccessPlanPublishesNothing() {
    JobEntity child = job(JobExecutionType.BATCH_CHILD);
    when(jobTerminalStore.commitCompletion(any()))
        .thenThrow(new IllegalStateException("stale dependency"));
    java.time.Instant now = java.time.Instant.now();
    assertThrows(
        IllegalStateException.class,
        () -> handler.completeSuccess(child, null, null, now, now, 0, 0));
    verifyNoInteractions(workflowScheduler, batchService, deadLetterService, pollerScheduler);
  }

  @Test
  void parentFollowupFailureDoesNotReverseSuccessfulChildCompletion() {
    JobEntity child = job(JobExecutionType.BATCH_CHILD);
    run.ratchet.store.dto.BatchProgress progress =
        new run.ratchet.store.dto.BatchProgress(UUID.randomUUID(), 1, 1, 0, null);
    when(jobTerminalStore.commitCompletion(any()))
        .thenReturn(new run.ratchet.store.dto.JobCompletionResult(true, progress));
    when(batchService.afterChildCompletion(progress))
        .thenThrow(new IllegalStateException("parent temporarily unavailable"));
    java.time.Instant now = java.time.Instant.now();
    assertTrue(handler.completeSuccess(child, null, null, now, now, 0, 0));
    verify(workflowScheduler).publishTerminalEvent(any());
    verifyNoInteractions(deadLetterService);
  }

  @Test
  void handleJobSuccess_withoutDownstreamWork_doesNotWakePoller() {
    JobEntity job = job(JobExecutionType.SINGLE);
    when(workflowScheduler.scheduleNext(job)).thenReturn(false);

    handler.handleJobSuccess(job);

    verify(workflowScheduler).scheduleNext(job);
    verify(batchService, never()).markChildSucceeded(job);
    verify(pollerScheduler, never()).wakeup();
  }

  @Test
  void handleJobSuccess_withDownstreamWork_wakesPoller() {
    JobEntity job = job(JobExecutionType.CHAIN_STEP);
    when(workflowScheduler.scheduleNext(job)).thenReturn(true);

    handler.handleJobSuccess(job);

    verify(workflowScheduler).scheduleNext(job);
    verify(pollerScheduler).wakeup();
  }

  @Test
  void handleBatchChildSuccess_withoutCompletedBatch_doesNotWakePoller() {
    JobEntity job = job(JobExecutionType.BATCH_CHILD);
    when(batchService.markChildSucceeded(job)).thenReturn(false);

    handler.handleJobSuccess(job);

    verify(batchService).markChildSucceeded(job);
    verify(workflowScheduler, never()).scheduleNext(job);
    verify(pollerScheduler, never()).wakeup();
  }

  @Test
  void handleJobSuccess_workflowBranchSchedulesNext() {
    JobEntity job = job(JobExecutionType.WORKFLOW_BRANCH);
    when(workflowScheduler.scheduleNext(job)).thenReturn(true);

    handler.handleJobSuccess(job);

    verify(workflowScheduler).scheduleNext(job);
    verify(batchService, never()).markChildSucceeded(job);
    verify(pollerScheduler).wakeup();
  }

  @Test
  void handleJobSuccess_workflowJoinDoesNotScheduleNextOrWakePoller() {
    JobEntity job = job(JobExecutionType.WORKFLOW_JOIN);

    handler.handleJobSuccess(job);

    verifyNoInteractions(
        batchService, workflowScheduler, deadLetterService, pollerScheduler, jobTerminalStore);
  }

  @Test
  void handleJobSuccess_defaultJobTypesDoNothing() {
    for (JobExecutionType jobType :
        List.of(
            JobExecutionType.RECURRING,
            JobExecutionType.BATCH_PARENT,
            JobExecutionType.WORKFLOW_JOIN)) {
      handler.handleJobSuccess(job(jobType));
    }

    verifyNoInteractions(
        batchService, workflowScheduler, deadLetterService, pollerScheduler, jobTerminalStore);
  }

  @Test
  void transitionOwningDlqCompositeUsesRequiresNewAndRollsBackOnCheckedExceptions() {
    Transactional transactional = PostExecutionHandler.class.getAnnotation(Transactional.class);

    assertTrue(transactional != null);
    assertTrue(transactional.value() == Transactional.TxType.REQUIRES_NEW);
    assertTrue(Arrays.asList(transactional.rollbackOn()).contains(Exception.class));
  }

  @Test
  void failureCompositeCommitsBatchAccountingBeforePublishing() {
    JobEntity job = job(JobExecutionType.BATCH_CHILD);
    UUID batchId = UUID.randomUUID();
    job.setDependsOn(batchId);
    RuntimeException failure = new RuntimeException("boom");
    run.ratchet.store.dto.BatchProgress progress =
        new run.ratchet.store.dto.BatchProgress(batchId, 1, 0, 1, null);
    when(jobTerminalStore.commitCompletion(any()))
        .thenReturn(new run.ratchet.store.dto.JobCompletionResult(true, progress));
    assertTrue(handler.moveToDlqAndHandlePermanentFailure(job, failure));
    InOrder order = inOrder(jobTerminalStore, batchService);
    order.verify(jobTerminalStore).commitCompletion(any());
    order.verify(batchService).afterChildCompletion(progress);
    verify(batchService, never()).markChildFailed(any());
  }

  @Test
  void failureCompositeSkipsEffectsWhenTerminalRaceIsLost() {
    JobEntity job = job(JobExecutionType.BATCH_CHILD);
    when(jobTerminalStore.commitCompletion(any()))
        .thenReturn(run.ratchet.store.dto.JobCompletionResult.notCommitted());
    assertFalse(handler.moveToDlqAndHandlePermanentFailure(job, new RuntimeException("boom")));
    verifyNoInteractions(batchService);
    verify(deadLetterService, never()).recordDlqTransitionInCurrentTransaction(any(), any(), any());
  }

  @Test
  void failureCompositePropagatesAtomicStoreFailureWithoutPublishing() {
    JobEntity job = job(JobExecutionType.BATCH_CHILD);
    when(jobTerminalStore.commitCompletion(any()))
        .thenThrow(new IllegalStateException("batch store unavailable"));
    assertThrows(
        IllegalStateException.class,
        () -> handler.moveToDlqAndHandlePermanentFailure(job, new RuntimeException("boom")));
    verifyNoInteractions(batchService, pollerScheduler);
    verify(deadLetterService, never()).recordDlqTransitionInCurrentTransaction(any(), any(), any());
  }

  @Test
  void failureCompositePublishesPreparedWorkflowAfterCommit() {
    JobEntity job = job(JobExecutionType.WORKFLOW_BRANCH);
    WorkflowCompletionPlan plan = new WorkflowCompletionPlan(List.of(), List.of(), true);
    when(workflowScheduler.planCompletion(any(), eq(false))).thenReturn(plan);
    when(jobTerminalStore.commitCompletion(any()))
        .thenReturn(new run.ratchet.store.dto.JobCompletionResult(true, null));
    assertTrue(handler.moveToDlqAndHandlePermanentFailure(job, new RuntimeException("boom")));
    verify(workflowScheduler).publishCompletion(plan);
    verify(pollerScheduler).wakeup();
  }

  @Test
  void handlePermanentFailure_batchChildWithoutCompletedBatch_recordsDlqWithoutWakingPoller() {
    JobEntity job = job(JobExecutionType.BATCH_CHILD);
    RuntimeException failure = new RuntimeException("boom");
    when(batchService.markChildFailed(job)).thenReturn(false);

    handler.handlePermanentFailure(job, failure);

    verify(batchService).markChildFailed(job);
    verify(deadLetterService).recordDlqTransition(job, failure);
    verify(workflowScheduler, never()).scheduleNext(job);
    verify(pollerScheduler, never()).wakeup();
  }

  @Test
  void handlePermanentFailure_batchChildCompletesBatch_wakesPoller() {
    JobEntity job = job(JobExecutionType.BATCH_CHILD);
    RuntimeException failure = new RuntimeException("boom");
    when(batchService.markChildFailed(job)).thenReturn(true);

    handler.handlePermanentFailure(job, failure);

    verify(batchService).markChildFailed(job);
    verify(deadLetterService).recordDlqTransition(job, failure);
    verify(pollerScheduler).wakeup();
  }

  @Test
  void handlePermanentFailure_singleMovesToDlqAndWithoutDownstreamWorkDoesNotWakePoller() {
    JobEntity job = job(JobExecutionType.SINGLE);
    RuntimeException failure = new RuntimeException("boom");
    when(workflowScheduler.scheduleNext(job)).thenReturn(false);

    handler.handlePermanentFailure(job, failure);

    verify(deadLetterService).recordDlqTransition(job, failure);
    verify(workflowScheduler).scheduleNext(job);
    verify(pollerScheduler, never()).wakeup();
  }

  @Test
  void handlePermanentFailure_chainStepMovesToDlqAndWithDownstreamWorkWakesPoller() {
    JobEntity job = job(JobExecutionType.CHAIN_STEP);
    RuntimeException failure = new RuntimeException("boom");
    when(workflowScheduler.scheduleNext(job)).thenReturn(true);

    handler.handlePermanentFailure(job, failure);

    verify(deadLetterService).recordDlqTransition(job, failure);
    verify(workflowScheduler).scheduleNext(job);
    verify(pollerScheduler).wakeup();
  }

  @Test
  void handlePermanentFailure_workflowBranchMovesToDlqAndSchedulesNext() {
    JobEntity job = job(JobExecutionType.WORKFLOW_BRANCH);
    RuntimeException failure = new RuntimeException("boom");
    when(workflowScheduler.scheduleNext(job)).thenReturn(true);

    handler.handlePermanentFailure(job, failure);

    verify(deadLetterService).recordDlqTransition(job, failure);
    verify(workflowScheduler).scheduleNext(job);
    verify(batchService, never()).markChildFailed(job);
    verify(pollerScheduler).wakeup();
  }

  @Test
  void handlePermanentFailure_recurringMovesToDlqWithoutSchedulingNext() {
    JobEntity job = job(JobExecutionType.RECURRING);
    RuntimeException failure = new RuntimeException("boom");

    handler.handlePermanentFailure(job, failure);

    verify(deadLetterService).recordDlqTransition(job, failure);
    verify(workflowScheduler, never()).scheduleNext(job);
    verify(pollerScheduler, never()).wakeup();
  }

  @Test
  void handlePermanentFailure_workflowJoinMovesToDlqWithoutSchedulingNext() {
    JobEntity job = job(JobExecutionType.WORKFLOW_JOIN);
    RuntimeException failure = new RuntimeException("boom");

    handler.handlePermanentFailure(job, failure);

    verify(deadLetterService).recordDlqTransition(job, failure);
    verify(workflowScheduler, never()).scheduleNext(job);
    verify(batchService, never()).markChildFailed(job);
    verify(pollerScheduler, never()).wakeup();
  }

  @Test
  void handlePermanentFailure_defaultJobTypesMoveToDlqOnly() {
    RuntimeException failure = new RuntimeException("boom");
    JobEntity batchParent = job(JobExecutionType.BATCH_PARENT);
    JobEntity workflowJoin = job(JobExecutionType.WORKFLOW_JOIN);

    handler.handlePermanentFailure(batchParent, failure);
    handler.handlePermanentFailure(workflowJoin, failure);

    verify(deadLetterService).recordDlqTransition(batchParent, failure);
    verify(deadLetterService).recordDlqTransition(workflowJoin, failure);
    verifyNoInteractions(batchService, workflowScheduler, pollerScheduler);
  }

  @Test
  void handleTimeoutTransition_nonTerminalOutcomeDoesNotRunFailureLifecycle() {
    RuntimeException failure = new RuntimeException("boom");
    when(timeoutTransition.get()).thenReturn(Optional.empty());

    handler.handleTimeoutTransition(failure, true, timeoutTransition);

    verify(timeoutTransition).get();
    verifyNoInteractions(
        batchService, workflowScheduler, deadLetterService, pollerScheduler, jobTerminalStore);
  }

  @Test
  void timeoutWrapperDoesNotDuplicateEventsOrBookkeepingCommittedByCallback() {
    JobEntity job = job(JobExecutionType.SINGLE);
    when(timeoutTransition.get())
        .thenReturn(Optional.of(new TerminalTimeoutTransition(job, List.of())));
    assertTrue(
        handler.handleTimeoutTransition(new RuntimeException("boom"), true, timeoutTransition));
    verify(timeoutTransition).get();
    verifyNoInteractions(deadLetterService, workflowScheduler, batchService);
  }

  @Test
  void timeoutCompletionRegistersTerminalEventsBeforeDependentEvents() {
    JobEntity job = job(JobExecutionType.SINGLE);
    job.setLastError("timed out");
    WorkflowCompletionPlan workflow = WorkflowCompletionPlan.empty();
    when(workflowScheduler.planCompletion(any(), eq(false))).thenReturn(workflow);
    when(jobTerminalStore.commitCompletion(any()))
        .thenReturn(new run.ratchet.store.dto.JobCompletionResult(true, null));
    RuntimeException timeout = new RuntimeException("timed out");
    assertTrue(
        handler.completeTimeoutFailure(
            job, run.ratchet.api.JobStatus.RUNNING, false, timeout, List.of()));
    InOrder order = inOrder(jobTerminalStore, deadLetterService, workflowScheduler);
    order.verify(workflowScheduler).planCompletion(any(), eq(false));
    order.verify(jobTerminalStore).commitCompletion(any());
    order
        .verify(deadLetterService)
        .recordDlqTransitionInCurrentTransaction(any(), eq(timeout), eq(List.of()));
    order.verify(workflowScheduler).publishCompletion(workflow);
  }

  @Test
  void handleJobSuccess_nullJobThrowsNullPointerException() {
    assertThrows(NullPointerException.class, () -> handler.handleJobSuccess(null));
    verifyNoInteractions(
        batchService, workflowScheduler, deadLetterService, pollerScheduler, jobTerminalStore);
  }

  @Test
  void handleJobSuccess_nullJobTypeThrowsNullPointerException() {
    JobEntity job = job(null);

    assertThrows(NullPointerException.class, () -> handler.handleJobSuccess(job));
    verifyNoInteractions(
        batchService, workflowScheduler, deadLetterService, pollerScheduler, jobTerminalStore);
  }

  @Test
  void handlePermanentFailure_nullJobThrowsNullPointerException() {
    RuntimeException failure = new RuntimeException("boom");

    assertThrows(NullPointerException.class, () -> handler.handlePermanentFailure(null, failure));
    verifyNoInteractions(
        batchService, workflowScheduler, deadLetterService, pollerScheduler, jobTerminalStore);
  }

  @Test
  void handlePermanentFailure_nullJobTypeThrowsNullPointerException() {
    JobEntity job = job(null);
    RuntimeException failure = new RuntimeException("boom");

    assertThrows(NullPointerException.class, () -> handler.handlePermanentFailure(job, failure));
    verifyNoInteractions(
        batchService, workflowScheduler, deadLetterService, pollerScheduler, jobTerminalStore);
  }
}
