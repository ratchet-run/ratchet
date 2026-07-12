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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
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
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;

@ExtendWith(MockitoExtension.class)
class PostExecutionHandlerTest {

  @Mock private BatchService batchService;
  @Mock private WorkflowScheduler workflowScheduler;
  @Mock private DeadLetterService deadLetterService;
  @Mock private PollerScheduler pollerScheduler;
  @Mock private Supplier<Optional<JobEntity>> timeoutTransition;

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
            batchService, workflowScheduler, deadLetterService, pollerScheduler);
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

    verifyNoInteractions(batchService, workflowScheduler, deadLetterService, pollerScheduler);
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

    verifyNoInteractions(batchService, workflowScheduler, deadLetterService, pollerScheduler);
  }

  @Test
  void transitionOwningDlqCompositeUsesRequiresNewAndRollsBackOnCheckedExceptions() {
    Transactional transactional = PostExecutionHandler.class.getAnnotation(Transactional.class);

    assertTrue(transactional != null);
    assertTrue(transactional.value() == Transactional.TxType.REQUIRES_NEW);
    assertTrue(Arrays.asList(transactional.rollbackOn()).contains(Exception.class));
  }

  @Test
  void moveToDlqAndHandlePermanentFailureTransitionsBeforeBatchBookkeeping() {
    JobEntity job = job(JobExecutionType.BATCH_CHILD);
    RuntimeException failure = new RuntimeException("boom");
    when(deadLetterService.moveToDlq(job, failure)).thenReturn(true);

    assertTrue(handler.moveToDlqAndHandlePermanentFailure(job, failure));

    InOrder order = inOrder(deadLetterService, batchService);
    order.verify(deadLetterService).moveToDlq(job, failure);
    order.verify(batchService).markChildFailed(job);
  }

  @Test
  void moveToDlqAndHandlePermanentFailureSkipsBookkeepingWhenTransitionLosesRace() {
    JobEntity job = job(JobExecutionType.BATCH_CHILD);
    RuntimeException failure = new RuntimeException("boom");

    assertFalse(handler.moveToDlqAndHandlePermanentFailure(job, failure));

    verify(deadLetterService).moveToDlq(job, failure);
    verify(batchService, never()).markChildFailed(job);
  }

  @Test
  void moveToDlqAndHandlePermanentFailurePropagatesBookkeepingFailureForRollback() {
    JobEntity job = job(JobExecutionType.BATCH_CHILD);
    RuntimeException failure = new RuntimeException("boom");
    when(deadLetterService.moveToDlq(job, failure)).thenReturn(true);
    when(batchService.markChildFailed(job))
        .thenThrow(new IllegalStateException("batch store unavailable"));

    assertThrows(
        IllegalStateException.class,
        () -> handler.moveToDlqAndHandlePermanentFailure(job, failure));

    verify(deadLetterService).moveToDlq(job, failure);
    verify(batchService).markChildFailed(job);
    verify(pollerScheduler, never()).wakeup();
  }

  @Test
  void moveToDlqAndHandlePermanentFailureAppliesWorkflowBookkeepingAndWakeup() {
    JobEntity job = job(JobExecutionType.WORKFLOW_BRANCH);
    RuntimeException failure = new RuntimeException("boom");
    when(deadLetterService.moveToDlq(job, failure)).thenReturn(true);
    when(workflowScheduler.scheduleNext(job)).thenReturn(true);

    assertTrue(handler.moveToDlqAndHandlePermanentFailure(job, failure));

    verify(workflowScheduler).scheduleNext(job);
    verify(batchService, never()).markChildFailed(job);
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
    verifyNoInteractions(batchService, workflowScheduler, deadLetterService, pollerScheduler);
  }

  @Test
  void handleTimeoutTransition_terminalHardTimeoutRoutesFailureInSameBoundary() {
    JobEntity job = job(JobExecutionType.SINGLE);
    RuntimeException failure = new RuntimeException("boom");
    when(timeoutTransition.get()).thenReturn(Optional.of(job));
    when(workflowScheduler.scheduleNext(job)).thenReturn(false);

    boolean terminal = handler.handleTimeoutTransition(failure, false, timeoutTransition);

    assertTrue(terminal);
    InOrder order = inOrder(timeoutTransition, deadLetterService, workflowScheduler);
    order.verify(timeoutTransition).get();
    order.verify(deadLetterService).recordDlqTransition(job, failure);
    order.verify(workflowScheduler).scheduleNext(job);
    verify(workflowScheduler, never()).cancelChain(job);
  }

  @Test
  void handleTimeoutTransition_terminalSignalTimeoutCancelsChainAfterFailureRouting() {
    JobEntity job = job(JobExecutionType.SINGLE);
    RuntimeException failure = new RuntimeException("boom");
    when(timeoutTransition.get()).thenReturn(Optional.of(job));
    when(workflowScheduler.scheduleNext(job)).thenReturn(false);

    handler.handleTimeoutTransition(failure, true, timeoutTransition);

    InOrder order = inOrder(timeoutTransition, deadLetterService, workflowScheduler);
    order.verify(timeoutTransition).get();
    order.verify(deadLetterService).recordDlqTransition(job, failure);
    order.verify(workflowScheduler).scheduleNext(job);
    order.verify(workflowScheduler).cancelChain(job);
  }

  @Test
  void handleJobSuccess_nullJobThrowsNullPointerException() {
    assertThrows(NullPointerException.class, () -> handler.handleJobSuccess(null));
    verifyNoInteractions(batchService, workflowScheduler, deadLetterService, pollerScheduler);
  }

  @Test
  void handleJobSuccess_nullJobTypeThrowsNullPointerException() {
    JobEntity job = job(null);

    assertThrows(NullPointerException.class, () -> handler.handleJobSuccess(job));
    verifyNoInteractions(batchService, workflowScheduler, deadLetterService, pollerScheduler);
  }

  @Test
  void handlePermanentFailure_nullJobThrowsNullPointerException() {
    RuntimeException failure = new RuntimeException("boom");

    assertThrows(NullPointerException.class, () -> handler.handlePermanentFailure(null, failure));
    verifyNoInteractions(batchService, workflowScheduler, deadLetterService, pollerScheduler);
  }

  @Test
  void handlePermanentFailure_nullJobTypeThrowsNullPointerException() {
    JobEntity job = job(null);
    RuntimeException failure = new RuntimeException("boom");

    assertThrows(NullPointerException.class, () -> handler.handlePermanentFailure(job, failure));
    verifyNoInteractions(batchService, workflowScheduler, deadLetterService, pollerScheduler);
  }
}
