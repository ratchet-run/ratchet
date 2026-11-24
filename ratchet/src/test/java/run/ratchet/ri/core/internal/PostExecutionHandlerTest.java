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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import jakarta.transaction.Transactional;
import java.time.Clock;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import run.ratchet.api.JobStatus;
import run.ratchet.api.event.JobCompletedEvent;
import run.ratchet.ri.core.BatchCompletionTransaction;
import run.ratchet.ri.core.BatchService;
import run.ratchet.ri.core.PollerScheduler;
import run.ratchet.ri.core.internal.PostExecutionHandler.TerminalTimeoutTransition;
import run.ratchet.spi.AfterCommitRegistrar;
import run.ratchet.spi.ErrorSanitizer;
import run.ratchet.store.dto.BatchProgress;
import run.ratchet.store.dto.JobCompletionPlan;
import run.ratchet.store.dto.JobCompletionResult;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.spi.JobTerminalStore;

@ExtendWith(MockitoExtension.class)
class PostExecutionHandlerTest {

  @Mock private AfterCommitRegistrar afterCommitRegistrar;
  @Mock private BatchCompletionTransaction completionTransaction;
  @Mock private JobTerminalStore jobTerminalStore;
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

  private void useCompletionTransaction() {
    handler =
        new PostExecutionHandler(
            batchService,
            workflowScheduler,
            deadLetterService,
            pollerScheduler,
            jobTerminalStore,
            afterCommitRegistrar,
            completionTransaction);
  }

  private BatchProgress stubBatchCompletion() {
    BatchProgress progress = new BatchProgress(UUID.randomUUID(), 1, 1, 0, null);
    when(jobTerminalStore.commitCompletion(any())).thenReturn(JobCompletionResult.joined(progress));
    return progress;
  }

  private void stubSuccessfulCommit(AtomicBoolean committed) {
    when(completionTransaction.complete(any()))
        .thenAnswer(
            invocation -> {
              Supplier<?> work = invocation.getArgument(0);
              Object result = work.get();
              committed.set(true);
              return result;
            });
  }

  private void assertFollowupAfterCommit(BatchProgress progress, AtomicBoolean committed) {
    when(batchService.afterChildCompletion(progress))
        .thenAnswer(
            invocation -> {
              assertTrue(committed.get());
              return false;
            });
  }

  private void assertDirectTimeoutUsesRegistrar(JobEntity child, BatchProgress progress) {
    when(afterCommitRegistrar.registerAfterCommit(any()))
        .thenReturn(AfterCommitRegistrar.Result.REGISTERED);
    assertTrue(handler.completeTimeoutFailure(child, JobStatus.RUNNING, false, null, List.of()));
    ArgumentCaptor<Runnable> followup = ArgumentCaptor.forClass(Runnable.class);
    verify(afterCommitRegistrar).registerAfterCommit(followup.capture());
    verify(batchService, never()).afterChildCompletion(progress);
    followup.getValue().run();
    verify(batchService).afterChildCompletion(progress);
  }

  private InternalEventPublisher useRealDeadLetterService() {
    InternalEventPublisher publisher = mock(InternalEventPublisher.class);
    DeadLetterService dlq =
        new DeadLetterService(
            null,
            null,
            jobTerminalStore,
            null,
            publisher,
            mock(ErrorSanitizer.class),
            Clock.systemUTC(),
            afterCommitRegistrar);
    handler =
        new PostExecutionHandler(
            batchService,
            workflowScheduler,
            dlq,
            pollerScheduler,
            jobTerminalStore,
            afterCommitRegistrar,
            completionTransaction);
    return publisher;
  }

  @ParameterizedTest
  @ValueSource(strings = {"events", "workflow", "wakeup"})
  void durablePostCommitFailuresDoNotSkipRemainingSteps(String failedStep) {
    InternalEventPublisher publisher = useRealDeadLetterService();
    JobEntity source = job(JobExecutionType.SINGLE);
    source.setLastError("failed");
    source.setAttempts(1);
    BatchProgress progress = new BatchProgress(UUID.randomUUID(), 1, 0, 1, null);
    WorkflowCompletionPlan workflow = new WorkflowCompletionPlan(List.of(), List.of(), true);
    when(workflowScheduler.planCompletion(any(), eq(false))).thenReturn(workflow);
    when(jobTerminalStore.commitCompletion(any()))
        .thenReturn(JobCompletionResult.durable(progress));
    when(afterCommitRegistrar.registerAfterCommit(any()))
        .thenReturn(AfterCommitRegistrar.Result.NO_ACTIVE_TRANSACTION);
    AtomicBoolean returned = new AtomicBoolean();
    stubSuccessfulCommit(returned);
    if (failedStep.equals("wakeup"))
      doThrow(new RejectedExecutionException("stopped")).when(pollerScheduler).wakeup();
    if (failedStep.equals("workflow"))
      doThrow(new IllegalStateException("publication failed"))
          .when(workflowScheduler)
          .publishCompletion(workflow);
    doAnswer(
            invocation -> {
              assertTrue(returned.get());
              if (failedStep.equals("events"))
                throw new IllegalStateException("event publication failed");
              return null;
            })
        .when(publisher)
        .publish(any());

    assertTrue(handler.completeFailure(source, JobStatus.RUNNING, false, null));

    verify(publisher, times(failedStep.equals("events") ? 1 : 2)).publish(any());
    verify(workflowScheduler).publishCompletion(workflow);
    verify(pollerScheduler).wakeup();
    verify(batchService).afterChildCompletion(progress);
    verify(jobTerminalStore).commitCompletion(any());
  }

  @ParameterizedTest
  @CsvSource({
    "failure,true",
    "failure,false",
    "dlq,true",
    "dlq,false",
    "timeout,true",
    "timeout,false",
    "success,true",
    "success,false",
    "minimal,true",
    "minimal,false"
  })
  void completionTransactionFailureHonorsStoreDurability(String entryPoint, boolean durable) {
    InternalEventPublisher publisher = useRealDeadLetterService();
    JobEntity source = job(JobExecutionType.BATCH_CHILD);
    source.setLastError("failed");
    source.setAttempts(1);
    BatchProgress progress = new BatchProgress(UUID.randomUUID(), 1, 0, 1, null);
    when(jobTerminalStore.commitCompletion(any()))
        .thenReturn(
            durable ? JobCompletionResult.durable(progress) : JobCompletionResult.joined(progress));
    boolean success = entryPoint.equals("success") || entryPoint.equals("minimal");
    if (durable && !success) {
      when(afterCommitRegistrar.registerAfterCommit(any()))
          .thenReturn(AfterCommitRegistrar.Result.NO_ACTIVE_TRANSACTION);
    }
    RuntimeException lateFailure = new IllegalStateException("completion transaction failed");
    when(completionTransaction.complete(any()))
        .thenAnswer(
            invocation -> {
              Supplier<?> work = invocation.getArgument(0);
              assertEquals(true, work.get());
              verify(publisher, never()).publish(any());
              verify(workflowScheduler, never()).publishTerminalEvent(any());
              verify(workflowScheduler, never()).publishCompletion(any());
              verifyNoInteractions(batchService);
              if (entryPoint.equals("dlq")) assertEquals(JobStatus.FAILED, source.getStatus());
              throw lateFailure;
            });
    Supplier<Boolean> complete =
        () ->
            switch (entryPoint) {
              case "failure" -> handler.completeFailure(source, JobStatus.RUNNING, false, null);
              case "dlq" -> handler.moveToDlqAndHandlePermanentFailure(source, lateFailure, null);
              case "timeout" ->
                  handler.handleTimeoutTransition(
                      lateFailure,
                      false,
                      () -> {
                        assertTrue(
                            handler.completeTimeoutFailure(
                                source, JobStatus.RUNNING, false, lateFailure, List.of()));
                        return Optional.of(new TerminalTimeoutTransition(source, List.of()));
                      });
              case "success" ->
                  handler.completeSuccess(
                      source, null, null, Instant.EPOCH, Instant.EPOCH, 0, 0, null);
              case "minimal" ->
                  handler.completeSuccessMinimal(source, Instant.EPOCH, Instant.EPOCH, 0, 0, null);
              default -> throw new AssertionError(entryPoint);
            };

    if (durable) {
      assertTrue(complete.get());
      verify(workflowScheduler).publishCompletion(any());
      verify(batchService).afterChildCompletion(progress);
      if (success) verify(workflowScheduler).publishTerminalEvent(any(JobCompletedEvent.class));
      else verify(publisher, times(entryPoint.equals("timeout") ? 1 : 2)).publish(any());
    } else {
      assertSame(lateFailure, assertThrows(IllegalStateException.class, complete::get));
      verifyNoInteractions(batchService, publisher);
      verify(workflowScheduler, never()).publishTerminalEvent(any());
      verify(workflowScheduler, never()).publishCompletion(any());
      verifyNoInteractions(afterCommitRegistrar);
    }
    verify(jobTerminalStore).commitCompletion(any());
    AtomicBoolean contextCleared = new AtomicBoolean();
    PostExecutionHandler.deferPostCommit(() -> contextCleared.set(true));
    assertTrue(contextCleared.get());
  }

  @Test
  void allDeferredStepsRunAfterCompletionTransactionReturns() {
    useCompletionTransaction();
    JobEntity child = job(JobExecutionType.BATCH_CHILD);
    BatchProgress progress = stubBatchCompletion();
    AtomicBoolean returned = new AtomicBoolean();
    when(completionTransaction.complete(any()))
        .thenAnswer(
            invocation -> {
              Supplier<?> work = invocation.getArgument(0);
              Object result = work.get();
              verify(workflowScheduler, never()).publishTerminalEvent(any());
              verify(workflowScheduler, never()).publishCompletion(any());
              verifyNoInteractions(batchService);
              returned.set(true);
              return result;
            });
    doAnswer(
            invocation -> {
              assertTrue(returned.get());
              return null;
            })
        .when(workflowScheduler)
        .publishTerminalEvent(any());
    doAnswer(
            invocation -> {
              assertTrue(returned.get());
              return null;
            })
        .when(workflowScheduler)
        .publishCompletion(any());
    assertFollowupAfterCommit(progress, returned);

    assertTrue(
        handler.completeSuccess(child, null, null, Instant.EPOCH, Instant.EPOCH, 0, 0, null));

    verify(workflowScheduler).publishTerminalEvent(any(JobCompletedEvent.class));
    verify(workflowScheduler).publishCompletion(any());
    verify(batchService).afterChildCompletion(progress);
  }

  @Test
  void errorPropagatesEvenAfterDurableCommitWithoutRunningDeferredSteps() {
    useCompletionTransaction();
    JobEntity child = job(JobExecutionType.BATCH_CHILD);
    BatchProgress progress = new BatchProgress(UUID.randomUUID(), 1, 1, 0, null);
    when(jobTerminalStore.commitCompletion(any()))
        .thenReturn(JobCompletionResult.durable(progress));
    AssertionError failure = new AssertionError("completion error");
    when(completionTransaction.complete(any()))
        .thenAnswer(
            invocation -> {
              Supplier<?> work = invocation.getArgument(0);
              work.get();
              throw failure;
            });

    assertSame(
        failure,
        assertThrows(
            AssertionError.class,
            () -> handler.completeFailure(child, JobStatus.RUNNING, false, null)));

    verify(workflowScheduler, never()).publishCompletion(any());
    verifyNoInteractions(batchService);
  }

  @Test
  void completionPlanRejectsAttemptsFenceWithoutClaimSequence() {
    JobEntity source = job(JobExecutionType.SINGLE);
    source.setClaimSeq(7L);
    source.setStatus(JobStatus.SUCCEEDED);
    var plan =
        PostExecutionHandler.completionPlan(
            source, JobStatus.RUNNING, null, WorkflowCompletionPlan.empty(), 2);
    assertEquals(2, plan.expectedAttempts());
    assertEquals(7L, plan.expectedClaimSeq());
    assertThrows(
        IllegalArgumentException.class,
        () ->
            PostExecutionHandler.completionPlan(
                source, JobStatus.WAITING, null, WorkflowCompletionPlan.empty(), 2));
    source.setClaimSeq(null);
    assertThrows(
        IllegalArgumentException.class,
        () ->
            PostExecutionHandler.completionPlan(
                source, JobStatus.RUNNING, null, WorkflowCompletionPlan.empty(), 2));
  }

  @Test
  void timeoutCompletionHasNoAttemptsFence() {
    JobEntity source = job(JobExecutionType.SINGLE);
    source.setClaimSeq(7L);
    when(workflowScheduler.planCompletion(any(), eq(false)))
        .thenReturn(WorkflowCompletionPlan.empty());
    when(jobTerminalStore.commitCompletion(any())).thenReturn(JobCompletionResult.notCommitted());

    assertFalse(handler.completeTimeoutFailure(source, JobStatus.RUNNING, false, null, List.of()));

    ArgumentCaptor<JobCompletionPlan> plan = ArgumentCaptor.forClass(JobCompletionPlan.class);
    verify(jobTerminalStore).commitCompletion(plan.capture());
    assertEquals(null, plan.getValue().expectedAttempts());
    assertEquals(7L, plan.getValue().expectedClaimSeq());
  }

  @Test
  void successRunsBatchFollowupAfterCompletionTransactionReturns() {
    useCompletionTransaction();
    JobEntity child = job(JobExecutionType.BATCH_CHILD);
    BatchProgress progress = stubBatchCompletion();
    AtomicBoolean committed = new AtomicBoolean();
    stubSuccessfulCommit(committed);
    assertFollowupAfterCommit(progress, committed);

    Instant now = Instant.now();
    assertTrue(handler.completeSuccess(child, null, null, now, now, 0, 0, null));

    verify(completionTransaction).complete(any());
    verify(batchService).afterChildCompletion(progress);
    verifyNoInteractions(afterCommitRegistrar);
  }

  @Test
  void failedCommitDiscardsBatchFollowupAndClearsCollector() {
    useCompletionTransaction();
    JobEntity child = job(JobExecutionType.BATCH_CHILD);
    BatchProgress progress = stubBatchCompletion();
    RuntimeException failure = new IllegalStateException("commit failed");
    when(completionTransaction.complete(any()))
        .thenAnswer(
            invocation -> {
              Supplier<?> work = invocation.getArgument(0);
              work.get();
              throw failure;
            });

    Instant now = Instant.now();
    assertSame(
        failure,
        assertThrows(
            IllegalStateException.class,
            () -> handler.completeSuccess(child, null, null, now, now, 0, 0, null)));
    verifyNoInteractions(batchService, afterCommitRegistrar);

    assertDirectTimeoutUsesRegistrar(child, progress);
    verify(completionTransaction).complete(any());
  }

  @Test
  void dlqCompositeUsesOneCompletionTransactionAndFollowsUpAfterCommit() {
    useCompletionTransaction();
    JobEntity child = job(JobExecutionType.BATCH_CHILD);
    BatchProgress progress = stubBatchCompletion();
    AtomicBoolean committed = new AtomicBoolean();
    when(deadLetterService.sanitizeForCompletion(any())).thenReturn("sanitized failure");
    when(completionTransaction.complete(any()))
        .thenAnswer(
            invocation -> {
              assertFalse(committed.get());
              Supplier<?> work = invocation.getArgument(0);
              Object result = work.get();
              assertEquals("sanitized failure", child.getLastError());
              assertEquals(JobStatus.FAILED, child.getStatus());
              verify(deadLetterService)
                  .recordDlqTransitionInCurrentTransaction(any(), any(), any());
              committed.set(true);
              return result;
            });
    assertFollowupAfterCommit(progress, committed);

    assertTrue(
        handler.moveToDlqAndHandlePermanentFailure(child, new RuntimeException("boom"), null));

    verify(completionTransaction).complete(any());
    verify(batchService).afterChildCompletion(progress);
    verifyNoInteractions(afterCommitRegistrar);
  }

  @Test
  void timeoutWrapperCollectsCallbackFollowupUntilCompletionTransactionReturns() {
    useCompletionTransaction();
    JobEntity child = job(JobExecutionType.BATCH_CHILD);
    BatchProgress progress = stubBatchCompletion();
    AtomicBoolean committed = new AtomicBoolean();
    stubSuccessfulCommit(committed);
    assertFollowupAfterCommit(progress, committed);
    RuntimeException failure = new RuntimeException("timeout");

    assertTrue(
        handler.handleTimeoutTransition(
            failure,
            false,
            () -> {
              assertTrue(
                  handler.completeTimeoutFailure(
                      child, JobStatus.RUNNING, false, failure, List.of()));
              verifyNoInteractions(batchService, afterCommitRegistrar);
              return Optional.of(new TerminalTimeoutTransition(child, List.of()));
            }));

    verify(completionTransaction).complete(any());
    verify(batchService).afterChildCompletion(progress);
    verifyNoInteractions(afterCommitRegistrar);
  }

  @Test
  void directTimeoutCompletionRegistersBatchFollowup() {
    useCompletionTransaction();
    JobEntity child = job(JobExecutionType.BATCH_CHILD);
    BatchProgress progress = stubBatchCompletion();

    assertDirectTimeoutUsesRegistrar(child, progress);

    verifyNoInteractions(completionTransaction);
  }

  @Test
  void successfulCompletionClearsCollectorBeforeDirectTimeoutCompletion() {
    useCompletionTransaction();
    JobEntity child = job(JobExecutionType.BATCH_CHILD);
    BatchProgress completedProgress = new BatchProgress(UUID.randomUUID(), 1, 1, 0, null);
    BatchProgress directProgress = new BatchProgress(UUID.randomUUID(), 1, 0, 1, null);
    when(jobTerminalStore.commitCompletion(any()))
        .thenReturn(
            JobCompletionResult.joined(completedProgress),
            JobCompletionResult.joined(directProgress));
    AtomicBoolean committed = new AtomicBoolean();
    stubSuccessfulCommit(committed);
    Instant now = Instant.now();

    assertTrue(handler.completeSuccess(child, null, null, now, now, 0, 0, null));
    verify(batchService).afterChildCompletion(completedProgress);
    verifyNoInteractions(afterCommitRegistrar);
    assertDirectTimeoutUsesRegistrar(child, directProgress);
    verify(completionTransaction).complete(any());
  }

  @Test
  void completionEntryPointsOverrideClassTransactionAndTimeoutCompletionJoins()
      throws NoSuchMethodException {
    for (String methodName :
        List.of(
            "completeSuccess",
            "completeSuccessMinimal",
            "completeFailure",
            "moveToDlqAndHandlePermanentFailure",
            "handleTimeoutTransition")) {
      Transactional transactional =
          Arrays.stream(PostExecutionHandler.class.getDeclaredMethods())
              .filter(method -> method.getName().equals(methodName))
              .findFirst()
              .orElseThrow()
              .getAnnotation(Transactional.class);
      assertEquals(Transactional.TxType.NOT_SUPPORTED, transactional.value(), methodName);
    }
    Transactional timeout =
        PostExecutionHandler.class
            .getMethod(
                "completeTimeoutFailure",
                JobEntity.class,
                JobStatus.class,
                boolean.class,
                Throwable.class,
                List.class)
            .getAnnotation(Transactional.class);
    assertEquals(Transactional.TxType.REQUIRED, timeout.value());
    assertTrue(Arrays.asList(timeout.rollbackOn()).contains(Exception.class));
    Transactional completion =
        BatchCompletionTransaction.class
            .getMethod("complete", Supplier.class)
            .getAnnotation(Transactional.class);
    assertEquals(Transactional.TxType.REQUIRES_NEW, completion.value());
    assertTrue(Arrays.asList(completion.rollbackOn()).contains(Exception.class));
  }

  @Test
  void successUsesOneAtomicPlanAndDoesNotRepeatBatchCounterUpdates() {
    JobEntity child = job(JobExecutionType.BATCH_CHILD);
    UUID batchId = UUID.randomUUID();
    child.setDependsOn(batchId);
    BatchProgress progress = new BatchProgress(batchId, 2, 1, 0, null);
    when(jobTerminalStore.commitCompletion(any())).thenReturn(JobCompletionResult.joined(progress));
    Instant now = Instant.parse("2026-09-15T00:00:00Z");
    assertTrue(handler.completeSuccess(child, "42", "java.lang.Integer", now, now, 5L, 2L, null));
    ArgumentCaptor<JobCompletionPlan> plan = ArgumentCaptor.forClass(JobCompletionPlan.class);
    verify(jobTerminalStore).commitCompletion(plan.capture());
    assertEquals(batchId, plan.getValue().batchId());
    assertEquals(JobStatus.SUCCEEDED, plan.getValue().terminalStatus());
    assertEquals("42", plan.getValue().resultJson());
    InOrder order = inOrder(jobTerminalStore, workflowScheduler, batchService);
    order.verify(jobTerminalStore).commitCompletion(any());
    ArgumentCaptor<JobCompletedEvent> completed = ArgumentCaptor.forClass(JobCompletedEvent.class);
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
    when(jobTerminalStore.commitCompletion(any())).thenReturn(JobCompletionResult.notCommitted());
    Instant now = Instant.now();
    assertFalse(handler.completeSuccess(child, null, null, now, now, 0, 0, null));
    verifyNoInteractions(workflowScheduler, batchService, deadLetterService, pollerScheduler);
  }

  @Test
  void staleSuccessPlanPublishesNothing() {
    JobEntity child = job(JobExecutionType.BATCH_CHILD);
    when(jobTerminalStore.commitCompletion(any()))
        .thenThrow(new IllegalStateException("stale dependency"));
    Instant now = Instant.now();
    assertThrows(
        IllegalStateException.class,
        () -> handler.completeSuccess(child, null, null, now, now, 0, 0, null));
    verifyNoInteractions(workflowScheduler, batchService, deadLetterService, pollerScheduler);
  }

  @Test
  void parentFollowupFailureDoesNotReverseSuccessfulChildCompletion() {
    JobEntity child = job(JobExecutionType.BATCH_CHILD);
    BatchProgress progress = new BatchProgress(UUID.randomUUID(), 1, 1, 0, null);
    when(jobTerminalStore.commitCompletion(any())).thenReturn(JobCompletionResult.joined(progress));
    when(batchService.afterChildCompletion(progress))
        .thenThrow(new IllegalStateException("parent temporarily unavailable"));
    Instant now = Instant.now();
    assertTrue(handler.completeSuccess(child, null, null, now, now, 0, 0, null));
    verify(workflowScheduler).publishTerminalEvent(any());
    verifyNoInteractions(deadLetterService);
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
    BatchProgress progress = new BatchProgress(batchId, 1, 0, 1, null);
    when(jobTerminalStore.commitCompletion(any())).thenReturn(JobCompletionResult.joined(progress));
    assertTrue(handler.moveToDlqAndHandlePermanentFailure(job, failure, null));
    InOrder order = inOrder(jobTerminalStore, batchService);
    order.verify(jobTerminalStore).commitCompletion(any());
    order.verify(batchService).afterChildCompletion(progress);
    verify(batchService, never()).markChildFailed(any());
  }

  @Test
  void failureCompositeSkipsEffectsWhenTerminalRaceIsLost() {
    JobEntity job = job(JobExecutionType.BATCH_CHILD);
    when(jobTerminalStore.commitCompletion(any())).thenReturn(JobCompletionResult.notCommitted());
    assertFalse(
        handler.moveToDlqAndHandlePermanentFailure(job, new RuntimeException("boom"), null));
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
        () -> handler.moveToDlqAndHandlePermanentFailure(job, new RuntimeException("boom"), null));
    verifyNoInteractions(batchService, pollerScheduler);
    verify(deadLetterService, never()).recordDlqTransitionInCurrentTransaction(any(), any(), any());
  }

  @Test
  void failureCompositePublishesPreparedWorkflowAfterCommit() {
    JobEntity job = job(JobExecutionType.WORKFLOW_BRANCH);
    WorkflowCompletionPlan plan = new WorkflowCompletionPlan(List.of(), List.of(), true);
    when(workflowScheduler.planCompletion(any(), eq(false))).thenReturn(plan);
    when(jobTerminalStore.commitCompletion(any())).thenReturn(JobCompletionResult.joined(null));
    assertTrue(handler.moveToDlqAndHandlePermanentFailure(job, new RuntimeException("boom"), null));
    verify(workflowScheduler).publishCompletion(plan);
    verify(pollerScheduler).wakeup();
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
    when(jobTerminalStore.commitCompletion(any())).thenReturn(JobCompletionResult.joined(null));
    RuntimeException timeout = new RuntimeException("timed out");
    assertTrue(handler.completeTimeoutFailure(job, JobStatus.RUNNING, false, timeout, List.of()));
    InOrder order = inOrder(jobTerminalStore, deadLetterService, workflowScheduler);
    order.verify(workflowScheduler).planCompletion(any(), eq(false));
    order.verify(jobTerminalStore).commitCompletion(any());
    order
        .verify(deadLetterService)
        .recordDlqTransitionInCurrentTransaction(any(), eq(timeout), eq(List.of()));
    order.verify(workflowScheduler).publishCompletion(workflow);
  }

  @Test
  void rejectedCompletionKeepsOwnerFenceAndHasNoDlqOrPublishedEffects() {
    JobEntity job = job(JobExecutionType.BATCH_CHILD);
    job.setClaimSeq(37L);
    when(jobTerminalStore.commitCompletion(any())).thenReturn(JobCompletionResult.notCommitted());
    assertFalse(
        handler.moveToDlqAndHandlePermanentFailure(job, new RuntimeException("stale"), null));
    var plan = ArgumentCaptor.forClass(JobCompletionPlan.class);
    verify(jobTerminalStore).commitCompletion(plan.capture());
    assertEquals(37L, plan.getValue().expectedClaimSeq().longValue());
    verifyNoInteractions(batchService);
    verify(deadLetterService, never()).recordDlqTransitionInCurrentTransaction(any(), any(), any());
    verify(workflowScheduler, never()).publishTerminalEvent(any());
    verify(workflowScheduler, never()).publishCompletion(any());
  }
}
