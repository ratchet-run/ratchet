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
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import jakarta.transaction.Transactional;
import java.sql.SQLTransientException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.InOrder;
import run.ratchet.api.CircuitBreakerProtected;
import run.ratchet.api.JobContext;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobStatus;
import run.ratchet.api.RatchetOptions;
import run.ratchet.api.TransactionalJob;
import run.ratchet.api.exception.CircuitBreakerOpenException;
import run.ratchet.ri.core.JobTransaction;
import run.ratchet.ri.core.TransactionalCompletion;
import run.ratchet.ri.security.DefaultErrorSanitizer;
import run.ratchet.ri.testutil.JsonbTestPayloadSerializer;
import run.ratchet.spi.AfterCommitRegistrar;
import run.ratchet.spi.CircuitBreakerExceptionFilter;
import run.ratchet.spi.JobLogger;
import run.ratchet.spi.ResilienceStrategy;
import run.ratchet.spi.ResultPersistenceStrategy;
import run.ratchet.spi.RetryPolicy;
import run.ratchet.spi.SerializedJobResult;
import run.ratchet.spi.TracingCollector;
import run.ratchet.store.dto.JobClaimDto;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionEntity;
import run.ratchet.store.entity.JobExecutionEntity.ExecutionStatus;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.entity.JobPayload;
import run.ratchet.store.spi.JobStateStore;
import run.ratchet.store.spi.JobStateStore.StateWriteResult;
import run.ratchet.store.spi.JobStore;
import run.ratchet.store.spi.TerminalOutcome;
import run.ratchet.store.spi.TransactionalCompletionStore;

class JobTaskTransactionalTest {
  private static final Instant NOW = Instant.parse("2026-11-13T12:00:00Z");
  private static final ThreadLocal<Callable<Object>> BODY = new ThreadLocal<>();
  private final JobStore store = mock(JobStore.class);
  private final TransactionalCompletionStore transactions =
      mock(TransactionalCompletionStore.class);
  private final PostExecutionHandler lifecycle = mock(PostExecutionHandler.class);
  private final ExecutionObserver observer = mock(ExecutionObserver.class);
  private final PreExecutionValidator validator = mock(PreExecutionValidator.class);
  private final JobSuccessFinalizer finalizer = mock(JobSuccessFinalizer.class);
  private final JobTransaction carrier = spy(new JobTransaction());
  private final ResilienceStrategy resilience = mock(ResilienceStrategy.class);
  private final ResultPersistenceStrategy results = mock(ResultPersistenceStrategy.class);
  private final RetryPolicy retries = mock(RetryPolicy.class);
  private final JobStateFlusher flusher = mock(JobStateFlusher.class);
  private final TransactionalCompletion completion = new TransactionalCompletion(List.of());
  private final AtomicReference<CircuitBreakerExceptionFilter> callFilter = new AtomicReference<>();
  private final AtomicReference<Throwable> supplierFailure = new AtomicReference<>();
  private JobEntity job;
  private JobExecutionEntity execution;
  private JobAttemptControl attempt;
  private JobTask task;
  private final JobTimeoutHandler timeoutHandler = mock(JobTimeoutHandler.class);

  @TransactionalJob
  @CircuitBreakerProtected(
      recordExceptions = RuntimeException.class,
      ignoreExceptions = UnsupportedOperationException.class)
  public static Object transactionalBody() throws Exception {
    return BODY.get().call();
  }

  public static Object ordinaryBody() throws Exception {
    return BODY.get().call();
  }

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setUp() throws Exception {
    job = new JobEntity();
    job.setId(UUID.randomUUID());
    job.setJobType(JobExecutionType.SINGLE);
    job.setStatus(JobStatus.RUNNING);
    job.setPriority(JobPriority.NORMAL);
    job.setPickedBy("node");
    job.setPickedAt(NOW);
    job.setClaimSeq(7L);
    job.setMaxRetries(3);
    job.setPayload(payload("transactionalBody", "()Ljava/lang/Object;"));
    execution = JobExecutionEntity.start(job.getId(), 1, "node");
    attempt = new JobAttemptControl(job.getId(), NOW.plusSeconds(30), 30, NOW, 0, 7L, "node");
    task =
        new JobTask(
            store,
            null,
            lifecycle,
            () -> "node",
            observer,
            validator,
            new JobPayloadInvoker(null, name -> true),
            carrier,
            finalizer,
            retries,
            resilience,
            new DefaultErrorSanitizer(),
            context -> mock(JobLogger.class),
            results,
            null,
            new JsonbTestPayloadSerializer(),
            mock(AfterCommitRegistrar.class),
            flusher,
            timeoutHandler,
            Clock.fixed(NOW, ZoneOffset.UTC),
            null,
            RatchetOptions.defaults());
    task.init(job);
    task.attach(attempt);
    when(store.capability(TransactionalCompletionStore.class))
        .thenReturn(Optional.of(transactions));
    when(store.getJobStatus(job.getId())).thenReturn(JobStatus.RUNNING);
    when(observer.startExecutionScope(job))
        .thenReturn(TracingCollector.NoOpExecutionScope.INSTANCE);
    when(observer.startExecution(eq(job.getId()), anyInt(), anyString())).thenReturn(execution);
    when(resilience.isServiceAvailable(anyString())).thenReturn(true);
    when(resilience.execute(anyString(), any(), any(Callable.class)))
        .thenAnswer(
            invocation -> {
              callFilter.set(invocation.getArgument(1));
              try {
                return ((Callable<?>) invocation.getArgument(2)).call();
              } catch (Exception failure) {
                supplierFailure.set(failure);
                throw failure;
              }
            });
    when(results.serialize(any(), any()))
        .thenReturn(new SerializedJobResult("42", "java.lang.Integer"));
    when(lifecycle.completeSuccessInCurrentTransaction(
            eq(job), any(), any(), any(), any(), anyLong(), anyLong()))
        .thenReturn(completion);
    BODY.set(() -> 42);
  }

  @AfterEach
  void cleanUp() {
    BODY.remove();
    Thread.interrupted();
    JobContext.clear();
  }

  @Test
  void watchdogInterruptEscapesRollbackBoundaryAndDefersTimeoutWithoutGenericFailure()
      throws Exception {
    InterruptedException interrupted = new InterruptedException("watchdog interrupt");
    AtomicBoolean rollbackBoundaryReached = new AtomicBoolean();
    doAnswer(
            invocation -> {
              try {
                return invocation.callRealMethod();
              } catch (Exception failure) {
                // A unit-test carrier has no container interceptor. Observe the exception crossing
                // the rollbackOn=Exception boundary rather than simulating a committed transaction.
                assertSame(interrupted, failure);
                assertFalse(JobTransaction.active());
                rollbackBoundaryReached.set(true);
                throw failure;
              }
            })
        .when(carrier)
        .run(any());
    BODY.set(
        () -> {
          assertTrue(JobTransaction.active());
          when(timeoutHandler.isWatchdogCancelled(job.getId())).thenReturn(true);
          Thread.currentThread().interrupt();
          throw interrupted;
        });
    task.call();
    assertTrue(rollbackBoundaryReached.get());
    assertTrue(
        Arrays.asList(
                JobTransaction.class
                    .getMethod("run", Callable.class)
                    .getAnnotation(Transactional.class)
                    .rollbackOn())
            .contains(Exception.class));
    verify(carrier).run(any());
    verify(timeoutHandler).isWatchdogCancelled(job.getId());
    verify(observer).saveExecution(execution);
    assertEquals(ExecutionStatus.FAILED, execution.getStatus());
    assertEquals(TimeoutException.class.getName(), execution.getErrorClass());
    assertEquals(
        TimeoutException.class.getName() + ": Hard timeout exceeded (30s)",
        execution.getErrorMessage());
    verify(store, never()).incrementRetryAttempt(any(), any());
    verify(timeoutHandler, never()).processCooperativeTimeout(any());
    verify(lifecycle, never()).completeFailure(any(), any(), anyBoolean());
    verify(lifecycle, never())
        .completeSuccessInCurrentTransaction(
            any(), any(), any(), any(), any(), anyLong(), anyLong());
    verify(lifecycle, never()).runFollowups(any());
    verifyNoInteractions(finalizer);
  }

  @Test
  void committedRunsAfterCommitWorkAndCallbackWithoutFinalizer() throws Exception {
    AtomicInteger callbacks = new AtomicInteger();
    // A separate callback target proves callbacks run after the carrier has returned.
    job.setOnSuccessPayload(payload("afterCommit", "()V"));
    CALLBACK.set(
        () -> {
          assertFalse(JobTransaction.active());
          callbacks.incrementAndGet();
        });
    try {
      BODY.set(
          () -> {
            assertTrue(JobTransaction.active());
            return 42;
          });
      doAnswer(
              inv -> {
                assertFalse(JobTransaction.active());
                return null;
              })
          .when(lifecycle)
          .runFollowups(completion);
      task.call();
      verify(carrier).run(any());
      verify(lifecycle).runFollowups(completion);
      verify(observer).recordJobSuccess(job, 0L);
      assertEquals(1, callbacks.get());
      assertEquals(ExecutionStatus.SUCCEEDED, execution.getStatus());
      verifyNoInteractions(finalizer);
    } finally {
      CALLBACK.remove();
    }
  }

  private static final ThreadLocal<Runnable> CALLBACK = new ThreadLocal<>();

  public static void afterCommit() {
    CALLBACK.get().run();
  }

  @Test
  void transientCompletionRetriesBodyAndTransactionWithoutSpendingJobAttempt() throws Exception {
    AtomicInteger bodies = new AtomicInteger();
    BODY.set(() -> bodies.incrementAndGet());
    when(lifecycle.completeSuccessInCurrentTransaction(
            eq(job), any(), any(), any(), any(), anyLong(), anyLong()))
        .thenThrow(new IllegalStateException(new SQLTransientException("deadlock")))
        .thenReturn(completion);
    task.call();
    assertEquals(2, bodies.get());
    verify(carrier, times(2)).run(any());
    verify(store, never()).incrementRetryAttempt(any(), any());
    verify(lifecycle).runFollowups(completion);
  }

  @Test
  void fiveTransientFailuresUseOrdinaryFailureAndAreIgnoredByBreaker() throws Exception {
    SQLTransientException original = new SQLTransientException("deadlock");
    AtomicInteger bodies = new AtomicInteger();
    BODY.set(() -> bodies.incrementAndGet());
    when(lifecycle.completeSuccessInCurrentTransaction(
            eq(job), any(), any(), any(), any(), anyLong(), anyLong()))
        .thenThrow(new IllegalStateException(original));
    when(store.incrementRetryAttempt(job.getId(), 7L)).thenReturn(1);
    task.call();
    assertEquals(5, bodies.get());
    verify(carrier, times(5)).run(any());
    verify(store).incrementRetryAttempt(job.getId(), 7L);
    verify(lifecycle, never()).runFollowups(any());
    assertEquals(
        CircuitBreakerExceptionFilter.Outcome.RECORDED,
        callFilter.get().classify(supplierFailure.get().getCause()));
    assertEquals(
        CircuitBreakerExceptionFilter.Outcome.IGNORED,
        callFilter.get().classify(supplierFailure.get()));
    CircuitBreakerExceptionFilter profile =
        new CircuitBreakerExceptionFilter(
            List.of(RuntimeException.class), List.of(), ignored -> true);
    assertEquals(
        CircuitBreakerExceptionFilter.Outcome.IGNORED,
        profile.merge(callFilter.get()).classify(supplierFailure.get()));
  }

  @Test
  void deterministicBodyFailureIsUnwrappedAndUsesJobFilter() throws Exception {
    IllegalArgumentException original = new IllegalArgumentException("body");
    BODY.set(
        () -> {
          throw original;
        });
    AtomicReference<Throwable> observed = new AtomicReference<>();
    doAnswer(
            inv -> {
              CircuitBreakerExceptionFilter filter = inv.getArgument(1);
              try {
                return ((Callable<?>) inv.getArgument(2)).call();
              } catch (Exception failure) {
                observed.set(failure);
                assertEquals(
                    CircuitBreakerExceptionFilter.Outcome.RECORDED, filter.classify(failure));
                throw failure;
              }
            })
        .when(resilience)
        .execute(anyString(), any(), any());
    task.call();
    assertSame(original, observed.get());
    verify(carrier).run(any());
    verify(store).incrementRetryAttempt(job.getId(), 7L);
  }

  @Test
  void deterministicBodyFailureRetainsItsOwnIgnoreRule() {
    UnsupportedOperationException original = new UnsupportedOperationException("body");
    BODY.set(
        () -> {
          throw original;
        });
    task.call();
    assertSame(original, supplierFailure.get());
    assertEquals(
        CircuitBreakerExceptionFilter.Outcome.IGNORED,
        callFilter.get().classify(supplierFailure.get()));
    verify(store).incrementRetryAttempt(job.getId(), 7L);
  }

  @Test
  void deterministicCompletionErrorDoesNotRetryWholeTransaction() throws Exception {
    when(lifecycle.completeSuccessInCurrentTransaction(
            eq(job), any(), any(), any(), any(), anyLong(), anyLong()))
        .thenThrow(new IllegalArgumentException("constraint violation at flush"));
    task.call();
    assertEquals(IllegalArgumentException.class, supplierFailure.get().getClass());
    verify(carrier).run(any());
    verify(store).incrementRetryAttempt(job.getId(), 7L);
  }

  @Test
  void progressStaysEnabledAndFinalFlushFollowsCommitAndFollowups() {
    JobStateStore state = mock(JobStateStore.class);
    when(store.capability(JobStateStore.class)).thenReturn(Optional.of(state));
    when(state.initState(eq(job.getId()), eq(JobProgressWriter.NAMESPACE), anyString(), eq(7L)))
        .thenAnswer(
            inv -> {
              assertFalse(JobTransaction.active());
              return StateWriteResult.UPDATED;
            });
    BODY.set(
        () -> {
          JobContext.current().progress(1, 2);
          verify(state, never()).initState(any(), anyString(), anyString(), anyLong());
          return 42;
        });
    task.call();
    InOrder order = inOrder(lifecycle, state, observer);
    order.verify(lifecycle).runFollowups(completion);
    order
        .verify(state)
        .initState(eq(job.getId()), eq(JobProgressWriter.NAMESPACE), anyString(), eq(7L));
    order.verify(observer).recordJobSuccess(job, 0L);
    assertEquals(ExecutionStatus.SUCCEEDED, execution.getStatus());
  }

  @ParameterizedTest
  @EnumSource(
      value = JobStatus.class,
      names = {"CANCELED", "RUNNING"})
  void fenceRejectionDistinguishesCancellationAndSupersession(JobStatus status) {
    BODY.set(
        () -> {
          when(store.getJobStatus(job.getId())).thenReturn(status);
          return 42;
        });
    when(lifecycle.completeSuccessInCurrentTransaction(
            eq(job), any(), any(), any(), any(), anyLong(), anyLong()))
        .thenThrow(new CompletionRejectedException());
    task.call();
    if (status == JobStatus.CANCELED) {
      assertEquals(ExecutionStatus.CANCELED, execution.getStatus());
      verify(observer).recordJobCancellation(job);
    } else {
      assertTrue(attempt.isSuperseded());
      assertEquals(JobExecutionEntity.RESULT_DISCARDED_ERROR_MESSAGE, execution.getErrorMessage());
    }
    verify(lifecycle, never()).runFollowups(any());
    verifyNoInteractions(finalizer);
    verify(store, never()).incrementRetryAttempt(any(), any());
  }

  @ParameterizedTest
  @EnumSource(TerminalOutcome.class)
  void unknownCommitResolvesEveryTerminalOutcomeWithInterruptClearedAndRestored(
      TerminalOutcome terminal) throws Exception {
    IllegalStateException lostAck = new IllegalStateException("lost commit acknowledgement");
    doAnswer(
            inv -> {
              ((Callable<?>) inv.getArgument(0)).call();
              Thread.currentThread().interrupt();
              throw lostAck;
            })
        .when(carrier)
        .run(any());
    when(transactions.terminalOutcome(job.getId(), 7L))
        .thenAnswer(
            inv -> {
              assertFalse(Thread.currentThread().isInterrupted());
              return terminal;
            });
    task.call();
    assertTrue(Thread.currentThread().isInterrupted());
    verify(carrier).run(any());
    switch (terminal) {
      case COMMITTED -> {
        verify(lifecycle).runFollowups(completion);
        assertEquals(ExecutionStatus.SUCCEEDED, execution.getStatus());
      }
      case ROLLED_BACK -> verify(store).incrementRetryAttempt(job.getId(), 7L);
      case CANCELED -> assertEquals(ExecutionStatus.CANCELED, execution.getStatus());
      case SUPERSEDED -> assertTrue(attempt.isSuperseded());
    }
    verifyNoInteractions(finalizer);
  }

  @Test
  void unknownCommitRetriesTransientResolutionFailure() throws Exception {
    loseCommitAcknowledgement();
    when(transactions.terminalOutcome(job.getId(), 7L))
        .thenThrow(new IllegalStateException(new SQLTransientException("read unavailable")))
        .thenReturn(TerminalOutcome.COMMITTED);
    task.call();
    verify(transactions, times(2)).terminalOutcome(job.getId(), 7L);
    verify(carrier).run(any());
    verify(lifecycle).runFollowups(completion);
  }

  @Test
  void unresolvedCommitLeavesRecoveryInCharge() throws Exception {
    loseCommitAcknowledgement();
    when(transactions.terminalOutcome(job.getId(), 7L))
        .thenThrow(new IllegalStateException(new SQLTransientException("read unavailable")));
    task.call();
    verify(transactions, times(5)).terminalOutcome(job.getId(), 7L);
    verify(store, never()).incrementRetryAttempt(any(), any());
    verify(lifecycle, never()).runFollowups(any());
    verify(lifecycle, never()).completeFailure(any(), any(), anyBoolean());
    assertEquals(ExecutionStatus.RUNNING, execution.getStatus());
  }

  private void loseCommitAcknowledgement() throws Exception {
    doAnswer(
            inv -> {
              ((Callable<?>) inv.getArgument(0)).call();
              throw new IllegalStateException("lost acknowledgement");
            })
        .when(carrier)
        .run(any());
  }

  @Test
  void staleSameNodeClaimStopsBeforeBody() {
    job.setClaimSeq(8L);
    when(store.findById(job.getId())).thenReturn(Optional.of(job));
    task.initFromClaim(
        new JobClaimDto(
            job.getId(),
            JobStatus.RUNNING,
            JobExecutionType.SINGLE,
            JobPriority.NORMAL,
            NOW,
            1,
            30,
            "node",
            NOW,
            null,
            0,
            3,
            null,
            null,
            7L));
    AtomicInteger bodies = new AtomicInteger();
    BODY.set(() -> bodies.incrementAndGet());
    task.call();
    assertEquals(0, bodies.get());
    verifyNoInteractions(carrier, finalizer);
    verify(observer, never()).startExecution(any(), anyInt(), anyString());
  }

  @Test
  void checkpointAndContinuationRejectButLastCheckpointIsEmpty() {
    BODY.set(
        () -> {
          JobContext context = JobContext.current();
          assertTrue(context.lastCheckpoint().isEmpty());
          assertTrue(
              assertThrows(IllegalStateException.class, () -> context.checkpoint("x"))
                  .getMessage()
                  .contains("@TransactionalJob"));
          assertTrue(
              assertThrows(IllegalStateException.class, context::requestContinuation)
                  .getMessage()
                  .contains("@TransactionalJob"));
          return 42;
        });
    task.call();
    assertEquals(ExecutionStatus.SUCCEEDED, execution.getStatus());
  }

  @Test
  void missingCapabilityFailsWithoutBodyOrRetryAttempt() {
    when(store.capability(TransactionalCompletionStore.class)).thenReturn(Optional.empty());
    task.call();
    verifyNoInteractions(carrier, finalizer);
    verify(store, never()).incrementRetryAttempt(any(), any());
    verify(lifecycle).completeFailure(job, JobStatus.RUNNING, false);
  }

  @Test
  void interruptedBackoffStopsWithOriginalTransientFailure() throws Exception {
    BODY.set(
        () -> {
          Thread.currentThread().interrupt();
          return 42;
        });
    when(lifecycle.completeSuccessInCurrentTransaction(
            eq(job), any(), any(), any(), any(), anyLong(), anyLong()))
        .thenThrow(new IllegalStateException(new SQLTransientException("deadlock")));
    task.call();
    assertTrue(Thread.currentThread().isInterrupted());
    verify(carrier).run(any());
    verify(store).incrementRetryAttempt(job.getId(), 7L);
  }

  @Test
  void strategyFallbackAfterRollbackUsesNormalSuccessPath() throws Exception {
    BODY.set(
        () -> {
          throw new IllegalStateException("body");
        });
    doAnswer(
            inv -> {
              try {
                return ((Callable<?>) inv.getArgument(2)).call();
              } catch (Exception ignored) {
                return 42;
              }
            })
        .when(resilience)
        .execute(anyString(), any(), any());
    when(finalizer.finalizeSuccess(eq(job), any(), any(), any(), any(), anyLong(), anyLong()))
        .thenReturn(JobSuccessFinalizer.Outcome.COMPLETED_FULL);
    task.call();
    verify(finalizer).finalizeSuccess(eq(job), any(), any(), any(), any(), anyLong(), anyLong());
    assertEquals(ExecutionStatus.SUCCEEDED, execution.getStatus());
  }

  @Test
  void circuitOpenFromStrategyKeepsReschedulePath() throws Exception {
    doThrow(new CircuitBreakerOpenException("open"))
        .when(resilience)
        .execute(anyString(), any(), any());
    when(resilience.getRetryDelay(anyString())).thenReturn(Duration.ZERO);
    task.call();
    verify(store).scheduleJobRetry(eq(job.getId()), anyString(), eq(NOW), eq(0), eq(7L));
    verify(store, never()).incrementRetryAttempt(any(), any());
  }

  @Test
  void nonTransactionalJobUsesExistingFinalizerAndNeverCarrier() {
    job.setPayload(payload("ordinaryBody", "()Ljava/lang/Object;"));
    when(finalizer.finalizeSuccess(eq(job), any(), any(), any(), any(), anyLong(), anyLong()))
        .thenReturn(JobSuccessFinalizer.Outcome.COMPLETED_FULL);
    task.call();
    verify(finalizer).finalizeSuccess(eq(job), any(), any(), any(), any(), anyLong(), anyLong());
    verifyNoInteractions(carrier);
    verify(lifecycle, never())
        .completeSuccessInCurrentTransaction(
            any(), any(), any(), any(), any(), anyLong(), anyLong());
    assertEquals(ExecutionStatus.SUCCEEDED, execution.getStatus());
  }

  private static JobPayload payload(String method, String descriptor) {
    return new JobPayload(
        JobTaskTransactionalTest.class.getName(), method, descriptor, true, List.of(), null);
  }
}
