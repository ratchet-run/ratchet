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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.lenient;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.InOrder;
import run.ratchet.api.CircuitBreakerProtected;
import run.ratchet.api.JobContext;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobStatus;
import run.ratchet.api.RatchetOptions;
import run.ratchet.api.TransactionalJob;
import run.ratchet.api.exception.CircuitBreakerOpenException;
import run.ratchet.ri.core.JobTransaction;
import run.ratchet.ri.core.ResourcePermitService;
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
import run.ratchet.store.spi.ResourcePermitStore.AcquireResult;
import run.ratchet.store.spi.TerminalOutcome;
import run.ratchet.store.spi.TransactionalCompletionStore;

class JobTaskTransactionalTest {
  private static final Instant NOW = Instant.parse("2026-11-13T12:00:00Z");
  private static final ThreadLocal<Callable<Object>> BODY = new ThreadLocal<>();
  private final ResourcePermitService permits = mock(ResourcePermitService.class);
  private final JobStore store = mock(JobStore.class);
  private final TransactionalCompletionStore transactions =
      mock(TransactionalCompletionStore.class);
  private final PostExecutionHandler lifecycle = mock(PostExecutionHandler.class);
  private final ExecutionObserver observer = mock(ExecutionObserver.class);
  private final PreExecutionValidator validator = mock(PreExecutionValidator.class);
  private final JobSuccessFinalizer finalizer = mock(JobSuccessFinalizer.class);
  private final FakeCarrier carrier = spy(new FakeCarrier());
  private final AfterCommitRegistrar registrar = mock(AfterCommitRegistrar.class);
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
    lenient().when(store.incrementRetryAttempt(any(), any(), any())).thenReturn(1);
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
            permits,
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
            registrar,
            flusher,
            timeoutHandler,
            new HeldKeyReleases(store, () -> mock(ScheduledExecutorService.class)),
            new PermitReleases(permits, () -> mock(ScheduledExecutorService.class)),
            new ClaimReschedules(store, () -> mock(ScheduledExecutorService.class)),
            new OutcomeWriteRetries(() -> mock(ScheduledExecutorService.class)),
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
            eq(job), any(), any(), any(), any(), anyLong(), anyLong(), nullable(Integer.class)))
        .thenReturn(completion);
    when(registrar.registerAfterCommit(any()))
        .thenAnswer(inv -> carrier.register(inv.getArgument(0)));
    BODY.set(() -> 42);
  }

  @AfterEach
  void cleanUp() {
    BODY.remove();
    Thread.interrupted();
    JobContext.clear();
  }

  private void initHydratedAttemptFenceClaim() {
    job.setAttempts(3);
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
            2,
            3,
            null,
            null,
            7L));
    attempt = new JobAttemptControl(job.getId(), NOW.plusSeconds(30), 30, NOW, 2, 7L, "node");
    task.attach(attempt);
  }

  @Test
  void transactionalSuccessUsesClaimAttemptsAfterHydrationCharge() {
    initHydratedAttemptFenceClaim();

    task.call();

    verify(lifecycle)
        .completeSuccessInCurrentTransaction(
            eq(job), any(), any(), any(), any(), anyLong(), anyLong(), eq(2));
    assertEquals(ExecutionStatus.SUCCEEDED, execution.getStatus());
    verify(store, never()).incrementRetryAttempt(any(), any(), any());
  }

  @Test
  void rejectedAttemptsFenceRollsBackTransactionalSuccess() throws Exception {
    initHydratedAttemptFenceClaim();
    AtomicBoolean rolledBack = new AtomicBoolean();
    doAnswer(
            inv -> {
              try {
                return inv.callRealMethod();
              } catch (CompletionRejectedException failure) {
                rolledBack.set(true);
                throw failure;
              }
            })
        .when(carrier)
        .run(any());
    when(lifecycle.completeSuccessInCurrentTransaction(
            eq(job), any(), any(), any(), any(), anyLong(), anyLong(), eq(2)))
        .thenThrow(new CompletionRejectedException());

    task.call();

    assertTrue(rolledBack.get());
    verify(lifecycle)
        .completeSuccessInCurrentTransaction(
            eq(job), any(), any(), any(), any(), anyLong(), anyLong(), eq(2));
    assertTrue(attempt.isSuperseded());
    assertEquals(JobExecutionEntity.RESULT_DISCARDED_ERROR_MESSAGE, execution.getErrorMessage());
    verify(lifecycle, never()).runFollowups(any());
    verify(store, never()).incrementRetryAttempt(any(), any(), any());
  }

  @Test
  void unitInTransactionalJobThrows() {
    AtomicBoolean ran = new AtomicBoolean();
    BODY.set(
        () -> {
          assertThrows(
              IllegalStateException.class,
              () ->
                  JobContext.current()
                      .runInTransaction(
                          after -> {
                            ran.set(true);
                            return "next";
                          }));
          return 42;
        });
    task.call();
    assertFalse(ran.get());
    assertEquals(ExecutionStatus.SUCCEEDED, execution.getStatus());
  }

  @Test
  void watchdogCancellationDuringSuccessWriteDoesNotInterruptWorker() throws Exception {
    job.setPayload(payload("ordinaryBody", "()Ljava/lang/Object;"));
    FutureTask<Void> future = new FutureTask<>(task);
    AtomicBoolean wrote = new AtomicBoolean();
    when(finalizer.finalizeSuccess(
            eq(job), any(), any(), any(), any(), anyLong(), anyLong(), nullable(Integer.class)))
        .thenAnswer(
            inv -> {
              assertTrue(attempt.cancelExecution(future));
              assertFalse(Thread.currentThread().isInterrupted());
              wrote.set(true);
              return JobSuccessFinalizer.Outcome.COMPLETED_FULL;
            });
    AtomicReference<Throwable> failure = new AtomicReference<>();
    Thread worker =
        new Thread(
            () -> {
              BODY.set(() -> 42);
              try {
                future.run();
              } catch (Throwable thrown) {
                failure.set(thrown);
              } finally {
                BODY.remove();
                JobContext.clear();
              }
            });
    worker.start();
    worker.join(5000);
    assertFalse(worker.isAlive());
    assertNull(failure.get());
    assertTrue(wrote.get());
    assertEquals(ExecutionStatus.SUCCEEDED, execution.getStatus());
  }

  @Test
  void watchdogCancellationBeforeBodyDefersInterruptedFailureWithoutInvokingBody() {
    AtomicBoolean invoked = new AtomicBoolean();
    BODY.set(
        () -> {
          invoked.set(true);
          return 42;
        });
    when(timeoutHandler.isWatchdogCancelled(job.getId())).thenReturn(true);
    attempt.cancelExecution(mock(Future.class));

    task.call();

    assertFalse(invoked.get());
    assertTrue(supplierFailure.get() instanceof InterruptedException);
    verify(timeoutHandler).isWatchdogCancelled(job.getId());
    assertEquals(TimeoutException.class.getName(), execution.getErrorClass());
    verify(store, never()).incrementRetryAttempt(any(), any(), any());
    verify(lifecycle, never()).completeFailure(any(), any(), anyBoolean(), nullable(Integer.class));
    verifyNoInteractions(finalizer);
  }

  @Test
  void interruptedOrdinarySuccessClearsStoreCallsAndPermitReleaseAndSkipsFlush() {
    job.setPayload(payload("ordinaryBody", "()Ljava/lang/Object;"));
    job.setResourceName("limited");
    when(permits.tryAcquire("limited", job.getId(), 7L, "node")).thenReturn(AcquireResult.ACQUIRED);
    List<Boolean> flags = new ArrayList<>();
    when(store.getJobStatus(job.getId()))
        .thenAnswer(
            inv -> {
              flags.add(Thread.currentThread().isInterrupted());
              return JobStatus.RUNNING;
            });
    when(finalizer.finalizeSuccess(
            eq(job), any(), any(), any(), any(), anyLong(), anyLong(), nullable(Integer.class)))
        .thenAnswer(
            inv -> {
              flags.add(Thread.currentThread().isInterrupted());
              return JobSuccessFinalizer.Outcome.COMPLETED_FULL;
            });
    doAnswer(
            inv -> {
              flags.add(Thread.currentThread().isInterrupted());
              return null;
            })
        .when(permits)
        .release("limited", job.getId(), 7L);
    JobStateStore state = mock(JobStateStore.class);
    when(store.capability(JobStateStore.class)).thenReturn(Optional.of(state));
    BODY.set(
        () -> {
          JobContext.current().progress(1, 2);
          Thread.currentThread().interrupt();
          return 42;
        });

    task.call();

    assertTrue(Thread.currentThread().isInterrupted());
    assertTrue(flags.size() >= 4, "cancel checks, success write and permit release must run");
    assertFalse(flags.contains(true));
    verify(permits).release("limited", job.getId(), 7L);
    verify(flusher).markDirty(any(JobProgressWriter.class));
    verifyNoInteractions(state);
  }

  @Test
  void interruptedOrdinaryFailureClearsFailureWritesAndRestoresInterrupt() {
    job.setPayload(payload("ordinaryBody", "()Ljava/lang/Object;"));
    List<Boolean> flags = new ArrayList<>();
    when(store.incrementRetryAttempt(job.getId(), 7L, 0))
        .thenAnswer(
            inv -> {
              flags.add(Thread.currentThread().isInterrupted());
              return 1;
            });
    when(lifecycle.completeFailure(eq(job), any(), anyBoolean(), nullable(Integer.class)))
        .thenAnswer(
            inv -> {
              flags.add(Thread.currentThread().isInterrupted());
              return true;
            });
    doAnswer(
            inv -> {
              flags.add(Thread.currentThread().isInterrupted());
              return null;
            })
        .when(observer)
        .saveExecution(execution);
    BODY.set(
        () -> {
          Thread.currentThread().interrupt();
          throw new IllegalStateException("body failed");
        });

    task.call();

    assertTrue(Thread.currentThread().isInterrupted());
    assertTrue(flags.size() >= 3);
    assertFalse(flags.contains(true));
    verify(lifecycle).completeFailure(eq(job), any(), anyBoolean(), nullable(Integer.class));
  }

  @Test
  void interruptedTransactionalBodyClearsCompletionAndCommit() throws Exception {
    List<Boolean> flags = new ArrayList<>();
    BODY.set(
        () -> {
          Thread.currentThread().interrupt();
          return 42;
        });
    when(lifecycle.completeSuccessInCurrentTransaction(
            eq(job), any(), any(), any(), any(), anyLong(), anyLong(), nullable(Integer.class)))
        .thenAnswer(
            inv -> {
              flags.add(Thread.currentThread().isInterrupted());
              return completion;
            });
    doAnswer(
            inv -> {
              Object result = inv.callRealMethod();
              // The transaction interceptor commits after the carrier's work returns.
              flags.add(Thread.currentThread().isInterrupted());
              return result;
            })
        .when(carrier)
        .run(any());

    task.call();

    assertTrue(Thread.currentThread().isInterrupted());
    assertEquals(List.of(false, false), flags);
    verify(lifecycle).runFollowups(completion);
  }

  @Test
  void interruptedFinalizerReportsBeforeMinimalStoreWrite() {
    job.setPayload(payload("ordinaryBody", "()Ljava/lang/Object;"));
    when(finalizer.finalizeSuccess(
            eq(job), any(), any(), any(), any(), anyLong(), anyLong(), nullable(Integer.class)))
        .thenReturn(JobSuccessFinalizer.Outcome.INTERRUPTED);
    List<Boolean> flags = new ArrayList<>();
    when(finalizer.finalizeMinimalSuccess(
            eq(job),
            any(),
            any(),
            any(),
            any(),
            anyLong(),
            anyLong(),
            eq(true),
            nullable(Integer.class)))
        .thenAnswer(
            inv -> {
              flags.add(Thread.currentThread().isInterrupted());
              return JobSuccessFinalizer.Outcome.COMPLETED_MINIMAL;
            });

    task.call();

    assertTrue(Thread.currentThread().isInterrupted());
    assertEquals(List.of(false), flags);
  }

  @Test
  void interruptedCommitReadBackoffDispatchesWithFlagClear() throws Exception {
    loseCommitAcknowledgement();
    List<Boolean> flags = new ArrayList<>();
    AtomicInteger reads = new AtomicInteger();
    when(transactions.terminalOutcome(job.getId(), 7L))
        .thenAnswer(
            inv -> {
              flags.add(Thread.currentThread().isInterrupted());
              if (reads.incrementAndGet() == 1) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(new SQLTransientException("read unavailable"));
              }
              return TerminalOutcome.COMMITTED;
            });
    doAnswer(
            inv -> {
              flags.add(Thread.currentThread().isInterrupted());
              return null;
            })
        .when(lifecycle)
        .runFollowups(completion);
    doAnswer(
            inv -> {
              flags.add(Thread.currentThread().isInterrupted());
              return null;
            })
        .when(observer)
        .saveExecution(execution);

    task.call();

    assertTrue(Thread.currentThread().isInterrupted());
    assertEquals(List.of(false, false, false, false), flags);
  }

  @Test
  void interruptedCommitOutcomeSleepLeavesDispatchedWritesClear() throws Exception {
    loseCommitAcknowledgement();
    task.setTerminalRetryDelays(10_000, 10_000);
    CountDownLatch initialReadsExhausted = new CountDownLatch(1);
    AtomicInteger reads = new AtomicInteger();
    List<Boolean> flags = new ArrayList<>();
    AtomicReference<Throwable> workerFailure = new AtomicReference<>();
    AtomicBoolean restored = new AtomicBoolean();
    when(transactions.terminalOutcome(job.getId(), 7L))
        .thenAnswer(
            inv -> {
              flags.add(Thread.currentThread().isInterrupted());
              if (reads.incrementAndGet() <= 5) {
                if (reads.get() == 5) initialReadsExhausted.countDown();
                throw new IllegalStateException(new SQLTransientException("read unavailable"));
              }
              return TerminalOutcome.COMMITTED;
            });
    doAnswer(
            inv -> {
              flags.add(Thread.currentThread().isInterrupted());
              return null;
            })
        .when(lifecycle)
        .runFollowups(completion);
    doAnswer(
            inv -> {
              flags.add(Thread.currentThread().isInterrupted());
              return null;
            })
        .when(observer)
        .saveExecution(execution);
    Thread worker =
        new Thread(
            () -> {
              BODY.set(() -> 42);
              try {
                task.call();
                restored.set(Thread.currentThread().isInterrupted());
              } catch (Throwable failure) {
                workerFailure.set(failure);
              } finally {
                BODY.remove();
                Thread.interrupted();
                JobContext.clear();
              }
            });
    worker.start();
    try {
      assertTrue(initialReadsExhausted.await(5, TimeUnit.SECONDS));
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while (worker.getState() != Thread.State.TIMED_WAITING && System.nanoTime() < deadline) {
        Thread.sleep(1);
      }
      assertEquals(Thread.State.TIMED_WAITING, worker.getState());
      worker.interrupt();
      worker.join(5_000);
    } finally {
      if (worker.isAlive()) {
        attempt.revoke();
        worker.interrupt();
        worker.join(5_000);
      }
    }
    assertFalse(worker.isAlive());
    assertNull(workerFailure.get());
    assertTrue(restored.get());
    assertEquals(6, reads.get());
    assertEquals(8, flags.size());
    assertFalse(flags.contains(true));
    assertEquals(ExecutionStatus.SUCCEEDED, execution.getStatus());
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
    verify(store, never()).incrementRetryAttempt(any(), any(), any());
    verify(timeoutHandler, never()).processCooperativeTimeout(any());
    verify(lifecycle, never()).completeFailure(any(), any(), anyBoolean(), nullable(Integer.class));
    verify(lifecycle, never())
        .completeSuccessInCurrentTransaction(
            any(), any(), any(), any(), any(), anyLong(), anyLong(), nullable(Integer.class));
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

  @Test
  void committedTransactionReleasesPermitAfterCommit() {
    job.setResourceName("res");
    when(permits.tryAcquire("res", job.getId(), 7L, "node")).thenReturn(AcquireResult.ACQUIRED);
    BODY.set(
        () -> {
          assertTrue(JobTransaction.active());
          verify(permits, never()).release(any(), any(), anyLong());
          return 42;
        });
    doAnswer(
            invocation -> {
              assertFalse(JobTransaction.active());
              return null;
            })
        .when(permits)
        .release("res", job.getId(), 7L);

    task.call();

    InOrder order = inOrder(lifecycle, permits);
    order
        .verify(lifecycle)
        .completeSuccessInCurrentTransaction(
            eq(job), any(), any(), any(), any(), anyLong(), anyLong(), nullable(Integer.class));
    order.verify(permits).release("res", job.getId(), 7L);
    verify(permits, times(1)).release("res", job.getId(), 7L);
    verify(lifecycle).runFollowups(completion);
  }

  @Test
  void rolledBackTransactionReleasesPermit() {
    job.setResourceName("res");
    when(permits.tryAcquire("res", job.getId(), 7L, "node")).thenReturn(AcquireResult.ACQUIRED);
    BODY.set(
        () -> {
          throw new IllegalArgumentException("body");
        });
    doAnswer(
            invocation -> {
              assertFalse(JobTransaction.active());
              return null;
            })
        .when(permits)
        .release("res", job.getId(), 7L);

    task.call();

    verify(permits, times(1)).release("res", job.getId(), 7L);
    verify(store).incrementRetryAttempt(job.getId(), 7L, 0);
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
            eq(job), any(), any(), any(), any(), anyLong(), anyLong(), nullable(Integer.class)))
        .thenThrow(new IllegalStateException(new SQLTransientException("deadlock")))
        .thenReturn(completion);
    task.call();
    assertEquals(2, bodies.get());
    verify(carrier, times(2)).run(any());
    verify(store, never()).incrementRetryAttempt(any(), any(), any());
    verify(lifecycle).runFollowups(completion);
  }

  @Test
  void cancellationBetweenTransactionsPreventsRetryBodyWithoutInterruptingWorker()
      throws Exception {
    AtomicInteger bodies = new AtomicInteger();
    BODY.set(() -> bodies.incrementAndGet());
    FutureTask<Void> future = new FutureTask<>(task);
    when(lifecycle.completeSuccessInCurrentTransaction(
            eq(job), any(), any(), any(), any(), anyLong(), anyLong(), nullable(Integer.class)))
        .thenAnswer(
            invocation -> {
              assertTrue(attempt.cancelExecution(future));
              assertFalse(Thread.currentThread().isInterrupted());
              throw new IllegalStateException(new SQLTransientException("deadlock"));
            });

    future.run();

    assertEquals(1, bodies.get());
    verify(carrier, times(2)).run(any());
    verify(lifecycle, times(1))
        .completeSuccessInCurrentTransaction(
            eq(job), any(), any(), any(), any(), anyLong(), anyLong(), nullable(Integer.class));
    assertTrue(supplierFailure.get() instanceof InterruptedException);
    verify(observer).recordJobFailure(eq(job), any(InterruptedException.class), anyInt());
    verify(lifecycle)
        .completeFailure(eq(job), eq(JobStatus.RUNNING), anyBoolean(), nullable(Integer.class));
    assertFalse(Thread.currentThread().isInterrupted());
    verify(lifecycle, never()).runFollowups(any());
  }

  @Test
  void fiveTransientFailuresUseOrdinaryFailureAndAreIgnoredByBreaker() throws Exception {
    SQLTransientException original = new SQLTransientException("deadlock");
    AtomicInteger bodies = new AtomicInteger();
    BODY.set(() -> bodies.incrementAndGet());
    when(lifecycle.completeSuccessInCurrentTransaction(
            eq(job), any(), any(), any(), any(), anyLong(), anyLong(), nullable(Integer.class)))
        .thenThrow(new IllegalStateException(original));
    when(store.incrementRetryAttempt(job.getId(), 7L, 0)).thenReturn(1);
    task.call();
    assertEquals(5, bodies.get());
    verify(carrier, times(5)).run(any());
    verify(store).incrementRetryAttempt(job.getId(), 7L, 0);
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
    verify(store).incrementRetryAttempt(job.getId(), 7L, 0);
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
    verify(store).incrementRetryAttempt(job.getId(), 7L, 0);
  }

  @Test
  void deterministicCompletionErrorDoesNotRetryWholeTransaction() throws Exception {
    when(lifecycle.completeSuccessInCurrentTransaction(
            eq(job), any(), any(), any(), any(), anyLong(), anyLong(), nullable(Integer.class)))
        .thenThrow(new IllegalArgumentException("constraint violation at flush"));
    task.call();
    assertEquals(IllegalArgumentException.class, supplierFailure.get().getClass());
    verify(carrier).run(any());
    verify(store).incrementRetryAttempt(job.getId(), 7L, 0);
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
            eq(job), any(), any(), any(), any(), anyLong(), anyLong(), nullable(Integer.class)))
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
    verify(store, never()).incrementRetryAttempt(any(), any(), any());
  }

  @Test
  void userCancelledBodyThatStopsEndsCanceledAndReleasesItsKeyHold() {
    // The body sees the stop request and throws; the rolled-back attempt ends CANCELED.
    job.setBusinessKey("transactional-key");
    BODY.set(
        () -> {
          attempt.requestUserCancel();
          when(store.getJobStatus(job.getId())).thenReturn(JobStatus.CANCELED);
          throw new IllegalStateException("stopped after the cancel request");
        });
    task.call();
    assertEquals(ExecutionStatus.CANCELED, execution.getStatus());
    verify(observer).recordJobCancellation(job);
    verify(observer, never()).recordJobFailure(any(), any(), anyInt());
    verify(store, times(1)).releaseBusinessKeyHold(job.getId(), 7L);
    verify(store, never()).incrementRetryAttempt(any(), any(), any());
    verify(lifecycle, never()).completeFailure(any(), any(), anyBoolean(), nullable(Integer.class));
    verify(lifecycle, never())
        .completeSuccessInCurrentTransaction(
            any(), any(), any(), any(), any(), anyLong(), anyLong(), nullable(Integer.class));
    verifyNoInteractions(finalizer);
  }

  @Test
  void userCancelledBodyThatReturnsEndsCanceledAndReleasesItsKeyHold() {
    // The cancel moved the job out of RUNNING, so the in-transaction success write misses.
    job.setBusinessKey("transactional-key");
    BODY.set(
        () -> {
          attempt.requestUserCancel();
          when(store.getJobStatus(job.getId())).thenReturn(JobStatus.CANCELED);
          return 42;
        });
    when(lifecycle.completeSuccessInCurrentTransaction(
            eq(job), any(), any(), any(), any(), anyLong(), anyLong(), nullable(Integer.class)))
        .thenThrow(new CompletionRejectedException());
    task.call();
    assertEquals(ExecutionStatus.CANCELED, execution.getStatus());
    verify(observer).recordJobCancellation(job);
    verify(store, times(1)).releaseBusinessKeyHold(job.getId(), 7L);
    verify(lifecycle, never()).runFollowups(any());
    verify(store, never()).incrementRetryAttempt(any(), any(), any());
    verifyNoInteractions(finalizer);
  }

  @ParameterizedTest
  @EnumSource(
      value = JobStatus.class,
      names = {"CANCELED", "RUNNING"})
  void supersededBodyRollsBackBeforeAcceptedCompletion(JobStatus status) throws Exception {
    AtomicBoolean rolledBack = new AtomicBoolean();
    doAnswer(
            invocation -> {
              try {
                return invocation.callRealMethod();
              } catch (CompletionRejectedException failure) {
                rolledBack.set(true);
                throw failure;
              }
            })
        .when(carrier)
        .run(any());
    BODY.set(
        () -> {
          attempt.markSuperseded();
          when(store.getJobStatus(job.getId())).thenReturn(status);
          return 42;
        });
    // setUp leaves completion accepting this still-intact claim.
    task.call();
    assertTrue(rolledBack.get());
    verify(carrier).run(any());
    verify(lifecycle, never())
        .completeSuccessInCurrentTransaction(
            any(), any(), any(), any(), any(), anyLong(), anyLong(), nullable(Integer.class));
    verifyNoInteractions(results);
    assertEquals(
        CircuitBreakerExceptionFilter.Outcome.IGNORED,
        callFilter.get().classify(supplierFailure.get()));
    verify(store, never()).incrementRetryAttempt(any(), any(), any());
    if (status == JobStatus.CANCELED) {
      assertEquals(ExecutionStatus.CANCELED, execution.getStatus());
      verify(observer).recordJobCancellation(job);
    } else {
      assertEquals(JobExecutionEntity.RESULT_DISCARDED_ERROR_MESSAGE, execution.getErrorMessage());
    }
    verify(lifecycle, never()).runFollowups(any());
  }

  @ParameterizedTest
  @EnumSource(TerminalOutcome.class)
  void unknownCommitResolvesEveryTerminalOutcomeWithInterruptClearedAndRestored(
      TerminalOutcome terminal) throws Exception {
    IllegalStateException lostAck = new IllegalStateException("lost commit acknowledgement");
    doAnswer(
            inv -> {
              inv.callRealMethod();
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
      case ROLLED_BACK -> verify(store).incrementRetryAttempt(job.getId(), 7L, 0);
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

  @ParameterizedTest
  @EnumSource(TerminalOutcome.class)
  void unknownCommitKeepsResolvingAfterFiveTransientFailures(TerminalOutcome terminal)
      throws Exception {
    loseCommitAcknowledgement();
    task.setTerminalRetryDelays(1, 2);
    AtomicInteger bodies = new AtomicInteger();
    BODY.set(() -> bodies.incrementAndGet());
    AtomicInteger reads = new AtomicInteger();
    when(transactions.terminalOutcome(job.getId(), 7L))
        .thenAnswer(
            inv -> {
              if (reads.incrementAndGet() <= 5) {
                throw new IllegalStateException(new SQLTransientException("read unavailable"));
              }
              return terminal;
            });
    task.call();
    assertEquals(1, bodies.get());
    verify(transactions, times(6)).terminalOutcome(job.getId(), 7L);
    verify(carrier).run(any());
    switch (terminal) {
      case COMMITTED -> {
        verify(lifecycle).runFollowups(completion);
        assertEquals(ExecutionStatus.SUCCEEDED, execution.getStatus());
      }
      case ROLLED_BACK -> verify(store).incrementRetryAttempt(job.getId(), 7L, 0);
      case CANCELED -> assertEquals(ExecutionStatus.CANCELED, execution.getStatus());
      case SUPERSEDED -> assertTrue(attempt.isSuperseded());
    }
    verifyNoInteractions(finalizer);
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 6})
  void fenceBounceDoesNotStopCommitResolution(int bounceRead) throws Exception {
    loseCommitAcknowledgement();
    task.setTerminalRetryDelays(1, 2);
    AtomicInteger bodies = new AtomicInteger();
    AtomicInteger reads = new AtomicInteger();
    AtomicInteger callbacks = new AtomicInteger();
    BODY.set(bodies::incrementAndGet);
    job.setOnSuccessPayload(payload("afterCommit", "()V"));
    CALLBACK.set(callbacks::incrementAndGet);
    when(transactions.terminalOutcome(job.getId(), 7L))
        .thenAnswer(
            inv -> {
              int read = reads.incrementAndGet();
              if (read == bounceRead) {
                // Stand in for a background progress flush whose fenced write bounced.
                attempt.markSuperseded();
              }
              if (read <= bounceRead) {
                throw new IllegalStateException(new SQLTransientException("read unavailable"));
              }
              return TerminalOutcome.COMMITTED;
            });
    try {
      task.call();
      assertTrue(attempt.isSuperseded());
      assertFalse(attempt.isRevoked());
      assertEquals(bounceRead + 1, reads.get());
      assertEquals(1, bodies.get());
      assertEquals(1, callbacks.get());
      assertEquals(ExecutionStatus.SUCCEEDED, execution.getStatus());
      verify(lifecycle).runFollowups(completion);
      verify(observer).recordJobSuccess(job, 0L);
      verify(observer).saveExecution(execution);
      verify(carrier).run(any());
      verifyNoInteractions(finalizer);
    } finally {
      CALLBACK.remove();
    }
  }

  @Test
  void revokedSlowResolutionExitsAndRestoresInterrupt() throws Exception {
    loseCommitAcknowledgement();
    task.setTerminalRetryDelays(1, 2);
    AtomicInteger bodies = new AtomicInteger();
    AtomicInteger reads = new AtomicInteger();
    CountDownLatch slowRead = new CountDownLatch(1);
    CountDownLatch shutdown = new CountDownLatch(1);
    AtomicBoolean interruptedAfterReturn = new AtomicBoolean();
    AtomicReference<Throwable> workerFailure = new AtomicReference<>();
    when(transactions.terminalOutcome(job.getId(), 7L))
        .thenAnswer(
            inv -> {
              if (reads.incrementAndGet() == 6) {
                slowRead.countDown();
                try {
                  assertTrue(shutdown.await(5, TimeUnit.SECONDS));
                } catch (InterruptedException interrupted) {
                  Thread.currentThread().interrupt();
                }
              }
              throw new IllegalStateException(new SQLTransientException("read unavailable"));
            });
    Thread worker =
        new Thread(
            () -> {
              BODY.set(() -> bodies.incrementAndGet());
              try {
                task.call();
                interruptedAfterReturn.set(Thread.currentThread().isInterrupted());
              } catch (Throwable failure) {
                workerFailure.set(failure);
              } finally {
                BODY.remove();
                JobContext.clear();
              }
            });
    worker.start();
    try {
      assertTrue(slowRead.await(5, TimeUnit.SECONDS));
      attempt.revoke();
      worker.interrupt();
    } finally {
      shutdown.countDown();
      worker.join(5_000);
      if (worker.isAlive()) {
        attempt.revoke();
        worker.interrupt();
        worker.join(5_000);
      }
    }
    assertFalse(worker.isAlive());
    assertEquals(null, workerFailure.get());
    assertTrue(interruptedAfterReturn.get());
    assertEquals(1, bodies.get());
    verify(store, never()).incrementRetryAttempt(any(), any(), any());
    verify(lifecycle, never()).runFollowups(any());
    verify(lifecycle, never()).completeFailure(any(), any(), anyBoolean(), nullable(Integer.class));
    assertEquals(ExecutionStatus.RUNNING, execution.getStatus());
  }

  @Test
  void cancelFoundWhileTheCommitOutcomeIsUnknown_endsTheExecutionCanceled() throws Exception {
    // The commit outcome stays unknown. The watchdog revokes the attempt and then finds the job
    // canceled, so the attempt that wrote no outcome ends CANCELED.
    loseCommitAcknowledgement();
    task.setTerminalRetryDelays(1, 2);
    CountDownLatch unresolved = new CountDownLatch(1);
    when(transactions.terminalOutcome(job.getId(), 7L))
        .thenAnswer(
            inv -> {
              unresolved.countDown();
              throw new IllegalStateException(new SQLTransientException("read unavailable"));
            });
    FutureTask<Void> run =
        new FutureTask<>(
            () -> {
              BODY.set(() -> 42);
              try {
                return task.call();
              } finally {
                BODY.remove();
                JobContext.clear();
              }
            });
    Thread worker = new Thread(run);
    worker.start();
    try {
      assertTrue(unresolved.await(5, TimeUnit.SECONDS));
      // What the watchdog does once it reads CANCELED: revoke the attempt, then finish the cancel.
      attempt.revoke();
      attempt.finishCancelFoundByTimeout();
      run.get(10, TimeUnit.SECONDS);
    } finally {
      worker.join(5_000);
    }

    assertEquals(ExecutionStatus.CANCELED, execution.getStatus());
    verify(observer).recordJobCancellation(job);
    verify(lifecycle, never()).runFollowups(any());
    verify(lifecycle, never()).completeFailure(any(), any(), anyBoolean(), nullable(Integer.class));
  }

  @Test
  void nonTransientCommitReadFailureLeavesRecoveryInCharge() throws Exception {
    loseCommitAcknowledgement();
    when(transactions.terminalOutcome(job.getId(), 7L))
        .thenThrow(new IllegalArgumentException("invalid outcome read"));
    task.call();
    verify(transactions).terminalOutcome(job.getId(), 7L);
    verify(store, never()).incrementRetryAttempt(any(), any(), any());
    verify(lifecycle, never()).runFollowups(any());
    verify(lifecycle, never()).completeFailure(any(), any(), anyBoolean(), nullable(Integer.class));
    assertEquals(ExecutionStatus.RUNNING, execution.getStatus());
  }

  private void loseCommitAcknowledgement() throws Exception {
    doAnswer(
            inv -> {
              inv.callRealMethod();
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
    verify(store, never()).incrementRetryAttempt(any(), any(), any());
    verify(lifecycle)
        .completeFailure(eq(job), eq(JobStatus.RUNNING), eq(false), nullable(Integer.class));
  }

  @Test
  void interruptedBackoffStopsWithOriginalTransientFailure() throws Exception {
    BODY.set(
        () -> {
          Thread.currentThread().interrupt();
          return 42;
        });
    when(lifecycle.completeSuccessInCurrentTransaction(
            eq(job), any(), any(), any(), any(), anyLong(), anyLong(), nullable(Integer.class)))
        .thenThrow(new IllegalStateException(new SQLTransientException("deadlock")));
    task.call();
    assertTrue(Thread.currentThread().isInterrupted());
    verify(carrier).run(any());
    verify(store).incrementRetryAttempt(job.getId(), 7L, 0);
  }

  @Test
  void strategyFallbackAfterBodyFailureStillFailsAttempt() throws Exception {
    BODY.set(
        () -> {
          throw new IllegalStateException("body");
        });
    stubStrategyFallback();
    when(store.incrementRetryAttempt(job.getId(), 7L, 0)).thenReturn(1);
    task.call();
    assertFallbackDidNotSucceed("body");
  }

  @Test
  void strategyFallbackAfterRollbackOnlyBodyStillFailsAttempt() throws Exception {
    BODY.set(
        () -> {
          carrier.markRollbackOnly();
          return 42;
        });
    stubStrategyFallback();
    when(store.incrementRetryAttempt(job.getId(), 7L, 0)).thenReturn(1);
    task.call();
    verify(lifecycle, never())
        .completeSuccessInCurrentTransaction(
            any(), any(), any(), any(), any(), anyLong(), anyLong(), nullable(Integer.class));
    assertFallbackDidNotSucceed("marked for rollback");
  }

  @Test
  void strategyFallbackAfterSilentRollbackStillFailsAttempt() throws Exception {
    carrier.rollBackSilently();
    BODY.set(() -> 42);
    stubStrategyFallback();
    when(store.incrementRetryAttempt(job.getId(), 7L, 0)).thenReturn(1);
    task.call();
    assertFallbackDidNotSucceed("rolled back without an error");
  }

  @Test
  void strategyFallbackWithoutRunningTheTransactionFailsAttempt() throws Exception {
    doReturn(42).when(resilience).execute(anyString(), any(), any());
    stubSuccessFinalizer();
    when(store.incrementRetryAttempt(job.getId(), 7L, 0)).thenReturn(1);
    task.call();
    verify(carrier, never()).run(any());
    assertFallbackDidNotSucceed("without a committed transaction");
  }

  @Test
  void strategyRetryThatCommitsStillSucceeds() throws Exception {
    AtomicInteger bodies = new AtomicInteger();
    BODY.set(
        () -> {
          if (bodies.incrementAndGet() == 1) throw new IllegalStateException("first");
          return 42;
        });
    doAnswer(
            inv -> {
              Callable<?> work = inv.getArgument(2);
              try {
                return work.call();
              } catch (Exception ignored) {
                return work.call();
              }
            })
        .when(resilience)
        .execute(anyString(), any(), any());
    task.call();
    verify(carrier, times(2)).run(any());
    verify(lifecycle, times(1))
        .completeSuccessInCurrentTransaction(
            any(), any(), any(), any(), any(), anyLong(), anyLong(), nullable(Integer.class));
    assertEquals(ExecutionStatus.SUCCEEDED, execution.getStatus());
    verify(store, never()).incrementRetryAttempt(any(), any(), any());
  }

  private void stubStrategyFallback() throws Exception {
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
    stubSuccessFinalizer();
  }

  private void stubSuccessFinalizer() {
    when(finalizer.finalizeSuccess(
            eq(job), any(), any(), any(), any(), anyLong(), anyLong(), nullable(Integer.class)))
        .thenReturn(JobSuccessFinalizer.Outcome.COMPLETED_FULL);
  }

  private void assertFallbackDidNotSucceed(String phrase) {
    assertEquals(ExecutionStatus.FAILED, execution.getStatus());
    assertEquals(IllegalStateException.class.getName(), execution.getErrorClass());
    assertTrue(execution.getErrorMessage().contains(phrase), execution.getErrorMessage());
    verify(store, times(1)).incrementRetryAttempt(job.getId(), 7L, 0);
    verify(observer, never()).recordJobSuccess(any(), anyLong());
    verify(observer, times(1))
        .recordJobFailure(eq(job), any(IllegalStateException.class), anyInt());
    verify(lifecycle, never()).runFollowups(any());
    verify(transactions, never()).terminalOutcome(any(), anyLong());
    verify(finalizer, never())
        .finalizeSuccess(
            any(), any(), any(), any(), any(), anyLong(), anyLong(), nullable(Integer.class));
  }

  @Test
  void circuitOpenFromStrategyKeepsReschedulePath() throws Exception {
    doThrow(new CircuitBreakerOpenException("open"))
        .when(resilience)
        .execute(anyString(), any(), any());
    when(resilience.getRetryDelay(anyString())).thenReturn(Duration.ZERO);
    task.call();
    verify(store).releaseClaim(eq(job.getId()), anyString(), eq(NOW), eq(0), eq(7L));
    verify(store, never()).scheduleJobRetry(any(), any(), any(), anyInt(), any());
    verify(store, never()).incrementRetryAttempt(any(), any(), any());
  }

  @Test
  void nonTransactionalJobUsesExistingFinalizerAndNeverCarrier() {
    job.setPayload(payload("ordinaryBody", "()Ljava/lang/Object;"));
    when(finalizer.finalizeSuccess(
            eq(job), any(), any(), any(), any(), anyLong(), anyLong(), nullable(Integer.class)))
        .thenReturn(JobSuccessFinalizer.Outcome.COMPLETED_FULL);
    task.call();
    verify(finalizer)
        .finalizeSuccess(
            eq(job), any(), any(), any(), any(), anyLong(), anyLong(), nullable(Integer.class));
    verifyNoInteractions(carrier);
    verify(lifecycle, never())
        .completeSuccessInCurrentTransaction(
            any(), any(), any(), any(), any(), anyLong(), anyLong(), nullable(Integer.class));
    assertEquals(ExecutionStatus.SUCCEEDED, execution.getStatus());
  }

  static class FakeCarrier extends JobTransaction {
    final List<Runnable> pending = new CopyOnWriteArrayList<>();
    final AtomicBoolean rollbackOnly = new AtomicBoolean();
    final AtomicBoolean silentRollback = new AtomicBoolean();

    void markRollbackOnly() {
      rollbackOnly.set(true);
    }

    void rollBackSilently() {
      silentRollback.set(true);
    }

    @Override
    public boolean isRollbackOnly() {
      return rollbackOnly.get();
    }

    AfterCommitRegistrar.Result register(Runnable action) {
      if (!JobTransaction.active()) return AfterCommitRegistrar.Result.NO_ACTIVE_TRANSACTION;
      pending.add(action);
      return AfterCommitRegistrar.Result.REGISTERED;
    }

    @Override
    public <T> T run(Callable<T> work) throws Exception {
      pending.clear();
      rollbackOnly.set(false);
      try {
        T value = insideCarrier(work);
        if (!rollbackOnly.get() && !silentRollback.get()) {
          for (Runnable action : List.copyOf(pending)) action.run();
        }
        return value;
      } finally {
        rollbackOnly.set(false);
        pending.clear();
      }
    }
  }

  @Test
  void rollbackOnlyBodyFailsAttemptBeforeCompletionWrite() throws Exception {
    assertRejectedTransaction("marked for rollback", true, false);
  }

  @Test
  void registrarRollbackOnlyFailsAttemptBeforeCompletionWrite() throws Exception {
    when(registrar.registerAfterCommit(any()))
        .thenReturn(AfterCommitRegistrar.Result.ROLLBACK_ONLY);
    assertRejectedTransaction("marked for rollback", false, false);
  }

  @ParameterizedTest
  @EnumSource(
      value = AfterCommitRegistrar.Result.class,
      names = {"REGISTRATION_FAILED", "NO_ACTIVE_TRANSACTION"})
  void unconfirmableCommitFailsAttemptBeforeCompletionWrite(AfterCommitRegistrar.Result result)
      throws Exception {
    when(registrar.registerAfterCommit(any())).thenReturn(result);
    assertRejectedTransaction("cannot be confirmed", false, false);
  }

  @Test
  void silentRollbackAfterCompletionFailsAttemptWithoutReadingOutcome() throws Exception {
    carrier.rollBackSilently();
    assertRejectedTransaction("rolled back without an error", false, true);
  }

  private void assertRejectedTransaction(String phrase, boolean markRollback, boolean completed)
      throws Exception {
    when(store.incrementRetryAttempt(job.getId(), 7L, 0)).thenReturn(1);
    AtomicInteger bodies = new AtomicInteger();
    AtomicInteger successCallbacks = new AtomicInteger();
    job.setOnSuccessPayload(payload("afterCommit", "()V"));
    CALLBACK.set(() -> successCallbacks.incrementAndGet());
    BODY.set(
        () -> {
          bodies.incrementAndGet();
          if (markRollback) carrier.markRollbackOnly();
          return 42;
        });
    try {
      task.call();
      assertEquals(1, bodies.get());
      verify(carrier, times(1)).run(any());
      verify(lifecycle, times(completed ? 1 : 0))
          .completeSuccessInCurrentTransaction(
              any(), any(), any(), any(), any(), anyLong(), anyLong(), nullable(Integer.class));
      if (!completed) verifyNoInteractions(results);
      verify(transactions, never()).terminalOutcome(any(), anyLong());
      verify(lifecycle, never()).runFollowups(any());
      verify(observer, never()).recordJobSuccess(any(), anyLong());
      verify(observer, times(1))
          .recordJobFailure(eq(job), any(IllegalStateException.class), anyInt());
      verify(store, times(1)).incrementRetryAttempt(job.getId(), 7L, 0);
      assertEquals(ExecutionStatus.FAILED, execution.getStatus());
      assertEquals(IllegalStateException.class.getName(), execution.getErrorClass());
      assertTrue(execution.getErrorMessage().contains(phrase), execution.getErrorMessage());
      assertTrue(supplierFailure.get() instanceof IllegalStateException);
      assertEquals(
          CircuitBreakerExceptionFilter.Outcome.RECORDED,
          callFilter.get().classify(supplierFailure.get()));
      assertEquals(0, successCallbacks.get());
      verifyNoInteractions(finalizer);
    } finally {
      CALLBACK.remove();
    }
  }

  @Test
  void cancelRequestedBodyLeftRollbackOnlyEndsCanceled() {
    job.setBusinessKey("transactional-key");
    BODY.set(
        () -> {
          attempt.requestUserCancel();
          when(store.getJobStatus(job.getId())).thenReturn(JobStatus.CANCELED);
          carrier.markRollbackOnly();
          return 42;
        });
    task.call();
    assertEquals(ExecutionStatus.CANCELED, execution.getStatus());
    verify(observer).recordJobCancellation(job);
    verify(observer, never()).recordJobFailure(any(), any(), anyInt());
    verify(store, never()).incrementRetryAttempt(any(), any(), any());
    verify(lifecycle, never())
        .completeSuccessInCurrentTransaction(
            any(), any(), any(), any(), any(), anyLong(), anyLong(), nullable(Integer.class));
    verify(lifecycle, never()).runFollowups(any());
    verify(store, times(1)).releaseBusinessKeyHold(job.getId(), 7L);
    verifyNoInteractions(finalizer);
  }

  @Test
  void commitFailureAfterProofStillReadsOutcome() throws Exception {
    doAnswer(
            inv -> {
              inv.callRealMethod();
              throw new IllegalStateException("after-commit listener failed");
            })
        .when(carrier)
        .run(any());
    when(transactions.terminalOutcome(job.getId(), 7L)).thenReturn(TerminalOutcome.COMMITTED);
    task.call();
    verify(transactions, times(1)).terminalOutcome(job.getId(), 7L);
    verify(lifecycle, times(1)).runFollowups(completion);
    assertEquals(ExecutionStatus.SUCCEEDED, execution.getStatus());
    verify(store, never()).incrementRetryAttempt(any(), any(), any());
  }

  @ParameterizedTest
  @EnumSource(
      value = JobStatus.class,
      names = {"CANCELED", "RUNNING"})
  void supersededBodyLeftRollbackOnlyKeepsSupersededPath(JobStatus status) throws Exception {
    AtomicBoolean rolledBack = new AtomicBoolean();
    doAnswer(
            invocation -> {
              try {
                return invocation.callRealMethod();
              } catch (CompletionRejectedException failure) {
                rolledBack.set(true);
                throw failure;
              }
            })
        .when(carrier)
        .run(any());
    BODY.set(
        () -> {
          attempt.markSuperseded();
          carrier.markRollbackOnly();
          when(store.getJobStatus(job.getId())).thenReturn(status);
          return 42;
        });
    // setUp leaves completion accepting this still-intact claim.
    task.call();
    assertTrue(rolledBack.get());
    verify(carrier).run(any());
    verify(lifecycle, never())
        .completeSuccessInCurrentTransaction(
            any(), any(), any(), any(), any(), anyLong(), anyLong(), nullable(Integer.class));
    verifyNoInteractions(results);
    assertEquals(
        CircuitBreakerExceptionFilter.Outcome.IGNORED,
        callFilter.get().classify(supplierFailure.get()));
    verify(store, never()).incrementRetryAttempt(any(), any(), any());
    if (status == JobStatus.CANCELED) {
      assertEquals(ExecutionStatus.CANCELED, execution.getStatus());
      verify(observer).recordJobCancellation(job);
    } else {
      assertEquals(JobExecutionEntity.RESULT_DISCARDED_ERROR_MESSAGE, execution.getErrorMessage());
    }
    verify(lifecycle, never()).runFollowups(any());
  }

  private static JobPayload payload(String method, String descriptor) {
    return new JobPayload(
        JobTaskTransactionalTest.class.getName(), method, descriptor, true, List.of(), null);
  }
}
