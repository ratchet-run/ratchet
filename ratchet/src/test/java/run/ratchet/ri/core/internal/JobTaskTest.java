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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.clearInvocations;
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
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.jboss.logging.MDC;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import run.ratchet.api.CircuitBreakerProtected;
import run.ratchet.api.JobContext;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobStatus;
import run.ratchet.api.RatchetOptions;
import run.ratchet.api.SerializableBiConsumer;
import run.ratchet.api.SerializableCheckedRunnable;
import run.ratchet.api.SerializableConsumer;
import run.ratchet.api.SignalDecision;
import run.ratchet.api.event.JobCallbackFailedEvent;
import run.ratchet.api.event.JobCompletedEvent;
import run.ratchet.api.event.JobDlqEvent;
import run.ratchet.api.event.JobFailedEvent;
import run.ratchet.api.event.JobRetryingEvent;
import run.ratchet.api.event.JobStartedEvent;
import run.ratchet.api.exception.AttemptSupersededException;
import run.ratchet.api.exception.CancellationRequestedException;
import run.ratchet.api.exception.CircuitBreakerOpenException;
import run.ratchet.api.exception.KeyProviderUnavailableException;
import run.ratchet.api.exception.PayloadDecryptionException;
import run.ratchet.api.exception.RatchetTransientStoreException;
import run.ratchet.api.exception.ResourceNotConfiguredException;
import run.ratchet.api.exception.SignalOutcomeHydrationException;
import run.ratchet.api.exception.UnsupportedEnvelopeVersionException;
import run.ratchet.ri.core.DefaultJobSchedulerService;
import run.ratchet.ri.core.DefaultResultPersistenceStrategy;
import run.ratchet.ri.core.JBossLoggingJobLogger;
import run.ratchet.ri.core.JobTransaction;
import run.ratchet.ri.core.ResourcePermitService;
import run.ratchet.ri.payload.JobPayloadFactory;
import run.ratchet.ri.security.DefaultErrorSanitizer;
import run.ratchet.ri.testsupport.EncryptionTestKit;
import run.ratchet.ri.testutil.JsonbTestPayloadSerializer;
import run.ratchet.spi.AfterCommitRegistrar;
import run.ratchet.spi.BeanResolver;
import run.ratchet.spi.CircuitBreakerExceptionFilter;
import run.ratchet.spi.ClassPolicy;
import run.ratchet.spi.EncryptionContext;
import run.ratchet.spi.EncryptionKey;
import run.ratchet.spi.ErrorSanitizer;
import run.ratchet.spi.JobInvocation;
import run.ratchet.spi.JobLogger;
import run.ratchet.spi.KeyProvider;
import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.spi.PayloadEncryption;
import run.ratchet.spi.PayloadSerializer;
import run.ratchet.spi.PreExecutionArgResolver;
import run.ratchet.spi.ResilienceStrategy;
import run.ratchet.spi.ResultPersistenceStrategy;
import run.ratchet.spi.RetryPolicy;
import run.ratchet.spi.SerializedJobResult;
import run.ratchet.spi.TracingCollector;
import run.ratchet.store.converter.EncryptionHolder;
import run.ratchet.store.converter.JobPayloadConverter;
import run.ratchet.store.dto.JobClaimDto;
import run.ratchet.store.dto.JobCompletionPlan;
import run.ratchet.store.dto.JobCompletionResult;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionEntity;
import run.ratchet.store.entity.JobExecutionEntity.ExecutionStatus;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.entity.JobPayload;
import run.ratchet.store.spi.ClaimState;
import run.ratchet.store.spi.JobStateStore;
import run.ratchet.store.spi.JobStateStore.StateWriteResult;
import run.ratchet.store.spi.JobStore;
import run.ratchet.store.spi.ResourcePermitStore.AcquireResult;
import run.ratchet.store.spi.TransactionalCompletionStore;
import run.ratchet.store.util.EncryptionTarget;
import run.ratchet.store.util.PayloadEncryptor;

@ExtendWith(MockitoExtension.class)
class JobTaskTest {

  private final ScheduledExecutorService holdReleaseScheduler =
      mock(ScheduledExecutorService.class);
  private final ScheduledExecutorService permitReleaseScheduler =
      mock(ScheduledExecutorService.class);

  private final ScheduledExecutorService outcomeScheduler = mock(ScheduledExecutorService.class);
  private final AtomicLong outcomeClock = new AtomicLong();
  private final OutcomeWriteRetries outcomeRetries =
      new OutcomeWriteRetries(() -> outcomeScheduler, null, outcomeClock::get);
  private final List<Runnable> outcomePending = new ArrayList<>();
  private final List<Long> outcomeDelays = new ArrayList<>();

  private void recordOutcomeRetries() {
    doAnswer(
            invocation -> {
              outcomePending.add(invocation.getArgument(0));
              outcomeDelays.add(invocation.getArgument(1));
              return null;
            })
        .when(outcomeScheduler)
        .schedule(any(Runnable.class), anyLong(), eq(TimeUnit.MILLISECONDS));
  }

  private void runOutcomeRetry() {
    outcomeClock.addAndGet(
        TimeUnit.MILLISECONDS.toNanos(outcomeDelays.get(outcomeDelays.size() - 1)));
    outcomePending.remove(0).run();
  }

  private static final String CONTINUATION_NS = "ratchet.continuation";
  private static final ThreadLocal<Runnable> CONTINUATION_PAYLOAD = new ThreadLocal<>();

  public static String continuationPayload() {
    CONTINUATION_PAYLOAD.get().run();
    return "ignored";
  }

  private record ContinuationScenario(
      JobEntity job,
      JobStateStore store,
      JobAttemptControl attempt,
      JobExecutionEntity execution,
      ResultPersistenceStrategy results) {}

  private ContinuationScenario continuationScenario(Runnable payload, int limit) throws Exception {
    return continuationScenario(payload, limit, TracingCollector.NoOpExecutionScope.INSTANCE);
  }

  @SuppressWarnings("unchecked")
  private ContinuationScenario continuationScenario(
      Runnable payload, int limit, TracingCollector.ExecutionScope scope) throws Exception {
    ResultPersistenceStrategy results = mock(ResultPersistenceStrategy.class);
    jobTask =
        new JobTask(
            jobStore,
            resourcePermitService,
            lifecycleFacade,
            nodeIdProvider,
            observabilityFacade,
            validationFacade,
            new JobPayloadInvoker(beanResolver, classPolicy),
            new JobTransaction(),
            new JobSuccessFinalizer(lifecycleFacade, observabilityFacade),
            retryPolicy,
            resilienceStrategy,
            errorSanitizer,
            context -> noopLogger(),
            results,
            null,
            new JsonbTestPayloadSerializer(),
            checkpointRegistrar,
            jobStateFlusher,
            null,
            new HeldKeyReleases(jobStore, () -> holdReleaseScheduler),
            new PermitReleases(resourcePermitService, () -> permitReleaseScheduler, null),
            claimReschedules,
            outcomeRetries,
            FIXED_CLOCK,
            null,
            RatchetOptions.builder().execution(e -> e.maxContinuations(limit)).build());
    JobEntity job = createTestJob();
    job.setClaimSeq(7L);
    job.setStatus(JobStatus.RUNNING);
    job.setLastError("previous failure");
    job.setPayload(
        new JobPayload(
            JobTaskTest.class.getName(),
            "continuationPayload",
            "()Ljava/lang/String;",
            true,
            List.of(),
            null));
    JobAttemptControl attempt =
        new JobAttemptControl(JOB_UUID, FIXED_NOW.plusSeconds(30), 30, FIXED_NOW, 0, 7L, "node-1");
    jobTask.init(job);
    when(nodeIdProvider.getNodeId()).thenReturn("node-1");
    // Lenient: a claim parked at the permit stops before execution recording.
    lenient().when(observabilityFacade.startExecutionScope(any(JobEntity.class))).thenReturn(scope);
    jobTask.attach(attempt);
    JobExecutionEntity execution = JobExecutionEntity.start(JOB_UUID, 1, "node-1");
    lenient()
        .when(observabilityFacade.startExecution(any(UUID.class), anyInt(), anyString()))
        .thenReturn(execution);
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING);
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);
    lenient()
        .when(resilienceStrategy.execute(anyString(), any(), any(Callable.class)))
        .thenAnswer(inv -> ((Callable<?>) inv.getArgument(2)).call());
    JobStateStore store = mock(JobStateStore.class);
    lenient().when(jobStore.capability(JobStateStore.class)).thenReturn(Optional.of(store));
    CONTINUATION_PAYLOAD.set(payload);
    return new ContinuationScenario(job, store, attempt, execution, results);
  }

  private ContinuationScenario hydratedClaimScenario(Runnable body) throws Exception {
    var scenario = continuationScenario(body, 2);
    scenario.job().setAttempts(3);
    initJobTaskFromClaimWithDefaultStubs(attemptFenceClaim(), scenario.job());
    var attempt =
        new JobAttemptControl(JOB_UUID, FIXED_NOW.plusSeconds(30), 30, FIXED_NOW, 2, 7L, "node-1");
    jobTask.attach(attempt);
    return new ContinuationScenario(
        scenario.job(), scenario.store(), attempt, scenario.execution(), scenario.results());
  }

  private static JobClaimDto attemptFenceClaim() {
    return new JobClaimDto(
        JOB_UUID,
        JobStatus.RUNNING,
        JobExecutionType.SINGLE,
        JobPriority.NORMAL,
        Instant.EPOCH,
        1,
        30,
        "node-1",
        Instant.EPOCH,
        null,
        2,
        3,
        null,
        null,
        7L);
  }

  @Test
  void successUsesClaimAttemptsAfterHydrationCharge() throws Exception {
    var scenario = hydratedClaimScenario(() -> {});
    when(scenario.results().serialize(any(), any())).thenReturn(SerializedJobResult.empty());
    when(lifecycleFacade.completeSuccess(
            eq(scenario.job()), any(), any(), any(), any(), anyLong(), anyLong(), eq(2)))
        .thenReturn(false);

    jobTask.call();

    verify(lifecycleFacade)
        .completeSuccess(
            eq(scenario.job()), any(), any(), any(), any(), anyLong(), anyLong(), eq(2));
    Assertions.assertTrue(scenario.attempt().isSuperseded());
    Assertions.assertEquals(
        JobExecutionEntity.RESULT_DISCARDED_ERROR_MESSAGE, scenario.execution().getErrorMessage());
    verify(observabilityFacade).saveExecution(scenario.execution());
    Assertions.assertEquals(3, scenario.job().getAttempts());
  }

  @Test
  void minimalSuccessUsesClaimAttemptsAfterHydrationCharge() throws Exception {
    var scenario = hydratedClaimScenario(() -> {});
    when(scenario.results().serialize(any(), any())).thenReturn(SerializedJobResult.empty());
    when(lifecycleFacade.completeSuccess(
            eq(scenario.job()), any(), any(), any(), any(), anyLong(), anyLong(), eq(2)))
        .thenThrow(new RatchetTransientStoreException("busy"));
    when(lifecycleFacade.completeSuccessMinimal(
            eq(scenario.job()), any(), any(), anyLong(), anyLong(), eq(2)))
        .thenReturn(false);

    jobTask.call();

    verify(lifecycleFacade)
        .completeSuccessMinimal(eq(scenario.job()), any(), any(), anyLong(), anyLong(), eq(2));
    Assertions.assertTrue(scenario.attempt().isSuperseded());
    Assertions.assertEquals(
        JobExecutionEntity.RESULT_DISCARDED_ERROR_MESSAGE, scenario.execution().getErrorMessage());
  }

  @Test
  void nonRetryableFailureUsesClaimAttemptsAfterHydrationCharge() throws Exception {
    RuntimeException failure = new IllegalStateException("body");
    var scenario =
        hydratedClaimScenario(
            () -> {
              throw failure;
            });
    when(validationFacade.shouldNotRetry(failure)).thenReturn(true);
    when(lifecycleFacade.completeFailure(scenario.job(), JobStatus.RUNNING, false, 2))
        .thenReturn(true);

    jobTask.call();

    verify(lifecycleFacade).completeFailure(scenario.job(), JobStatus.RUNNING, false, 2);
    verify(jobStore, never()).incrementRetryAttempt(any(), any(), any());
  }

  @Test
  void continuationLimitUsesClaimAttemptsAfterHydrationCharge() throws Exception {
    var scenario = hydratedClaimScenario(() -> JobContext.current().requestContinuation());
    when(scenario.store().getState(JOB_UUID, CONTINUATION_NS))
        .thenReturn(Optional.of(new JobStateStore.VersionedState("{\"count\":2}", 5)));
    when(lifecycleFacade.completeFailure(scenario.job(), JobStatus.RUNNING, false, 2))
        .thenReturn(true);

    jobTask.call();

    verify(lifecycleFacade).completeFailure(scenario.job(), JobStatus.RUNNING, false, 2);
    verify(jobStore, never()).incrementRetryAttempt(any(), any(), any());
  }

  @Test
  void dlqUsesWorkersIncrementedAttemptCount() throws Exception {
    RuntimeException failure = new IllegalStateException("body");
    var scenario =
        hydratedClaimScenario(
            () -> {
              throw failure;
            });
    scenario.job().setAttempts(5);
    when(jobStore.incrementRetryAttempt(JOB_UUID, 7L, 2)).thenReturn(3);
    when(retryPolicy.shouldRetry(3, failure)).thenReturn(false);
    AtomicInteger writtenAttempts = new AtomicInteger();
    when(lifecycleFacade.completeFailure(scenario.job(), JobStatus.RUNNING, false, 3))
        .thenAnswer(
            invocation -> {
              writtenAttempts.set(invocation.getArgument(0, JobEntity.class).getAttempts());
              return true;
            });

    jobTask.call();

    verify(jobStore).incrementRetryAttempt(JOB_UUID, 7L, 2);
    verify(lifecycleFacade).completeFailure(scenario.job(), JobStatus.RUNNING, false, 3);
    Assertions.assertEquals(3, writtenAttempts.get());
    Assertions.assertEquals(JobStatus.FAILED, scenario.job().getStatus());
  }

  @Test
  void watchdogClaimBeforeWorkerHoldDefersWithoutCharge() throws Exception {
    RuntimeException failure = new IllegalStateException("body");
    var scenario =
        hydratedClaimScenario(
            () -> {
              throw failure;
            });
    AtomicBoolean claimed = new AtomicBoolean();
    when(errorSanitizer.sanitize(failure))
        .thenAnswer(
            invocation -> {
              if (claimed.compareAndSet(false, true)) {
                Assertions.assertTrue(scenario.attempt().claimTimeoutForWatchdog());
              }
              return "safe body failure";
            });

    jobTask.call();

    verify(jobStore, never()).incrementRetryAttempt(any(), any(), any());
    Assertions.assertTrue(scenario.attempt().isSuperseded());
    Assertions.assertEquals(3, scenario.job().getAttempts());
    verify(lifecycleFacade, never())
        .completeFailure(any(), any(), anyBoolean(), nullable(Integer.class));
    verify(jobStore, never()).scheduleJobRetry(any(), any(), any(), anyInt(), any());
    verify(observabilityFacade).saveExecution(scenario.execution());
    Assertions.assertEquals(ExecutionStatus.FAILED, scenario.execution().getStatus());
    Assertions.assertEquals(failure.getClass().getName(), scenario.execution().getErrorClass());
    Assertions.assertEquals("safe body failure", scenario.execution().getErrorMessage());
  }

  @Test
  void workerHoldStopsWatchdogFromOverridingDlqDecision() throws Exception {
    RuntimeException failure = new IllegalStateException("body");
    var scenario =
        hydratedClaimScenario(
            () -> {
              throw failure;
            });
    when(jobStore.incrementRetryAttempt(JOB_UUID, 7L, 2))
        .thenAnswer(
            invocation -> {
              Assertions.assertFalse(scenario.attempt().claimTimeoutForWatchdog());
              return 3;
            });
    when(retryPolicy.shouldRetry(3, failure)).thenReturn(false);
    when(lifecycleFacade.completeFailure(scenario.job(), JobStatus.RUNNING, false, 3))
        .thenReturn(true);

    jobTask.call();

    verify(lifecycleFacade).completeFailure(scenario.job(), JobStatus.RUNNING, false, 3);
    Assertions.assertEquals(JobStatus.FAILED, scenario.job().getStatus());
    Assertions.assertFalse(scenario.attempt().handBackTimeoutToWatchdog());
    Assertions.assertTrue(scenario.attempt().holdTimeoutForWorker());
    verify(jobStore, never()).scheduleJobRetry(any(), any(), any(), anyInt(), any());
  }

  @Test
  void nonRetryableFailureHoldsTimeoutBeforeItsWrite() throws Exception {
    RuntimeException failure = new IllegalStateException("body");
    var scenario =
        hydratedClaimScenario(
            () -> {
              throw failure;
            });
    when(validationFacade.shouldNotRetry(failure)).thenReturn(true);
    AtomicReference<Boolean> watchdogClaimed = new AtomicReference<>();
    when(lifecycleFacade.completeFailure(scenario.job(), JobStatus.RUNNING, false, 2))
        .thenAnswer(
            invocation -> {
              watchdogClaimed.set(scenario.attempt().claimTimeoutForWatchdog());
              return true;
            });

    jobTask.call();

    Assertions.assertEquals(Boolean.FALSE, watchdogClaimed.get());
    verify(lifecycleFacade).completeFailure(scenario.job(), JobStatus.RUNNING, false, 2);
    verify(jobStore, never()).incrementRetryAttempt(any(), any(), any());
  }

  @Test
  void forcedFailureRetriesUntilItLandsWithoutTouchingWorkerState() throws Exception {
    RuntimeException failure = new IllegalStateException("body");
    var scenario =
        hydratedClaimScenario(
            () -> {
              throw failure;
            });
    recordOutcomeRetries();
    CallbackRecorder.reset();
    SerializableBiConsumer<JobContext, Throwable> callback = CallbackRecorder::failure;
    scenario.job().setOnFailurePayload(JobPayloadFactory.fromLambda(callback));
    when(validationFacade.shouldNotRetry(failure)).thenThrow(new IllegalStateException("policy"));
    AtomicInteger writes = new AtomicInteger();
    AtomicReference<Boolean> watchdogClaimed = new AtomicReference<>();
    when(lifecycleFacade.completeFailure(any(), eq(JobStatus.RUNNING), eq(false), eq(2)))
        .thenAnswer(
            invocation -> {
              watchdogClaimed.set(scenario.attempt().claimTimeoutForWatchdog());
              if (writes.incrementAndGet() <= 3) throw new IllegalStateException("write failed");
              return true;
            });

    jobTask.call();

    Assertions.assertEquals(Boolean.FALSE, watchdogClaimed.get());
    Assertions.assertFalse(scenario.attempt().isTimeoutHandedBack());
    Assertions.assertEquals(2, writes.get());
    JobStatus status = scenario.job().getStatus();
    String error = scenario.job().getLastError();
    runOutcomeRetry();
    Assertions.assertEquals(1, outcomePending.size());
    runOutcomeRetry();

    Assertions.assertEquals(List.of(5_000L, 10_000L), outcomeDelays);
    Assertions.assertTrue(outcomePending.isEmpty());
    Assertions.assertEquals(4, writes.get());
    Assertions.assertEquals(status, scenario.job().getStatus());
    Assertions.assertEquals(error, scenario.job().getLastError());
    Assertions.assertSame(failure, CallbackRecorder.failure);
    verify(validationFacade, times(1))
        .validateSecurity(
            argThat(
                payload ->
                    payload != null && payload.target().equals(CallbackRecorder.class.getName())));
    verify(observabilityFacade, times(1)).saveExecution(scenario.execution());
    ArgumentCaptor<JobEntity> copies = ArgumentCaptor.forClass(JobEntity.class);
    verify(lifecycleFacade, times(4))
        .completeFailure(copies.capture(), eq(JobStatus.RUNNING), eq(false), eq(2));
    Assertions.assertNotSame(scenario.job(), copies.getValue());
  }

  @Test
  void failedIncrementForcedFailureNeverGuessesCountWhenReadFails() throws Exception {
    RuntimeException failure = new IllegalStateException("body");
    var scenario =
        hydratedClaimScenario(
            () -> {
              throw failure;
            });
    recordOutcomeRetries();
    CallbackRecorder.reset();
    SerializableBiConsumer<JobContext, Throwable> callback = CallbackRecorder::failure;
    scenario.job().setOnFailurePayload(JobPayloadFactory.fromLambda(callback));
    when(jobStore.incrementRetryAttempt(JOB_UUID, 7L, 2))
        .thenThrow(new IllegalStateException("unknown"));
    JobEntity stored = createTestJob();
    stored.setAttempts(5);
    when(jobStore.findByIdLatest(JOB_UUID))
        .thenThrow(new IllegalStateException("worker read"))
        .thenThrow(new IllegalStateException("finisher read"))
        .thenReturn(Optional.of(stored));

    jobTask.call();

    verify(lifecycleFacade, never())
        .completeFailure(any(), any(), anyBoolean(), nullable(Integer.class));
    Assertions.assertFalse(scenario.attempt().claimTimeoutForWatchdog());
    Assertions.assertFalse(scenario.attempt().isTimeoutHandedBack());
    AtomicInteger writtenCount = new AtomicInteger();
    when(lifecycleFacade.completeFailure(any(), eq(JobStatus.RUNNING), eq(false), isNull()))
        .thenAnswer(
            invocation -> {
              writtenCount.set(invocation.getArgument(0, JobEntity.class).getAttempts());
              return true;
            });
    runOutcomeRetry();

    Assertions.assertEquals(5, writtenCount.get());
    Assertions.assertSame(failure, CallbackRecorder.failure);
    verify(validationFacade, times(1))
        .validateSecurity(
            argThat(
                payload ->
                    payload != null && payload.target().equals(CallbackRecorder.class.getName())));
    Assertions.assertTrue(outcomePending.isEmpty());
    verify(lifecycleFacade, times(1))
        .completeFailure(any(), eq(JobStatus.RUNNING), eq(false), isNull());
  }

  @Test
  void deletedJobStopsForcedFinisherAfterUnknownIncrement() throws Exception {
    var scenario =
        hydratedClaimScenario(
            () -> {
              throw new IllegalStateException("body");
            });
    recordOutcomeRetries();
    CallbackRecorder.reset();
    SerializableBiConsumer<JobContext, Throwable> callback = CallbackRecorder::failure;
    scenario.job().setOnFailurePayload(JobPayloadFactory.fromLambda(callback));
    when(jobStore.incrementRetryAttempt(JOB_UUID, 7L, 2))
        .thenThrow(new IllegalStateException("unknown"));
    when(jobStore.findByIdLatest(JOB_UUID))
        .thenReturn(Optional.empty())
        .thenThrow(new IllegalStateException("read failed"))
        .thenReturn(Optional.empty());

    jobTask.call();

    Assertions.assertEquals(1, outcomePending.size());
    runOutcomeRetry();
    Assertions.assertTrue(outcomePending.isEmpty());
    Assertions.assertTrue(scenario.attempt().isSuperseded());
    verify(jobStore, times(3)).findByIdLatest(JOB_UUID);
    verify(lifecycleFacade, never())
        .completeFailure(any(), any(), anyBoolean(), nullable(Integer.class));
    Assertions.assertNull(CallbackRecorder.failure);
    verify(validationFacade, never())
        .validateSecurity(
            argThat(
                payload ->
                    payload != null && payload.target().equals(CallbackRecorder.class.getName())));
  }

  @Test
  void forcedFailureAfterFailedIncrementRereadsOnEveryRetry() throws Exception {
    RuntimeException failure = new IllegalStateException("body");
    var scenario =
        hydratedClaimScenario(
            () -> {
              throw failure;
            });
    recordOutcomeRetries();
    when(jobStore.incrementRetryAttempt(JOB_UUID, 7L, 2))
        .thenThrow(new IllegalStateException("unknown"));
    JobEntity stored = createTestJob();
    stored.setAttempts(4);
    when(jobStore.findByIdLatest(JOB_UUID)).thenReturn(Optional.of(stored));
    AtomicInteger calls = new AtomicInteger();
    List<Integer> counts = new ArrayList<>();
    when(lifecycleFacade.completeFailure(any(), eq(JobStatus.RUNNING), eq(false), isNull()))
        .thenAnswer(
            invocation -> {
              counts.add(invocation.getArgument(0, JobEntity.class).getAttempts());
              if (calls.incrementAndGet() < 3) throw new IllegalStateException("write failed");
              return true;
            });

    jobTask.call();
    stored.setAttempts(5);
    runOutcomeRetry();

    Assertions.assertEquals(List.of(4, 4, 5), counts);
    Assertions.assertTrue(outcomePending.isEmpty());
  }

  @Test
  void forcedFinisherMissFinishesCancelWhenNoOutcomeWriteThrew() throws Exception {
    RuntimeException failure = new IllegalStateException("body");
    var scenario =
        hydratedClaimScenario(
            () -> {
              throw failure;
            });
    recordOutcomeRetries();
    when(jobStore.incrementRetryAttempt(JOB_UUID, 7L, 2))
        .thenThrow(new IllegalStateException("unknown"));
    when(jobStore.findByIdLatest(JOB_UUID)).thenThrow(new IllegalStateException("read"));
    jobTask.call();
    Runnable record = mock(Runnable.class);
    Runnable release = mock(Runnable.class);
    scenario.attempt().attachCancelFinish(record, release);
    doReturn(Optional.of(scenario.job())).when(jobStore).findByIdLatest(JOB_UUID);
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.CANCELED);

    runOutcomeRetry();

    Assertions.assertTrue(scenario.attempt().isSuperseded());
    verify(record).run();
    verify(release).run();
    Assertions.assertTrue(outcomePending.isEmpty());
  }

  @Test
  void forcedFinisherMissSkipsCancelAfterUnknownOutcomeWrite() throws Exception {
    assertForcedFinisherCancel(true);
  }

  @Test
  void forcedWriteThatThrowsThenCancelRecordsTheCancel() throws Exception {
    assertForcedFinisherCancel(false);
  }

  private void assertForcedFinisherCancel(boolean liveWriteUnknown) throws Exception {
    RuntimeException failure = new IllegalStateException("body");
    var scenario =
        hydratedClaimScenario(
            () -> {
              throw failure;
            });
    recordOutcomeRetries();
    CancelRace race = cancelRace();
    int expected;
    if (liveWriteUnknown) {
      when(jobStore.incrementRetryAttempt(JOB_UUID, 7L, 2)).thenReturn(3);
      when(retryPolicy.shouldRetry(3, failure)).thenReturn(true);
      when(retryPolicy.getDelay(3)).thenReturn(Duration.ofSeconds(1));
      when(jobStore.scheduleJobRetry(eq(JOB_UUID), any(), any(), eq(3), eq(7L)))
          .thenAnswer(
              invocation -> {
                race.status().set(JobStatus.PENDING);
                throw new IllegalStateException("retry committed then threw");
              });
      expected = 3;
    } else {
      when(validationFacade.shouldNotRetry(failure)).thenThrow(new IllegalStateException("policy"));
      expected = 2;
    }
    when(lifecycleFacade.completeFailure(any(), eq(JobStatus.RUNNING), eq(false), eq(expected)))
        .thenThrow(new IllegalStateException("worker write"))
        .thenThrow(new IllegalStateException("inline retry"))
        .thenReturn(false);

    jobTask.call();

    Assertions.assertEquals(
        liveWriteUnknown ? JobStatus.PENDING : JobStatus.RUNNING, race.status().get());
    Assertions.assertEquals(1, outcomePending.size());
    Runnable record = mock(Runnable.class);
    Runnable release = mock(Runnable.class);
    scenario.attempt().attachCancelFinish(record, release);
    race.cancel();
    runOutcomeRetry();

    Assertions.assertTrue(scenario.attempt().isSuperseded());
    verify(record, times(liveWriteUnknown ? 0 : 1)).run();
    verify(release, times(liveWriteUnknown ? 0 : 1)).run();
    Assertions.assertTrue(outcomePending.isEmpty());
  }

  @Test
  void transientPermitAcquireThenAcquiredKeepsCancelDetection() throws Exception {
    AtomicReference<JobStatus> status = new AtomicReference<>(JobStatus.RUNNING);
    var scenario = hydratedClaimScenario(() -> status.set(JobStatus.CANCELED));
    scenario.job().setResourceName("gpu");
    when(jobStore.getJobStatus(JOB_UUID)).thenAnswer(invocation -> status.get());
    when(resourcePermitService.tryAcquire("gpu", JOB_UUID, 7L, "node-1"))
        .thenThrow(new RatchetTransientStoreException("acquire deadlock"))
        .thenReturn(AcquireResult.ACQUIRED);

    jobTask.call();

    verify(resourcePermitService, times(2)).tryAcquire("gpu", JOB_UUID, 7L, "node-1");
    Assertions.assertEquals(ExecutionStatus.CANCELED, scenario.execution().getStatus());
    verify(observabilityFacade, times(1)).recordJobCancellation(scenario.job());
    verify(resourcePermitService).release("gpu", JOB_UUID, 7L);
  }

  @Test
  void transientPermitAcquireThatParkedKeepsFlag() throws Exception {
    AtomicBoolean bodyRan = new AtomicBoolean();
    var scenario = hydratedClaimScenario(() -> bodyRan.set(true));
    scenario.job().setResourceName("gpu");
    CancelRace race = cancelRace();
    when(resourcePermitService.tryAcquire("gpu", JOB_UUID, 7L, "node-1"))
        .thenAnswer(
            invocation -> {
              race.status().set(JobStatus.PENDING);
              throw new RatchetTransientStoreException("park committed then threw");
            })
        .thenReturn(AcquireResult.SUPERSEDED);
    when(resourcePermitService.getRetryDelay("gpu")).thenReturn(250);
    when(jobStore.releaseClaim(eq(JOB_UUID), anyString(), any(), eq(2), eq(7L))).thenReturn(false);

    jobTask.call();

    Assertions.assertEquals(JobStatus.PENDING, race.status().get());
    race.cancel();
    scenario.attempt().finishCancelFoundByTimeout();
    Assertions.assertFalse(bodyRan.get());
    verify(resourcePermitService, times(2)).tryAcquire("gpu", JOB_UUID, 7L, "node-1");
    verify(jobStore).releaseClaim(eq(JOB_UUID), anyString(), any(), eq(2), eq(7L));
    verify(observabilityFacade, never()).recordJobCancellation(any());
    verify(observabilityFacade, never()).saveExecution(any());
  }

  @Test
  void rejectedCountWithoutWatchdogKeepsRejectedWritePath() throws Exception {
    RuntimeException failure = new IllegalStateException("body");
    var scenario =
        hydratedClaimScenario(
            () -> {
              throw failure;
            });
    when(jobStore.incrementRetryAttempt(JOB_UUID, 7L, 2)).thenReturn(-1);
    when(errorSanitizer.sanitize(failure)).thenReturn("safe body failure");

    jobTask.call();

    verify(jobStore).incrementRetryAttempt(JOB_UUID, 7L, 2);
    Assertions.assertTrue(scenario.attempt().isSuperseded());
    Assertions.assertEquals(3, scenario.job().getAttempts());
    verify(lifecycleFacade, never())
        .completeFailure(any(), any(), anyBoolean(), nullable(Integer.class));
    verify(jobStore, never()).scheduleJobRetry(any(), any(), any(), anyInt(), any());
    verify(observabilityFacade).saveExecution(scenario.execution());
    Assertions.assertEquals(ExecutionStatus.FAILED, scenario.execution().getStatus());
    Assertions.assertEquals(failure.getClass().getName(), scenario.execution().getErrorClass());
    Assertions.assertEquals("safe body failure", scenario.execution().getErrorMessage());
  }

  @Test
  void forcedFailureMissSavesDiscardedExecutionWithClaimAttempts() throws Exception {
    RuntimeException failure = new IllegalStateException("body");
    var scenario =
        hydratedClaimScenario(
            () -> {
              throw failure;
            });
    when(validationFacade.shouldNotRetry(failure)).thenThrow(new IllegalStateException("policy"));
    when(lifecycleFacade.completeFailure(scenario.job(), JobStatus.RUNNING, false, 2))
        .thenReturn(false);

    jobTask.call();

    verify(lifecycleFacade).completeFailure(scenario.job(), JobStatus.RUNNING, false, 2);
    Assertions.assertTrue(scenario.attempt().isSuperseded());
    verify(observabilityFacade).saveExecution(scenario.execution());
    Assertions.assertEquals(
        JobExecutionEntity.RESULT_DISCARDED_ERROR_MESSAGE, scenario.execution().getErrorMessage());
  }

  @Test
  void forcedFailureThrowDefersWhenWatchdogOwnsTimeout() throws Exception {
    RuntimeException failure = new IllegalStateException("body");
    var scenario =
        hydratedClaimScenario(
            () -> {
              throw failure;
            });
    scenario.attempt().requestUserCancel();
    when(jobStore.getJobStatus(JOB_UUID))
        .thenReturn(JobStatus.RUNNING)
        .thenThrow(new LinkageError("cancel check"))
        .thenReturn(JobStatus.RUNNING);
    AtomicReference<Boolean> watchdogClaimed = new AtomicReference<>();
    when(errorSanitizer.sanitize(failure))
        .thenAnswer(
            invocation -> {
              watchdogClaimed.set(scenario.attempt().claimTimeoutForWatchdog());
              return "safe body failure";
            })
        .thenReturn("safe body failure");

    jobTask.call();

    Assertions.assertEquals(Boolean.TRUE, watchdogClaimed.get());

    verify(lifecycleFacade, never())
        .completeFailure(any(), any(), anyBoolean(), nullable(Integer.class));
    Assertions.assertFalse(scenario.attempt().isTimeoutHandedBack());
    verify(observabilityFacade).saveExecution(scenario.execution());
    Assertions.assertEquals(ExecutionStatus.FAILED, scenario.execution().getStatus());
    Assertions.assertEquals(failure.getClass().getName(), scenario.execution().getErrorClass());
  }

  @Test
  void failedIncrementLeavesForcedFailureFencedOnlyOnClaim() throws Exception {
    RuntimeException failure = new IllegalStateException("body");
    var scenario =
        hydratedClaimScenario(
            () -> {
              throw failure;
            });
    when(jobStore.incrementRetryAttempt(JOB_UUID, 7L, 2))
        .thenThrow(new IllegalStateException("unknown"));
    when(lifecycleFacade.completeFailure(scenario.job(), JobStatus.RUNNING, false, null))
        .thenReturn(true);

    jobTask.call();

    verify(lifecycleFacade).completeFailure(scenario.job(), JobStatus.RUNNING, false, null);
    Assertions.assertEquals(JobStatus.FAILED, scenario.job().getStatus());
  }

  @Test
  void workerHoldAfterSupersedeCheckPreventsWatchdogForcedFailure() throws Exception {
    RuntimeException failure = new IllegalStateException("body");
    var scenario =
        hydratedClaimScenario(
            () -> {
              throw failure;
            });
    when(jobStore.incrementRetryAttempt(JOB_UUID, 7L, 2))
        .thenThrow(new IllegalStateException("unknown"));
    AtomicInteger calls = new AtomicInteger();
    AtomicReference<Boolean> watchdogClaimed = new AtomicReference<>();
    when(errorSanitizer.sanitize(failure))
        .thenAnswer(
            invocation -> {
              if (calls.incrementAndGet() == 2) {
                watchdogClaimed.set(scenario.attempt().claimTimeoutForWatchdog());
              }
              return "safe body failure";
            });
    when(lifecycleFacade.completeFailure(scenario.job(), JobStatus.RUNNING, false, null))
        .thenReturn(true);

    jobTask.call();

    Assertions.assertEquals(Boolean.FALSE, watchdogClaimed.get());

    verify(lifecycleFacade).completeFailure(scenario.job(), JobStatus.RUNNING, false, null);
    verify(jobStore).findByIdLatest(JOB_UUID);
    verify(observabilityFacade).saveExecution(scenario.execution());
    Assertions.assertEquals(ExecutionStatus.FAILED, scenario.execution().getStatus());
    Assertions.assertEquals(failure.getClass().getName(), scenario.execution().getErrorClass());
    Assertions.assertEquals("safe body failure", scenario.execution().getErrorMessage());
  }

  @Test
  void workerHoldsTimeoutBeforeForcedFailure() throws Exception {
    RuntimeException failure = new IllegalStateException("body");
    var scenario =
        hydratedClaimScenario(
            () -> {
              throw failure;
            });
    when(jobStore.incrementRetryAttempt(JOB_UUID, 7L, 2))
        .thenThrow(new IllegalStateException("unknown"));
    AtomicReference<Boolean> watchdogClaimed = new AtomicReference<>();
    when(lifecycleFacade.completeFailure(scenario.job(), JobStatus.RUNNING, false, null))
        .thenAnswer(
            invocation -> {
              watchdogClaimed.set(scenario.attempt().claimTimeoutForWatchdog());
              return true;
            });

    jobTask.call();

    Assertions.assertEquals(Boolean.FALSE, watchdogClaimed.get());
    Assertions.assertEquals(JobStatus.FAILED, scenario.job().getStatus());
  }

  @Test
  void forcedFailureWithKnownAttemptsHoldsTimeout() throws Exception {
    RuntimeException failure = new IllegalStateException("body");
    var scenario =
        hydratedClaimScenario(
            () -> {
              throw failure;
            });
    when(validationFacade.shouldNotRetry(failure)).thenThrow(new IllegalStateException("policy"));
    AtomicReference<Boolean> watchdogClaimed = new AtomicReference<>();
    when(lifecycleFacade.completeFailure(scenario.job(), JobStatus.RUNNING, false, 2))
        .thenAnswer(
            invocation -> {
              watchdogClaimed.set(scenario.attempt().claimTimeoutForWatchdog());
              return true;
            });

    jobTask.call();

    Assertions.assertEquals(Boolean.FALSE, watchdogClaimed.get());
    verify(jobStore, never()).findByIdLatest(any());
  }

  @Test
  void failedIncrementForcedFailureKeepsCommittedCount() throws Exception {
    RuntimeException failure = new IllegalStateException("body");
    var scenario =
        hydratedClaimScenario(
            () -> {
              throw failure;
            });
    when(jobStore.incrementRetryAttempt(JOB_UUID, 7L, 2))
        .thenThrow(new IllegalStateException("unknown"));
    JobEntity stored = createTestJob();
    stored.setAttempts(4);
    when(jobStore.findByIdLatest(JOB_UUID)).thenReturn(Optional.of(stored));
    AtomicInteger writtenAttempts = new AtomicInteger();
    when(lifecycleFacade.completeFailure(scenario.job(), JobStatus.RUNNING, false, null))
        .thenAnswer(
            invocation -> {
              writtenAttempts.set(scenario.job().getAttempts());
              return true;
            });

    jobTask.call();

    verify(lifecycleFacade).completeFailure(scenario.job(), JobStatus.RUNNING, false, null);
    Assertions.assertEquals(4, writtenAttempts.get());
  }

  @Test
  void unboundCallbackUsesClaimAttemptsAfterHydrationCharge() throws Exception {
    var scenario = hydratedClaimScenario(() -> {});
    var original = scenario.job().getPayload();
    scenario
        .job()
        .setPayload(
            new JobPayload(
                original.target(),
                original.method(),
                original.methodDescriptor(),
                original.isStatic(),
                Arrays.asList((Object) null),
                List.of(0)));
    when(lifecycleFacade.moveToDlqAndHandlePermanentFailure(eq(scenario.job()), any(), eq(2)))
        .thenReturn(true);

    jobTask.call();

    verify(lifecycleFacade).moveToDlqAndHandlePermanentFailure(eq(scenario.job()), any(), eq(2));
    verify(jobStore, never()).incrementRetryAttempt(any(), any(), any());
  }

  @Test
  void batchClosedCancelUsesClaimAttemptsAfterHydrationCharge() throws Exception {
    JobEntity job = createTestJob();
    job.setAttempts(3);
    job.setStatus(JobStatus.RUNNING);
    initJobTaskFromClaimWithDefaultStubs(attemptFenceClaim(), job);
    UUID parent = UUID.randomUUID();
    job.setJobType(JobExecutionType.BATCH_CHILD);
    job.setDependsOn(parent);
    when(jobStore.getJobStatus(parent)).thenReturn(JobStatus.CANCELED);
    when(jobStore.commitCompletion(any())).thenReturn(JobCompletionResult.notCommitted());

    jobTask.call();

    ArgumentCaptor<JobCompletionPlan> plan = ArgumentCaptor.forClass(JobCompletionPlan.class);
    verify(jobStore).commitCompletion(plan.capture());
    Assertions.assertEquals(2, plan.getValue().expectedAttempts());
    Assertions.assertEquals(7L, plan.getValue().expectedClaimSeq());
    Assertions.assertEquals(JobStatus.CANCELED, plan.getValue().terminalStatus());
    verify(observabilityFacade, never()).startExecution(any(), anyInt(), any());
  }

  @Test
  void continuationReleaseQuickRetryDoesNotChargeAttempt() throws Exception {
    var scenario = preparedContinuation();
    when(jobStore.releaseClaim(JOB_UUID, null, FIXED_NOW, 0, 7L))
        .thenThrow(new RatchetTransientStoreException("unavailable"))
        .thenReturn(true);
    jobTask.call();
    Assertions.assertEquals(JobStatus.PENDING, scenario.job().getStatus());
    Assertions.assertEquals(ExecutionStatus.SUCCEEDED, scenario.execution().getStatus());
    verify(scenario.store()).initState(JOB_UUID, CONTINUATION_NS, "{\"count\":1}", 7L);
    verify(jobStore, times(2)).releaseClaim(JOB_UUID, null, FIXED_NOW, 0, 7L);
    assertContinuationUncharged(scenario);
  }

  @Test
  void continuationReleaseRejectedAfterTransientFailureIsDiscarded() throws Exception {
    var scenario = preparedContinuation();
    // This pins the ambiguous commit case: the worker cannot tell whether the first try committed.
    // The row reads CANCELED only after that try, and the worker must not trust the read.
    when(jobStore.releaseClaim(JOB_UUID, null, FIXED_NOW, 0, 7L))
        .thenAnswer(
            inv -> {
              lenient().when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.CANCELED);
              throw new RatchetTransientStoreException("unavailable");
            })
        .thenReturn(false);

    jobTask.call();

    verify(jobStore, times(2)).releaseClaim(JOB_UUID, null, FIXED_NOW, 0, 7L);
    verify(claimReschedules, never())
        .handOff(any(), anyLong(), any(), any(), anyInt(), any(), any(), any(), any());
    assertContinuationDiscarded(scenario);
    verify(observabilityFacade, never()).recordJobCancellation(any());
  }

  private ContinuationScenario preparedContinuation() throws Exception {
    var scenario = continuationScenario(() -> JobContext.current().requestContinuation(), 1000);
    when(scenario.store().initState(eq(JOB_UUID), eq(CONTINUATION_NS), anyString(), eq(7L)))
        .thenReturn(StateWriteResult.UPDATED);
    return scenario;
  }

  private ContinuationScenario handedOffContinuation() throws Exception {
    var scenario = preparedContinuation();
    scenario.job().setBusinessKey("continuation-key");
    when(jobStore.releaseClaim(JOB_UUID, null, FIXED_NOW, 0, 7L))
        .thenThrow(new RatchetTransientStoreException("unavailable"));
    when(claimReschedules.handOff(
            any(), anyLong(), any(), any(), anyInt(), any(), any(), any(), any()))
        .thenReturn(true);
    jobTask.call();
    Assertions.assertEquals(ExecutionStatus.RUNNING, scenario.execution().getStatus());
    verify(observabilityFacade, never()).saveExecution(scenario.execution());
    assertContinuationUncharged(scenario);
    return scenario;
  }

  private Runnable continuationCallback(int index) {
    ArgumentCaptor<Runnable> accepted = ArgumentCaptor.forClass(Runnable.class);
    ArgumentCaptor<Runnable> rejected = ArgumentCaptor.forClass(Runnable.class);
    ArgumentCaptor<Runnable> dropped = ArgumentCaptor.forClass(Runnable.class);
    verify(claimReschedules)
        .handOff(
            eq(JOB_UUID),
            eq(7L),
            isNull(),
            eq(FIXED_NOW),
            eq(0),
            any(),
            accepted.capture(),
            rejected.capture(),
            dropped.capture());
    return switch (index) {
      case 0 -> accepted.getValue();
      case 1 -> rejected.getValue();
      default -> dropped.getValue();
    };
  }

  private void assertContinuationUncharged(ContinuationScenario scenario) {
    Assertions.assertEquals(0, scenario.job().getAttempts());
    verify(jobStore, never()).incrementRetryAttempt(any(), any(), any());
    verify(jobStore, never()).scheduleJobRetry(any(), any(), any(), anyInt(), any());
    verify(observabilityFacade, never()).recordJobFailure(any(), any(), anyInt());
  }

  private void assertContinuationDiscarded(ContinuationScenario scenario) {
    Assertions.assertEquals(ExecutionStatus.FAILED, scenario.execution().getStatus());
    Assertions.assertEquals(
        JobExecutionEntity.RESULT_DISCARDED_ERROR_MESSAGE, scenario.execution().getErrorMessage());
    verify(observabilityFacade).saveExecution(scenario.execution());
    assertContinuationUncharged(scenario);
  }

  @Test
  void continuationHandedOffThenAccepted() throws Exception {
    var scenario = handedOffContinuation();
    continuationCallback(0).run();
    Assertions.assertEquals(ExecutionStatus.SUCCEEDED, scenario.execution().getStatus());
    Assertions.assertEquals(JobStatus.PENDING, scenario.job().getStatus());
    verify(observabilityFacade).saveExecution(scenario.execution());
    assertContinuationUncharged(scenario);
  }

  @Test
  void continuationHandedOffThenRejectedReleasesHold() throws Exception {
    var scenario = handedOffContinuation();
    continuationCallback(1).run();
    assertContinuationDiscarded(scenario);
    Assertions.assertTrue(scenario.attempt().isSuperseded());
    verify(jobStore).releaseBusinessKeyHold(JOB_UUID, 7L);
  }

  @Test
  void continuationHandedOffThenRejectedCanceledRowIsDiscarded() throws Exception {
    var scenario = handedOffContinuation();
    // A try threw, so the outcome is unknown and the worker must not trust this CANCELED read.
    lenient().when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.CANCELED);
    continuationCallback(1).run();
    assertContinuationDiscarded(scenario);
    verify(observabilityFacade, never()).recordJobCancellation(any());
  }

  @Test
  void continuationInlineRejectionRecordsCancellationOnce() throws Exception {
    var scenario = preparedContinuation();
    when(jobStore.releaseClaim(JOB_UUID, null, FIXED_NOW, 0, 7L))
        .thenAnswer(
            inv -> {
              when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.CANCELED);
              return false;
            });
    jobTask.call();
    Assertions.assertEquals(ExecutionStatus.CANCELED, scenario.execution().getStatus());
    verify(observabilityFacade).recordJobCancellation(scenario.job());
    assertContinuationUncharged(scenario);
  }

  @Test
  void continuationHandedOffThenDroppedIsDiscarded() throws Exception {
    var scenario = handedOffContinuation();
    continuationCallback(2).run();
    assertContinuationDiscarded(scenario);
  }

  @Test
  void continuationRevokedDuringQuickRetriesIsDiscarded() throws Exception {
    var scenario = preparedContinuation();
    when(jobStore.releaseClaim(JOB_UUID, null, FIXED_NOW, 0, 7L))
        .thenAnswer(
            inv -> {
              scenario.attempt().revoke();
              throw new RatchetTransientStoreException("unavailable");
            });
    jobTask.call();
    verifyNoInteractions(claimReschedules);
    assertContinuationDiscarded(scenario);
  }

  @Test
  void continuationPreservesAttemptsClearsErrorAndSkipsSuccessProcessing() throws Exception {
    var scenario =
        continuationScenario(
            () -> JobContext.current().requestContinuation(Duration.ofSeconds(2)), 1000);
    when(scenario.store().initState(eq(JOB_UUID), eq(CONTINUATION_NS), anyString(), eq(7L)))
        .thenReturn(StateWriteResult.UPDATED);
    when(jobStore.releaseClaim(JOB_UUID, null, FIXED_NOW.plusSeconds(2), 0, 7L)).thenReturn(true);
    scenario
        .job()
        .setOnSuccessPayload(
            JobPayloadFactory.fromLambda(
                (SerializableConsumer<JobContext>) CallbackRecorder::success));
    jobTask.call();
    Assertions.assertNull(CallbackRecorder.context);
    Assertions.assertNull(scenario.job().getJobResult());
    InOrder order = inOrder(scenario.store(), observabilityFacade, jobStore);
    order.verify(scenario.store()).getState(JOB_UUID, CONTINUATION_NS);
    order.verify(scenario.store()).initState(JOB_UUID, CONTINUATION_NS, "{\"count\":1}", 7L);
    order.verify(jobStore).releaseClaim(JOB_UUID, null, FIXED_NOW.plusSeconds(2), 0, 7L);
    order.verify(observabilityFacade).saveExecution(scenario.execution());
    Assertions.assertEquals(JobStatus.PENDING, scenario.job().getStatus());
    Assertions.assertNull(scenario.job().getLastError());
    Assertions.assertEquals(0, scenario.job().getAttempts());
    Assertions.assertEquals(ExecutionStatus.SUCCEEDED, scenario.execution().getStatus());
    verify(scenario.results(), never()).serialize(any(), any());
    verify(lifecycleFacade, never())
        .completeSuccess(
            any(), any(), any(), any(), any(), anyLong(), anyLong(), nullable(Integer.class));
    verify(observabilityFacade, never()).recordJobSuccess(any(), anyLong());
    verify(observabilityFacade, never()).publishEvent(any(JobRetryingEvent.class));
    verify(jobStore, never()).incrementRetryAttempt(any(), any(), any());
  }

  @Test
  void existingCounterAdvancesAcrossRetriesWithoutChangingAttempts() throws Exception {
    var scenario = continuationScenario(() -> JobContext.current().requestContinuation(), 1000);
    scenario.job().setAttempts(2);
    jobTask.init(scenario.job());
    when(scenario.store().getState(JOB_UUID, CONTINUATION_NS))
        .thenReturn(Optional.of(new JobStateStore.VersionedState("{\"count\":4}", 9)));
    when(scenario.store().updateState(JOB_UUID, CONTINUATION_NS, "{\"count\":5}", 9, 7L))
        .thenReturn(StateWriteResult.UPDATED);
    when(jobStore.releaseClaim(JOB_UUID, null, FIXED_NOW, 2, 7L)).thenReturn(true);
    TracingCollector.ExecutionScope scope = mock(TracingCollector.ExecutionScope.class);
    when(observabilityFacade.startExecutionScope(scenario.job())).thenReturn(scope);
    jobTask.call();
    verify(scenario.store()).updateState(JOB_UUID, CONTINUATION_NS, "{\"count\":5}", 9, 7L);
    verify(jobStore).releaseClaim(JOB_UUID, null, FIXED_NOW, 2, 7L);
    Assertions.assertEquals(2, scenario.job().getAttempts());
    verify(scope).success(0L);
    verify(scope).close();
  }

  @Test
  void continuationUsesClaimAttemptsAfterHydrationCharge() throws Exception {
    var scenario = continuationScenario(() -> JobContext.current().requestContinuation(), 1000);
    scenario.job().setAttempts(3);
    JobClaimDto claim =
        new JobClaimDto(
            JOB_UUID,
            JobStatus.RUNNING,
            JobExecutionType.SINGLE,
            JobPriority.NORMAL,
            Instant.EPOCH,
            1,
            30,
            "node-1",
            Instant.EPOCH,
            null,
            2,
            3,
            null,
            null,
            7L);
    initJobTaskFromClaimWithDefaultStubs(claim, scenario.job());
    when(scenario.store().initState(eq(JOB_UUID), eq(CONTINUATION_NS), anyString(), eq(7L)))
        .thenReturn(StateWriteResult.UPDATED);
    // The watchdog charged before hydration, so the stored count is 3 and a release fenced on the
    // claim's count of 2 misses.
    when(jobStore.releaseClaim(JOB_UUID, null, FIXED_NOW, 2, 7L)).thenReturn(false);
    jobTask.call();
    verify(jobStore).releaseClaim(any(), any(), any(), anyInt(), anyLong());
    verify(jobStore).releaseClaim(JOB_UUID, null, FIXED_NOW, 2, 7L);
    verify(jobStore, never()).incrementRetryAttempt(any(), any(), any());
    verify(jobStore, never()).scheduleJobRetry(any(), any(), any(), anyInt(), any());
  }

  @Test
  void continuationReleaseMissAfterTimeoutChargeDiscardsExecution() throws Exception {
    var scenario = continuationScenario(() -> JobContext.current().requestContinuation(), 1000);
    when(scenario.store().initState(eq(JOB_UUID), eq(CONTINUATION_NS), anyString(), eq(7L)))
        .thenReturn(StateWriteResult.UPDATED);
    when(jobStore.releaseClaim(JOB_UUID, null, FIXED_NOW, 0, 7L)).thenReturn(false);
    jobTask.call();
    verify(jobStore).releaseClaim(JOB_UUID, null, FIXED_NOW, 0, 7L);
    verify(observabilityFacade).saveExecution(scenario.execution());
    Assertions.assertEquals(ExecutionStatus.FAILED, scenario.execution().getStatus());
    Assertions.assertEquals(
        JobExecutionEntity.RESULT_DISCARDED_ERROR_MESSAGE, scenario.execution().getErrorMessage());
    verify(observabilityFacade, never())
        .saveExecution(argThat(execution -> execution.getStatus() == ExecutionStatus.SUCCEEDED));
    Assertions.assertEquals(JobStatus.RUNNING, scenario.job().getStatus());
    Assertions.assertTrue(scenario.attempt().isSuperseded());
    verify(jobStore, never()).incrementRetryAttempt(any(), any(), any());
    verify(jobStore, never()).scheduleJobRetry(any(), any(), any(), anyInt(), any());
  }

  @Test
  void requestThenThrowUsesFailureHandling() throws Exception {
    var scenario =
        continuationScenario(
            () -> {
              JobContext.current().requestContinuation();
              throw new IllegalStateException("payload failed");
            },
            1000);
    when(jobStore.incrementRetryAttempt(JOB_UUID, 7L, 0)).thenReturn(1);
    jobTask.call();
    verify(jobStore).incrementRetryAttempt(JOB_UUID, 7L, 0);
    verify(scenario.store(), never()).getState(JOB_UUID, CONTINUATION_NS);
  }

  @Test
  @SuppressWarnings("unchecked")
  void failedResilienceInvocationCannotRequestContinuationForSuccessfulRerun() throws Exception {
    AtomicInteger calls = new AtomicInteger();
    var scenario =
        continuationScenario(
            () -> {
              if (calls.getAndIncrement() == 0) {
                JobContext.current().requestContinuation();
                throw new IllegalStateException("transient");
              }
            },
            1000);
    when(resilienceStrategy.execute(anyString(), any(), any(Callable.class)))
        .thenAnswer(
            inv -> {
              Callable<?> payload = inv.getArgument(2);
              Assertions.assertThrows(IllegalStateException.class, payload::call);
              return payload.call();
            });
    when(scenario.results().serialize(any(), any())).thenReturn(SerializedJobResult.empty());
    when(lifecycleFacade.completeSuccess(
            any(), any(), any(), any(), any(), anyLong(), anyLong(), nullable(Integer.class)))
        .thenReturn(true);
    jobTask.call();
    Assertions.assertEquals(2, calls.get());
    verify(scenario.store(), never()).getState(JOB_UUID, CONTINUATION_NS);
    verify(observabilityFacade).recordJobSuccess(any(), anyLong());
  }

  @Test
  void storeWithoutJobStateRejectsContinuation() throws Exception {
    AtomicReference<Throwable> rejected = new AtomicReference<>();
    var scenario =
        continuationScenario(
            () -> {
              try {
                JobContext.current().requestContinuation();
              } catch (UnsupportedOperationException expected) {
                rejected.set(expected);
              }
            },
            1000);
    when(jobStore.capability(JobStateStore.class)).thenReturn(Optional.empty());
    when(scenario.results().serialize(any(), any())).thenReturn(SerializedJobResult.empty());
    when(lifecycleFacade.completeSuccess(
            any(), any(), any(), any(), any(), anyLong(), anyLong(), nullable(Integer.class)))
        .thenReturn(true);
    jobTask.call();
    Assertions.assertInstanceOf(UnsupportedOperationException.class, rejected.get());
    verify(jobStore, never()).scheduleJobRetry(any(), any(), any(), anyInt(), any());
    verify(jobStore, never()).releaseClaim(any(), any(), any(), anyInt(), anyLong());
    verify(observabilityFacade).recordJobSuccess(any(), anyLong());
  }

  @Test
  void cancellationWinsOverContinuation() throws Exception {
    var scenario = continuationScenario(() -> JobContext.current().requestContinuation(), 1000);
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING, JobStatus.CANCELED);
    jobTask.call();
    verify(scenario.store(), never()).getState(JOB_UUID, CONTINUATION_NS);
    Assertions.assertEquals(ExecutionStatus.CANCELED, scenario.execution().getStatus());
  }

  @Test
  void cancelThatWinsTheContinuationWrite_endsCanceled() throws Exception {
    // A cancel that commits after the post-body check but before the reschedule ends canceled.
    var scenario = continuationScenario(() -> JobContext.current().requestContinuation(), 1000);
    when(scenario.store().initState(eq(JOB_UUID), eq(CONTINUATION_NS), anyString(), eq(7L)))
        .thenReturn(StateWriteResult.UPDATED);
    AtomicReference<JobStatus> status = new AtomicReference<>(JobStatus.RUNNING);
    when(jobStore.getJobStatus(JOB_UUID)).thenAnswer(invocation -> status.get());
    when(jobStore.releaseClaim(eq(JOB_UUID), any(), any(), anyInt(), eq(7L)))
        .thenAnswer(
            invocation -> {
              status.set(JobStatus.CANCELED);
              return false;
            });
    jobTask.call();
    verify(jobStore).releaseClaim(eq(JOB_UUID), any(), any(), anyInt(), eq(7L));
    Assertions.assertEquals(ExecutionStatus.CANCELED, scenario.execution().getStatus());
    verify(observabilityFacade).recordJobCancellation(scenario.job());
  }

  @Test
  void cancelThatSupersedesTheContinuationCounter_endsCanceled() throws Exception {
    var scenario = continuationScenario(() -> JobContext.current().requestContinuation(), 1000);
    AtomicReference<JobStatus> status = new AtomicReference<>(JobStatus.RUNNING);
    when(jobStore.getJobStatus(JOB_UUID)).thenAnswer(invocation -> status.get());
    when(scenario.store().initState(eq(JOB_UUID), eq(CONTINUATION_NS), anyString(), eq(7L)))
        .thenAnswer(
            invocation -> {
              status.set(JobStatus.CANCELED);
              return StateWriteResult.SUPERSEDED;
            });
    jobTask.call();
    Assertions.assertEquals(ExecutionStatus.CANCELED, scenario.execution().getStatus());
    verify(observabilityFacade).recordJobCancellation(scenario.job());
    verify(scenario.store()).initState(eq(JOB_UUID), eq(CONTINUATION_NS), anyString(), eq(7L));
    verify(jobStore, never()).scheduleJobRetry(any(), any(), any(), anyInt(), any());
    verify(jobStore, never()).releaseClaim(any(), any(), any(), anyInt(), anyLong());
  }

  @Test
  void supersededPrecheckSkipsCounter() throws Exception {
    AtomicReference<JobAttemptControl> attempt = new AtomicReference<>();
    var scenario =
        continuationScenario(
            () -> {
              JobContext.current().requestContinuation();
              attempt.get().markSuperseded();
            },
            1000);
    attempt.set(scenario.attempt());
    jobTask.call();
    Assertions.assertEquals(ExecutionStatus.FAILED, scenario.execution().getStatus());
    Assertions.assertEquals(
        JobExecutionEntity.RESULT_DISCARDED_ERROR_MESSAGE, scenario.execution().getErrorMessage());
    verify(scenario.store(), never()).getState(JOB_UUID, CONTINUATION_NS);
    verify(jobStore, never()).scheduleJobRetry(any(), any(), any(), anyInt(), any());
    verify(jobStore, never()).releaseClaim(any(), any(), any(), anyInt(), anyLong());
  }

  @Test
  void supersededCounterWriteDiscardsExecution() throws Exception {
    assertRejectedContinuationCounter(StateWriteResult.SUPERSEDED);
  }

  @Test
  void conflictingCounterWriteRetriesWithFreshCounter() throws Exception {
    var scenario = continuationScenario(() -> JobContext.current().requestContinuation(), 1000);
    when(scenario.store().getState(JOB_UUID, CONTINUATION_NS))
        .thenReturn(
            Optional.of(new JobStateStore.VersionedState("{\"count\":4}", 9)),
            Optional.of(new JobStateStore.VersionedState("{\"count\":0}", 10)));
    when(scenario.store().updateState(JOB_UUID, CONTINUATION_NS, "{\"count\":5}", 9, 7L))
        .thenReturn(StateWriteResult.VERSION_CONFLICT);
    when(scenario.store().updateState(JOB_UUID, CONTINUATION_NS, "{\"count\":1}", 10, 7L))
        .thenReturn(StateWriteResult.UPDATED);
    when(jobStore.releaseClaim(JOB_UUID, null, FIXED_NOW, 0, 7L)).thenReturn(true);
    jobTask.call();
    InOrder order = inOrder(scenario.store(), jobStore);
    order.verify(scenario.store()).getState(JOB_UUID, CONTINUATION_NS);
    order.verify(scenario.store()).updateState(JOB_UUID, CONTINUATION_NS, "{\"count\":5}", 9, 7L);
    order.verify(scenario.store()).getState(JOB_UUID, CONTINUATION_NS);
    order.verify(scenario.store()).updateState(JOB_UUID, CONTINUATION_NS, "{\"count\":1}", 10, 7L);
    order.verify(jobStore).releaseClaim(JOB_UUID, null, FIXED_NOW, 0, 7L);
    Assertions.assertFalse(scenario.attempt().isSuperseded());
    Assertions.assertEquals(JobStatus.PENDING, scenario.job().getStatus());
    Assertions.assertEquals(ExecutionStatus.SUCCEEDED, scenario.execution().getStatus());
    verify(jobStore, never()).incrementRetryAttempt(any(), any(), any());
  }

  @Test
  void conflictingCounterInitializationRetriesWithPersistedCounter() throws Exception {
    var scenario = continuationScenario(() -> JobContext.current().requestContinuation(), 1000);
    when(scenario.store().getState(JOB_UUID, CONTINUATION_NS))
        .thenReturn(
            Optional.empty(), Optional.of(new JobStateStore.VersionedState("{\"count\":1}", 0)));
    when(scenario.store().initState(JOB_UUID, CONTINUATION_NS, "{\"count\":1}", 7L))
        .thenReturn(StateWriteResult.VERSION_CONFLICT);
    when(scenario.store().updateState(JOB_UUID, CONTINUATION_NS, "{\"count\":2}", 0, 7L))
        .thenReturn(StateWriteResult.UPDATED);
    when(jobStore.releaseClaim(JOB_UUID, null, FIXED_NOW, 0, 7L)).thenReturn(true);
    jobTask.call();
    verify(scenario.store(), times(2)).getState(JOB_UUID, CONTINUATION_NS);
    verify(scenario.store()).updateState(JOB_UUID, CONTINUATION_NS, "{\"count\":2}", 0, 7L);
    Assertions.assertFalse(scenario.attempt().isSuperseded());
    Assertions.assertEquals(JobStatus.PENDING, scenario.job().getStatus());
    verify(jobStore, never()).incrementRetryAttempt(any(), any(), any());
  }

  @Test
  void conflictingCounterWriteRechecksLimit() throws Exception {
    var scenario = continuationScenario(() -> JobContext.current().requestContinuation(), 2);
    when(scenario.store().getState(JOB_UUID, CONTINUATION_NS))
        .thenReturn(
            Optional.of(new JobStateStore.VersionedState("{\"count\":1}", 9)),
            Optional.of(new JobStateStore.VersionedState("{\"count\":2}", 10)));
    when(scenario.store().updateState(JOB_UUID, CONTINUATION_NS, "{\"count\":2}", 9, 7L))
        .thenReturn(StateWriteResult.VERSION_CONFLICT);
    when(lifecycleFacade.completeFailure(
            eq(scenario.job()), eq(JobStatus.RUNNING), eq(false), nullable(Integer.class)))
        .thenReturn(true);
    jobTask.call();
    verify(scenario.store(), times(2)).getState(JOB_UUID, CONTINUATION_NS);
    verify(scenario.store()).updateState(JOB_UUID, CONTINUATION_NS, "{\"count\":0}", 10);
    Assertions.assertFalse(scenario.attempt().isSuperseded());
    Assertions.assertEquals(JobStatus.FAILED, scenario.job().getStatus());
    Assertions.assertEquals(ExecutionStatus.FAILED, scenario.execution().getStatus());
    verify(jobStore, never()).incrementRetryAttempt(any(), any(), any());
    verify(jobStore, never()).scheduleJobRetry(any(), any(), any(), anyInt(), any());
    verify(jobStore, never()).releaseClaim(any(), any(), any(), anyInt(), anyLong());
  }

  @Test
  void exhaustedCounterWriteConflictsUseFailureHandling() throws Exception {
    var scenario = continuationScenario(() -> JobContext.current().requestContinuation(), 1000);
    when(scenario.store().getState(JOB_UUID, CONTINUATION_NS))
        .thenReturn(Optional.of(new JobStateStore.VersionedState("{\"count\":4}", 9)));
    when(scenario.store().updateState(JOB_UUID, CONTINUATION_NS, "{\"count\":5}", 9, 7L))
        .thenReturn(StateWriteResult.VERSION_CONFLICT);
    when(jobStore.incrementRetryAttempt(JOB_UUID, 7L, 0)).thenReturn(1);
    when(lifecycleFacade.completeFailure(
            eq(scenario.job()), eq(JobStatus.RUNNING), eq(false), nullable(Integer.class)))
        .thenReturn(true);
    jobTask.call();
    verify(scenario.store(), times(3)).getState(JOB_UUID, CONTINUATION_NS);
    verify(scenario.store(), times(3))
        .updateState(JOB_UUID, CONTINUATION_NS, "{\"count\":5}", 9, 7L);
    verify(jobStore).incrementRetryAttempt(JOB_UUID, 7L, 0);
    Assertions.assertFalse(scenario.attempt().isSuperseded());
    Assertions.assertEquals(1, scenario.job().getAttempts());
    Assertions.assertEquals(JobStatus.FAILED, scenario.job().getStatus());
    Assertions.assertEquals(ExecutionStatus.FAILED, scenario.execution().getStatus());
    Assertions.assertEquals(
        IllegalStateException.class.getName(), scenario.execution().getErrorClass());
    verify(jobStore, never()).scheduleJobRetry(any(), any(), any(), anyInt(), any());
    verify(jobStore, never()).releaseClaim(any(), any(), any(), anyInt(), anyLong());
  }

  private void assertRejectedContinuationCounter(StateWriteResult result) throws Exception {
    var scenario = continuationScenario(() -> JobContext.current().requestContinuation(), 1000);
    when(scenario.store().initState(eq(JOB_UUID), eq(CONTINUATION_NS), anyString(), eq(7L)))
        .thenReturn(result);
    jobTask.call();
    Assertions.assertTrue(scenario.attempt().isSuperseded());
    Assertions.assertEquals(
        JobExecutionEntity.RESULT_DISCARDED_ERROR_MESSAGE, scenario.execution().getErrorMessage());
    verify(jobStore, never()).scheduleJobRetry(any(), any(), any(), anyInt(), any());
    verify(jobStore, never()).releaseClaim(any(), any(), any(), anyInt(), anyLong());
  }

  @Test
  void pausedTransitionKeepsSpentCounterAndDoesNotFail() throws Exception {
    var scenario = continuationScenario(() -> JobContext.current().requestContinuation(), 1000);
    TracingCollector.ExecutionScope scope = mock(TracingCollector.ExecutionScope.class);
    when(observabilityFacade.startExecutionScope(scenario.job())).thenReturn(scope);
    when(scenario.store().initState(eq(JOB_UUID), eq(CONTINUATION_NS), anyString(), eq(7L)))
        .thenReturn(StateWriteResult.UPDATED);
    when(jobStore.releaseClaim(JOB_UUID, null, FIXED_NOW, 0, 7L))
        .thenAnswer(
            inv -> {
              scenario.job().setStatus(JobStatus.PAUSED);
              return false;
            });
    doAnswer(
            invocation -> {
              JobExecutionEntity execution = invocation.getArgument(0);
              Assertions.assertEquals(ExecutionStatus.FAILED, execution.getStatus());
              return null;
            })
        .when(observabilityFacade)
        .saveExecution(scenario.execution());
    Assertions.assertDoesNotThrow(() -> jobTask.call());
    verify(scenario.store()).initState(JOB_UUID, CONTINUATION_NS, "{\"count\":1}", 7L);
    Assertions.assertEquals(ExecutionStatus.FAILED, scenario.execution().getStatus());
    Assertions.assertEquals(
        JobExecutionEntity.RESULT_DISCARDED_ERROR_MESSAGE, scenario.execution().getErrorMessage());
    verify(observabilityFacade, never())
        .saveExecution(argThat(execution -> execution.getStatus() == ExecutionStatus.SUCCEEDED));
    Assertions.assertEquals(JobStatus.PAUSED, scenario.job().getStatus());
    Assertions.assertTrue(scenario.attempt().isSuperseded());
    verify(jobStore, never()).incrementRetryAttempt(any(), any(), any());
    verify(scope, never()).success(anyLong());
    verify(scope).close();
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "{\"count\":-0.5}",
        "{\"count\":2.7}",
        "{\"count\":\"x\"}",
        "{}",
        "not json",
        "{\"count\":9223372036854775808}"
      })
  void malformedContinuationCounterIsRepaired(String json) throws Exception {
    assertStoredContinuationCounter(json, 1);
  }

  @Test
  void integralDecimalContinuationCounterIsAccepted() throws Exception {
    assertStoredContinuationCounter("{\"count\":3.0}", 4);
  }

  private void assertStoredContinuationCounter(String json, long nextCount) throws Exception {
    var scenario = continuationScenario(() -> JobContext.current().requestContinuation(), 1000);
    String repaired = "{\"count\":" + nextCount + "}";
    when(scenario.store().getState(JOB_UUID, CONTINUATION_NS))
        .thenReturn(Optional.of(new JobStateStore.VersionedState(json, 9)));
    when(scenario.store().updateState(JOB_UUID, CONTINUATION_NS, repaired, 9, 7L))
        .thenReturn(StateWriteResult.UPDATED);
    when(jobStore.releaseClaim(JOB_UUID, null, FIXED_NOW, 0, 7L)).thenReturn(true);
    Assertions.assertDoesNotThrow(() -> jobTask.call());
    verify(scenario.store()).updateState(JOB_UUID, CONTINUATION_NS, repaired, 9, 7L);
    Assertions.assertEquals(JobStatus.PENDING, scenario.job().getStatus());
    Assertions.assertEquals(ExecutionStatus.SUCCEEDED, scenario.execution().getStatus());
    verify(jobStore, never()).incrementRetryAttempt(any(), any(), any());
    verify(observabilityFacade, never()).recordJobFailure(any(), any(), anyInt());
    verify(lifecycleFacade, never())
        .completeFailure(any(), any(), anyBoolean(), nullable(Integer.class));
  }

  @Test
  void counterExceptionUsesFailureHandling() throws Exception {
    var scenario = continuationScenario(() -> JobContext.current().requestContinuation(), 1000);
    when(scenario.store().initState(eq(JOB_UUID), eq(CONTINUATION_NS), anyString(), eq(7L)))
        .thenThrow(new IllegalStateException("counter unavailable"));
    when(jobStore.incrementRetryAttempt(JOB_UUID, 7L, 0)).thenReturn(1);
    jobTask.call();
    verify(jobStore).incrementRetryAttempt(JOB_UUID, 7L, 0);
  }

  @Test
  void transitionExceptionUsesFailureHandlingAfterSpendingCounter() throws Exception {
    var scenario = continuationScenario(() -> JobContext.current().requestContinuation(), 1000);
    when(scenario.store().initState(eq(JOB_UUID), eq(CONTINUATION_NS), anyString(), eq(7L)))
        .thenReturn(StateWriteResult.UPDATED);
    when(jobStore.releaseClaim(JOB_UUID, null, FIXED_NOW, 0, 7L))
        .thenThrow(new IllegalStateException("transition unavailable"));
    when(jobStore.incrementRetryAttempt(JOB_UUID, 7L, 0)).thenReturn(1);
    jobTask.call();
    verify(scenario.store()).initState(JOB_UUID, CONTINUATION_NS, "{\"count\":1}", 7L);
    verify(jobStore).incrementRetryAttempt(JOB_UUID, 7L, 0);
  }

  @Test
  void limitFailureResetsOnlyAfterTerminalTransition() throws Exception {
    assertContinuationLimit(true);
  }

  @Test
  void rejectedLimitFailureDoesNotResetCounter() throws Exception {
    assertContinuationLimit(false);
  }

  @Test
  void continuationLimitRecordsOneFailureAfterTheFailureWrite() throws Exception {
    // A continuation-limit failure counts one failure and fails the tracing scope after the job
    // moves to FAILED.
    TracingCollector.ExecutionScope scope = mock(TracingCollector.ExecutionScope.class);
    var scenario = continuationScenario(() -> JobContext.current().requestContinuation(), 2, scope);
    when(scenario.store().getState(JOB_UUID, CONTINUATION_NS))
        .thenReturn(Optional.of(new JobStateStore.VersionedState("{\"count\":2}", 5)));
    when(lifecycleFacade.completeFailure(
            eq(scenario.job()), eq(JobStatus.RUNNING), eq(false), nullable(Integer.class)))
        .thenReturn(true);
    jobTask.call();
    InOrder order = inOrder(lifecycleFacade, observabilityFacade, scope);
    order
        .verify(lifecycleFacade)
        .completeFailure(
            eq(scenario.job()), eq(JobStatus.RUNNING), eq(false), nullable(Integer.class));
    order
        .verify(observabilityFacade)
        .recordJobFailure(eq(scenario.job()), any(IllegalStateException.class), eq(1));
    order.verify(scope).failure(eq(IllegalStateException.class.getName()), any(), eq(1));
    verify(observabilityFacade, times(1)).recordJobFailure(any(), any(), anyInt());
    Assertions.assertEquals(JobStatus.FAILED, scenario.job().getStatus());
  }

  @Test
  void continuationLimitThatLosesItsWriteToACancel_endsCanceled() throws Exception {
    // A cancel that commits after the post-body check wins the continuation-limit failure write.
    // The attempt ends canceled, with no failure metric and no failed tracing scope.
    TracingCollector.ExecutionScope scope = mock(TracingCollector.ExecutionScope.class);
    var scenario = continuationScenario(() -> JobContext.current().requestContinuation(), 2, scope);
    when(scenario.store().getState(JOB_UUID, CONTINUATION_NS))
        .thenReturn(Optional.of(new JobStateStore.VersionedState("{\"count\":2}", 5)));
    AtomicReference<JobStatus> status = new AtomicReference<>(JobStatus.RUNNING);
    when(jobStore.getJobStatus(JOB_UUID)).thenAnswer(invocation -> status.get());
    when(lifecycleFacade.completeFailure(
            eq(scenario.job()), eq(JobStatus.RUNNING), eq(false), nullable(Integer.class)))
        .thenAnswer(
            invocation -> {
              status.set(JobStatus.CANCELED);
              return false;
            });
    jobTask.call();
    verify(lifecycleFacade)
        .completeFailure(
            eq(scenario.job()), eq(JobStatus.RUNNING), eq(false), nullable(Integer.class));
    Assertions.assertEquals(ExecutionStatus.CANCELED, scenario.execution().getStatus());
    verify(observabilityFacade).recordJobCancellation(scenario.job());
    verify(observabilityFacade, never()).recordJobFailure(any(), any(), anyInt());
    verify(scope, never()).failure(any(), any(), anyInt());
    verify(scope).close();
    verify(scenario.store(), never()).updateState(any(), anyString(), anyString(), anyInt());
  }

  @Test
  void resetFailureDoesNotUndoAcceptedTerminalFailure() throws Exception {
    var scenario = continuationScenario(() -> JobContext.current().requestContinuation(), 2);
    when(scenario.store().getState(JOB_UUID, CONTINUATION_NS))
        .thenReturn(Optional.of(new JobStateStore.VersionedState("{\"count\":2}", 5)));
    when(lifecycleFacade.completeFailure(
            eq(scenario.job()), eq(JobStatus.RUNNING), eq(false), nullable(Integer.class)))
        .thenReturn(true);
    when(scenario.store().updateState(JOB_UUID, CONTINUATION_NS, "{\"count\":0}", 5))
        .thenThrow(new IllegalStateException("reset unavailable"));
    jobTask.call();
    Assertions.assertEquals(JobStatus.FAILED, scenario.job().getStatus());
    verify(jobStore, never()).incrementRetryAttempt(any(), any(), any());
  }

  private void assertContinuationLimit(boolean accepted) throws Exception {
    var scenario = continuationScenario(() -> JobContext.current().requestContinuation(), 2);
    when(scenario.store().getState(JOB_UUID, CONTINUATION_NS))
        .thenReturn(Optional.of(new JobStateStore.VersionedState("{\"count\":2}", 5)));
    when(lifecycleFacade.completeFailure(
            eq(scenario.job()), eq(JobStatus.RUNNING), eq(false), nullable(Integer.class)))
        .thenReturn(accepted);
    when(errorSanitizer.sanitize(any()))
        .thenAnswer(inv -> ((Throwable) inv.getArgument(0)).getMessage());
    jobTask.call();
    Assertions.assertTrue(scenario.job().getLastError().contains("limit 2"));
    Assertions.assertTrue(
        scenario.job().getLastError().contains("ratchet.execution.max-continuations"));
    if (accepted) {
      InOrder order = inOrder(lifecycleFacade, scenario.store());
      order
          .verify(lifecycleFacade)
          .completeFailure(
              eq(scenario.job()), eq(JobStatus.RUNNING), eq(false), nullable(Integer.class));
      order.verify(scenario.store()).updateState(JOB_UUID, CONTINUATION_NS, "{\"count\":0}", 5);
    } else {
      verify(scenario.store(), never()).updateState(any(), anyString(), anyString(), anyInt());
    }
    verify(jobStore, never()).incrementRetryAttempt(any(), any(), any());
  }

  private static final UUID JOB_UUID = new UUID(0L, 42L);
  private static final Instant FIXED_NOW = Instant.parse("2026-05-05T12:00:00Z");
  private static final Clock FIXED_CLOCK = Clock.fixed(FIXED_NOW, ZoneOffset.UTC);
  private static final ThreadLocal<SignalDecision> OBSERVED_SIGNAL_DECISION = new ThreadLocal<>();
  private static final ThreadLocal<String> OBSERVED_SIGNAL_STRING = new ThreadLocal<>();
  private static final ThreadLocal<CustomArgument> OBSERVED_CUSTOM_ARGUMENT = new ThreadLocal<>();

  private static final ThreadLocal<Supplier<RuntimeException>> COOPERATIVE_FAILURE =
      new ThreadLocal<>();
  private static final ThreadLocal<JobContext> COOPERATIVE_CONTEXT = new ThreadLocal<>();

  public static String cooperativePayload() {
    COOPERATIVE_CONTEXT.set(JobContext.current());
    Supplier<RuntimeException> failure = COOPERATIVE_FAILURE.get();
    if (failure != null) {
      throw failure.get();
    }
    return "done";
  }

  @Test
  @SuppressWarnings("unchecked")
  void checkpointsFlushBeforeSuccessAndCloseOnExit() throws Exception {
    assertCheckpointFinalFlush(false, false, false);
  }

  @Test
  @SuppressWarnings("unchecked")
  void throwingFinalFlushDoesNotChangeSuccess() throws Exception {
    assertCheckpointFinalFlush(false, true, false);
  }

  @Test
  @SuppressWarnings("unchecked")
  void checkpointsFlushBeforeFailureHandling() throws Exception {
    assertCheckpointFinalFlush(true, false, false);
  }

  @Test
  @SuppressWarnings("unchecked")
  void checkpointsFlushBeforeFailureHandlingEvenWhenFlushThrows() throws Exception {
    assertCheckpointFinalFlush(true, true, false);
  }

  @Test
  @SuppressWarnings("unchecked")
  void interruptedWorkerSkipsFinalFlush() throws Exception {
    assertCheckpointFinalFlush(false, false, true);
  }

  @Test
  void finalFlushSkipsCheckpointWriterInUnitMode() throws Exception {
    assertUnitTask(false);
  }

  @Test
  void supersededUnitByCancelEndsAttemptAsCanceled() throws Exception {
    assertUnitTask(true);
  }

  @Test
  void unitUsesStoreCapabilityAndCarrier() throws Exception {
    assertUnitTask(false);
    verify(jobStore).capability(TransactionalCompletionStore.class);
    verify(unitTransactions).run(any());
  }

  private JobTransaction unitTransactions;

  private void assertUnitTask(boolean superseded) throws Exception {
    JobEntity job = createTestJob();
    initJobTaskWithDefaultStubs(job);
    JobAttemptControl attempt =
        new JobAttemptControl(JOB_UUID, FIXED_NOW.plusSeconds(30), 30, FIXED_NOW, 0, 7L, "node-1");
    jobTask.attach(attempt);
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING);
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);
    JobStateStore state = pendingCheckpointStore();
    // The carrier has no container here: report no open transaction, and run the unit's
    // commit proof after the work, as a real commit would.
    List<Runnable> commits = new ArrayList<>();
    when(checkpointRegistrar.registerAfterCommit(any()))
        .thenAnswer(
            call -> {
              commits.add(call.getArgument(0));
              return AfterCommitRegistrar.Result.REGISTERED;
            });
    doReturn(false).when(unitTransactions).hasActiveTransaction();
    doAnswer(
            call -> {
              Object result = call.callRealMethod();
              commits.forEach(Runnable::run);
              return result;
            })
        .when(unitTransactions)
        .run(any());
    TransactionalCompletionStore transactions = mock(TransactionalCompletionStore.class);
    when(jobStore.capability(TransactionalCompletionStore.class))
        .thenReturn(Optional.of(transactions));
    when(transactions.writeClaimState(
            JOB_UUID, JobCheckpointWriter.NAMESPACE, "{\"value\":\"page-1\"}", 7L, null))
        .thenAnswer(
            call -> {
              if (superseded) when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.CANCELED);
              return superseded ? StateWriteResult.SUPERSEDED : StateWriteResult.UPDATED;
            });
    AtomicBoolean ran = new AtomicBoolean();
    when(resilienceStrategy.execute(anyString(), any(), any(Callable.class)))
        .thenAnswer(
            call -> {
              JobContext.current()
                  .runInTransaction(
                      after -> {
                        Assertions.assertTrue(JobTransaction.active());
                        ran.set(true);
                        return "page-1";
                      });
              return "done";
            });
    if (!superseded) {
      when(lifecycleFacade.completeSuccess(
              any(JobEntity.class),
              any(),
              any(),
              any(),
              any(),
              anyLong(),
              anyLong(),
              nullable(Integer.class)))
          .thenReturn(true);
    }
    jobTask.call();
    Assertions.assertTrue(ran.get());
    verify(transactions)
        .writeClaimState(
            JOB_UUID, JobCheckpointWriter.NAMESPACE, "{\"value\":\"page-1\"}", 7L, null);
    verify(jobStateFlusher, never()).markDirty(any(JobCheckpointWriter.class));
    verify(state, never())
        .initState(any(), eq(JobCheckpointWriter.NAMESPACE), anyString(), anyLong());
    verify(state, never())
        .updateState(any(), eq(JobCheckpointWriter.NAMESPACE), anyString(), anyInt(), anyLong());
    if (superseded) {
      verify(observabilityFacade).recordJobCancellation(job);
      verify(lifecycleFacade, never())
          .completeFailure(any(), any(), anyBoolean(), nullable(Integer.class));
      verify(jobStore, never()).scheduleJobRetry(any(), anyString(), any(), anyInt(), anyLong());
    } else {
      verify(lifecycleFacade)
          .completeSuccess(
              eq(job), any(), any(), any(), any(), anyLong(), anyLong(), nullable(Integer.class));
    }
  }

  @SuppressWarnings("unchecked")
  private void assertCheckpointFinalFlush(boolean failure, boolean flushThrows, boolean interrupted)
      throws Exception {
    JobEntity job = createTestJob();
    initJobTaskWithDefaultStubs(job);
    JobAttemptControl attempt =
        new JobAttemptControl(JOB_UUID, FIXED_NOW.plusSeconds(30), 30, FIXED_NOW, 0, 7L, "node-1");
    jobTask.attach(attempt);
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING);
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);
    JobStateStore stateStore = pendingCheckpointStore();
    lenient()
        .when(stateStore.getState(JOB_UUID, JobProgressWriter.NAMESPACE))
        .thenReturn(Optional.empty());
    lenient()
        .when(
            stateStore.initState(
                eq(JOB_UUID), eq(JobProgressWriter.NAMESPACE), anyString(), eq(7L)))
        .thenReturn(StateWriteResult.UPDATED);
    if (flushThrows) {
      when(stateStore.initState(
              eq(JOB_UUID), eq(JobCheckpointWriter.NAMESPACE), anyString(), eq(7L)))
          .thenThrow(new IllegalStateException("store failed"));
    }
    RuntimeException error = new IllegalStateException("payload failed");
    when(resilienceStrategy.execute(anyString(), any(), any(Callable.class)))
        .thenAnswer(
            call -> {
              JobContext.current().checkpoint("pending");
              checkpointCommit.run();
              JobContext.current().progress(5, 5);
              if (interrupted) Thread.currentThread().interrupt();
              if (failure) throw error;
              return "done";
            });
    if (failure) {
      when(validationFacade.shouldNotRetry(error)).thenReturn(true);
      when(lifecycleFacade.completeFailure(
              eq(job), eq(JobStatus.RUNNING), eq(false), nullable(Integer.class)))
          .thenReturn(true);
    } else {
      when(lifecycleFacade.completeSuccess(
              any(JobEntity.class),
              any(),
              any(),
              any(),
              any(),
              anyLong(),
              anyLong(),
              nullable(Integer.class)))
          .thenReturn(true);
    }
    try {
      Assertions.assertDoesNotThrow(jobTask::call);
      ArgumentCaptor<JobCheckpointWriter> writer =
          ArgumentCaptor.forClass(JobCheckpointWriter.class);
      verify(jobStateFlusher).markDirty(writer.capture());
      ArgumentCaptor<JobProgressWriter> progress = ArgumentCaptor.forClass(JobProgressWriter.class);
      verify(jobStateFlusher).markDirty(progress.capture());
      InOrder order = inOrder(stateStore, lifecycleFacade, jobStateFlusher);
      if (interrupted) {
        verify(stateStore, never()).initState(any(), anyString(), anyString(), anyLong());
        verify(stateStore, never()).getState(any(), anyString());
      } else {
        order.verify(stateStore).getState(JOB_UUID, JobCheckpointWriter.NAMESPACE);
        order
            .verify(stateStore)
            .initState(eq(JOB_UUID), eq(JobCheckpointWriter.NAMESPACE), anyString(), eq(7L));
      }
      if (!interrupted) {
        order
            .verify(stateStore)
            .initState(eq(JOB_UUID), eq(JobProgressWriter.NAMESPACE), anyString(), eq(7L));
      }
      if (failure)
        order
            .verify(lifecycleFacade)
            .completeFailure(eq(job), eq(JobStatus.RUNNING), eq(false), nullable(Integer.class));
      else
        order
            .verify(lifecycleFacade)
            .completeSuccess(
                any(JobEntity.class),
                any(),
                any(),
                any(),
                any(),
                anyLong(),
                anyLong(),
                nullable(Integer.class));
      order.verify(jobStateFlusher).remove(writer.getValue());
      Assertions.assertFalse(writer.getValue().hasUnflushed());
      verify(jobStateFlusher).remove(progress.getValue());
      if (interrupted) {
        // The worker has already closed the writer; the watchdog must still flush its pending
        // value.
        Thread.interrupted();
        attempt.markSuperseded();
        attempt.flushStateQuietly();
        verify(stateStore)
            .initState(eq(JOB_UUID), eq(JobProgressWriter.NAMESPACE), anyString(), eq(7L));
        verify(stateStore)
            .initState(eq(JOB_UUID), eq(JobCheckpointWriter.NAMESPACE), anyString(), eq(7L));
      }
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  void tokenlessAttemptCreatesNoCheckpointWriter() throws Exception {
    JobEntity job = createTestJob();
    initJobTaskWithDefaultStubs(job);
    jobTask.attach(
        new JobAttemptControl(JOB_UUID, FIXED_NOW.plusSeconds(30), 30, FIXED_NOW, 0, 0L, "node-1"));
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING);
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);
    AtomicReference<JobContext> context = new AtomicReference<>();
    when(resilienceStrategy.execute(anyString(), any(), any(Callable.class)))
        .thenAnswer(
            call -> {
              context.set(JobContext.current());
              return "done";
            });
    when(lifecycleFacade.completeSuccess(
            any(JobEntity.class),
            any(),
            any(),
            any(),
            any(),
            anyLong(),
            anyLong(),
            nullable(Integer.class)))
        .thenReturn(true);
    jobTask.call();
    Assertions.assertNotNull(context.get());
    context.get().progress(5, 5);
    Assertions.assertThrows(IllegalStateException.class, () -> context.get().checkpoint("unbound"));
    verify(jobStore, never()).capability(JobStateStore.class);
    verify(jobStateFlusher, never()).remove(any());
    verify(jobStateFlusher, never()).markDirty(any());
  }

  @Test
  @SuppressWarnings("unchecked")
  void missingStateCapabilityCreatesNoProgressWriter() throws Exception {
    JobEntity job = createTestJob();
    initJobTaskWithDefaultStubs(job);
    jobTask.attach(
        new JobAttemptControl(JOB_UUID, FIXED_NOW.plusSeconds(30), 30, FIXED_NOW, 0, 7L, "node-1"));
    when(jobStore.capability(JobStateStore.class)).thenReturn(Optional.empty());
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING);
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);
    when(resilienceStrategy.execute(anyString(), any(), any(Callable.class)))
        .thenAnswer(
            call -> {
              JobContext.current().progress(5, 5);
              return "done";
            });
    jobTask.call();
    verify(jobStateFlusher, never()).markDirty(any(JobProgressWriter.class));
    verify(jobStateFlusher, never()).remove(any(JobProgressWriter.class));
  }

  @Test
  @SuppressWarnings("unchecked")
  void cooperativeCheckpointFlushOwnsTimeoutBeforeTransition() throws Exception {
    JobTimeoutHandler timeout = mock(JobTimeoutHandler.class);
    JobTask task = newJobTaskWithTimeoutHandler(timeout);
    JobEntity job = createTestJob();
    initJobTaskWithDefaultStubs(task, job);
    JobAttemptControl attempt =
        new JobAttemptControl(JOB_UUID, FIXED_NOW.plusSeconds(30), 30, FIXED_NOW, 0, 7L, "node-1");
    task.attach(attempt);
    attempt.requestCancellation();
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING);
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);
    JobStateStore stateStore = pendingCheckpointStore();
    AtomicReference<Boolean> watchdogClaimed = new AtomicReference<>();
    when(stateStore.initState(eq(JOB_UUID), eq(JobCheckpointWriter.NAMESPACE), anyString(), eq(7L)))
        .thenAnswer(
            call -> {
              watchdogClaimed.set(attempt.claimTimeoutForWatchdog());
              return StateWriteResult.UPDATED;
            });
    when(resilienceStrategy.execute(anyString(), any(), any(Callable.class)))
        .thenAnswer(
            call -> {
              JobContext.current().checkpoint("pending");
              checkpointCommit.run();
              throw new CancellationRequestedException("stop");
            });
    task.call();
    ArgumentCaptor<JobCheckpointWriter> writer = ArgumentCaptor.forClass(JobCheckpointWriter.class);
    verify(jobStateFlusher).markDirty(writer.capture());
    InOrder order = inOrder(stateStore, timeout, jobStateFlusher);
    order
        .verify(stateStore)
        .initState(eq(JOB_UUID), eq(JobCheckpointWriter.NAMESPACE), anyString(), eq(7L));
    order.verify(timeout).processCooperativeTimeout(attempt);
    order.verify(jobStateFlusher).remove(writer.getValue());
    Assertions.assertEquals(
        Boolean.FALSE, watchdogClaimed.get(), "worker must already own timeout");
    verify(stateStore, times(1))
        .initState(eq(JOB_UUID), eq(JobCheckpointWriter.NAMESPACE), anyString(), eq(7L));
  }

  private JobStateStore pendingCheckpointStore() {
    JobStateStore stateStore = mock(JobStateStore.class);
    when(jobStore.capability(JobStateStore.class)).thenReturn(Optional.of(stateStore));
    lenient()
        .when(stateStore.getState(JOB_UUID, JobCheckpointWriter.NAMESPACE))
        .thenReturn(Optional.empty());
    lenient()
        .when(
            stateStore.initState(
                eq(JOB_UUID), eq(JobCheckpointWriter.NAMESPACE), anyString(), eq(7L)))
        .thenReturn(StateWriteResult.UPDATED);
    when(checkpointRegistrar.registerAfterCommit(any()))
        .thenAnswer(
            call -> {
              checkpointCommit = call.getArgument(0);
              return AfterCommitRegistrar.Result.REGISTERED;
            });
    return stateStore;
  }

  @Test
  void requestedCancellationUsesTimeoutTransition() throws Exception {
    runCooperativeScenario(true, false, attempt -> new CancellationRequestedException("stop"));
  }

  @Test
  void watchdogClaimedCancellationDefersTransition() throws Exception {
    runCooperativeScenario(true, true, attempt -> new CancellationRequestedException("stop"));
  }

  @Test
  void unrequestedCancellationIsOrdinaryFailure() throws Exception {
    runCooperativeScenario(false, false, attempt -> new CancellationRequestedException("stop"));
  }

  @Test
  void cancellationCreatedBeforeRequestIsOrdinaryFailure() throws Exception {
    runCooperativeScenario(
        false,
        false,
        attempt -> {
          CancellationRequestedException early = new CancellationRequestedException("stop");
          // The watchdog requests cancellation while the exception unwinds.
          attempt.requestCancellation();
          return early;
        });
  }

  @Test
  void wrappedCancellationCreatedBeforeRequestIsOrdinaryFailure() throws Exception {
    runCooperativeScenario(
        false,
        false,
        attempt -> {
          CancellationRequestedException early = new CancellationRequestedException("stop");
          attempt.requestCancellation();
          return new RuntimeException("wrapped after the request", early);
        });
  }

  @Test
  void normalReturnAfterRequestStillSucceedsAndContextExposesAttempt() throws Exception {
    runCooperativeScenario(true, false, null);
  }

  @Test
  void wrappedCancellationUsesTimeoutTransition() throws Exception {
    runCooperativeScenario(
        true, false, attempt -> new RuntimeException(new CancellationRequestedException("stop")));
  }

  @Test
  void cooperativeTransitionFailureHandsTimeoutBack() throws Exception {
    runCooperativeFailureScenario(false, false, false);
  }

  @Test
  void cooperativeExecutionSaveFailureHandsTimeoutBack() throws Exception {
    runCooperativeFailureScenario(true, false, false);
  }

  @Test
  void cooperativeExecutionSaveFailureRetriesSaveWhenWatchdogAlreadyPassed() throws Exception {
    runCooperativeFailureScenario(true, true, false);
  }

  @Test
  void repeatedCooperativeExecutionSaveFailureAfterWatchdogPassedDoesNotEscape() throws Exception {
    runCooperativeFailureScenario(true, true, true);
  }

  @Test
  void staleCancellationFromEarlierAttemptIsOrdinaryFailure() throws Exception {
    CancellationRequestedException stale = cancellationFromPreviousAttempt();
    runCooperativeScenario(true, false, attempt -> stale, false, false, false, false, false);
  }

  @Test
  void wrappedStaleCancellationFromEarlierAttemptIsOrdinaryFailure() throws Exception {
    CancellationRequestedException stale = cancellationFromPreviousAttempt();
    runCooperativeScenario(
        true,
        false,
        attempt -> new RuntimeException("wrapped", stale),
        false,
        false,
        false,
        false,
        false);
  }

  @Test
  @SuppressWarnings("unchecked")
  void executingContextExposesTokenAndReadsCurrentClaim() throws Exception {
    JobEntity job = createTestJob();
    job.setClaimSeq(7L);
    job.setPayload(
        new JobPayload(
            JobTaskTest.class.getName(),
            "cooperativePayload",
            "()Ljava/lang/String;",
            true,
            List.of(),
            null));
    JobAttemptControl attempt =
        new JobAttemptControl(JOB_UUID, FIXED_NOW.plusSeconds(30), 30, FIXED_NOW, 0, 7L, "node-1");
    initJobTaskWithDefaultStubs(job);
    jobTask.attach(attempt);
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING);
    when(jobStore.currentClaim(JOB_UUID))
        .thenReturn(Optional.of(new ClaimState(JobStatus.RUNNING, 7)));
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);
    when(resilienceStrategy.execute(anyString(), any(), any(Callable.class)))
        .thenAnswer(
            invocation -> {
              JobContext context = JobContext.current();
              Assertions.assertEquals(attempt.token(), context.attemptToken());
              Assertions.assertTrue(context.isCurrentAttempt());
              return invocation.<Callable<?>>getArgument(2).call();
            });
    when(lifecycleFacade.completeSuccess(
            any(), any(), any(), any(), any(), anyLong(), anyLong(), nullable(Integer.class)))
        .thenReturn(true);
    jobTask.call();
    verify(jobStore).currentClaim(JOB_UUID);
  }

  private CancellationRequestedException cancellationFromPreviousAttempt() {
    JobAttemptControl previous =
        new JobAttemptControl(JOB_UUID, FIXED_NOW.plusSeconds(30), 30, FIXED_NOW, 0, 0L, "node-1");
    previous.requestCancellation();
    JobContext.bind(
        JOB_UUID,
        null,
        Map.of(),
        null,
        null,
        previous.deadline(),
        previous::isCancellationRequested,
        previous.cancellationIdentity());
    try {
      CancellationRequestedException stale = new CancellationRequestedException("stop");
      Assertions.assertTrue(stale.isCancellationRequested());
      return stale;
    } finally {
      JobContext.clear();
    }
  }

  @Test
  void cooperativeFailureRetriesWhenWatchdogAlreadyPassed() throws Exception {
    runCooperativeFailureScenario(false, true, false);
  }

  @Test
  void repeatedCooperativeFailureAfterWatchdogPassedDoesNotEscape() throws Exception {
    runCooperativeFailureScenario(false, true, true);
  }

  @SuppressWarnings("unchecked")
  private void runCooperativeScenario(
      boolean requested, boolean claimed, Function<JobAttemptControl, RuntimeException> failure)
      throws Exception {
    runCooperativeScenario(requested, claimed, failure, false, false, false, false, requested);
  }

  private void runCooperativeFailureScenario(
      boolean saveFails, boolean watchdogPassed, boolean retryFails) throws Exception {
    runCooperativeScenario(
        true,
        false,
        attempt -> new CancellationRequestedException("stop"),
        true,
        saveFails,
        watchdogPassed,
        retryFails,
        true);
  }

  @SuppressWarnings("unchecked")
  private void runCooperativeScenario(
      boolean requested,
      boolean claimed,
      Function<JobAttemptControl, RuntimeException> failure,
      boolean handlingFails,
      boolean saveFails,
      boolean watchdogPassed,
      boolean retryFails,
      boolean expectCooperative)
      throws Exception {
    JobTimeoutHandler timeoutHandler = mock(JobTimeoutHandler.class);
    JobAttemptControl attempt =
        new JobAttemptControl(
            JOB_UUID,
            FIXED_NOW.plusSeconds(30),
            30,
            FIXED_NOW,
            expectCooperative ? 2 : 0,
            0L,
            "node-1");
    if (requested) {
      attempt.requestCancellation();
    }
    if (claimed) {
      Assertions.assertTrue(attempt.claimTimeoutForWatchdog());
    }
    JobTask task = newJobTaskWithTimeoutHandler(timeoutHandler);
    JobEntity job = createTestJob();
    if (expectCooperative) {
      job.setAttempts(2);
    }
    job.setPayload(
        new JobPayload(
            JobTaskTest.class.getName(),
            "cooperativePayload",
            "()Ljava/lang/String;",
            true,
            List.of(),
            null));
    initJobTaskWithDefaultStubs(task, job);
    task.attach(attempt);
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING);
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);
    when(resilienceStrategy.execute(anyString(), any(), any(Callable.class)))
        .thenAnswer(invocation -> invocation.<Callable<?>>getArgument(2).call());
    if (failure == null) {
      when(lifecycleFacade.completeSuccess(
              any(JobEntity.class),
              any(),
              any(),
              any(),
              any(),
              anyLong(),
              anyLong(),
              nullable(Integer.class)))
          .thenReturn(true);
    } else if (!expectCooperative) {
      when(jobStore.incrementRetryAttempt(JOB_UUID, 0L, 0)).thenReturn(1);
    }
    if (handlingFails) {
      if (saveFails) {
        AtomicInteger saves = new AtomicInteger();
        doAnswer(
                invocation -> {
                  Assertions.assertEquals(
                      ExecutionStatus.FAILED,
                      invocation.<JobExecutionEntity>getArgument(0).getStatus());
                  if (saves.incrementAndGet() == 1) {
                    if (watchdogPassed) {
                      Assertions.assertFalse(attempt.claimTimeoutForWatchdog());
                    }
                    throw new IllegalStateException("execution store down");
                  }
                  if (!watchdogPassed || retryFails) {
                    throw new IllegalStateException("execution store still down");
                  }
                  return null;
                })
            .when(observabilityFacade)
            .saveExecution(any(JobExecutionEntity.class));
      } else {
        AtomicInteger calls = new AtomicInteger();
        doAnswer(
                invocation -> {
                  Assertions.assertSame(attempt, invocation.getArgument(0));
                  Assertions.assertEquals(
                      2, invocation.<JobAttemptControl>getArgument(0).baselineAttempts());
                  if (calls.incrementAndGet() == 1) {
                    if (watchdogPassed) {
                      Assertions.assertFalse(attempt.claimTimeoutForWatchdog());
                    }
                    throw new IllegalStateException("timeout store down");
                  }
                  if (retryFails) {
                    throw new IllegalStateException("timeout store still down");
                  }
                  return null;
                })
            .when(timeoutHandler)
            .processCooperativeTimeout(attempt);
      }
    }
    COOPERATIVE_FAILURE.set(failure == null ? null : () -> failure.apply(attempt));
    try {
      Assertions.assertDoesNotThrow(task::call);
      JobContext observed = COOPERATIVE_CONTEXT.get();
      Assertions.assertNotNull(observed);
      Assertions.assertEquals(Optional.of(attempt.deadline()), observed.deadline());
      Assertions.assertEquals(
          attempt.isCancellationRequested(), observed.isCancellationRequested());
      if (failure != null && expectCooperative && claimed) {
        ArgumentCaptor<JobExecutionEntity> execution =
            ArgumentCaptor.forClass(JobExecutionEntity.class);
        verify(observabilityFacade).saveExecution(execution.capture());
        Assertions.assertEquals(ExecutionStatus.FAILED, execution.getValue().getStatus());
        Assertions.assertEquals(
            CancellationRequestedException.class.getName(), execution.getValue().getErrorClass());
        Assertions.assertEquals(
            CancellationRequestedException.class.getName(), execution.getValue().getErrorMessage());
      }
      if (failure != null && expectCooperative && !claimed) {
        verify(
                timeoutHandler,
                times(saveFails ? (watchdogPassed && !retryFails ? 1 : 0) : watchdogPassed ? 2 : 1))
            .processCooperativeTimeout(attempt);
        Assertions.assertEquals(2, attempt.baselineAttempts());
        Assertions.assertEquals(handlingFails && !watchdogPassed, attempt.isTimeoutHandedBack());
        ArgumentCaptor<JobExecutionEntity> execution =
            ArgumentCaptor.forClass(JobExecutionEntity.class);
        verify(observabilityFacade, times(saveFails && watchdogPassed ? 2 : 1))
            .saveExecution(execution.capture());
        for (JobExecutionEntity saved : execution.getAllValues()) {
          Assertions.assertEquals(ExecutionStatus.FAILED, saved.getStatus());
          // The mock sanitizer returns null, so history falls back to the class, never raw text.
          Assertions.assertEquals(saved.getErrorClass(), saved.getErrorMessage());
        }
      } else {
        verify(timeoutHandler, never()).processCooperativeTimeout(any());
      }
      if (failure != null && !expectCooperative) {
        verify(jobStore).incrementRetryAttempt(JOB_UUID, 0L, 0);
      } else {
        verify(jobStore, never()).incrementRetryAttempt(any(UUID.class), eq(0L), any());
      }
      if (expectCooperative) {
        verify(lifecycleFacade, never())
            .completeFailure(any(JobEntity.class), any(), anyBoolean(), nullable(Integer.class));
        verify(observabilityFacade, never()).publishEvent(any(JobDlqEvent.class));
      }
      if (failure == null) {
        verify(lifecycleFacade)
            .completeSuccess(
                eq(job), any(), any(), any(), any(), anyLong(), anyLong(), nullable(Integer.class));
      }
      attempt.requestCancellation();
      Assertions.assertTrue(observed.isCancellationRequested());
      Assertions.assertNull(JobContext.currentOrNull());
    } finally {
      COOPERATIVE_FAILURE.remove();
      COOPERATIVE_CONTEXT.remove();
    }
  }

  private final ClassPolicy classPolicy = className -> true;
  private Runnable checkpointCommit;

  @Mock private AfterCommitRegistrar checkpointRegistrar;
  @Mock private JobStateFlusher jobStateFlusher;
  @Mock private JobStore jobStore;
  @Mock private ResourcePermitService resourcePermitService;
  @Mock private PostExecutionHandler lifecycleFacade;
  @Mock private NodeIdentityProvider nodeIdProvider;
  @Mock private ExecutionObserver observabilityFacade;
  @Mock private PreExecutionValidator validationFacade;
  @Mock private BeanResolver beanResolver;
  @Mock private RetryPolicy retryPolicy;
  @Mock private ResilienceStrategy resilienceStrategy;
  @Mock private ErrorSanitizer errorSanitizer;
  @Mock private ClaimReschedules claimReschedules;

  private JobTask jobTask;

  private static final AtomicReference<CountDownLatch> BODY_ENTERED = new AtomicReference<>();

  /** A body that runs until the watchdog interrupts it. */
  public static String waitForTheWatchdog() throws InterruptedException {
    BODY_ENTERED.get().countDown();
    Thread.sleep(30_000);
    throw new AssertionError("the watchdog did not interrupt the body");
  }

  public static String testJobMethod() {
    return "done";
  }

  public static String captureSignalDecision() {
    OBSERVED_SIGNAL_DECISION.set(JobContext.current().signalPayload(SignalDecision.class));
    return "done";
  }

  public static String captureSignalString() {
    OBSERVED_SIGNAL_STRING.set(JobContext.current().signalPayload(String.class));
    return "done";
  }

  public static void failingCallback() {
    throw new IllegalStateException("callback boom");
  }

  public static void captureCustomArgument(CustomArgument argument) {
    OBSERVED_CUSTOM_ARGUMENT.set(argument);
  }

  @Test
  @SuppressWarnings("unchecked")
  void successCallbacksReceiveTheCurrentContextForInlineAndMethodReference() throws Exception {
    for (SerializableConsumer<JobContext> callback :
        List.<SerializableConsumer<JobContext>>of(
            ctx -> CallbackRecorder.success(ctx), CallbackRecorder::success)) {
      CallbackRecorder.reset();
      JobEntity job = createTestJob();
      job.setOnSuccessPayload(JobPayloadFactory.fromLambda(callback));
      initJobTaskWithDefaultStubs(job);
      when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING);
      when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);
      when(resilienceStrategy.execute(anyString(), any(), any(Callable.class)))
          .thenAnswer(invocation -> ((Callable<?>) invocation.getArgument(2)).call());
      when(lifecycleFacade.completeSuccess(
              any(JobEntity.class),
              any(),
              any(),
              any(),
              any(),
              anyLong(),
              anyLong(),
              nullable(Integer.class)))
          .thenReturn(true);

      jobTask.call();

      Assertions.assertNotNull(CallbackRecorder.context);
      Assertions.assertEquals(JOB_UUID, CallbackRecorder.context.jobId());
      Assertions.assertSame(CallbackRecorder.currentContext, CallbackRecorder.context);
      Assertions.assertNull(JobContext.currentOrNull());
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  void terminalFailureCallbacksReceiveTheOriginalExceptionForBothForms() throws Exception {
    for (SerializableBiConsumer<JobContext, Throwable> callback :
        List.<SerializableBiConsumer<JobContext, Throwable>>of(
            (ctx, err) -> CallbackRecorder.failure(ctx, err), CallbackRecorder::failure)) {
      CallbackRecorder.reset();
      JobEntity job = createTestJob();
      job.setMaxRetries(0);
      job.setOnFailurePayload(JobPayloadFactory.fromLambda(callback));
      initJobTaskWithDefaultStubs(job);
      when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING);
      when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);
      RuntimeException failure = new IllegalStateException("final attempt failed");
      doThrow(failure).when(resilienceStrategy).execute(anyString(), any(), any(Callable.class));
      when(validationFacade.shouldNotRetry(failure)).thenReturn(true);
      when(lifecycleFacade.completeFailure(
              eq(job), eq(JobStatus.RUNNING), eq(false), nullable(Integer.class)))
          .thenReturn(true);

      jobTask.call();

      Assertions.assertNotNull(CallbackRecorder.context);
      Assertions.assertEquals(JOB_UUID, CallbackRecorder.context.jobId());
      Assertions.assertSame(CallbackRecorder.currentContext, CallbackRecorder.context);
      Assertions.assertSame(failure, CallbackRecorder.failure);
      Assertions.assertNull(JobContext.currentOrNull());
    }
  }

  @Test
  void constructorRejectsNullClock() {
    Assertions.assertThrows(
        NullPointerException.class,
        () ->
            new JobTask(
                jobStore,
                resourcePermitService,
                lifecycleFacade,
                nodeIdProvider,
                observabilityFacade,
                validationFacade,
                new JobPayloadInvoker(beanResolver, classPolicy),
                new JobTransaction(),
                new JobSuccessFinalizer(lifecycleFacade, observabilityFacade),
                retryPolicy,
                resilienceStrategy,
                errorSanitizer,
                context -> null,
                null,
                null,
                null,
                mock(AfterCommitRegistrar.class),
                mock(JobStateFlusher.class),
                null,
                new HeldKeyReleases(jobStore, () -> holdReleaseScheduler),
                new PermitReleases(resourcePermitService, () -> permitReleaseScheduler, null),
                claimReschedules,
                outcomeRetries,
                null,
                null,
                RatchetOptions.builder().build()));
  }

  private static JobLogger noopLogger() {
    return new JobLogger() {
      @Override
      public void info(String message) {}

      @Override
      public void debug(String message) {}

      @Override
      public void warn(String message) {}

      @Override
      public void error(String message) {}

      @Override
      public void trace(String message) {}
    };
  }

  @BeforeEach
  void setUp() {
    lenient().when(jobStore.incrementRetryAttempt(any(), any(), any())).thenReturn(1);
    CallbackRecorder.reset();
    OBSERVED_SIGNAL_DECISION.remove();
    OBSERVED_CUSTOM_ARGUMENT.remove();
    JsonbTestPayloadSerializer serializer = new JsonbTestPayloadSerializer();
    unitTransactions = spy(new JobTransaction());
    jobTask =
        new JobTask(
            jobStore,
            resourcePermitService,
            lifecycleFacade,
            nodeIdProvider,
            observabilityFacade,
            validationFacade,
            new JobPayloadInvoker(beanResolver, classPolicy),
            unitTransactions,
            new JobSuccessFinalizer(lifecycleFacade, observabilityFacade),
            retryPolicy,
            resilienceStrategy,
            errorSanitizer,
            context -> new JBossLoggingJobLogger(context.jobId(), null),
            new DefaultResultPersistenceStrategy(RatchetOptions.defaults(), serializer, null),
            null,
            serializer,
            checkpointRegistrar,
            jobStateFlusher,
            null,
            new HeldKeyReleases(jobStore, () -> holdReleaseScheduler),
            new PermitReleases(resourcePermitService, () -> permitReleaseScheduler, null),
            claimReschedules,
            outcomeRetries,
            Clock.systemUTC(),
            null,
            RatchetOptions.defaults());
  }

  @AfterEach
  void tearDown() {
    CONTINUATION_PAYLOAD.remove();
    CallbackRecorder.reset();
    OBSERVED_SIGNAL_DECISION.remove();
    OBSERVED_SIGNAL_STRING.remove();
    OBSERVED_CUSTOM_ARGUMENT.remove();
    EncryptionHolder.disable();
  }

  @Test
  @SuppressWarnings("unchecked")
  void call_restoresCustomArgumentTypeAfterPersistenceRoundTrip() throws Exception {
    CustomArgument argument = new CustomArgument("invoice-42", 3);
    JobPayload scheduled =
        JobPayloadFactory.fromLambda(
            (SerializableCheckedRunnable) () -> captureCustomArgument(argument));
    JobPayloadConverter converter = new JobPayloadConverter();
    JobPayload reloaded =
        converter.convertToEntityAttribute(converter.convertToDatabaseColumn(scheduled));
    Assertions.assertInstanceOf(Map.class, reloaded.args().get(0));

    JobEntity job = createTestJob();
    job.setPayload(reloaded);
    initJobTaskWithDefaultStubs(job);
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING);
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);
    when(resilienceStrategy.execute(anyString(), any(), any(Callable.class)))
        .thenAnswer(invocation -> ((Callable<?>) invocation.getArgument(2)).call());
    when(lifecycleFacade.completeSuccess(
            any(JobEntity.class),
            any(),
            any(),
            any(),
            any(),
            anyLong(),
            anyLong(),
            nullable(Integer.class)))
        .thenReturn(true);

    jobTask.call();

    Assertions.assertEquals(argument, OBSERVED_CUSTOM_ARGUMENT.get());
  }

  @Test
  @SuppressWarnings("unchecked")
  void call_invokesResilienceStrategyExecute() throws Exception {
    JobEntity job = createTestJob();
    initJobTaskWithDefaultStubs(job);
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING);
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);
    when(resilienceStrategy.execute(anyString(), any(), any(Callable.class)))
        .thenAnswer(inv -> ((Callable<?>) inv.getArgument(2)).call());
    when(lifecycleFacade.completeSuccess(
            any(JobEntity.class),
            any(),
            any(),
            any(),
            any(),
            anyLong(),
            anyLong(),
            nullable(Integer.class)))
        .thenReturn(true);

    jobTask.call();

    verify(resilienceStrategy)
        .execute(
            eq(JobTaskTest.class.getSimpleName() + ".testJobMethod"), any(), any(Callable.class));
  }

  @Test
  @SuppressWarnings("unchecked")
  void call_failsJobWhenExecutionHistoryStartWriteFails() throws Exception {
    // The execution-history write (startExecution) is durable state, not a metric. When it fails
    // before the payload runs, the job must fail through normal handling — not escape to the
    // worker thread and strand the claimed job in RUNNING until orphan recovery.
    JobMdcContext.clear();
    JobEntity job = createTestJob();
    job.setMaxRetries(3);
    jobTask.init(job);
    when(nodeIdProvider.getNodeId()).thenReturn("node-1");
    RuntimeException auditError = new RuntimeException("audit store down");
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING);
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);
    when(observabilityFacade.startExecution(any(UUID.class), anyInt(), anyString()))
        .thenThrow(auditError);
    when(validationFacade.shouldNotRetry(auditError)).thenReturn(false);
    when(jobStore.incrementRetryAttempt(JOB_UUID, 0L, 0)).thenReturn(1);
    when(retryPolicy.shouldRetry(1, auditError)).thenReturn(false);
    when(lifecycleFacade.completeFailure(
            any(JobEntity.class), eq(JobStatus.RUNNING), eq(false), nullable(Integer.class)))
        .thenReturn(true);

    jobTask.call();

    // The payload never ran, and the job was failed cleanly through the terminal DLQ path.
    verify(resilienceStrategy, never()).execute(anyString(), any(), any(Callable.class));
    verify(lifecycleFacade)
        .completeFailure(eq(job), eq(JobStatus.RUNNING), eq(false), nullable(Integer.class));
    // No JobStartedEvent is published when the job fails before it begins executing.
    verify(observabilityFacade, never()).publishEvent(any(JobStartedEvent.class));
    Assertions.assertNull(
        MDC.get(JobMdcContext.MDC_JOB_ID),
        "MDC job id must be cleared after an execution-history write failure");
    Assertions.assertThrows(
        IllegalStateException.class,
        JobContext::current,
        "JobContext must be unbound after an execution-history write failure");
  }

  @Test
  @SuppressWarnings("unchecked")
  void call_passesMethodScopedServiceName() throws Exception {
    JobEntity job = createTestJob();
    initJobTaskWithDefaultStubs(job);
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING);
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);
    when(resilienceStrategy.execute(anyString(), any(), any(Callable.class)))
        .thenAnswer(inv -> ((Callable<?>) inv.getArgument(2)).call());
    when(lifecycleFacade.completeSuccess(
            any(JobEntity.class),
            any(),
            any(),
            any(),
            any(),
            anyLong(),
            anyLong(),
            nullable(Integer.class)))
        .thenReturn(true);

    jobTask.call();

    ArgumentCaptor<String> serviceNameCaptor = ArgumentCaptor.forClass(String.class);
    verify(resilienceStrategy).execute(serviceNameCaptor.capture(), any(), any(Callable.class));
    Assertions.assertEquals(
        JobTaskTest.class.getSimpleName() + ".testJobMethod", serviceNameCaptor.getValue());
  }

  @Test
  @SuppressWarnings("unchecked")
  void call_usesAnnotatedServiceNameWhenPresent() throws Exception {
    JobEntity job = createTestJob();
    job.setPayload(
        new JobPayload(
            AnnotatedJobTarget.class.getName(),
            "annotatedJobMethod",
            "()Ljava/lang/String;",
            true,
            List.of(),
            null));
    initJobTaskWithDefaultStubs(job);
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING);
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);
    when(resilienceStrategy.execute(anyString(), any(), any(Callable.class)))
        .thenAnswer(inv -> ((Callable<?>) inv.getArgument(2)).call());
    when(lifecycleFacade.completeSuccess(
            any(JobEntity.class),
            any(),
            any(),
            any(),
            any(),
            anyLong(),
            anyLong(),
            nullable(Integer.class)))
        .thenReturn(true);

    jobTask.call();

    ArgumentCaptor<CircuitBreakerExceptionFilter> filterCaptor =
        ArgumentCaptor.forClass(CircuitBreakerExceptionFilter.class);
    verify(resilienceStrategy)
        .execute(eq("external-api"), filterCaptor.capture(), any(Callable.class));
    Assertions.assertEquals(
        List.of(IllegalStateException.class), filterCaptor.getValue().recordExceptions());
    Assertions.assertEquals(
        List.of(IllegalArgumentException.class), filterCaptor.getValue().ignoreExceptions());
  }

  @Test
  void call_checksServiceAvailableBeforeExecution() throws Exception {
    JobEntity job = createTestJob();
    initJobTaskWithDefaultStubs(job);
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING);
    when(resilienceStrategy.isServiceAvailable(
            JobTaskTest.class.getSimpleName() + ".testJobMethod"))
        .thenReturn(true);
    when(resilienceStrategy.execute(anyString(), any(), any(Callable.class)))
        .thenAnswer(inv -> ((Callable<?>) inv.getArgument(2)).call());
    when(lifecycleFacade.completeSuccess(
            any(JobEntity.class),
            any(),
            any(),
            any(),
            any(),
            anyLong(),
            anyLong(),
            nullable(Integer.class)))
        .thenReturn(true);

    jobTask.call();

    verify(resilienceStrategy)
        .isServiceAvailable(JobTaskTest.class.getSimpleName() + ".testJobMethod");
  }

  @Test
  @SuppressWarnings("unchecked")
  void call_serviceUnavailable_skipsExecution() throws Exception {
    JobEntity job = createTestJob();
    initJobTaskWithDefaultStubs(job);
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING);
    when(resilienceStrategy.isServiceAvailable(
            JobTaskTest.class.getSimpleName() + ".testJobMethod"))
        .thenReturn(false);

    jobTask.call();

    verify(resilienceStrategy, never()).execute(anyString(), any(), any(Callable.class));
    verify(jobStore).releaseClaim(eq(JOB_UUID), anyString(), any(), anyInt(), eq(0L));
  }

  @Test
  @SuppressWarnings("unchecked")
  void call_executeOpenCircuitException_reschedulesWithoutCountingTaskFailure() throws Exception {
    JobEntity job = createTestJob();
    job.setAttempts(2);
    initJobTaskWithDefaultStubs(job);
    String serviceName = JobTaskTest.class.getSimpleName() + ".testJobMethod";
    CircuitBreakerOpenException rejection =
        new CircuitBreakerOpenException("Circuit breaker OPEN for service: " + serviceName);
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING);
    when(resilienceStrategy.isServiceAvailable(serviceName)).thenReturn(true);
    when(resilienceStrategy.execute(eq(serviceName), any(), any(Callable.class)))
        .thenThrow(rejection);
    when(resilienceStrategy.getRetryDelay(serviceName)).thenReturn(Duration.ofMillis(250));

    jobTask.call();

    verify(resilienceStrategy).getRetryDelay(serviceName);
    verify(jobStore)
        .releaseClaim(
            eq(JOB_UUID),
            eq("Circuit breaker OPEN for service: " + serviceName),
            any(),
            eq(2),
            eq(0L));
    verify(observabilityFacade).saveExecution(any(JobExecutionEntity.class));
    verify(jobStore, never()).incrementRetryAttempt(any(UUID.class), eq(0L), any());
    verify(retryPolicy, never()).shouldRetry(anyInt(), any());
    verify(lifecycleFacade, never()).moveToDlq(any(), any());
    verify(observabilityFacade, never()).recordJobFailure(any(), any(), anyInt());
  }

  @Test
  @SuppressWarnings("unchecked")
  void handleFailure_consultsRetryPolicy() throws Exception {
    JobEntity job = createTestJob();
    job.setMaxRetries(3);
    initJobTaskWithDefaultStubs(job);
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING);
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);

    RuntimeException error = new RuntimeException("boom");
    when(resilienceStrategy.execute(anyString(), any(), any(Callable.class))).thenThrow(error);
    when(validationFacade.shouldNotRetry(error)).thenReturn(false);
    when(jobStore.incrementRetryAttempt(JOB_UUID, 0L, 0)).thenReturn(1);
    when(retryPolicy.shouldRetry(1, error)).thenReturn(true);
    when(retryPolicy.getDelay(1)).thenReturn(Duration.ofSeconds(5));
    when(errorSanitizer.sanitize(error)).thenReturn("safe boom");
    when(jobStore.scheduleJobRetry(any(UUID.class), anyString(), any(), anyInt(), eq(0L)))
        .thenReturn(true);

    jobTask.call();

    verify(retryPolicy).shouldRetry(1, error);
    verify(jobStore, never()).getJobStatus(argThat(id -> !JOB_UUID.equals(id)));
    verify(errorSanitizer, times(1)).sanitize(error);
  }

  @Test
  void cancelledBatchChildDoesNotRetryFailure() throws Exception {
    assertBatchChildFailureRetry(JobStatus.CANCELED);
  }

  @Test
  void pendingBatchChildRetriesFailure() throws Exception {
    assertBatchChildFailureRetry(JobStatus.PENDING);
  }

  @Test
  void pausedBatchChildRetriesFailure() throws Exception {
    assertBatchChildFailureRetry(JobStatus.PAUSED);
  }

  @SuppressWarnings("unchecked")
  private void assertBatchChildFailureRetry(JobStatus parentStatus) throws Exception {
    UUID parentId = UUID.randomUUID();
    JobEntity job = createTestJob();
    job.setJobType(JobExecutionType.BATCH_CHILD);
    job.setDependsOn(parentId);
    job.setMaxRetries(3);
    initJobTaskWithDefaultStubs(job);
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING);
    when(jobStore.getJobStatus(parentId)).thenReturn(JobStatus.PENDING, parentStatus);
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);
    RuntimeException error = new RuntimeException("batch failure");
    when(errorSanitizer.sanitize(error)).thenReturn("batch failure");
    when(resilienceStrategy.execute(anyString(), any(), any(Callable.class))).thenThrow(error);
    when(validationFacade.shouldNotRetry(error)).thenReturn(false);
    when(jobStore.incrementRetryAttempt(JOB_UUID, 0L, 0)).thenReturn(1);
    when(retryPolicy.shouldRetry(1, error)).thenReturn(true);
    if (parentStatus != JobStatus.CANCELED) {
      when(retryPolicy.getDelay(1)).thenReturn(Duration.ofSeconds(5));
      when(jobStore.scheduleJobRetry(any(UUID.class), anyString(), any(), anyInt(), eq(0L)))
          .thenReturn(true);
    } else {
      when(lifecycleFacade.completeFailure(
              any(JobEntity.class), eq(JobStatus.RUNNING), eq(false), nullable(Integer.class)))
          .thenReturn(true);
    }

    jobTask.call();

    verify(jobStore, times(2)).getJobStatus(parentId);
    if (parentStatus != JobStatus.CANCELED) {
      verify(jobStore).scheduleJobRetry(eq(JOB_UUID), anyString(), any(), eq(1), eq(0L));
    } else {
      verify(jobStore, never()).scheduleJobRetry(any(), any(), any(), anyInt(), any());
      verify(lifecycleFacade)
          .completeFailure(eq(job), eq(JobStatus.RUNNING), eq(false), nullable(Integer.class));
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  void handleFailure_retryPolicyDenies_movesToFailed() throws Exception {
    JobEntity job = createTestJob();
    job.setMaxRetries(3);
    initJobTaskWithDefaultStubs(job);
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING);
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);

    RuntimeException error = new RuntimeException("permanent");
    when(resilienceStrategy.execute(anyString(), any(), any(Callable.class))).thenThrow(error);
    when(validationFacade.shouldNotRetry(error)).thenReturn(false);
    when(jobStore.incrementRetryAttempt(JOB_UUID, 0L, 0)).thenReturn(1);
    when(retryPolicy.shouldRetry(1, error)).thenReturn(false);
    when(lifecycleFacade.completeFailure(
            any(JobEntity.class), eq(JobStatus.RUNNING), eq(false), nullable(Integer.class)))
        .thenReturn(true);

    jobTask.call();

    verify(lifecycleFacade)
        .completeFailure(eq(job), eq(JobStatus.RUNNING), eq(false), nullable(Integer.class));
  }

  @Test
  void handleSuccess_releasedClaimClosesExecutionWithDiscardedResult() throws Exception {
    assertDiscardedSuccessExecution(false);
  }

  @Test
  void handleSuccess_discardedExecutionSaveFailureDoesNotEscape() throws Exception {
    assertDiscardedSuccessExecution(true);
  }

  @SuppressWarnings("unchecked")
  private void assertDiscardedSuccessExecution(boolean saveFails) throws Exception {
    JobEntity job = createTestJob();
    initJobTaskWithDefaultStubs(job);
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING);
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);
    when(resilienceStrategy.execute(anyString(), any(), any(Callable.class)))
        .thenAnswer(inv -> ((Callable<?>) inv.getArgument(2)).call());
    when(lifecycleFacade.completeSuccess(
            any(JobEntity.class),
            any(),
            any(),
            any(),
            any(),
            anyLong(),
            anyLong(),
            nullable(Integer.class)))
        .thenReturn(false);
    if (saveFails) {
      doThrow(new IllegalStateException("execution store down"))
          .when(observabilityFacade)
          .saveExecution(any(JobExecutionEntity.class));
    }

    Assertions.assertDoesNotThrow(jobTask::call);

    ArgumentCaptor<JobExecutionEntity> execution =
        ArgumentCaptor.forClass(JobExecutionEntity.class);
    verify(observabilityFacade).saveExecution(execution.capture());
    Assertions.assertEquals(ExecutionStatus.FAILED, execution.getValue().getStatus());
    Assertions.assertNull(execution.getValue().getErrorClass());
    Assertions.assertEquals(
        JobExecutionEntity.RESULT_DISCARDED_ERROR_MESSAGE, execution.getValue().getErrorMessage());
    verify(observabilityFacade, never()).recordJobSuccess(any(), anyLong());
    verify(jobStore, never()).incrementRetryAttempt(any(), anyLong(), any());
  }

  @Test
  void watchdogClaimedCheckpointFailureDefersOutcomeBeforeMarker() throws Exception {
    assertSupersededCheckpointFailure(false, false, false);
  }

  @Test
  void watchdogClaimedWrappedCheckpointFailureDefersOutcomeBeforeMarker() throws Exception {
    assertSupersededCheckpointFailure(true, false, false);
  }

  @Test
  void watchdogClaimedNonRetryableCheckpointFailureDefersOutcome() throws Exception {
    assertSupersededCheckpointFailure(false, true, false);
  }

  @Test
  void supersededExecutionSaveFailureDoesNotForceAnOutcome() throws Exception {
    assertSupersededCheckpointFailure(false, true, true);
  }

  @SuppressWarnings("unchecked")
  private void assertSupersededCheckpointFailure(
      boolean wrapped, boolean nonRetryable, boolean saveFails) throws Exception {
    CallbackRecorder.reset();
    JobEntity job = createTestJob();
    job.setStatus(JobStatus.RUNNING);
    job.setClaimSeq(7L);
    job.setOnFailurePayload(
        JobPayloadFactory.fromLambda(
            (SerializableBiConsumer<JobContext, Throwable>) CallbackRecorder::failure));
    JobTimeoutHandler timeout = mock(JobTimeoutHandler.class);
    JobTask task = newJobTaskWithTimeoutHandler(timeout);
    initJobTaskWithDefaultStubs(task, job);
    TracingCollector.ExecutionScope scope = mock(TracingCollector.ExecutionScope.class);
    when(observabilityFacade.startExecutionScope(job)).thenReturn(scope);
    JobAttemptControl attempt =
        new JobAttemptControl(JOB_UUID, FIXED_NOW.plusSeconds(30), 30, FIXED_NOW, 0, 7L, "node-1");
    task.attach(attempt);
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING);
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);
    JobStateStore stateStore = pendingCheckpointStore();
    when(resilienceStrategy.execute(anyString(), any(), any(Callable.class)))
        .thenAnswer(
            call -> {
              JobContext.current().checkpoint("committed");
              checkpointCommit.run();
              // Pause the watchdog after ownership is claimed, before cancellation and its marker.
              Assertions.assertTrue(attempt.claimTimeoutForWatchdog());
              AttemptSupersededException failure =
                  Assertions.assertThrows(
                      AttemptSupersededException.class,
                      () -> JobContext.current().checkpoint("rolled back"));
              if (wrapped) throw new RuntimeException("wrapped", failure);
              throw failure;
            });
    lenient().when(validationFacade.shouldNotRetry(any())).thenReturn(nonRetryable);
    lenient()
        .when(
            lifecycleFacade.completeFailure(
                eq(job), eq(JobStatus.RUNNING), eq(false), nullable(Integer.class)))
        .thenReturn(true);
    if (saveFails) {
      doThrow(new IllegalStateException("execution store down"))
          .when(observabilityFacade)
          .saveExecution(any(JobExecutionEntity.class));
    }

    Assertions.assertDoesNotThrow(task::call);

    ArgumentCaptor<JobExecutionEntity> execution =
        ArgumentCaptor.forClass(JobExecutionEntity.class);
    verify(observabilityFacade).saveExecution(execution.capture());
    Assertions.assertEquals(
        JobExecutionEntity.ExecutionStatus.FAILED, execution.getValue().getStatus());
    Assertions.assertNotNull(execution.getValue().getEndedAt());
    Assertions.assertEquals(
        (wrapped ? RuntimeException.class : AttemptSupersededException.class).getName(),
        execution.getValue().getErrorClass());
    verify(jobStore, never()).incrementRetryAttempt(any(), anyLong(), any());
    verify(jobStore, never()).scheduleJobRetry(any(), any(), any(), anyInt(), any());
    verify(lifecycleFacade, never())
        .completeFailure(any(), any(), anyBoolean(), nullable(Integer.class));
    verify(lifecycleFacade, never()).moveToDlq(any(), any());
    verify(observabilityFacade, never()).recordJobFailure(any(), any(), anyInt());
    verify(scope, never()).failure(anyString(), anyString(), anyInt());
    verify(observabilityFacade, never()).publishEvent(any(JobFailedEvent.class));
    verify(observabilityFacade, never()).publishEvent(any(JobDlqEvent.class));
    verify(observabilityFacade, never()).publishEvent(any(JobRetryingEvent.class));
    verify(timeout, never()).processCooperativeTimeout(any());
    Assertions.assertNull(CallbackRecorder.context);
    Assertions.assertEquals(JobStatus.RUNNING, job.getStatus());
    verify(stateStore, never()).initState(any(), anyString(), anyString(), anyLong());
    // The watchdog can still flush the committed value after the worker closes its writer.
    attempt.flushStateQuietly();
    verify(stateStore)
        .initState(eq(JOB_UUID), eq(JobCheckpointWriter.NAMESPACE), anyString(), eq(7L));
  }

  @Test
  @SuppressWarnings("unchecked")
  void supersededAttemptDefersOrdinaryFailureWithoutWatchdog() throws Exception {
    JobEntity job = createTestJob();
    initJobTaskWithDefaultStubs(job);
    JobAttemptControl attempt =
        new JobAttemptControl(JOB_UUID, FIXED_NOW.plusSeconds(30), 30, FIXED_NOW, 0, 7L, "node-1");
    jobTask.attach(attempt);
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING);
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);
    when(resilienceStrategy.execute(anyString(), any(), any(Callable.class)))
        .thenAnswer(
            call -> {
              attempt.markSuperseded();
              throw new IllegalStateException("claim lost");
            });

    jobTask.call();

    verify(jobStore, never()).incrementRetryAttempt(any(), anyLong(), any());
    verify(lifecycleFacade, never())
        .completeFailure(any(), any(), anyBoolean(), nullable(Integer.class));
    verify(observabilityFacade, never()).recordJobFailure(any(), any(), anyInt());
    ArgumentCaptor<JobExecutionEntity> execution =
        ArgumentCaptor.forClass(JobExecutionEntity.class);
    verify(observabilityFacade).saveExecution(execution.capture());
    Assertions.assertEquals(
        JobExecutionEntity.ExecutionStatus.FAILED, execution.getValue().getStatus());
  }

  @Test
  @SuppressWarnings("unchecked")
  void workerHoldDuringFailureHandlingPreventsWatchdogOutcome() throws Exception {
    JobEntity job = createTestJob();
    initJobTaskWithDefaultStubs(job);
    JobAttemptControl attempt =
        new JobAttemptControl(JOB_UUID, FIXED_NOW.plusSeconds(30), 30, FIXED_NOW, 0, 7L, "node-1");
    jobTask.attach(attempt);
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING);
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);
    RuntimeException failure = new IllegalStateException("payload failed");
    when(resilienceStrategy.execute(anyString(), any(), any(Callable.class))).thenThrow(failure);
    when(validationFacade.shouldNotRetry(failure))
        .thenAnswer(
            call -> {
              Assertions.assertFalse(attempt.claimTimeoutForWatchdog());
              throw new IllegalStateException("failure classification failed");
            });

    when(lifecycleFacade.completeFailure(
            eq(job), eq(JobStatus.RUNNING), eq(false), nullable(Integer.class)))
        .thenReturn(true);

    Assertions.assertDoesNotThrow(jobTask::call);

    verify(jobStore, never()).incrementRetryAttempt(any(), anyLong(), any());
    verify(lifecycleFacade)
        .completeFailure(eq(job), eq(JobStatus.RUNNING), eq(false), nullable(Integer.class));
    verify(observabilityFacade, never()).recordJobFailure(any(), any(), anyInt());
  }

  @Test
  void watchdogInterruptClosesExecutionWithHardTimeout() throws Exception {
    assertWatchdogInterruptExecution(false, false);
  }

  @Test
  void supersededWatchdogInterruptStillClosesExecutionWithHardTimeout() throws Exception {
    assertWatchdogInterruptExecution(false, true);
  }

  @Test
  void deferredExecutionSaveFailureDoesNotEscapeOrChangeDeferral() throws Exception {
    assertWatchdogInterruptExecution(true, false);
  }

  @SuppressWarnings("unchecked")
  private void assertWatchdogInterruptExecution(boolean saveFails, boolean superseded)
      throws Exception {
    JobEntity job = createTestJob();
    JobTimeoutHandler timeoutHandler = mock(JobTimeoutHandler.class);
    JobTask task = newJobTaskWithTimeoutHandler(timeoutHandler);
    initJobTaskWithDefaultStubs(task, job);
    JobAttemptControl attempt =
        new JobAttemptControl(JOB_UUID, FIXED_NOW.plusSeconds(30), 30, FIXED_NOW, 0, 0L, "node-1");
    task.attach(attempt);
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING);
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);
    when(resilienceStrategy.execute(anyString(), any(), any(Callable.class)))
        .thenAnswer(
            call -> {
              if (superseded) Assertions.assertTrue(attempt.claimTimeoutForWatchdog());
              throw new InterruptedException("watchdog interrupt");
            });
    when(timeoutHandler.isWatchdogCancelled(JOB_UUID)).thenReturn(true);
    when(errorSanitizer.sanitize(any(Throwable.class)))
        .thenAnswer(invocation -> invocation.<Throwable>getArgument(0).getMessage());
    if (saveFails) {
      doThrow(new IllegalStateException("execution store down"))
          .when(observabilityFacade)
          .saveExecution(any(JobExecutionEntity.class));
    }

    Assertions.assertDoesNotThrow(task::call);

    ArgumentCaptor<JobExecutionEntity> execution =
        ArgumentCaptor.forClass(JobExecutionEntity.class);
    verify(observabilityFacade).saveExecution(execution.capture());
    Assertions.assertEquals(ExecutionStatus.FAILED, execution.getValue().getStatus());
    Assertions.assertEquals(TimeoutException.class.getName(), execution.getValue().getErrorClass());
    Assertions.assertEquals("Hard timeout exceeded (30s)", execution.getValue().getErrorMessage());
    verify(jobStore, never()).incrementRetryAttempt(any(), anyLong(), any());
    verify(timeoutHandler, never()).processCooperativeTimeout(any());
    verify(lifecycleFacade, never())
        .completeFailure(any(), any(), anyBoolean(), nullable(Integer.class));
  }

  @Test
  @SuppressWarnings("unchecked")
  void hardTimeoutAndInterruptedWorker_consumeExactlyOneAttempt() throws Exception {
    // A hard timeout interrupts the worker. The watchdog (processHardTimeout) and the interrupted
    // worker (handleFailure) both run while the row is still RUNNING. Without coordination both
    // increment the attempt and one timeout burns two attempts, dead-lettering at half maxRetries.
    JobEntity job = createTestJob();
    job.setMaxRetries(3);
    job.setStatus(JobStatus.RUNNING);

    AtomicInteger attempts = new AtomicInteger(0);
    // Mirror the store: incrementRetryAttempt only matches a RUNNING/WAITING row. The watchdog's
    // reschedule moves the row off RUNNING, so a later increment returns -1.
    AtomicInteger running = new AtomicInteger(1);
    when(jobStore.incrementRetryAttempt(eq(JOB_UUID), eq(0L), nullable(Integer.class)))
        .thenAnswer(inv -> running.get() == 1 ? attempts.incrementAndGet() : -1);
    when(jobStore.findById(JOB_UUID)).thenReturn(Optional.of(job));
    when(lifecycleFacade.handleTimeoutTransition(any(), eq(false), any(Supplier.class)))
        .thenAnswer(
            invocation -> {
              Optional<?> terminalJob =
                  (Optional<?>) invocation.getArgument(2, Supplier.class).get();
              return terminalJob.isPresent();
            });

    JobTimeoutHandler timeoutHandler =
        new JobTimeoutHandler(
            jobStore,
            jobStore,
            jobStore,
            lifecycleFacade,
            80,
            60L,
            FIXED_CLOCK,
            null,
            null,
            null,
            JobTimeoutHandler.DEFAULT_SIGNAL_TIMEOUT_BATCH_SIZE);

    JobTask task = newJobTaskWithTimeoutHandler(timeoutHandler);
    initJobTaskWithDefaultStubs(task, job);
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING);
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);

    InterruptedException interrupt = new InterruptedException("cancelled by hard timeout");
    when(resilienceStrategy.execute(anyString(), any(), any(Callable.class))).thenThrow(interrupt);
    // The deferring worker never reaches shouldNotRetry, so keep this lenient.
    lenient().when(validationFacade.shouldNotRetry(interrupt)).thenReturn(false);

    // Reproduce the both-increment-before-either-CAS window: the worker's handleFailure runs while
    // the watchdog still holds the marker and the row is still RUNNING — i.e. exactly when the
    // watchdog is mid-reschedule. scheduleJobRetry runs the worker first, THEN flips the row off
    // RUNNING, just as the real CAS would. With the fix the worker defers (no second increment).
    when(jobStore.scheduleJobRetry(eq(JOB_UUID), any(), any(), anyInt(), eq(0L)))
        .thenAnswer(
            inv -> {
              task.call();
              running.set(0);
              return true;
            });

    FutureTask<Void> future = new FutureTask<>(() -> null);
    Method handleHard =
        JobTimeoutHandler.class.getDeclaredMethod(
            "handleHardTimeoutById",
            JobAttemptControl.class,
            Future.class,
            OutcomeWriteRetries.class);
    handleHard.setAccessible(true);
    handleHard.invoke(
        timeoutHandler,
        timeoutHandler.newAttempt(JOB_UUID, 30, FIXED_NOW, 0, 0L, "node-1"),
        future,
        null);

    Assertions.assertEquals(
        1, attempts.get(), "a single hard timeout must consume exactly one attempt");
    ArgumentCaptor<Integer> count = ArgumentCaptor.forClass(Integer.class);
    verify(jobStore, times(1)).incrementRetryAttempt(eq(JOB_UUID), any(), nullable(Integer.class));
    verify(jobStore).incrementRetryAttempt(eq(JOB_UUID), any(), count.capture());
    Assertions.assertEquals(0, count.getValue());
  }

  @Test
  @SuppressWarnings("unchecked")
  void normalInterruptedWorker_stillCountsOneAttempt() throws Exception {
    // A genuine, non-watchdog interrupt (no timeout marker) must still count as a failed attempt.
    JobEntity job = createTestJob();
    job.setMaxRetries(3);

    JobTimeoutHandler timeoutHandler =
        new JobTimeoutHandler(
            jobStore,
            jobStore,
            jobStore,
            lifecycleFacade,
            80,
            60L,
            FIXED_CLOCK,
            null,
            null,
            null,
            JobTimeoutHandler.DEFAULT_SIGNAL_TIMEOUT_BATCH_SIZE);

    JobTask task = newJobTaskWithTimeoutHandler(timeoutHandler);
    initJobTaskWithDefaultStubs(task, job);
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING);
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);

    InterruptedException interrupt = new InterruptedException("not a timeout");
    when(resilienceStrategy.execute(anyString(), any(), any(Callable.class))).thenThrow(interrupt);
    when(validationFacade.shouldNotRetry(interrupt)).thenReturn(false);
    when(jobStore.incrementRetryAttempt(JOB_UUID, 0L, 0)).thenReturn(1);
    when(retryPolicy.shouldRetry(1, interrupt)).thenReturn(false);
    when(lifecycleFacade.completeFailure(
            any(JobEntity.class), eq(JobStatus.RUNNING), eq(false), nullable(Integer.class)))
        .thenReturn(true);

    task.call();

    verify(jobStore, times(1)).incrementRetryAttempt(JOB_UUID, 0L, 0);
  }

  @Test
  void cancelledBlockingBody_restoresInterruptAfterFailureWrites() throws Exception {
    JobPayloadInvoker invoker = mock(JobPayloadInvoker.class);
    JobAttemptControl attempt = initInterruptibleBodyTask(invoker);
    CountDownLatch started = new CountDownLatch(1);
    CountDownLatch finished = new CountDownLatch(1);
    AtomicBoolean interruptedAfterReturn = new AtomicBoolean();
    AtomicReference<Throwable> workerFailure = new AtomicReference<>();
    AtomicReference<Boolean> interruptedDuringRetryWrite = new AtomicReference<>();
    AtomicReference<Boolean> interruptedDuringFailureWrite = new AtomicReference<>();
    when(invoker.invoke(any()))
        .thenAnswer(
            inv -> {
              started.countDown();
              Thread.sleep(30_000);
              throw new AssertionError("Body was not interrupted");
            });
    when(jobStore.incrementRetryAttempt(JOB_UUID, 0L, 0))
        .thenAnswer(
            inv -> {
              interruptedDuringRetryWrite.set(Thread.currentThread().isInterrupted());
              return 1;
            });
    when(lifecycleFacade.completeFailure(
            any(), eq(JobStatus.RUNNING), eq(false), nullable(Integer.class)))
        .thenAnswer(
            inv -> {
              interruptedDuringFailureWrite.set(Thread.currentThread().isInterrupted());
              return true;
            });

    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      Future<?> future =
          executor.submit(
              () -> {
                try {
                  jobTask.call();
                  interruptedAfterReturn.set(Thread.currentThread().isInterrupted());
                } catch (Throwable failure) {
                  workerFailure.set(failure);
                } finally {
                  Thread.interrupted();
                  finished.countDown();
                }
              });
      Assertions.assertTrue(started.await(5, TimeUnit.SECONDS));
      Assertions.assertTrue(attempt.cancelExecution(future));
      // A cancelled Future completes before the worker finishes cleanup.
      Assertions.assertTrue(finished.await(5, TimeUnit.SECONDS));
      Assertions.assertNull(workerFailure.get());
      Assertions.assertEquals(Boolean.FALSE, interruptedDuringRetryWrite.get());
      Assertions.assertEquals(Boolean.FALSE, interruptedDuringFailureWrite.get());
      Assertions.assertTrue(interruptedAfterReturn.get());
      verify(observabilityFacade).recordJobFailure(any(), any(InterruptedException.class), eq(1));
    } finally {
      executor.shutdownNow();
      Assertions.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  @Test
  void wrappedBodyInterruption_restoresInterruptWithoutThreadFlag() throws Exception {
    JobPayloadInvoker invoker = mock(JobPayloadInvoker.class);
    initInterruptibleBodyTask(invoker);
    RuntimeException failure = new RuntimeException(new InterruptedException("body interrupted"));
    when(invoker.invoke(any()))
        .thenAnswer(
            inv -> {
              Assertions.assertFalse(Thread.currentThread().isInterrupted());
              throw failure;
            });

    try {
      jobTask.call();
      Assertions.assertTrue(Thread.currentThread().isInterrupted());
      verify(observabilityFacade).recordJobFailure(any(), eq(failure), eq(1));
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  void thrownBodyInterruption_flushesCheckpointBeforeFailureWrites() throws Exception {
    assertBodyInterruptionFlush(false);
  }

  @Test
  void bodyLeavingInterruptFlagSet_skipsCheckpointFlush() throws Exception {
    assertBodyInterruptionFlush(true);
  }

  private void assertBodyInterruptionFlush(boolean flagSet) throws Exception {
    JobPayloadInvoker invoker = mock(JobPayloadInvoker.class);
    // Checkpoints need a serializer, the shared commit and flush mocks, and a claimed attempt.
    initInterruptibleBodyTask(
        invoker,
        callbackTask(
            invoker, new JsonbTestPayloadSerializer(), checkpointRegistrar, jobStateFlusher));
    jobTask.attach(
        new JobAttemptControl(JOB_UUID, FIXED_NOW.plusSeconds(30), 30, FIXED_NOW, 0, 7L, "node-1"));
    JobStateStore stateStore = pendingCheckpointStore();
    when(invoker.invoke(any()))
        .thenAnswer(
            inv -> {
              JobContext.current().checkpoint("committed");
              checkpointCommit.run();
              if (flagSet) Thread.currentThread().interrupt();
              // Thread.sleep and similar calls clear the flag before they throw.
              throw new InterruptedException("body interrupted");
            });

    try {
      jobTask.call();
      Assertions.assertTrue(Thread.currentThread().isInterrupted());
      if (flagSet) {
        verify(stateStore, never()).initState(any(), anyString(), anyString(), anyLong());
        verify(jobStore).incrementRetryAttempt(JOB_UUID, 0L, 0);
      } else {
        InOrder order = inOrder(stateStore, jobStore);
        order
            .verify(stateStore)
            .initState(eq(JOB_UUID), eq(JobCheckpointWriter.NAMESPACE), anyString(), eq(7L));
        order.verify(jobStore).incrementRetryAttempt(JOB_UUID, 0L, 0);
      }
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  void cancelledBeforeBodyStarts_doesNotRestoreInterrupt() throws Exception {
    JobPayloadInvoker invoker = mock(JobPayloadInvoker.class);
    JobAttemptControl attempt = initInterruptibleBodyTask(invoker);
    FutureTask<Void> future = new FutureTask<>(() -> null);
    Assertions.assertTrue(attempt.cancelExecution(future));

    try {
      jobTask.call();
      Assertions.assertFalse(Thread.currentThread().isInterrupted());
      verify(invoker, never()).invoke(any());
      verify(observabilityFacade)
          .recordJobFailure(
              any(),
              argThat(
                  failure ->
                      failure instanceof InterruptedException
                          && "Attempt cancelled before its body started"
                              .equals(failure.getMessage())),
              eq(1));
    } finally {
      Thread.interrupted();
    }
  }

  @SuppressWarnings("unchecked")
  private JobAttemptControl initInterruptibleBodyTask(JobPayloadInvoker invoker) throws Exception {
    return initInterruptibleBodyTask(invoker, callbackTask(invoker));
  }

  private JobAttemptControl initInterruptibleBodyTask(JobPayloadInvoker invoker, JobTask task)
      throws Exception {
    jobTask = task;
    JobEntity job = createTestJob();
    job.setMaxRetries(3);
    initJobTaskWithDefaultStubs(job);
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING);
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);
    when(invoker.materializeArguments(any(), any())).thenAnswer(inv -> inv.getArgument(0));
    when(resilienceStrategy.execute(anyString(), any(), any(Callable.class)))
        .thenAnswer(inv -> ((Callable<?>) inv.getArgument(2)).call());
    when(jobStore.incrementRetryAttempt(JOB_UUID, 0L, 0)).thenReturn(1);
    JobAttemptControl attempt =
        new JobAttemptControl(JOB_UUID, FIXED_NOW.plusSeconds(30), 30, FIXED_NOW, 0, 0L, "node-1");
    jobTask.attach(attempt);
    return attempt;
  }

  @Test
  void unfilledBatchResultSlotFailsPermanentlyBeforeInvocation() throws Exception {
    JobEntity job = createTestJob();
    JobPayload original = job.getPayload();
    job.setMaxRetries(10);
    job.setJobType(JobExecutionType.WORKFLOW_BRANCH);
    job.setPayload(
        new JobPayload(
            original.target(),
            original.method(),
            original.methodDescriptor(),
            original.isStatic(),
            Arrays.asList((Object) null),
            List.of(0)));
    initJobTaskWithDefaultStubs(job);
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING);
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);
    when(lifecycleFacade.moveToDlqAndHandlePermanentFailure(any(), any(), nullable(Integer.class)))
        .thenReturn(true);
    jobTask.call();
    ArgumentCaptor<Throwable> failure = ArgumentCaptor.forClass(Throwable.class);
    verify(lifecycleFacade)
        .moveToDlqAndHandlePermanentFailure(eq(job), failure.capture(), nullable(Integer.class));
    Assertions.assertEquals(
        "Batch success callback was never given the batch result", failure.getValue().getMessage());
    ArgumentCaptor<JobExecutionEntity> execution =
        ArgumentCaptor.forClass(JobExecutionEntity.class);
    verify(observabilityFacade).saveExecution(execution.capture());
    Assertions.assertEquals(ExecutionStatus.FAILED, execution.getValue().getStatus());
    verify(resilienceStrategy, never()).execute(anyString(), any(), any(Callable.class));
    verify(jobStore, never()).scheduleJobRetry(any(), any(), any(), anyInt(), any());
  }

  @Test
  void call_hydrationDecryptPoison_delegatesAtomicTerminalHandlingWithoutRetry() {
    // A claimed RUNNING job whose payload fails to decrypt during hydration is poison: the
    // ciphertext cannot be recovered by re-running. It must be dead-lettered, not swallowed and
    // left to stall until lease recovery (the regression this guards).
    JobClaimDto claim =
        new JobClaimDto(
            JOB_UUID,
            JobStatus.RUNNING,
            JobExecutionType.SINGLE,
            JobPriority.HIGH,
            Instant.EPOCH,
            1,
            30,
            "node-1",
            Instant.EPOCH,
            "poisoned-job",
            2,
            3,
            null,
            null,
            0L);
    jobTask.initFromClaim(claim);
    when(jobStore.findById(JOB_UUID))
        .thenThrow(new PayloadDecryptionException("ciphertext failed authentication"));
    when(lifecycleFacade.moveToDlqAndHandlePermanentFailure(
            any(JobEntity.class), any(PayloadDecryptionException.class), eq(2)))
        .thenReturn(true);

    jobTask.call();

    ArgumentCaptor<JobEntity> dlqJob = ArgumentCaptor.forClass(JobEntity.class);
    verify(lifecycleFacade)
        .moveToDlqAndHandlePermanentFailure(
            dlqJob.capture(), any(PayloadDecryptionException.class), eq(2));
    Assertions.assertEquals("poisoned-job", dlqJob.getValue().getBusinessKey());
    Assertions.assertEquals(JobExecutionType.SINGLE, dlqJob.getValue().getJobType());
    Assertions.assertEquals(JobPriority.HIGH, dlqJob.getValue().getPriority());
    Assertions.assertEquals("node-1", dlqJob.getValue().getPickedBy());
    Assertions.assertEquals(2, dlqJob.getValue().getAttempts());
    Assertions.assertEquals(3, dlqJob.getValue().getMaxRetries());
    Assertions.assertEquals(JobStatus.RUNNING, dlqJob.getValue().getStatus());
    verify(lifecycleFacade, never())
        .completeFailure(any(JobEntity.class), eq(JobStatus.RUNNING), eq(false), eq(2));
    verify(observabilityFacade, never()).publishEvent(any(JobFailedEvent.class));
    verify(observabilityFacade, never()).publishEvent(any(JobDlqEvent.class));
    // Non-retryable: never rescheduled, never increments the attempt counter.
    verify(jobStore, never())
        .scheduleJobRetry(any(UUID.class), anyString(), any(), anyInt(), eq(0L));
    verify(jobStore, never()).releaseClaim(any(UUID.class), anyString(), any(), anyInt(), eq(0L));
    verify(jobStore, never()).incrementRetryAttempt(any(UUID.class), eq(0L), any());
  }

  @Test
  void call_hydrationDecryptPoison_lostTransitionRacePublishesNoTerminalEvents() {
    JobClaimDto claim = claimForNode("node-1");
    jobTask.initFromClaim(claim);
    when(jobStore.findById(JOB_UUID))
        .thenThrow(new PayloadDecryptionException("ciphertext failed authentication"));

    jobTask.call();

    verify(lifecycleFacade)
        .moveToDlqAndHandlePermanentFailure(
            any(JobEntity.class), any(PayloadDecryptionException.class), nullable(Integer.class));
    verify(lifecycleFacade, never())
        .completeFailure(
            any(JobEntity.class), eq(JobStatus.RUNNING), eq(false), nullable(Integer.class));
    verify(observabilityFacade, never()).publishEvent(any(JobFailedEvent.class));
    verify(observabilityFacade, never()).publishEvent(any(JobDlqEvent.class));
  }

  @Test
  void call_hydrationDecryptPoison_updatesBatchChildFailureProgress() {
    UUID parentId = UUID.randomUUID();
    JobClaimDto claim =
        new JobClaimDto(
            JOB_UUID,
            JobStatus.RUNNING,
            JobExecutionType.BATCH_CHILD,
            JobPriority.NORMAL,
            Instant.EPOCH,
            1,
            30,
            "node-1",
            Instant.EPOCH,
            null,
            0,
            0,
            null,
            parentId,
            0L);
    jobTask.initFromClaim(claim);
    when(jobStore.findById(JOB_UUID))
        .thenThrow(new PayloadDecryptionException("ciphertext failed authentication"));
    when(lifecycleFacade.moveToDlqAndHandlePermanentFailure(
            any(JobEntity.class), any(PayloadDecryptionException.class), nullable(Integer.class)))
        .thenReturn(true);

    jobTask.call();

    ArgumentCaptor<JobEntity> failedChild = ArgumentCaptor.forClass(JobEntity.class);
    verify(lifecycleFacade)
        .moveToDlqAndHandlePermanentFailure(
            failedChild.capture(), any(PayloadDecryptionException.class), nullable(Integer.class));
    Assertions.assertEquals(JOB_UUID, failedChild.getValue().getId());
    Assertions.assertEquals(parentId, failedChild.getValue().getDependsOn());
  }

  @Test
  void call_hydrationDecryptPoison_compositeFailureLeavesRecoveryToStaleRunningPath() {
    JobClaimDto claim = claimForNode("node-1");
    jobTask.initFromClaim(claim);
    when(jobStore.findById(JOB_UUID))
        .thenThrow(new PayloadDecryptionException("ciphertext failed authentication"));
    doThrow(new IllegalStateException("batch store unavailable"))
        .when(lifecycleFacade)
        .moveToDlqAndHandlePermanentFailure(
            any(JobEntity.class), any(PayloadDecryptionException.class), nullable(Integer.class));

    jobTask.call();

    verify(lifecycleFacade, never())
        .completeFailure(
            any(JobEntity.class), eq(JobStatus.RUNNING), eq(false), nullable(Integer.class));
    verify(jobStore, never())
        .scheduleJobRetry(any(UUID.class), anyString(), any(), anyInt(), eq(0L));
    verify(jobStore, never()).releaseClaim(any(UUID.class), anyString(), any(), anyInt(), eq(0L));
    verify(observabilityFacade, never()).publishEvent(any(JobFailedEvent.class));
    verify(observabilityFacade, never()).publishEvent(any(JobDlqEvent.class));
  }

  @Test
  void call_hydrationFutureEnvelopeVersion_requeuesForUpgradeNotDlq() {
    // A claimed RUNNING job whose payload was written by a newer Ratchet (an envelope version this
    // node cannot read yet) is valid data, not poison. It must be released back to the pool with
    // backoff for an already-upgraded peer — never dead-lettered, never left stuck RUNNING.
    JobClaimDto claim = claimForNode("node-1");
    jobTask.initFromClaim(claim);
    UnsupportedEnvelopeVersionException skew = new UnsupportedEnvelopeVersionException(2, 1);
    when(jobStore.findById(JOB_UUID)).thenThrow(skew);
    when(errorSanitizer.sanitize(skew)).thenReturn("sanitized skew");
    when(jobStore.releaseClaim(any(UUID.class), anyString(), any(), anyInt(), eq(0L)))
        .thenReturn(true);

    jobTask.call();

    // Released for an upgraded peer (attempt count preserved), with a skew metric.
    verify(jobStore)
        .releaseClaim(eq(JOB_UUID), eq("sanitized skew"), any(), eq(claim.attempts()), eq(0L));
    verify(observabilityFacade).recordEnvelopeVersionSkew(JOB_UUID, 2, 1);
    // Not poison: never dead-lettered.
    verify(lifecycleFacade, never())
        .completeFailure(
            any(JobEntity.class), eq(JobStatus.RUNNING), eq(false), nullable(Integer.class));
    verify(observabilityFacade, never()).publishEvent(any(JobDlqEvent.class));
  }

  @Test
  void requeueForUpgrade_entityInitPath_preservesPersistedAttempts() throws Exception {
    // A task initialized via the entity path (buffered-entity resubmission) has claim == null but
    // job != null. requeueForUpgrade must release the job with its persisted attempt count, not 0 —
    // an upgrade requeue is not a failed attempt and must never reset the count.
    JobEntity job = createTestJob();
    job.setAttempts(2);
    jobTask.init(job);
    when(jobStore.releaseClaim(any(UUID.class), any(), any(), anyInt(), eq(0L))).thenReturn(true);

    Method requeue =
        JobTask.class.getDeclaredMethod(
            "requeueForUpgrade", UUID.class, UnsupportedEnvelopeVersionException.class);
    requeue.setAccessible(true);
    requeue.invoke(jobTask, JOB_UUID, new UnsupportedEnvelopeVersionException(2, 1));

    verify(jobStore).releaseClaim(eq(JOB_UUID), any(), any(), eq(2), eq(0L));
  }

  @Test
  void call_signalDecryptTransientKeyOutage_retriesInsteadOfDlq() {
    // A transient key-provider outage during signal-payload decrypt must stay retryable, not be
    // flattened into a non-retryable IllegalArgumentException that dead-letters a recoverable job.
    EncryptionTestKit.install(false);
    String framedSignal =
        PayloadEncryptor.encryptValue("\"payload\"", true, EncryptionTarget.signal("sig-key"));
    EncryptionHolder.install(
        List.of(new EncryptionTestKit.AesGcmEngine()),
        EncryptionTestKit.ALGORITHM_ID,
        new TransientKeyProvider(),
        false);

    JobEntity job = createTestJob();
    job.setSignalKey("sig-key");
    job.setSignalPayload(framedSignal);
    jobTask.init(job);
    // Exercise the real do-not-retry classification, not the mocked facade's default.
    DoNotRetryPolicy realPolicy = new DoNotRetryPolicy();
    when(validationFacade.shouldNotRetry(any()))
        .thenAnswer(inv -> realPolicy.shouldNotRetry(inv.getArgument(0)));
    when(jobStore.incrementRetryAttempt(JOB_UUID, 0L, 0)).thenReturn(1);
    when(retryPolicy.shouldRetry(eq(1), any())).thenReturn(true);
    when(retryPolicy.getDelay(1)).thenReturn(Duration.ofSeconds(5));
    when(errorSanitizer.sanitize(any())).thenReturn("safe transient failure");
    when(jobStore.scheduleJobRetry(any(UUID.class), anyString(), any(), anyInt(), eq(0L)))
        .thenReturn(true);

    jobTask.call();

    // Transient outage is retryable: the job is rescheduled, never dead-lettered.
    verify(jobStore).scheduleJobRetry(any(UUID.class), anyString(), any(), anyInt(), eq(0L));
    verify(lifecycleFacade, never()).moveToDlq(any(), any());
  }

  /** A key provider that is transiently unreachable on every lookup. */
  private static final class TransientKeyProvider implements KeyProvider {
    @Override
    public EncryptionKey currentKey() {
      throw new KeyProviderUnavailableException("KMS unreachable");
    }

    @Override
    public EncryptionKey keyById(String keyId) {
      throw new KeyProviderUnavailableException("KMS unreachable");
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  void handleFailure_nonRetryable_movesToFailedWithoutIncrementingRetryAttempt() throws Exception {
    JobEntity job = createTestJob();
    job.setAttempts(2);
    job.setMaxRetries(3);
    initJobTaskWithDefaultStubs(job);
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING);
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);

    RuntimeException error = new RuntimeException("do not retry");
    when(resilienceStrategy.execute(anyString(), any(), any(Callable.class))).thenThrow(error);
    when(validationFacade.shouldNotRetry(error)).thenReturn(true);
    when(errorSanitizer.sanitize(error)).thenReturn("safe do not retry");
    when(lifecycleFacade.completeFailure(
            any(JobEntity.class), eq(JobStatus.RUNNING), eq(false), nullable(Integer.class)))
        .thenReturn(true);

    jobTask.call();

    verify(jobStore, never()).incrementRetryAttempt(any(UUID.class), eq(0L), any());
    verify(retryPolicy, never()).shouldRetry(anyInt(), any());
    verify(jobStore, never())
        .scheduleJobRetry(any(UUID.class), anyString(), any(), anyInt(), eq(0L));
    verify(jobStore, never()).releaseClaim(any(UUID.class), anyString(), any(), anyInt(), eq(0L));
    // The stored count stays at 2; observers see the third attempt, the one that failed.
    verify(observabilityFacade).recordJobFailure(job, error, 3);
    Assertions.assertEquals(2, job.getAttempts());
    verify(errorSanitizer, times(1)).sanitize(error);
    Assertions.assertEquals("safe do not retry", job.getLastError());
    verify(lifecycleFacade)
        .completeFailure(eq(job), eq(JobStatus.RUNNING), eq(false), nullable(Integer.class));
    verify(lifecycleFacade)
        .completeFailure(eq(job), eq(JobStatus.RUNNING), eq(false), nullable(Integer.class));
  }

  @Test
  @SuppressWarnings("unchecked")
  void workerFailureLosingTerminalCasDoesNotInvokeOnFailure() throws Exception {
    JobPayloadInvoker payloadInvoker = mock(JobPayloadInvoker.class);
    when(payloadInvoker.resilienceTarget(any()))
        .thenReturn(
            new JobPayloadInvoker.ResilienceTarget(
                "worker", CircuitBreakerExceptionFilter.RECORD_ALL, false));
    JobTask jobTask =
        new JobTask(
            jobStore,
            resourcePermitService,
            lifecycleFacade,
            nodeIdProvider,
            observabilityFacade,
            validationFacade,
            payloadInvoker,
            new JobTransaction(),
            new JobSuccessFinalizer(lifecycleFacade, observabilityFacade),
            retryPolicy,
            resilienceStrategy,
            errorSanitizer,
            context -> noopLogger(),
            (jobId, result) -> SerializedJobResult.empty(),
            null,
            null,
            mock(AfterCommitRegistrar.class),
            mock(JobStateFlusher.class),
            null,
            new HeldKeyReleases(jobStore, () -> holdReleaseScheduler),
            new PermitReleases(resourcePermitService, () -> permitReleaseScheduler, null),
            claimReschedules,
            outcomeRetries,
            FIXED_CLOCK,
            null,
            RatchetOptions.defaults());
    JobEntity job = createTestJob();
    JobPayload callback =
        new JobPayload(getClass().getName(), "onFailure", "()V", true, List.of(), null);
    job.setOnFailurePayload(callback);
    initJobTaskWithDefaultStubs(jobTask, job);
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING);
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);
    RuntimeException error = new RuntimeException("worker failed after timeout");
    when(resilienceStrategy.execute(anyString(), any(), any(Callable.class))).thenThrow(error);
    when(validationFacade.shouldNotRetry(error)).thenReturn(true);
    when(lifecycleFacade.completeFailure(
            eq(job), eq(JobStatus.RUNNING), eq(false), nullable(Integer.class)))
        .thenReturn(false);

    jobTask.call();

    verify(lifecycleFacade)
        .completeFailure(eq(job), eq(JobStatus.RUNNING), eq(false), nullable(Integer.class));
    verify(payloadInvoker, never()).invoke(callback);
  }

  private JobTask callbackTask(JobPayloadInvoker invoker) {
    return callbackTask(
        invoker, null, mock(AfterCommitRegistrar.class), mock(JobStateFlusher.class));
  }

  private JobTask callbackTask(
      JobPayloadInvoker invoker,
      PayloadSerializer serializer,
      AfterCommitRegistrar registrar,
      JobStateFlusher flusher) {
    // Hydration/signal poison exits before service-name lookup.
    lenient()
        .when(invoker.resilienceTarget(any()))
        .thenReturn(
            new JobPayloadInvoker.ResilienceTarget(
                "Target", CircuitBreakerExceptionFilter.RECORD_ALL, false));
    return new JobTask(
        jobStore,
        resourcePermitService,
        lifecycleFacade,
        nodeIdProvider,
        observabilityFacade,
        validationFacade,
        invoker,
        new JobTransaction(),
        new JobSuccessFinalizer(lifecycleFacade, observabilityFacade),
        retryPolicy,
        resilienceStrategy,
        errorSanitizer,
        context -> noopLogger(),
        (jobId, result) -> SerializedJobResult.empty(),
        null,
        serializer,
        registrar,
        flusher,
        null,
        new HeldKeyReleases(jobStore, () -> holdReleaseScheduler),
        new PermitReleases(resourcePermitService, () -> permitReleaseScheduler, null),
        claimReschedules,
        outcomeRetries,
        FIXED_CLOCK,
        null,
        RatchetOptions.defaults());
  }

  private JobEntity jobWithFailureCallback() {
    JobEntity job = createTestJob();
    job.setOnFailurePayload(
        new JobPayload(
            "Target",
            "onFailure",
            "(Lrun/ratchet/api/JobContext;Ljava/lang/Throwable;)V",
            true,
            Arrays.asList(null, null),
            List.of(0, 1)));
    return job;
  }

  @SuppressWarnings("unchecked")
  private void stubWorkerFailure(JobTask task, JobEntity job, RuntimeException error)
      throws Exception {
    initJobTaskWithDefaultStubs(task, job);
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING);
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);
    when(resilienceStrategy.execute(anyString(), any(), any(Callable.class))).thenThrow(error);
    when(validationFacade.shouldNotRetry(error)).thenReturn(true);
  }

  @Test
  void lastResortFailureRunsOnFailureWithOriginalException() throws Exception {
    JobPayloadInvoker invoker = mock(JobPayloadInvoker.class);
    JobTask task = callbackTask(invoker);
    JobEntity job = jobWithFailureCallback();
    RuntimeException original = new RuntimeException("original");
    stubWorkerFailure(task, job, original);
    // The failure metric is recorded after the failure write, so break the handler before it.
    when(validationFacade.shouldNotRetry(original))
        .thenThrow(new IllegalStateException("validation failed"));
    when(lifecycleFacade.completeFailure(
            eq(job), eq(JobStatus.RUNNING), eq(false), nullable(Integer.class)))
        .thenReturn(true);
    when(invoker.materializeArguments(any(), any())).thenAnswer(inv -> inv.getArgument(0));

    task.call();

    ArgumentCaptor<JobPayload> invoked = ArgumentCaptor.forClass(JobPayload.class);
    verify(invoker).invoke(invoked.capture());
    Assertions.assertSame(original, invoked.getValue().args().get(1));
    Assertions.assertNotNull(invoked.getValue().args().get(0));
    Assertions.assertEquals(JobStatus.FAILED, job.getStatus());
  }

  @Test
  void lastResortLostTransitionSkipsOnFailure() throws Exception {
    JobPayloadInvoker invoker = mock(JobPayloadInvoker.class);
    JobTask task = callbackTask(invoker);
    JobEntity job = jobWithFailureCallback();
    RuntimeException original = new RuntimeException("original");
    stubWorkerFailure(task, job, original);
    // The failure metric is recorded after the failure write, so break the handler before it.
    when(validationFacade.shouldNotRetry(original))
        .thenThrow(new IllegalStateException("validation failed"));
    when(lifecycleFacade.completeFailure(
            eq(job), eq(JobStatus.RUNNING), eq(false), nullable(Integer.class)))
        .thenReturn(false);

    task.call();

    verify(invoker, never()).invoke(any());
    verify(lifecycleFacade)
        .completeFailure(eq(job), eq(JobStatus.RUNNING), eq(false), nullable(Integer.class));
  }

  @Test
  void lastResortCallbackErrorDoesNotEscape() throws Exception {
    JobPayloadInvoker invoker = mock(JobPayloadInvoker.class);
    JobTask task = callbackTask(invoker);
    JobEntity job = jobWithFailureCallback();
    RuntimeException original = new RuntimeException("original");
    stubWorkerFailure(task, job, original);
    // The failure metric is recorded after the failure write, so break the handler before it.
    when(validationFacade.shouldNotRetry(original))
        .thenThrow(new IllegalStateException("validation failed"));
    when(lifecycleFacade.completeFailure(
            eq(job), eq(JobStatus.RUNNING), eq(false), nullable(Integer.class)))
        .thenReturn(true);
    when(invoker.materializeArguments(any(), any())).thenAnswer(inv -> inv.getArgument(0));
    doThrow(new LinkageError("boom")).when(invoker).invoke(any());

    Assertions.assertDoesNotThrow(task::call);

    verify(invoker).invoke(any());
    Assertions.assertEquals(JobStatus.FAILED, job.getStatus());
  }

  @Test
  void nonRetryableFailureRecordsOneFailureMetricAfterTheFailureWrite() throws Exception {
    // An ordinary non-retryable failure counts one failure, after the job moves to FAILED.
    JobPayloadInvoker invoker = mock(JobPayloadInvoker.class);
    JobTask task = callbackTask(invoker);
    JobEntity job = jobWithFailureCallback();
    RuntimeException error = new RuntimeException("do not retry");
    stubWorkerFailure(task, job, error);
    int failedAttempt = job.getAttempts() + 1;
    when(lifecycleFacade.completeFailure(
            eq(job), eq(JobStatus.RUNNING), eq(false), nullable(Integer.class)))
        .thenReturn(true);
    when(invoker.materializeArguments(any(), any())).thenAnswer(inv -> inv.getArgument(0));

    task.call();

    InOrder order = inOrder(lifecycleFacade, observabilityFacade);
    order
        .verify(lifecycleFacade)
        .completeFailure(eq(job), eq(JobStatus.RUNNING), eq(false), nullable(Integer.class));
    order.verify(observabilityFacade).recordJobFailure(job, error, failedAttempt);
    verify(observabilityFacade, times(1)).recordJobFailure(any(), any(), anyInt());
  }

  @Test
  void onFailureCallbackErrorIsContainedOnNormalDlqPath() throws Exception {
    JobPayloadInvoker invoker = mock(JobPayloadInvoker.class);
    JobTask task = callbackTask(invoker);
    JobEntity job = jobWithFailureCallback();
    RuntimeException original = new RuntimeException("original");
    stubWorkerFailure(task, job, original);
    when(lifecycleFacade.completeFailure(
            eq(job), eq(JobStatus.RUNNING), eq(false), nullable(Integer.class)))
        .thenReturn(true);
    when(invoker.materializeArguments(any(), any())).thenAnswer(inv -> inv.getArgument(0));
    LinkageError callbackError = new LinkageError("boom");
    doThrow(callbackError).when(invoker).invoke(any());

    Assertions.assertDoesNotThrow(task::call);

    verify(invoker).invoke(any());
    verify(lifecycleFacade)
        .completeFailure(eq(job), eq(JobStatus.RUNNING), eq(false), nullable(Integer.class));
    verify(observabilityFacade).recordCallbackFailure(job, callbackError, 1);
  }

  @Test
  void hydrationPoisonDoesNotRunOnFailure() throws Exception {
    JobPayloadInvoker invoker = mock(JobPayloadInvoker.class);
    JobTask task = callbackTask(invoker);
    task.initFromClaim(claimForNode("node-1"));
    when(jobStore.findById(JOB_UUID))
        .thenThrow(new PayloadDecryptionException("ciphertext failed authentication"));
    when(lifecycleFacade.moveToDlqAndHandlePermanentFailure(any(), any(), nullable(Integer.class)))
        .thenReturn(true);

    task.call();

    verify(invoker, never()).invoke(any());
    verify(jobStore).findById(JOB_UUID);
  }

  @Test
  @SuppressWarnings("unchecked")
  void cancelDuringExecutionDoesNotRunOnFailure() throws Exception {
    JobPayloadInvoker invoker = mock(JobPayloadInvoker.class);
    JobTask task = callbackTask(invoker);
    JobEntity job = jobWithFailureCallback();
    initJobTaskWithDefaultStubs(task, job);
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING, JobStatus.CANCELED);
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);
    when(resilienceStrategy.execute(anyString(), any(), any(Callable.class)))
        .thenAnswer(inv -> ((Callable<?>) inv.getArgument(2)).call());
    when(invoker.materializeArguments(any(), any())).thenAnswer(inv -> inv.getArgument(0));

    task.call();

    verify(invoker).invoke(job.getPayload());
    verify(invoker, never()).invoke(argThat(payload -> payload.method().equals("onFailure")));
    verify(lifecycleFacade, never())
        .completeFailure(any(), any(), anyBoolean(), nullable(Integer.class));
    verify(observabilityFacade).recordJobCancellation(job);
  }

  @Test
  void signalPayloadPoisonRunsOnFailureWithoutBoundContext() throws Exception {
    EncryptionHolder.install(
        List.of(new FailingDecryptEngine()),
        FailingDecryptEngine.ALGORITHM_ID,
        new EncryptionTestKit.Provider(),
        true);
    JobPayloadInvoker invoker = mock(JobPayloadInvoker.class);
    JobTask task = callbackTask(invoker);
    JobEntity job = jobWithFailureCallback();
    job.setSignalPayload(
        PayloadEncryptor.encryptValue(
            "\"hello\"", true, EncryptionTarget.signal(job.getSignalKey())));
    task.init(job);
    when(nodeIdProvider.getNodeId()).thenReturn("node-1");
    // The failure metric is recorded after the failure write, so break the handler before it.
    when(validationFacade.shouldNotRetry(any()))
        .thenThrow(new IllegalStateException("validation failed"));
    when(lifecycleFacade.completeFailure(
            eq(job), eq(JobStatus.RUNNING), eq(false), nullable(Integer.class)))
        .thenReturn(true);
    when(invoker.materializeArguments(any(), any())).thenAnswer(inv -> inv.getArgument(0));
    JobMdcContext.clear();

    task.call();

    ArgumentCaptor<JobPayload> invoked = ArgumentCaptor.forClass(JobPayload.class);
    verify(invoker).invoke(invoked.capture());
    Assertions.assertTrue(invoked.getValue().args().get(1) instanceof PayloadDecryptionException);
    Assertions.assertNotNull(invoked.getValue().args().get(0));
    ArgumentCaptor<Throwable> original = ArgumentCaptor.forClass(Throwable.class);
    verify(validationFacade).shouldNotRetry(original.capture());
    Assertions.assertSame(original.getValue(), invoked.getValue().args().get(1));
    Assertions.assertNull(JobContext.currentOrNull());
  }

  @Test
  @SuppressWarnings("unchecked")
  void handleFailureFallbackPublishesFailureEventAndDelegatesDlqEvent() throws Exception {
    JobEntity job = createTestJob();
    initJobTaskWithDefaultStubs(job);
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING);
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);

    RuntimeException error = new RuntimeException("original");
    when(resilienceStrategy.execute(anyString(), any(), any(Callable.class))).thenThrow(error);
    when(validationFacade.shouldNotRetry(error)).thenReturn(false);
    when(jobStore.incrementRetryAttempt(JOB_UUID, 0L, 0)).thenReturn(1);
    // The failure metric is recorded after the retry or failure write, so break the handler
    // before it.
    when(retryPolicy.shouldRetry(1, error)).thenThrow(new IllegalStateException("policy failed"));
    when(errorSanitizer.sanitize(error)).thenReturn("safe original");
    when(lifecycleFacade.completeFailure(
            any(JobEntity.class), eq(JobStatus.RUNNING), eq(false), nullable(Integer.class)))
        .thenReturn(true);

    jobTask.call();

    verify(observabilityFacade, never()).publishEvent(any(JobFailedEvent.class));
    verify(observabilityFacade, never()).publishEvent(any(JobDlqEvent.class));
    verify(lifecycleFacade)
        .completeFailure(eq(job), eq(JobStatus.RUNNING), eq(false), nullable(Integer.class));
    Assertions.assertEquals(1, job.getAttempts());
  }

  @Test
  @SuppressWarnings("unchecked")
  void handleFailure_batchChild_marksBatchChildFailedInsteadOfSchedulingNext() throws Exception {
    JobEntity job = createTestJob();
    job.setJobType(JobExecutionType.BATCH_CHILD);
    initJobTaskWithDefaultStubs(job);
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING);
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);

    RuntimeException error = new RuntimeException("batch child failed");
    when(resilienceStrategy.execute(anyString(), any(), any(Callable.class))).thenThrow(error);
    when(validationFacade.shouldNotRetry(error)).thenReturn(true);
    when(lifecycleFacade.completeFailure(
            any(JobEntity.class), eq(JobStatus.RUNNING), eq(false), nullable(Integer.class)))
        .thenReturn(true);

    jobTask.call();

    verify(lifecycleFacade)
        .completeFailure(eq(job), eq(JobStatus.RUNNING), eq(false), nullable(Integer.class));
    verify(lifecycleFacade, never()).scheduleNext(job);
  }

  @Test
  @SuppressWarnings("unchecked")
  void handleCanceledDuringExecution_batchChild_leavesCountingToCancelTransition()
      throws Exception {
    JobEntity job = createTestJob();
    job.setJobType(JobExecutionType.BATCH_CHILD);
    initJobTaskWithDefaultStubs(job);
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING, JobStatus.CANCELED);
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);
    when(resilienceStrategy.execute(anyString(), any(), any(Callable.class)))
        .thenAnswer(inv -> ((Callable<?>) inv.getArgument(2)).call());

    jobTask.call();

    verify(lifecycleFacade, never()).markBatchChildFailed(job);
    verify(lifecycleFacade, never()).cancelChain(job);
  }

  @Test
  @SuppressWarnings("unchecked")
  void handleSuccess_jobCanceledDuringExecution_discardsResultAndPublishesCancellation()
      throws Exception {
    // A cancellation discards the result while leaving chain cascading to the cancel boundary.
    JobEntity job = createTestJob();
    initJobTaskWithDefaultStubs(job);
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING, JobStatus.CANCELED);
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);
    when(resilienceStrategy.execute(anyString(), any(), any(Callable.class)))
        .thenAnswer(inv -> ((Callable<?>) inv.getArgument(2)).call());

    jobTask.call();

    verify(lifecycleFacade, never())
        .completeSuccess(
            any(JobEntity.class),
            any(),
            any(),
            any(),
            any(),
            anyLong(),
            anyLong(),
            nullable(Integer.class));
    verify(observabilityFacade).recordJobCancellation(job);
    verify(observabilityFacade, never()).publishEvent(any(JobCompletedEvent.class));
    verify(observabilityFacade, never()).recordJobSuccess(any(), anyLong());
    verify(lifecycleFacade, never()).cancelChain(any());
  }

  @Test
  @SuppressWarnings("unchecked")
  void handleSuccess_publishesCompletedEvent() throws Exception {
    JobEntity job = createTestJob();
    initJobTaskWithDefaultStubs(job);
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING);
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);
    when(resilienceStrategy.execute(anyString(), any(), any(Callable.class)))
        .thenAnswer(inv -> ((Callable<?>) inv.getArgument(2)).call());
    when(lifecycleFacade.completeSuccess(
            any(JobEntity.class),
            any(),
            any(),
            any(),
            any(),
            anyLong(),
            anyLong(),
            nullable(Integer.class)))
        .thenReturn(true);

    jobTask.call();

    verify(observabilityFacade, never()).publishEvent(any(JobCompletedEvent.class));
  }

  @Test
  @SuppressWarnings("unchecked")
  void handleSuccessPublishesCompletionAndCallbackFailureEventsWithInjectedClock()
      throws Exception {
    JobTask fixedClockTask = newJobTaskWithClock(FIXED_CLOCK);
    JobEntity job = createTestJob();
    UUID recurringMasterId = UUID.fromString("019c1f33-09c0-7000-8000-000000000125");
    job.setRecurringMasterId(recurringMasterId);
    job.setOnSuccessPayload(
        new JobPayload(
            JobTaskTest.class.getName(), "failingCallback", "()V", true, List.of(), null));
    initJobTaskWithDefaultStubs(fixedClockTask, job);
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING);
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);
    when(resilienceStrategy.execute(anyString(), any(), any(Callable.class)))
        .thenAnswer(inv -> ((Callable<?>) inv.getArgument(2)).call());
    when(lifecycleFacade.completeSuccess(
            any(JobEntity.class),
            any(),
            any(),
            any(),
            any(),
            anyLong(),
            anyLong(),
            nullable(Integer.class)))
        .thenReturn(true);

    fixedClockTask.call();

    ArgumentCaptor<Object> eventCaptor = ArgumentCaptor.forClass(Object.class);
    verify(observabilityFacade, atLeastOnce()).publishEvent(eventCaptor.capture());
    JobStartedEvent startedEvent =
        eventCaptor.getAllValues().stream()
            .filter(JobStartedEvent.class::isInstance)
            .map(JobStartedEvent.class::cast)
            .findFirst()
            .orElseThrow();
    JobCallbackFailedEvent callbackEvent =
        eventCaptor.getAllValues().stream()
            .filter(JobCallbackFailedEvent.class::isInstance)
            .map(JobCallbackFailedEvent.class::cast)
            .findFirst()
            .orElseThrow();

    Assertions.assertEquals(recurringMasterId, startedEvent.getRecurringMasterId());
    Assertions.assertEquals(recurringMasterId, callbackEvent.getRecurringMasterId());
    verify(lifecycleFacade)
        .completeSuccess(
            eq(job),
            any(),
            any(),
            eq(FIXED_NOW),
            eq(FIXED_NOW),
            eq(0L),
            anyLong(),
            nullable(Integer.class));
    Assertions.assertEquals(FIXED_NOW, callbackEvent.getTimestamp());
    Assertions.assertEquals(1, callbackEvent.getCallbackAttempt());
  }

  @Test
  @SuppressWarnings("unchecked")
  void call_releasesResourcePermit_onSuccess() throws Exception {
    JobEntity job = createTestJob();
    job.setResourceName("api-gateway");
    initJobTaskWithDefaultStubs(job);
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING);
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);
    when(resourcePermitService.tryAcquire("api-gateway", JOB_UUID, 0L, "node-1"))
        .thenReturn(AcquireResult.ACQUIRED);
    when(resilienceStrategy.execute(anyString(), any(), any(Callable.class)))
        .thenAnswer(inv -> ((Callable<?>) inv.getArgument(2)).call());
    when(lifecycleFacade.completeSuccess(
            any(JobEntity.class),
            any(),
            any(),
            any(),
            any(),
            anyLong(),
            anyLong(),
            nullable(Integer.class)))
        .thenReturn(true);

    jobTask.call();

    verify(resourcePermitService).tryAcquire("api-gateway", JOB_UUID, 0L, "node-1");
    verify(resourcePermitService).release("api-gateway", JOB_UUID, 0L);
  }

  @Test
  @SuppressWarnings("unchecked")
  void call_transientAcquireAfterCommittedPermitRunsBodyOnce() throws Exception {
    JobEntity job = createTestJob();
    job.setResourceName("gpu");
    initJobTaskWithDefaultStubs(job);
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING);
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);
    when(resourcePermitService.tryAcquire("gpu", JOB_UUID, 0L, "node-1"))
        .thenThrow(new RatchetTransientStoreException("commit response lost"))
        .thenReturn(AcquireResult.ACQUIRED);
    when(resilienceStrategy.execute(anyString(), any(), any(Callable.class)))
        .thenAnswer(inv -> ((Callable<?>) inv.getArgument(2)).call());
    when(lifecycleFacade.completeSuccess(
            any(JobEntity.class),
            any(),
            any(),
            any(),
            any(),
            anyLong(),
            anyLong(),
            nullable(Integer.class)))
        .thenReturn(true);

    jobTask.call();

    verify(resourcePermitService, times(2)).tryAcquire("gpu", JOB_UUID, 0L, "node-1");
    verify(resilienceStrategy).execute(anyString(), any(), any(Callable.class));
    verify(resourcePermitService).release("gpu", JOB_UUID, 0L);
  }

  @Test
  void call_transientAfterCommittedParkStopsWithoutSpendingAttempt() throws Exception {
    assertTransientThenSuperseded(JobStatus.PENDING);
  }

  @Test
  void call_cancelledDuringAcquireRetryStopsWithoutSpendingAttempt() throws Exception {
    assertTransientThenSuperseded(JobStatus.CANCELED);
  }

  @SuppressWarnings("unchecked")
  private void assertTransientThenSuperseded(JobStatus committedStatus) throws Exception {
    JobEntity job = createTestJob();
    job.setResourceName("gpu");
    initJobTaskWithDefaultStubs(job);
    AtomicReference<JobStatus> queueStatus = new AtomicReference<>(JobStatus.RUNNING);
    when(jobStore.getJobStatus(JOB_UUID)).thenAnswer(inv -> queueStatus.get());
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);
    when(resourcePermitService.tryAcquire("gpu", JOB_UUID, 0L, "node-1"))
        .thenAnswer(
            inv -> {
              queueStatus.set(committedStatus);
              throw new RatchetTransientStoreException("response lost");
            })
        .thenReturn(AcquireResult.SUPERSEDED);
    // A PENDING or CANCELED queue row rejects the existing RUNNING/claim-fenced safety net.
    when(resourcePermitService.getRetryDelay("gpu")).thenReturn(100);
    when(jobStore.releaseClaim(eq(JOB_UUID), anyString(), any(), anyInt(), eq(0L)))
        .thenAnswer(inv -> queueStatus.get() == JobStatus.RUNNING);

    jobTask.call();

    verify(resourcePermitService, times(2)).tryAcquire("gpu", JOB_UUID, 0L, "node-1");
    verifyNoInteractions(lifecycleFacade);
    verify(resilienceStrategy, never()).execute(anyString(), any(), any(Callable.class));
    verify(jobStore).releaseClaim(eq(JOB_UUID), anyString(), any(), anyInt(), eq(0L));
    verify(jobStore, never()).incrementRetryAttempt(any(), any(), any());
    verifyNoAttemptRecorded();
  }

  @Test
  void call_exhaustedTransientAcquireReschedulesWithoutConsumingAttempt() throws Exception {
    assertTransientPermitFailureRescheduled(true);
  }

  @Test
  void call_interruptedAcquireBackoffStopsAndRestoresInterrupt() throws Exception {
    try {
      assertInterruptedAcquireFailure();
      Assertions.assertTrue(Thread.currentThread().isInterrupted());
    } finally {
      Thread.interrupted();
    }
  }

  private void assertInterruptedAcquireFailure() throws Exception {
    JobEntity job = createTestJob();
    job.setResourceName("gpu");
    initJobTaskWithDefaultStubs(job);
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING);
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);
    var failure = new RatchetTransientStoreException("deadlock");
    when(resourcePermitService.tryAcquire("gpu", JOB_UUID, 0L, "node-1"))
        .thenAnswer(
            inv -> {
              Thread.currentThread().interrupt();
              throw failure;
            });
    List<Boolean> flags = new ArrayList<>();
    doAnswer(
            inv -> {
              flags.add(Thread.currentThread().isInterrupted());
              return null;
            })
        .when(resourcePermitService)
        .release("gpu", JOB_UUID, 0L);
    when(jobStore.incrementRetryAttempt(JOB_UUID, 0L, 0))
        .thenAnswer(
            inv -> {
              flags.add(Thread.currentThread().isInterrupted());
              return 1;
            });
    when(lifecycleFacade.completeFailure(
            eq(job), eq(JobStatus.RUNNING), eq(false), nullable(Integer.class)))
        .thenAnswer(
            inv -> {
              flags.add(Thread.currentThread().isInterrupted());
              return true;
            });

    jobTask.call();

    verify(resourcePermitService, times(1)).tryAcquire("gpu", JOB_UUID, 0L, "node-1");
    InOrder order = inOrder(resourcePermitService, jobStore);
    order.verify(resourcePermitService).release("gpu", JOB_UUID, 0L);
    order.verify(jobStore).incrementRetryAttempt(JOB_UUID, 0L, 0);
    verify(lifecycleFacade)
        .completeFailure(eq(job), eq(JobStatus.RUNNING), eq(false), nullable(Integer.class));
    // The failure path's store writes run with the interrupt flag clear.
    Assertions.assertEquals(List.of(false, false, false), flags);
    verifyNoAttemptRecorded();
  }

  @Test
  @SuppressWarnings("unchecked")
  void call_releaseUsesSameClaimAsAcquire() throws Exception {
    JobEntity job = createTestJob();
    job.setResourceName("api-gateway");
    job.setClaimSeq(7L);
    initJobTaskWithDefaultStubs(job);
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING);
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);
    when(resourcePermitService.tryAcquire("api-gateway", JOB_UUID, 7L, "node-1"))
        .thenReturn(AcquireResult.ACQUIRED);
    when(resilienceStrategy.execute(anyString(), any(), any(Callable.class)))
        .thenAnswer(inv -> ((Callable<?>) inv.getArgument(2)).call());
    when(lifecycleFacade.completeSuccess(
            any(JobEntity.class),
            any(),
            any(),
            any(),
            any(),
            anyLong(),
            anyLong(),
            nullable(Integer.class)))
        .thenReturn(true);

    jobTask.call();

    verify(resourcePermitService).tryAcquire("api-gateway", JOB_UUID, 7L, "node-1");
    verify(resourcePermitService).release("api-gateway", JOB_UUID, 7L);
  }

  @Test
  @SuppressWarnings("unchecked")
  void call_resourcePermitUnavailable_storeAlreadyParkedWithoutExecutingPayload() throws Exception {
    JobEntity job = createTestJob();
    job.setResourceName("gpu");
    job.setAttempts(2);
    initJobTaskWithDefaultStubs(job);
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING);
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);
    when(resourcePermitService.tryAcquire("gpu", JOB_UUID, 0L, "node-1"))
        .thenReturn(AcquireResult.AT_CAPACITY);
    jobTask.call();

    verify(resourcePermitService, never()).getRetryDelay(anyString());
    verify(jobStore, never()).scheduleJobRetry(any(), anyString(), any(), anyInt(), anyLong());
    verify(jobStore, never()).releaseClaim(any(), anyString(), any(), anyInt(), anyLong());
    verify(resilienceStrategy, never()).execute(anyString(), any(), any(Callable.class));
    verify(resourcePermitService, never()).release(anyString(), any(UUID.class), anyLong());
    verifyNoAttemptRecorded();
  }

  @Test
  @SuppressWarnings("unchecked")
  void call_acquiresPermitBeforeRecordingStart() throws Exception {
    JobEntity job = createTestJob();
    job.setResourceName("api-gateway");
    initJobTaskWithDefaultStubs(job);
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING);
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);
    when(resourcePermitService.tryAcquire("api-gateway", JOB_UUID, 0L, "node-1"))
        .thenReturn(AcquireResult.ACQUIRED);
    when(resilienceStrategy.execute(anyString(), any(), any(Callable.class)))
        .thenAnswer(inv -> ((Callable<?>) inv.getArgument(2)).call());
    when(lifecycleFacade.completeSuccess(
            any(JobEntity.class),
            any(),
            any(),
            any(),
            any(),
            anyLong(),
            anyLong(),
            nullable(Integer.class)))
        .thenReturn(true);

    jobTask.call();

    InOrder order = inOrder(resourcePermitService, observabilityFacade);
    order.verify(resourcePermitService).tryAcquire("api-gateway", JOB_UUID, 0L, "node-1");
    order.verify(observabilityFacade).recordJobStart(job);
    order.verify(observabilityFacade).startExecution(JOB_UUID, 1, "node-1");
    order.verify(observabilityFacade).publishEvent(any(JobStartedEvent.class));
    order.verify(observabilityFacade).startExecutionScope(job);
    order.verify(resourcePermitService).release("api-gateway", JOB_UUID, 0L);
  }

  @Test
  @SuppressWarnings("unchecked")
  void call_executionHistoryWriteFailsAfterPermit_releasesPermit() throws Exception {
    JobEntity job = createTestJob();
    job.setResourceName("gpu");
    job.setClaimSeq(7L);
    jobTask.init(job);
    when(nodeIdProvider.getNodeId()).thenReturn("node-1");
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING);
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);
    when(resourcePermitService.tryAcquire("gpu", JOB_UUID, 7L, "node-1"))
        .thenReturn(AcquireResult.ACQUIRED);
    RuntimeException auditError = new RuntimeException("audit store down");
    when(observabilityFacade.startExecution(any(UUID.class), anyInt(), anyString()))
        .thenThrow(auditError);
    when(validationFacade.shouldNotRetry(auditError)).thenReturn(true);
    when(lifecycleFacade.completeFailure(
            eq(job), eq(JobStatus.RUNNING), eq(false), nullable(Integer.class)))
        .thenReturn(true);

    jobTask.call();

    InOrder order = inOrder(resourcePermitService, observabilityFacade, lifecycleFacade);
    order.verify(resourcePermitService).tryAcquire("gpu", JOB_UUID, 7L, "node-1");
    order.verify(observabilityFacade).startExecution(JOB_UUID, 1, "node-1");
    order
        .verify(lifecycleFacade)
        .completeFailure(eq(job), eq(JobStatus.RUNNING), eq(false), nullable(Integer.class));
    order.verify(resourcePermitService).release("gpu", JOB_UUID, 7L);
    verify(resilienceStrategy, never()).execute(anyString(), any(), any(Callable.class));
    verify(observabilityFacade, never()).publishEvent(any(JobStartedEvent.class));
  }

  @Test
  void call_unconfiguredResource_failsAttemptWithoutRecordingStart() throws Exception {
    JobEntity job = createTestJob();
    job.setResourceName("missing");
    initJobTaskWithDefaultStubs(job);
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING);
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);
    IllegalArgumentException unconfigured =
        new IllegalArgumentException("Resource is not configured: missing");
    when(resourcePermitService.tryAcquire("missing", JOB_UUID, 0L, "node-1"))
        .thenThrow(unconfigured);
    when(validationFacade.shouldNotRetry(unconfigured)).thenReturn(true);
    when(lifecycleFacade.completeFailure(
            eq(job), eq(JobStatus.RUNNING), eq(false), nullable(Integer.class)))
        .thenReturn(true);

    jobTask.call();

    verify(lifecycleFacade)
        .completeFailure(eq(job), eq(JobStatus.RUNNING), eq(false), nullable(Integer.class));
    verify(resourcePermitService).tryAcquire("missing", JOB_UUID, 0L, "node-1");
    verify(resilienceStrategy, never()).execute(anyString(), any(), any(Callable.class));
    verify(resourcePermitService, never()).release(anyString(), any(UUID.class), anyLong());
    verifyNoAttemptRecorded();
  }

  @Test
  void call_resourceWithNoLimit_deadLettersWithoutRecordingStart() throws Exception {
    JobEntity job = createTestJob();
    job.setResourceName("missing");
    initJobTaskWithDefaultStubs(job);
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING);
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);
    ResourceNotConfiguredException unconfigured = new ResourceNotConfiguredException("missing");
    when(resourcePermitService.tryAcquire("missing", JOB_UUID, 0L, "node-1"))
        .thenThrow(unconfigured);
    when(validationFacade.shouldNotRetry(unconfigured)).thenReturn(true);
    when(lifecycleFacade.completeFailure(
            eq(job), eq(JobStatus.RUNNING), eq(false), nullable(Integer.class)))
        .thenReturn(true);

    jobTask.call();

    verify(lifecycleFacade)
        .completeFailure(eq(job), eq(JobStatus.RUNNING), eq(false), nullable(Integer.class));
    verify(resourcePermitService).tryAcquire("missing", JOB_UUID, 0L, "node-1");
    verify(resilienceStrategy, never()).execute(anyString(), any(), any(Callable.class));
    verify(resourcePermitService, never()).release(anyString(), any(UUID.class), anyLong());
    verifyNoAttemptRecorded();
  }

  @Test
  void call_serviceUnavailable_takesNoPermitAndRecordsNoAttempt() throws Exception {
    JobEntity job = createTestJob();
    job.setResourceName("gpu");
    initJobTaskWithDefaultStubs(job);
    String serviceName = JobTaskTest.class.getSimpleName() + ".testJobMethod";
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING);
    when(resilienceStrategy.isServiceAvailable(serviceName)).thenReturn(false);
    when(jobStore.releaseClaim(eq(JOB_UUID), anyString(), any(), eq(0), eq(0L))).thenReturn(true);

    jobTask.call();

    verify(jobStore)
        .releaseClaim(
            eq(JOB_UUID),
            eq("Circuit breaker OPEN for service: " + serviceName),
            any(),
            eq(0),
            eq(0L));
    verify(resourcePermitService, never())
        .tryAcquire(anyString(), any(UUID.class), anyLong(), anyString());
    verifyNoAttemptRecorded();
  }

  @Test
  void call_canceledBeforeStart_takesNoPermitAndRecordsNoAttempt() throws Exception {
    // A pre-start cancellation takes no permit or attempt and never cascades the chain here.
    JobEntity job = createTestJob();
    job.setResourceName("gpu");
    initJobTaskWithDefaultStubs(job);
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.CANCELED);

    jobTask.call();

    verify(lifecycleFacade, never()).cancelChain(any());
    verify(resilienceStrategy, never()).isServiceAvailable(anyString());
    verify(resourcePermitService, never())
        .tryAcquire(anyString(), any(UUID.class), anyLong(), anyString());
    verifyNoAttemptRecorded();
  }

  @Test
  void call_supersededPermit_rejectedRescheduleDoesNotExecuteOrRelease() throws Exception {
    assertSupersededPermit(false);
  }

  @Test
  void call_supersededPermit_successfulRescheduleUsesAttemptClaim() throws Exception {
    assertSupersededPermit(true);
  }

  @SuppressWarnings("unchecked")
  private void assertSupersededPermit(boolean rescheduled) throws Exception {
    JobEntity job = createTestJob();
    job.setResourceName("gpu");
    job.setClaimSeq(7L);
    job.setAttempts(2);
    initJobTaskWithDefaultStubs(job);
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING);
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);
    when(resourcePermitService.tryAcquire("gpu", JOB_UUID, 7L, "node-1"))
        .thenReturn(AcquireResult.SUPERSEDED);
    when(resourcePermitService.getRetryDelay("gpu")).thenReturn(250);
    when(jobStore.releaseClaim(eq(JOB_UUID), anyString(), any(), eq(2), eq(7L)))
        .thenReturn(rescheduled);

    jobTask.call();

    verify(jobStore)
        .releaseClaim(eq(JOB_UUID), eq("Waiting for resource: gpu"), any(), eq(2), eq(7L));
    verify(resilienceStrategy, never()).execute(anyString(), any(), any(Callable.class));
    verify(resourcePermitService, never()).release(anyString(), any(UUID.class), anyLong());
    // The superseded attempt never ran its body; the claim that replaced it records its own.
    verifyNoAttemptRecorded();
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 2})
  void noChargePermitAndCircuitReleasesUseClaimAttemptsAfterHydrationCharge(int gate)
      throws Exception {
    JobEntity job = createTestJob();
    jobTask = newJobTaskWithTimeoutHandler(null);
    job.setStatus(JobStatus.RUNNING);
    job.setPickedBy("node-1");
    job.setResourceName("gpu");
    job.setAttempts(3);
    JobClaimDto claim = claimForNode("node-1");
    initJobTaskFromClaimWithDefaultStubs(claim, job);
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING);
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(gate != 2);
    if (gate == 2) {
      when(resilienceStrategy.getRetryDelay(anyString())).thenReturn(Duration.ofSeconds(1));
    } else if (gate == 1) {
      when(resourcePermitService.tryAcquire("gpu", JOB_UUID, claim.claimSeq(), "node-1"))
          .thenReturn(AcquireResult.SUPERSEDED);
    } else {
      when(resourcePermitService.tryAcquire("gpu", JOB_UUID, claim.claimSeq(), "node-1"))
          .thenThrow(new RatchetTransientStoreException("unavailable"));
    }
    jobTask.call();
    verify(jobStore)
        .releaseClaim(eq(JOB_UUID), anyString(), any(), eq(claim.attempts()), eq(claim.claimSeq()));
    verify(jobStore, never()).incrementRetryAttempt(any(), any(), any());
    verify(jobStore, never()).scheduleJobRetry(any(), any(), any(), anyInt(), any());
  }

  @Test
  @SuppressWarnings("unchecked")
  void call_transientPermitFailureReschedulesWithoutConsumingAttempt() throws Exception {
    assertTransientPermitFailureRescheduled(true);
  }

  @Test
  void call_transientPermitFailureRejectedRescheduleDoesNotFailAttempt() throws Exception {
    assertTransientPermitFailureRescheduled(false);
  }

  @SuppressWarnings("unchecked")
  private void assertTransientPermitFailureRescheduled(boolean rescheduled) throws Exception {
    jobTask = newJobTaskWithTimeoutHandler(null);
    JobEntity job = createTestJob();
    job.setResourceName("gpu");
    job.setAttempts(2);
    job.setClaimSeq(7L);
    initJobTaskWithDefaultStubs(job);
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING);
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);
    when(resourcePermitService.tryAcquire("gpu", JOB_UUID, 7L, "node-1"))
        .thenThrow(new RatchetTransientStoreException("deadlock"));
    when(resourcePermitService.getRetryDelay("gpu")).thenReturn(250);
    when(jobStore.releaseClaim(eq(JOB_UUID), anyString(), any(), eq(2), eq(7L)))
        .thenReturn(rescheduled);

    jobTask.call();

    verify(resourcePermitService, times(JobTask.TRANSIENT_STORE_TRIES))
        .tryAcquire("gpu", JOB_UUID, 7L, "node-1");
    InOrder order = inOrder(resourcePermitService, jobStore);
    order.verify(resourcePermitService).release("gpu", JOB_UUID, 7L);
    order
        .verify(jobStore)
        .releaseClaim(
            eq(JOB_UUID),
            eq("Permit check failed transiently for resource: gpu"),
            eq(FIXED_NOW.plusMillis(250)),
            eq(2),
            eq(7L));
    Assertions.assertEquals(2, job.getAttempts());
    verify(jobStore, never()).incrementRetryAttempt(any(UUID.class), anyLong(), any());
    verify(resilienceStrategy, never()).execute(anyString(), any(), any(Callable.class));
    verify(lifecycleFacade, never()).moveToDlq(any(), any());
    verify(lifecycleFacade, never())
        .completeFailure(any(), any(), anyBoolean(), nullable(Integer.class));
    verify(validationFacade, never()).shouldNotRetry(any());
    verifyNoAttemptRecorded();
  }

  @Test
  void call_transientPermitRescheduleRetriesWithoutChargingAttempt() throws Exception {
    assertNoChargeRescheduleRetries(false, false, false);
  }

  @Test
  void call_transientPermitRescheduleExhaustedHandsOff() throws Exception {
    assertNoChargeRescheduleRetries(false, false, true);
  }

  @Test
  void call_supersededPermitRescheduleRetriesWithoutChargingAttempt() throws Exception {
    assertNoChargeRescheduleRetries(true, false, false);
  }

  @Test
  void call_openCircuitRescheduleRetriesWithoutChargingAttempt() throws Exception {
    assertNoChargeRescheduleRetries(false, true, false);
  }

  @Test
  void call_supersededPermitRescheduleExhaustedHandsOff() throws Exception {
    assertNoChargeRescheduleRetries(true, false, true);
  }

  @Test
  void call_openCircuitRescheduleExhaustedHandsOff() throws Exception {
    assertNoChargeRescheduleRetries(false, true, true);
  }

  @Test
  void call_upgradeRescheduleExhaustedHandsOff() {
    JobClaimDto claim = claimForNode("node-1");
    jobTask.initFromClaim(claim);
    UnsupportedEnvelopeVersionException skew = new UnsupportedEnvelopeVersionException(2, 1);
    when(jobStore.findById(JOB_UUID)).thenThrow(skew);
    when(errorSanitizer.sanitize(skew)).thenReturn("sanitized skew");
    when(jobStore.releaseClaim(
            eq(JOB_UUID), eq("sanitized skew"), any(), eq(claim.attempts()), eq(0L)))
        .thenThrow(new RatchetTransientStoreException("unavailable"));
    when(claimReschedules.handOff(
            any(), anyLong(), anyString(), any(), anyInt(), any(), any(), any(), any()))
        .thenReturn(true);

    jobTask.call();

    ArgumentCaptor<Instant> scheduled = ArgumentCaptor.forClass(Instant.class);
    verify(jobStore, times(3))
        .releaseClaim(
            eq(JOB_UUID), eq("sanitized skew"), scheduled.capture(), eq(claim.attempts()), eq(0L));
    verify(claimReschedules)
        .handOff(
            eq(JOB_UUID),
            eq(0L),
            eq("sanitized skew"),
            eq(scheduled.getValue()),
            eq(claim.attempts()),
            any(),
            any(),
            any(),
            any());
    Assertions.assertTrue(scheduled.getAllValues().stream().allMatch(scheduled.getValue()::equals));
    verify(jobStore, never()).incrementRetryAttempt(any(), anyLong(), any());
    verify(lifecycleFacade, never()).moveToDlq(any(), any());
    verify(observabilityFacade, never()).publishEvent(any(JobDlqEvent.class));
  }

  @Test
  void call_batchGuardRescheduleExhaustedHandsOff() throws Exception {
    when(jobStore.releaseClaim(eq(JOB_UUID), any(), eq(FIXED_NOW.plusSeconds(30)), eq(2), eq(7L)))
        .thenThrow(new RatchetTransientStoreException("unavailable"));
    when(claimReschedules.handOff(
            any(), anyLong(), any(), any(), anyInt(), any(), any(), any(), any()))
        .thenReturn(true);
    assertBatchChildNeverStarts(
        parentId ->
            when(jobStore.getJobStatus(parentId))
                .thenThrow(new IllegalStateException("parent unavailable")),
        false);
    ArgumentCaptor<String> reason = ArgumentCaptor.forClass(String.class);
    verify(jobStore, times(3))
        .releaseClaim(eq(JOB_UUID), reason.capture(), eq(FIXED_NOW.plusSeconds(30)), eq(2), eq(7L));
    verify(claimReschedules)
        .handOff(
            eq(JOB_UUID),
            eq(7L),
            eq(reason.getValue()),
            eq(FIXED_NOW.plusSeconds(30)),
            eq(2),
            any(),
            any(),
            any(),
            any());
    verify(jobStore, never()).incrementRetryAttempt(any(), anyLong(), any());
    verify(lifecycleFacade, never()).moveToDlq(any(), any());
  }

  @Test
  void call_upgradeInterruptedBackoffRestoresInterruptAfterHandoff() {
    jobTask.initFromClaim(claimForNode("node-1"));
    UnsupportedEnvelopeVersionException skew = new UnsupportedEnvelopeVersionException(2, 1);
    when(jobStore.findById(JOB_UUID)).thenThrow(skew);
    when(errorSanitizer.sanitize(skew)).thenReturn("sanitized skew");
    when(jobStore.releaseClaim(eq(JOB_UUID), anyString(), any(), anyInt(), eq(0L)))
        .thenAnswer(
            inv -> {
              Assertions.assertFalse(Thread.currentThread().isInterrupted());
              Thread.currentThread().interrupt();
              throw new RatchetTransientStoreException("interrupted write");
            });
    when(claimReschedules.handOff(
            any(), anyLong(), any(), any(), anyInt(), any(), any(), any(), any()))
        .thenReturn(true);
    try {
      jobTask.call();
      Assertions.assertTrue(Thread.currentThread().isInterrupted());
      verify(jobStore).releaseClaim(eq(JOB_UUID), anyString(), any(), anyInt(), eq(0L));
      verify(claimReschedules)
          .handOff(
              eq(JOB_UUID),
              eq(0L),
              eq("sanitized skew"),
              any(),
              anyInt(),
              any(),
              any(),
              any(),
              any());
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  void call_batchGuardInterruptedBackoffRestoresInterruptAfterHandoff() throws Exception {
    when(jobStore.releaseClaim(eq(JOB_UUID), any(), any(), eq(2), eq(7L)))
        .thenAnswer(
            inv -> {
              Assertions.assertFalse(Thread.currentThread().isInterrupted());
              Thread.currentThread().interrupt();
              throw new RatchetTransientStoreException("interrupted write");
            });
    when(claimReschedules.handOff(
            any(), anyLong(), any(), any(), anyInt(), any(), any(), any(), any()))
        .thenReturn(true);
    try {
      assertBatchChildNeverStarts(
          parentId ->
              when(jobStore.getJobStatus(parentId))
                  .thenThrow(new IllegalStateException("parent unavailable")),
          false);
      Assertions.assertTrue(Thread.currentThread().isInterrupted());
      verify(jobStore)
          .releaseClaim(eq(JOB_UUID), any(), eq(FIXED_NOW.plusSeconds(30)), eq(2), eq(7L));
      verify(claimReschedules)
          .handOff(
              eq(JOB_UUID),
              eq(7L),
              any(),
              eq(FIXED_NOW.plusSeconds(30)),
              eq(2),
              any(),
              any(),
              any(),
              any());
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  void call_signalPayloadUpgradeInterruptedBackoffRestoresInterruptAfterHandoff() {
    JobEntity job = spy(createTestJob());
    jobTask.init(job);
    when(nodeIdProvider.getNodeId()).thenReturn("node-1");
    UnsupportedEnvelopeVersionException skew = new UnsupportedEnvelopeVersionException(2, 1);
    doThrow(skew).when(job).getSignalPayload();
    when(errorSanitizer.sanitize(skew)).thenReturn("sanitized skew");
    when(jobStore.releaseClaim(eq(JOB_UUID), anyString(), any(), anyInt(), eq(0L)))
        .thenAnswer(
            inv -> {
              Assertions.assertFalse(Thread.currentThread().isInterrupted());
              Thread.currentThread().interrupt();
              throw new RatchetTransientStoreException("interrupted write");
            });
    when(claimReschedules.handOff(
            any(), anyLong(), any(), any(), anyInt(), any(), any(), any(), any()))
        .thenReturn(true);
    try {
      jobTask.call();
      Assertions.assertTrue(Thread.currentThread().isInterrupted());
      verify(job).getSignalPayload();
      verify(jobStore).releaseClaim(eq(JOB_UUID), anyString(), any(), anyInt(), eq(0L));
      verify(claimReschedules)
          .handOff(
              eq(JOB_UUID),
              eq(0L),
              eq("sanitized skew"),
              any(),
              anyInt(),
              any(),
              any(),
              any(),
              any());
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  void reschedule_handoffSupplierTracksAttemptRevocation() throws Exception {
    JobAttemptControl attempt = prepareClaimReschedule();
    when(claimReschedules.handOff(
            any(), anyLong(), any(), any(), anyInt(), any(), any(), any(), any()))
        .thenReturn(true);
    invokeClaimReschedule();
    ArgumentCaptor<BooleanSupplier> revoked = ArgumentCaptor.forClass(BooleanSupplier.class);
    verify(claimReschedules)
        .handOff(
            eq(JOB_UUID),
            eq(7L),
            eq("release"),
            eq(FIXED_NOW),
            eq(2),
            revoked.capture(),
            any(),
            any(),
            any());
    Assertions.assertFalse(revoked.getValue().getAsBoolean());
    attempt.revoke();
    Assertions.assertTrue(revoked.getValue().getAsBoolean());
  }

  @Test
  void reschedule_revokedAttemptDoesNotHandOff() throws Exception {
    JobAttemptControl attempt = prepareClaimReschedule();
    attempt.revoke();
    invokeClaimReschedule();
    verifyNoInteractions(claimReschedules);
    verify(jobStore, times(3)).releaseClaim(JOB_UUID, "release", FIXED_NOW, 2, 7L);
  }

  @Test
  void reschedule_refusedHandoffKeepsWorkerUntilWriteSucceeds() throws Exception {
    prepareClaimReschedule();
    doThrow(new RatchetTransientStoreException("unavailable"))
        .doThrow(new RatchetTransientStoreException("unavailable"))
        .doThrow(new RatchetTransientStoreException("unavailable"))
        .doAnswer(
            inv -> {
              Assertions.assertFalse(Thread.currentThread().isInterrupted());
              return true;
            })
        .when(jobStore)
        .releaseClaim(JOB_UUID, "release", FIXED_NOW, 2, 7L);
    invokeClaimReschedule();
    verify(jobStore, times(4)).releaseClaim(JOB_UUID, "release", FIXED_NOW, 2, 7L);
    verify(claimReschedules)
        .handOff(
            eq(JOB_UUID), eq(7L), eq("release"), eq(FIXED_NOW), eq(2), any(), any(), any(), any());
  }

  @Test
  void reschedule_refusedHandoffRetriesOfferingAfterNextTransientRound() throws Exception {
    prepareClaimReschedule();
    when(claimReschedules.handOff(
            any(), anyLong(), any(), any(), anyInt(), any(), any(), any(), any()))
        .thenReturn(false, true);
    invokeClaimReschedule();
    verify(jobStore, times(4)).releaseClaim(JOB_UUID, "release", FIXED_NOW, 2, 7L);
    verify(claimReschedules, times(2))
        .handOff(
            eq(JOB_UUID), eq(7L), eq("release"), eq(FIXED_NOW), eq(2), any(), any(), any(), any());
  }

  @Test
  void reschedule_refusedHandoffStopsWhenInterruptedWithoutInterruptedStoreCall() throws Exception {
    prepareClaimReschedule();
    when(claimReschedules.handOff(
            any(), anyLong(), any(), any(), anyInt(), any(), any(), any(), any()))
        .thenAnswer(
            inv -> {
              Thread.currentThread().interrupt();
              return false;
            });
    try {
      invokeClaimReschedule();
      verify(jobStore, times(3)).releaseClaim(JOB_UUID, "release", FIXED_NOW, 2, 7L);
      Assertions.assertFalse(Thread.currentThread().isInterrupted());
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  void reschedule_interruptedQuickBackoffHandsOffWithoutAnotherStoreCall() throws Exception {
    prepareClaimReschedule();
    doAnswer(
            inv -> {
              Assertions.assertFalse(Thread.currentThread().isInterrupted());
              Thread.currentThread().interrupt();
              throw new RatchetTransientStoreException("interrupted write");
            })
        .when(jobStore)
        .releaseClaim(JOB_UUID, "release", FIXED_NOW, 2, 7L);
    when(claimReschedules.handOff(
            any(), anyLong(), any(), any(), anyInt(), any(), any(), any(), any()))
        .thenReturn(true);
    try {
      invokeClaimReschedule();
      verify(jobStore).releaseClaim(JOB_UUID, "release", FIXED_NOW, 2, 7L);
      verify(claimReschedules)
          .handOff(
              eq(JOB_UUID),
              eq(7L),
              eq("release"),
              eq(FIXED_NOW),
              eq(2),
              any(),
              any(),
              any(),
              any());
      Assertions.assertFalse(Thread.currentThread().isInterrupted());
    } finally {
      Thread.interrupted();
    }
  }

  private JobAttemptControl prepareClaimReschedule() {
    jobTask = newJobTaskWithTimeoutHandler(null);
    JobEntity job = createTestJob();
    job.setBusinessKey("held-key");
    job.setAttempts(2);
    job.setClaimSeq(7L);
    jobTask.init(job);
    JobAttemptControl attempt =
        new JobAttemptControl(JOB_UUID, FIXED_NOW.plusSeconds(30), 30, FIXED_NOW, 0, 7L, "node-1");
    jobTask.attach(attempt);
    lenient()
        .when(jobStore.releaseClaim(JOB_UUID, "release", FIXED_NOW, 2, 7L))
        .thenAnswer(
            inv -> {
              Assertions.assertFalse(Thread.currentThread().isInterrupted());
              throw new RatchetTransientStoreException("unavailable");
            });
    return attempt;
  }

  private void invokeClaimReschedule() throws Exception {
    Method method =
        JobTask.class.getDeclaredMethod(
            "rescheduleWithoutChargingAttempt",
            UUID.class,
            String.class,
            Instant.class,
            int.class,
            long.class,
            Runnable.class,
            Runnable.class,
            Runnable.class);
    method.setAccessible(true);
    method.invoke(
        jobTask,
        JOB_UUID,
        "release",
        FIXED_NOW,
        2,
        7L,
        (Runnable) () -> Assertions.fail("unexpected rejection"),
        (Runnable) () -> {},
        (Runnable) () -> {});
  }

  @Test
  void permitBackgroundRejectionReleasesHoldWithoutChangingExecution() throws Exception {
    JobAttemptControl attempt = assertNoChargeRescheduleRetries(false, false, true);
    Runnable callback = rejectedClaimCallback();
    callback.run();
    Assertions.assertTrue(attempt.isSuperseded());
    verify(jobStore).releaseBusinessKeyHold(JOB_UUID, 7L);
    verify(observabilityFacade, never()).saveExecution(any());
  }

  @Test
  void openCircuitBackgroundRejectionSkipsUncertainCancellationAndReleasesHold() throws Exception {
    assertOpenCircuitBackgroundRejection(true);
  }

  @Test
  void openCircuitBackgroundRejectionWithoutCancellationOnlyReleasesHold() throws Exception {
    assertOpenCircuitBackgroundRejection(false);
  }

  private void assertOpenCircuitBackgroundRejection(boolean canceled) throws Exception {
    jobTask = newJobTaskWithTimeoutHandler(null);
    JobEntity job = cancelScenario(true, 2);
    JobAttemptControl attempt = cancelAttempt();
    jobTask.attach(attempt);
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING);
    failBody(() -> new CircuitBreakerOpenException("Circuit breaker OPEN"));
    when(resilienceStrategy.getRetryDelay(anyString())).thenReturn(Duration.ofSeconds(1));
    when(jobStore.releaseClaim(eq(JOB_UUID), anyString(), any(), eq(2), eq(7L)))
        .thenThrow(new RatchetTransientStoreException("unavailable"));
    when(claimReschedules.handOff(
            any(), anyLong(), any(), any(), anyInt(), any(), any(), any(), any()))
        .thenReturn(true);
    jobTask.call();
    ArgumentCaptor<JobExecutionEntity> saved = ArgumentCaptor.forClass(JobExecutionEntity.class);
    verify(observabilityFacade).saveExecution(saved.capture());
    JobExecutionEntity execution = saved.getValue();
    Assertions.assertEquals(ExecutionStatus.FAILED, execution.getStatus());
    verify(jobStore, never()).releaseBusinessKeyHold(any(), anyLong());
    lenient()
        .when(jobStore.getJobStatus(JOB_UUID))
        .thenReturn(canceled ? JobStatus.CANCELED : JobStatus.RUNNING);
    clearInvocations(observabilityFacade);
    rejectedClaimCallback().run();
    verify(jobStore).releaseBusinessKeyHold(JOB_UUID, 7L);
    Assertions.assertTrue(attempt.isSuperseded());
    Assertions.assertEquals(ExecutionStatus.FAILED, execution.getStatus());
    verify(observabilityFacade, never()).saveExecution(any());
    verify(observabilityFacade, never()).recordJobCancellation(any());
  }

  private Runnable rejectedClaimCallback() {
    ArgumentCaptor<Runnable> callback = ArgumentCaptor.forClass(Runnable.class);
    verify(claimReschedules)
        .handOff(
            eq(JOB_UUID),
            eq(7L),
            anyString(),
            any(),
            eq(2),
            any(),
            any(),
            callback.capture(),
            any());
    return callback.getValue();
  }

  @SuppressWarnings("unchecked")
  private JobAttemptControl assertNoChargeRescheduleRetries(
      boolean superseded, boolean circuitOpen, boolean exhausted) throws Exception {
    jobTask = newJobTaskWithTimeoutHandler(null);
    JobEntity job = createTestJob();
    job.setResourceName("gpu");
    job.setBusinessKey("held-key");
    job.setAttempts(2);
    job.setClaimSeq(7L);
    initJobTaskWithDefaultStubs(job);
    JobAttemptControl attempt = cancelAttempt();
    jobTask.attach(attempt);
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING);
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(!circuitOpen);
    if (circuitOpen) {
      when(resilienceStrategy.getRetryDelay(anyString())).thenReturn(Duration.ofSeconds(1));
    } else {
      if (superseded) {
        when(resourcePermitService.tryAcquire("gpu", JOB_UUID, 7L, "node-1"))
            .thenReturn(AcquireResult.SUPERSEDED);
      } else {
        when(resourcePermitService.tryAcquire("gpu", JOB_UUID, 7L, "node-1"))
            .thenThrow(new RatchetTransientStoreException("acquire deadlock"));
      }
      when(resourcePermitService.getRetryDelay("gpu")).thenReturn(250);
    }
    var reschedule = when(jobStore.releaseClaim(eq(JOB_UUID), anyString(), any(), eq(2), eq(7L)));
    if (exhausted) {
      reschedule.thenThrow(new RatchetTransientStoreException("reschedule deadlock"));
    } else {
      reschedule
          .thenThrow(new RatchetTransientStoreException("reschedule deadlock"))
          .thenReturn(true);
    }

    if (exhausted)
      when(claimReschedules.handOff(
              any(), anyLong(), anyString(), any(), anyInt(), any(), any(), any(), any()))
          .thenReturn(true);
    jobTask.call();

    String reason =
        circuitOpen
            ? "Circuit breaker OPEN for service: JobTaskTest.testJobMethod"
            : superseded
                ? "Waiting for resource: gpu"
                : "Permit check failed transiently for resource: gpu";
    if (exhausted)
      verify(claimReschedules)
          .handOff(
              eq(JOB_UUID),
              eq(7L),
              eq(reason),
              eq(FIXED_NOW.plusMillis(circuitOpen ? 1000 : 250)),
              eq(2),
              any(),
              any(),
              any(),
              any());
    verify(jobStore, times(exhausted ? JobTask.TRANSIENT_STORE_TRIES : 2))
        .releaseClaim(
            eq(JOB_UUID),
            eq(reason),
            eq(FIXED_NOW.plusMillis(circuitOpen ? 1000 : 250)),
            eq(2),
            eq(7L));
    Assertions.assertEquals(2, job.getAttempts());
    verify(jobStore, never()).incrementRetryAttempt(any(UUID.class), anyLong(), any());
    verify(lifecycleFacade, never()).moveToDlq(any(), any());
    verify(lifecycleFacade, never())
        .completeFailure(any(), any(), anyBoolean(), nullable(Integer.class));
    verify(resilienceStrategy, never()).execute(anyString(), any(), any(Callable.class));
    verify(jobStore, never()).releaseBusinessKeyHold(any(), anyLong());
    verify(resourcePermitService, times(!circuitOpen && !superseded ? 1 : 0))
        .release("gpu", JOB_UUID, 7L);
    verifyNoAttemptRecorded();
    return attempt;
  }

  @Test
  void call_transientPermitRetryDelayFailureUsesDefault() throws Exception {
    assertPermitRetryFallback(false, false);
  }

  @Test
  void call_supersededPermitRetryDelayFailureUsesDefault() throws Exception {
    assertPermitRetryFallback(true, false);
  }

  @Test
  void call_permitReleaseFailureStillReschedules() throws Exception {
    assertPermitRetryFallback(false, true);
  }

  @SuppressWarnings("unchecked")
  private void assertPermitRetryFallback(boolean superseded, boolean releaseFails)
      throws Exception {
    jobTask = newJobTaskWithTimeoutHandler(null);
    JobEntity job = createTestJob();
    job.setResourceName("gpu");
    job.setAttempts(2);
    job.setClaimSeq(7L);
    initJobTaskWithDefaultStubs(job);
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING);
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);
    if (superseded) {
      when(resourcePermitService.tryAcquire("gpu", JOB_UUID, 7L, "node-1"))
          .thenReturn(AcquireResult.SUPERSEDED);
    } else {
      when(resourcePermitService.tryAcquire("gpu", JOB_UUID, 7L, "node-1"))
          .thenThrow(new RatchetTransientStoreException("deadlock"));
    }
    when(resourcePermitService.getRetryDelay("gpu"))
        .thenThrow(new RatchetTransientStoreException("lookup deadlock"));
    if (releaseFails) {
      doThrow(new RatchetTransientStoreException("release deadlock"))
          .when(resourcePermitService)
          .release("gpu", JOB_UUID, 7L);
    }
    when(jobStore.releaseClaim(eq(JOB_UUID), anyString(), any(), eq(2), eq(7L))).thenReturn(true);

    jobTask.call();

    InOrder order = inOrder(resourcePermitService, jobStore);
    if (!superseded) {
      verify(resourcePermitService, times(JobTask.TRANSIENT_STORE_TRIES))
          .tryAcquire("gpu", JOB_UUID, 7L, "node-1");
      order.verify(resourcePermitService).release("gpu", JOB_UUID, 7L);
    }
    order
        .verify(jobStore)
        .releaseClaim(
            eq(JOB_UUID),
            eq(
                superseded
                    ? "Waiting for resource: gpu"
                    : "Permit check failed transiently for resource: gpu"),
            eq(FIXED_NOW.plusMillis(ResourcePermitService.DEFAULT_RETRY_DELAY_MS)),
            eq(2),
            eq(7L));
    Assertions.assertEquals(2, job.getAttempts());
    verify(jobStore, never()).incrementRetryAttempt(any(UUID.class), anyLong(), any());
    verify(lifecycleFacade, never())
        .completeFailure(any(), any(), anyBoolean(), nullable(Integer.class));
    verify(resilienceStrategy, never()).execute(anyString(), any(), any(Callable.class));
    verifyNoAttemptRecorded();
  }

  @Test
  void claimedBatchChildOfCancelledParentNeverStarts() throws Exception {
    assertClosedBatchChildNeverStarts(JobStatus.CANCELED);
  }

  @Test
  void claimedBatchChildOfMissingParentNeverStarts() throws Exception {
    assertClosedBatchChildNeverStarts(null);
  }

  @Test
  void staleBatchChildClaimCannotCancelNewerAttempt() throws Exception {
    assertBatchChildNeverStarts(
        parentId -> {
          when(jobStore.getJobStatus(parentId)).thenReturn(JobStatus.CANCELED);
          when(jobStore.commitCompletion(any()))
              .thenAnswer(
                  invocation -> {
                    JobCompletionPlan plan = invocation.getArgument(0);
                    // Recovery has issued claim 8 while this task still owns claim 7.
                    Assertions.assertEquals(7L, plan.expectedClaimSeq());
                    return JobCompletionResult.notCommitted();
                  });
        },
        true);
    verify(jobStore, never()).scheduleJobRetry(any(), any(), any(), anyInt(), any());
    verify(jobStore, never()).releaseClaim(any(), any(), any(), anyInt(), anyLong());
  }

  @Test
  void claimedBatchChildNeverStartsWhenParentCheckFails() throws Exception {
    when(jobStore.releaseClaim(eq(JOB_UUID), any(), eq(FIXED_NOW.plusSeconds(30)), eq(2), eq(7L)))
        .thenReturn(true);
    assertBatchChildNeverStarts(
        parentId ->
            when(jobStore.getJobStatus(parentId))
                .thenThrow(new IllegalStateException("store unavailable")),
        false);
    verify(jobStore)
        .releaseClaim(eq(JOB_UUID), any(), eq(FIXED_NOW.plusSeconds(30)), eq(2), eq(7L));
  }

  @Test
  void parentCheckFailureLogsClaimReleaseFailureWithoutRethrowing() throws Exception {
    IllegalStateException releaseFailure = new IllegalStateException("release unavailable");
    when(jobStore.releaseClaim(eq(JOB_UUID), any(), eq(FIXED_NOW.plusSeconds(30)), eq(2), eq(7L)))
        .thenThrow(releaseFailure);
    Logger logger = Logger.getLogger(JobTask.class.getName());
    Level originalLevel = logger.getLevel();
    Handler handler = mock(Handler.class);
    logger.setLevel(Level.ALL);
    logger.addHandler(handler);
    try {
      assertBatchChildNeverStarts(
          parentId ->
              when(jobStore.getJobStatus(parentId))
                  .thenThrow(new IllegalStateException("parent unavailable")),
          false);

      ArgumentCaptor<LogRecord> records = ArgumentCaptor.forClass(LogRecord.class);
      verify(handler, atLeastOnce()).publish(records.capture());
      Assertions.assertTrue(
          records.getAllValues().stream()
              .anyMatch(
                  record ->
                      record.getThrown() == releaseFailure
                          && record.getLevel().intValue() >= Level.SEVERE.intValue()
                          && record.getMessage().contains("could not release claim")));
    } finally {
      logger.removeHandler(handler);
      logger.setLevel(originalLevel);
    }
  }

  @Test
  void claimedBatchChildNeverStartsWhenCancelFails() throws Exception {
    assertBatchChildNeverStarts(
        parentId -> {
          when(jobStore.getJobStatus(parentId)).thenReturn(JobStatus.CANCELED);
          when(jobStore.commitCompletion(any())).thenThrow(new IllegalStateException("deadlock"));
        },
        true);
  }

  private void assertClosedBatchChildNeverStarts(JobStatus parentStatus) throws Exception {
    assertBatchChildNeverStarts(
        parentId -> {
          when(jobStore.getJobStatus(parentId)).thenReturn(parentStatus);
          when(jobStore.commitCompletion(any())).thenReturn(new JobCompletionResult(true, null));
        },
        true);
  }

  private void assertBatchChildNeverStarts(Consumer<UUID> stubParent, boolean expectCancel)
      throws Exception {
    UUID parentId = UUID.randomUUID();
    JobEntity job = createTestJob();
    job.setStatus(JobStatus.RUNNING);
    job.setPickedBy("node-1");
    job.setJobType(JobExecutionType.BATCH_CHILD);
    job.setDependsOn(parentId);
    JobPayloadInvoker invoker = mock(JobPayloadInvoker.class);
    JobTask task = callbackTask(invoker);
    task.initFromClaim(
        new JobClaimDto(
            JOB_UUID,
            JobStatus.RUNNING,
            JobExecutionType.BATCH_CHILD,
            job.getPriority(),
            job.getScheduledTime(),
            1,
            30,
            "node-1",
            FIXED_NOW,
            null,
            2,
            3,
            null,
            parentId,
            7L));
    when(jobStore.findById(JOB_UUID)).thenReturn(Optional.of(job));
    when(nodeIdProvider.getNodeId()).thenReturn("node-1");
    stubParent.accept(parentId);
    MDC.put("jobId", "previous-job");
    MDC.put("node", "previous-node");

    task.call();

    Assertions.assertNull(MDC.get("jobId"));
    Assertions.assertNull(MDC.get("node"));
    verify(jobStore).getJobStatus(parentId);
    if (expectCancel) {
      ArgumentCaptor<JobCompletionPlan> plan = ArgumentCaptor.forClass(JobCompletionPlan.class);
      verify(jobStore).commitCompletion(plan.capture());
      Assertions.assertEquals(JOB_UUID, plan.getValue().jobId());
      Assertions.assertEquals(JobStatus.RUNNING, plan.getValue().expectedStatus());
      Assertions.assertEquals(JobStatus.CANCELED, plan.getValue().terminalStatus());
      Assertions.assertEquals(7L, plan.getValue().expectedClaimSeq());
      Assertions.assertEquals(parentId, plan.getValue().batchId());
    } else {
      verify(jobStore, never()).commitCompletion(any());
    }
    verify(jobStore, never()).cancelJob(any());
    verify(invoker, never()).invoke(any());
    verify(observabilityFacade, never()).recordJobStart(any());
    verify(observabilityFacade, never()).startExecution(any(), anyInt(), any());
    verify(observabilityFacade, never()).publishEvent(any());
    verify(lifecycleFacade, never())
        .completeFailure(any(), any(), anyBoolean(), nullable(Integer.class));
    Assertions.assertNull(JobContext.currentOrNull());
  }

  @Test
  void claimedBatchChildOfPendingParentRunsNormally() throws Exception {
    assertClaimedJobRuns(JobExecutionType.BATCH_CHILD, JobStatus.PENDING);
  }

  @Test
  void claimedBatchChildOfPausedParentRunsNormally() throws Exception {
    assertClaimedJobRuns(JobExecutionType.BATCH_CHILD, JobStatus.PAUSED);
  }

  @Test
  void claimedSingleJobDoesNotLookUpParent() throws Exception {
    assertClaimedJobRuns(JobExecutionType.SINGLE, null);
  }

  @SuppressWarnings("unchecked")
  private void assertClaimedJobRuns(JobExecutionType type, JobStatus parentStatus)
      throws Exception {
    UUID parentId = UUID.randomUUID();
    JobEntity job = createTestJob();
    job.setStatus(JobStatus.RUNNING);
    job.setPickedBy("node-1");
    job.setJobType(type);
    job.setDependsOn(parentId);
    JobPayloadInvoker invoker = mock(JobPayloadInvoker.class);
    JobTask task = callbackTask(invoker);
    initJobTaskWithDefaultStubs(task, job);
    task.initFromClaim(claimForNode("node-1"));
    when(jobStore.findById(JOB_UUID)).thenReturn(Optional.of(job));
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING);
    if (type == JobExecutionType.BATCH_CHILD) {
      when(jobStore.getJobStatus(parentId)).thenReturn(parentStatus);
    }
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);
    when(resilienceStrategy.execute(anyString(), any(), any(Callable.class)))
        .thenAnswer(inv -> ((Callable<?>) inv.getArgument(2)).call());
    when(invoker.materializeArguments(any(), any())).thenAnswer(inv -> inv.getArgument(0));
    when(lifecycleFacade.completeSuccess(
            any(JobEntity.class),
            any(),
            any(),
            any(),
            any(),
            anyLong(),
            anyLong(),
            nullable(Integer.class)))
        .thenReturn(true);

    task.call();

    verify(invoker).invoke(job.getPayload());
    verify(observabilityFacade).recordJobStart(job);
    verify(observabilityFacade).startExecution(eq(JOB_UUID), eq(1), eq("node-1"));
    verify(observabilityFacade).publishEvent(any(JobStartedEvent.class));
    verify(jobStore, never()).cancelJob(any());
    if (type == JobExecutionType.BATCH_CHILD) {
      verify(jobStore).getJobStatus(parentId);
    } else {
      verify(jobStore, never()).getJobStatus(parentId);
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  void call_claimedJobNoLongerRunning_skipsExecution() throws Exception {
    JobEntity job = createTestJob();
    job.setStatus(JobStatus.PENDING);
    job.setPickedBy("node-1");
    initJobTaskFromClaimWithDefaultStubs(claimForNode("node-1"), job);

    jobTask.call();

    verify(observabilityFacade, never()).startExecution(any(UUID.class), anyInt(), anyString());
    verify(resilienceStrategy, never()).execute(anyString(), any(), any(Callable.class));
    verify(jobStore, never()).getJobStatus(any(UUID.class));
  }

  @Test
  @SuppressWarnings("unchecked")
  void call_claimedJobPickedByAnotherNode_skipsExecution() throws Exception {
    JobEntity job = createTestJob();
    job.setStatus(JobStatus.RUNNING);
    job.setPickedBy("node-2");
    initJobTaskFromClaimWithDefaultStubs(claimForNode("node-1"), job);

    jobTask.call();

    verify(observabilityFacade, never()).startExecution(any(UUID.class), anyInt(), anyString());
    verify(resilienceStrategy, never()).execute(anyString(), any(), any(Callable.class));
    verify(jobStore, never()).getJobStatus(any(UUID.class));
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(booleans = {true, false})
  void completedPayloadSurvivesUnavailableCancellationReadWithoutReexecution(boolean transitionWins)
      throws Exception {
    JobEntity job = createTestJob();
    initJobTaskWithDefaultStubs(job);
    when(jobStore.getJobStatus(JOB_UUID))
        .thenReturn(JobStatus.RUNNING)
        .thenThrow(
            new IllegalStateException(
                "transaction rollback failed", new SQLException("connection closed", "08003")));
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);
    when(resilienceStrategy.execute(anyString(), any(), any(Callable.class)))
        .thenAnswer(inv -> ((Callable<?>) inv.getArgument(2)).call());
    when(lifecycleFacade.completeSuccess(
            any(JobEntity.class),
            any(),
            any(),
            any(),
            any(),
            anyLong(),
            anyLong(),
            nullable(Integer.class)))
        .thenReturn(transitionWins);

    jobTask.call();

    verify(resilienceStrategy, times(1)).execute(anyString(), any(), any(Callable.class));
    verify(lifecycleFacade, times(1))
        .completeSuccess(
            any(JobEntity.class),
            any(),
            any(),
            any(),
            any(),
            anyLong(),
            anyLong(),
            nullable(Integer.class));
    verify(jobStore, never()).incrementRetryAttempt(any(UUID.class), eq(0L), any());
    verify(lifecycleFacade, never()).moveToDlq(any(), any());
    verify(observabilityFacade, transitionWins ? times(1) : never())
        .recordJobSuccess(any(), anyLong());
  }

  @Test
  @SuppressWarnings("unchecked")
  void handleSuccess_retriesTransientFinalizationWithoutFailingJob() throws Exception {
    JobEntity job = createTestJob();
    initJobTaskWithDefaultStubs(job);
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING);
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);
    when(resilienceStrategy.execute(anyString(), any(), any(Callable.class)))
        .thenAnswer(inv -> ((Callable<?>) inv.getArgument(2)).call());
    when(lifecycleFacade.completeSuccess(
            any(JobEntity.class),
            any(),
            any(),
            any(),
            any(),
            anyLong(),
            anyLong(),
            nullable(Integer.class)))
        .thenThrow(new RatchetTransientStoreException("deadlock"))
        .thenReturn(true);

    jobTask.call();

    verify(lifecycleFacade, times(2))
        .completeSuccess(
            any(JobEntity.class),
            any(),
            any(),
            any(),
            any(),
            anyLong(),
            anyLong(),
            nullable(Integer.class));
    verify(lifecycleFacade, never())
        .completeSuccessMinimal(
            any(JobEntity.class), any(), any(), anyLong(), anyLong(), nullable(Integer.class));
    verify(jobStore, never()).incrementRetryAttempt(any(UUID.class), eq(0L), any());
    verify(lifecycleFacade, never()).moveToDlq(any(), any());
    verify(observabilityFacade).recordSuccessFinalizationRetry(job);
    verify(observabilityFacade, never()).recordSuccessFinalizationMinimal(any());
    verify(observabilityFacade, never()).recordSuccessFinalizationStuck(any());
    verify(observabilityFacade, never()).publishEvent(any(JobCompletedEvent.class));
  }

  @Test
  @SuppressWarnings("unchecked")
  void handleSuccess_fallsBackToMinimalSuccessAfterTransientFinalizationExhaustion()
      throws Exception {
    JobEntity job = createTestJob();
    initJobTaskWithDefaultStubs(job);
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING);
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);
    when(resilienceStrategy.execute(anyString(), any(), any(Callable.class)))
        .thenAnswer(inv -> ((Callable<?>) inv.getArgument(2)).call());
    when(lifecycleFacade.completeSuccess(
            any(JobEntity.class),
            any(),
            any(),
            any(),
            any(),
            anyLong(),
            anyLong(),
            nullable(Integer.class)))
        .thenThrow(new RatchetTransientStoreException("deadlock"));
    when(lifecycleFacade.completeSuccessMinimal(
            any(JobEntity.class), any(), any(), anyLong(), anyLong(), nullable(Integer.class)))
        .thenReturn(true);

    jobTask.call();

    verify(lifecycleFacade, times(5))
        .completeSuccess(
            any(JobEntity.class),
            any(),
            any(),
            any(),
            any(),
            anyLong(),
            anyLong(),
            nullable(Integer.class));
    verify(lifecycleFacade)
        .completeSuccessMinimal(
            any(JobEntity.class), any(), any(), anyLong(), anyLong(), nullable(Integer.class));
    verify(jobStore, never()).incrementRetryAttempt(any(UUID.class), eq(0L), any());
    verify(lifecycleFacade, never()).moveToDlq(any(), any());
    verify(observabilityFacade, times(5)).recordSuccessFinalizationRetry(job);
    verify(observabilityFacade).recordSuccessFinalizationMinimal(job);
    verify(observabilityFacade, never()).recordSuccessFinalizationStuck(any());
    verify(observabilityFacade, never()).publishEvent(any(JobCompletedEvent.class));
  }

  @Test
  @SuppressWarnings("unchecked")
  void handleSuccess_stuckFinalizationWithoutAttemptReturnsWithoutMinimalRetry() throws Exception {
    JobEntity job = createTestJob();
    initJobTaskWithDefaultStubs(job);
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING);
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);
    when(resilienceStrategy.execute(anyString(), any(), any(Callable.class)))
        .thenAnswer(inv -> ((Callable<?>) inv.getArgument(2)).call());
    when(lifecycleFacade.completeSuccess(
            any(JobEntity.class),
            any(),
            any(),
            any(),
            any(),
            anyLong(),
            anyLong(),
            nullable(Integer.class)))
        .thenThrow(new RatchetTransientStoreException("deadlock"));
    when(lifecycleFacade.completeSuccessMinimal(
            any(JobEntity.class), any(), any(), anyLong(), anyLong(), nullable(Integer.class)))
        .thenThrow(new RatchetTransientStoreException("deadlock"));

    jobTask.call();

    verify(lifecycleFacade, times(5))
        .completeSuccess(
            any(JobEntity.class),
            any(),
            any(),
            any(),
            any(),
            anyLong(),
            anyLong(),
            nullable(Integer.class));
    verify(lifecycleFacade)
        .completeSuccessMinimal(
            any(JobEntity.class), any(), any(), anyLong(), anyLong(), nullable(Integer.class));
    verify(jobStore, never()).incrementRetryAttempt(any(UUID.class), eq(0L), any());
    verify(lifecycleFacade, never()).moveToDlq(any(), any());
    verify(observabilityFacade, times(5)).recordSuccessFinalizationRetry(job);
    verify(observabilityFacade).recordSuccessFinalizationStuck(job);
    verify(observabilityFacade, never()).publishEvent(any(JobCompletedEvent.class));
    verify(observabilityFacade, never()).recordJobSuccess(any(), anyLong());
    verify(observabilityFacade, never()).saveExecution(any(JobExecutionEntity.class));
  }

  private ContinuationScenario stuckSuccessScenario(AtomicInteger bodies) throws Exception {
    var scenario = continuationScenario(bodies::incrementAndGet, 1000);
    when(scenario.results().serialize(any(), any())).thenReturn(SerializedJobResult.empty());
    jobTask.setTerminalRetryDelays(1, 2);
    when(lifecycleFacade.completeSuccess(
            any(), any(), any(), any(), any(), anyLong(), anyLong(), nullable(Integer.class)))
        .thenThrow(new RatchetTransientStoreException("busy"));
    return scenario;
  }

  @Test
  void handleSuccess_stuckFinalizationDoesNotMoveSuccessfulJobToFailurePath() throws Exception {
    AtomicInteger bodies = new AtomicInteger();
    var scenario = stuckSuccessScenario(bodies);
    when(lifecycleFacade.completeSuccessMinimal(
            any(), any(), any(), anyLong(), anyLong(), nullable(Integer.class)))
        .thenThrow(new RatchetTransientStoreException("busy"))
        .thenThrow(new RatchetTransientStoreException("busy"))
        .thenThrow(new RatchetTransientStoreException("busy"))
        .thenReturn(true);

    jobTask.call();

    Assertions.assertEquals(1, bodies.get());
    Assertions.assertEquals(JobStatus.SUCCEEDED, scenario.job().getStatus());
    Assertions.assertEquals(ExecutionStatus.SUCCEEDED, scenario.execution().getStatus());
    Assertions.assertNull(scenario.job().getJobResult());
    Assertions.assertNull(scenario.job().getResultType());
    verify(lifecycleFacade, times(4))
        .completeSuccessMinimal(any(), any(), any(), anyLong(), anyLong(), nullable(Integer.class));
    verify(observabilityFacade).recordJobSuccess(eq(scenario.job()), anyLong());
    verify(observabilityFacade).saveExecution(scenario.execution());
    verify(observabilityFacade).recordSuccessFinalizationStuck(scenario.job());
    verify(observabilityFacade).recordSuccessFinalizationMinimal(scenario.job());
    verify(jobStore, never()).incrementRetryAttempt(any(), any(), any());
    verify(lifecycleFacade, never())
        .completeFailure(any(), any(), anyBoolean(), nullable(Integer.class));
    verify(lifecycleFacade, never()).moveToDlq(any(), any());
  }

  @Test
  void handleSuccess_interruptedSlowFinalizationContinuesAndRestoresInterrupt() throws Exception {
    AtomicInteger bodies = new AtomicInteger();
    var scenario = stuckSuccessScenario(bodies);
    AtomicInteger writes = new AtomicInteger();
    when(lifecycleFacade.completeSuccessMinimal(
            any(), any(), any(), anyLong(), anyLong(), nullable(Integer.class)))
        .thenAnswer(
            inv -> {
              int write = writes.incrementAndGet();
              if (write == 2) Thread.currentThread().interrupt();
              if (write <= 2) throw new RatchetTransientStoreException("busy");
              return true;
            });

    try {
      jobTask.call();
      Assertions.assertTrue(Thread.currentThread().isInterrupted());
    } finally {
      Thread.interrupted();
    }

    Assertions.assertEquals(1, bodies.get());
    Assertions.assertEquals(3, writes.get());
    Assertions.assertEquals(ExecutionStatus.SUCCEEDED, scenario.execution().getStatus());
    verify(observabilityFacade).recordJobSuccess(eq(scenario.job()), anyLong());
    verify(observabilityFacade).saveExecution(scenario.execution());
    verify(jobStore, never()).incrementRetryAttempt(any(), any(), any());
    verify(lifecycleFacade, never())
        .completeFailure(any(), any(), anyBoolean(), nullable(Integer.class));
  }

  @Test
  void handleSuccess_stuckThenTerminalSkippedDiscardsExecution() throws Exception {
    AtomicInteger bodies = new AtomicInteger();
    var scenario = stuckSuccessScenario(bodies);
    when(lifecycleFacade.completeSuccessMinimal(
            any(), any(), any(), anyLong(), anyLong(), nullable(Integer.class)))
        .thenThrow(new RatchetTransientStoreException("busy"))
        .thenReturn(false);

    jobTask.call();

    Assertions.assertEquals(1, bodies.get());
    Assertions.assertTrue(scenario.attempt().isSuperseded());
    Assertions.assertEquals(ExecutionStatus.FAILED, scenario.execution().getStatus());
    verify(observabilityFacade).saveExecution(scenario.execution());
    verify(observabilityFacade, never()).recordJobSuccess(any(), anyLong());
    verify(jobStore, never()).incrementRetryAttempt(any(), any(), any());
    verify(lifecycleFacade, never())
        .completeFailure(any(), any(), anyBoolean(), nullable(Integer.class));
  }

  @Test
  void handleSuccess_fenceBounceDuringSlowFinalizationStillDiscardsExecution() throws Exception {
    AtomicInteger bodies = new AtomicInteger();
    var scenario = stuckSuccessScenario(bodies);
    AtomicInteger writes = new AtomicInteger();
    when(lifecycleFacade.completeSuccessMinimal(
            any(), any(), any(), anyLong(), anyLong(), nullable(Integer.class)))
        .thenAnswer(
            inv -> {
              int write = writes.incrementAndGet();
              if (write == 2) {
                scenario.attempt().markSuperseded();
              }
              if (write <= 2) throw new RatchetTransientStoreException("busy");
              return false;
            });

    jobTask.call();

    Assertions.assertEquals(1, bodies.get());
    Assertions.assertEquals(3, writes.get());
    Assertions.assertTrue(scenario.attempt().isSuperseded());
    Assertions.assertFalse(scenario.attempt().isRevoked());
    Assertions.assertEquals(ExecutionStatus.FAILED, scenario.execution().getStatus());
    Assertions.assertEquals(
        JobExecutionEntity.RESULT_DISCARDED_ERROR_MESSAGE, scenario.execution().getErrorMessage());
    verify(observabilityFacade).saveExecution(scenario.execution());
    verify(observabilityFacade, never()).recordJobSuccess(any(), anyLong());
    verify(jobStore, never()).incrementRetryAttempt(any(), any(), any());
    verify(lifecycleFacade, never())
        .completeFailure(any(), any(), anyBoolean(), nullable(Integer.class));
  }

  @Test
  void handleSuccess_revokedSlowFinalizationExitsAndRestoresInterrupt() throws Exception {
    AtomicInteger bodies = new AtomicInteger();
    var scenario = stuckSuccessScenario(bodies);
    CountDownLatch slowWrite = new CountDownLatch(1);
    CountDownLatch shutdown = new CountDownLatch(1);
    AtomicInteger writes = new AtomicInteger();
    AtomicBoolean interruptedAfterReturn = new AtomicBoolean();
    AtomicReference<Throwable> workerFailure = new AtomicReference<>();
    when(lifecycleFacade.completeSuccessMinimal(
            any(), any(), any(), anyLong(), anyLong(), nullable(Integer.class)))
        .thenAnswer(
            inv -> {
              if (writes.incrementAndGet() == 2) {
                slowWrite.countDown();
                try {
                  Assertions.assertTrue(shutdown.await(5, TimeUnit.SECONDS));
                } catch (InterruptedException interrupted) {
                  Thread.currentThread().interrupt();
                }
              }
              throw new RatchetTransientStoreException("busy");
            });
    Thread worker =
        new Thread(
            () -> {
              CONTINUATION_PAYLOAD.set(bodies::incrementAndGet);
              try {
                jobTask.call();
                interruptedAfterReturn.set(Thread.currentThread().isInterrupted());
              } catch (Throwable failure) {
                workerFailure.set(failure);
              } finally {
                CONTINUATION_PAYLOAD.remove();
                JobContext.clear();
              }
            });
    worker.start();
    try {
      Assertions.assertTrue(slowWrite.await(5, TimeUnit.SECONDS));
      scenario.attempt().revoke();
      worker.interrupt();
    } finally {
      shutdown.countDown();
      worker.join(5_000);
      if (worker.isAlive()) {
        scenario.attempt().revoke();
        worker.interrupt();
        worker.join(5_000);
      }
    }
    Assertions.assertFalse(worker.isAlive());
    Assertions.assertNull(workerFailure.get());
    Assertions.assertTrue(interruptedAfterReturn.get());
    Assertions.assertEquals(1, bodies.get());
    Assertions.assertEquals(ExecutionStatus.RUNNING, scenario.execution().getStatus());
    verify(observabilityFacade, never()).recordJobSuccess(any(), anyLong());
    verify(observabilityFacade, never()).saveExecution(any());
    verify(jobStore, never()).incrementRetryAttempt(any(), any(), any());
    verify(lifecycleFacade, never())
        .completeFailure(any(), any(), anyBoolean(), nullable(Integer.class));
  }

  @Test
  @SuppressWarnings("unchecked")
  void call_deserializesStructuredSignalDecisionIntoJobContext() throws Exception {
    SignalDecision decision = SignalDecision.rejected("payload", "denied");
    PayloadSerializer signalSerializer =
        new PayloadSerializer() {
          @Override
          public String serialize(Object payload) {
            return null;
          }

          @Override
          @SuppressWarnings("unchecked")
          public <T> T deserialize(String json, Class<T> type) {
            // Inner payload is deserialized as Object; return the String value
            return (T) "payload";
          }
        };
    ResultPersistenceStrategy resultPersistenceStrategy =
        (jobId, result) -> SerializedJobResult.empty();
    JobTask signalTask =
        new JobTask(
            jobStore,
            resourcePermitService,
            lifecycleFacade,
            nodeIdProvider,
            observabilityFacade,
            validationFacade,
            new JobPayloadInvoker(beanResolver, classPolicy),
            new JobTransaction(),
            new JobSuccessFinalizer(lifecycleFacade, observabilityFacade),
            retryPolicy,
            resilienceStrategy,
            errorSanitizer,
            context -> noopLogger(),
            resultPersistenceStrategy,
            null,
            signalSerializer,
            mock(AfterCommitRegistrar.class),
            mock(JobStateFlusher.class),
            null,
            new HeldKeyReleases(jobStore, () -> holdReleaseScheduler),
            new PermitReleases(resourcePermitService, () -> permitReleaseScheduler, null),
            claimReschedules,
            outcomeRetries,
            Clock.systemUTC(),
            null,
            RatchetOptions.defaults());
    JobEntity job = createTestJob();
    job.setPayload(
        new JobPayload(
            JobTaskTest.class.getName(),
            "captureSignalDecision",
            "()Ljava/lang/String;",
            true,
            List.of(),
            null));
    job.setSignalPayload("\"payload\"");
    job.setSignalPayloadType(DefaultJobSchedulerService.SIGNAL_PAYLOAD_TYPE_DECISION);
    job.setSignalOutcome("REJECTED");
    job.setSignalRejectionReason("denied");
    initJobTaskWithDefaultStubs(signalTask, job);
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING);
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);
    when(resilienceStrategy.execute(anyString(), any(), any(Callable.class)))
        .thenAnswer(inv -> ((Callable<?>) inv.getArgument(2)).call());
    when(lifecycleFacade.completeSuccess(
            any(JobEntity.class),
            any(),
            any(),
            any(),
            any(),
            anyLong(),
            anyLong(),
            nullable(Integer.class)))
        .thenReturn(true);

    signalTask.call();

    Assertions.assertEquals(decision, OBSERVED_SIGNAL_DECISION.get());
  }

  @Test
  void call_invalidPersistedSignalOutcomeFailsWithJobContext() throws Exception {
    JobEntity job = createTestJob();
    job.setSignalPayloadType(DefaultJobSchedulerService.SIGNAL_PAYLOAD_TYPE_DECISION);
    job.setSignalOutcome("UNKNOWN");
    jobTask.init(job);
    when(nodeIdProvider.getNodeId()).thenReturn("node-1");
    DoNotRetryPolicy realPolicy = new DoNotRetryPolicy();
    when(validationFacade.shouldNotRetry(any()))
        .thenAnswer(inv -> realPolicy.shouldNotRetry(inv.getArgument(0)));
    when(errorSanitizer.sanitize(any())).thenReturn("safe invalid signal outcome");
    when(lifecycleFacade.completeFailure(
            any(JobEntity.class), eq(JobStatus.RUNNING), eq(false), nullable(Integer.class)))
        .thenReturn(true);

    jobTask.call();

    ArgumentCaptor<Throwable> failure = ArgumentCaptor.forClass(Throwable.class);
    verify(observabilityFacade).recordJobFailure(eq(job), failure.capture(), eq(1));
    SignalOutcomeHydrationException exception =
        Assertions.assertInstanceOf(SignalOutcomeHydrationException.class, failure.getValue());
    Assertions.assertEquals(
        "Failed to hydrate signal outcome for job "
            + JOB_UUID
            + ": persisted value 'UNKNOWN' is not a recognized SignalDecision.Outcome",
        exception.getMessage());
    Assertions.assertEquals(JOB_UUID, exception.getJobId());
    Assertions.assertEquals("UNKNOWN", exception.getPersistedOutcome());
    Assertions.assertInstanceOf(IllegalArgumentException.class, exception.getCause());
    verify(jobStore, never()).incrementRetryAttempt(any(UUID.class), eq(0L), any());
    verify(lifecycleFacade)
        .completeFailure(eq(job), eq(JobStatus.RUNNING), eq(false), nullable(Integer.class));
  }

  @Test
  @SuppressWarnings("unchecked")
  void call_deserializesRawSerializableSignalIntoJobContext() throws Exception {
    // Real Yasson: deserializing to Serializable.class (the old form) throws because the abstract
    // target cannot be instantiated, which is how the payload was silently lost. Object.class
    // round-trips the JSON-native value, so the executing job observes it.
    JsonbTestPayloadSerializer signalSerializer = new JsonbTestPayloadSerializer();
    ResultPersistenceStrategy resultPersistenceStrategy =
        (jobId, result) -> SerializedJobResult.empty();
    JobTask signalTask =
        new JobTask(
            jobStore,
            resourcePermitService,
            lifecycleFacade,
            nodeIdProvider,
            observabilityFacade,
            validationFacade,
            new JobPayloadInvoker(beanResolver, classPolicy),
            new JobTransaction(),
            new JobSuccessFinalizer(lifecycleFacade, observabilityFacade),
            retryPolicy,
            resilienceStrategy,
            errorSanitizer,
            context -> noopLogger(),
            resultPersistenceStrategy,
            null,
            signalSerializer,
            mock(AfterCommitRegistrar.class),
            mock(JobStateFlusher.class),
            null,
            new HeldKeyReleases(jobStore, () -> holdReleaseScheduler),
            new PermitReleases(resourcePermitService, () -> permitReleaseScheduler, null),
            claimReschedules,
            outcomeRetries,
            Clock.systemUTC(),
            null,
            RatchetOptions.defaults());
    JobEntity job = createTestJob();
    job.setPayload(
        new JobPayload(
            JobTaskTest.class.getName(),
            "captureSignalString",
            "()Ljava/lang/String;",
            true,
            List.of(),
            null));
    job.setSignalPayload(signalSerializer.serialize("hello"));
    // Any non-DECISION marker drives the raw-Serializable branch.
    job.setSignalPayloadType("RAW");
    initJobTaskWithDefaultStubs(signalTask, job);
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING);
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);
    when(resilienceStrategy.execute(anyString(), any(), any(Callable.class)))
        .thenAnswer(inv -> ((Callable<?>) inv.getArgument(2)).call());
    when(lifecycleFacade.completeSuccess(
            any(JobEntity.class),
            any(),
            any(),
            any(),
            any(),
            anyLong(),
            anyLong(),
            nullable(Integer.class)))
        .thenReturn(true);

    signalTask.call();

    Assertions.assertEquals("hello", OBSERVED_SIGNAL_STRING.get());
  }

  @Test
  void call_signalPayloadDecryptionFailureMovesJobToFailurePath() throws Exception {
    EncryptionHolder.install(
        List.of(new FailingDecryptEngine()),
        FailingDecryptEngine.ALGORITHM_ID,
        new EncryptionTestKit.Provider(),
        true);
    JobEntity job = createTestJob();
    job.setSignalPayload(
        PayloadEncryptor.encryptValue(
            "\"hello\"", true, EncryptionTarget.signal(job.getSignalKey())));
    jobTask.init(job);
    when(nodeIdProvider.getNodeId()).thenReturn("node-1");
    when(validationFacade.shouldNotRetry(any(Throwable.class))).thenReturn(true);
    when(errorSanitizer.sanitize(any(Throwable.class))).thenReturn("safe decrypt failure");
    when(lifecycleFacade.completeFailure(
            any(JobEntity.class), eq(JobStatus.RUNNING), eq(false), nullable(Integer.class)))
        .thenReturn(true);

    jobTask.call();

    verify(lifecycleFacade)
        .completeFailure(
            any(JobEntity.class), eq(JobStatus.RUNNING), eq(false), nullable(Integer.class));
    // Poison surfaces with its true type now (no IllegalArgumentException wrap); it is still DLQ'd.
    verify(observabilityFacade)
        .recordJobFailure(eq(job), any(PayloadDecryptionException.class), eq(1));
    verify(observabilityFacade, never()).startExecution(any(UUID.class), anyInt(), anyString());
    verify(resilienceStrategy, never()).execute(anyString(), any(), any(Callable.class));
    verify(lifecycleFacade)
        .completeFailure(eq(job), eq(JobStatus.RUNNING), eq(false), nullable(Integer.class));
  }

  private static final AtomicReference<String> OBSERVED_ARG = new AtomicReference<>();

  public static String captureArg(String value) {
    OBSERVED_ARG.set(value);
    return "done";
  }

  @Test
  @SuppressWarnings("unchecked")
  void call_consultsArgResolverAndDispatchesPatchedArguments() throws Exception {
    OBSERVED_ARG.set(null);
    JsonbTestPayloadSerializer serializer = new JsonbTestPayloadSerializer();
    // The resolver patches the argument AND tries to redirect the target; only the arguments are
    // honored — the dispatch target stays pinned to the payload the security gate validated.
    PreExecutionArgResolver resolver =
        (jobId, invocation) ->
            new JobInvocation(
                "com.example.Evil",
                "evil",
                invocation.methodDescriptor(),
                true,
                List.of("patched"),
                null);
    JobTask resolving =
        new JobTask(
            jobStore,
            resourcePermitService,
            lifecycleFacade,
            nodeIdProvider,
            observabilityFacade,
            validationFacade,
            new JobPayloadInvoker(beanResolver, classPolicy),
            new JobTransaction(),
            new JobSuccessFinalizer(lifecycleFacade, observabilityFacade),
            retryPolicy,
            resilienceStrategy,
            errorSanitizer,
            context -> new JBossLoggingJobLogger(context.jobId(), null),
            new DefaultResultPersistenceStrategy(RatchetOptions.defaults(), serializer, null),
            null,
            serializer,
            mock(AfterCommitRegistrar.class),
            mock(JobStateFlusher.class),
            null,
            new HeldKeyReleases(jobStore, () -> holdReleaseScheduler),
            new PermitReleases(resourcePermitService, () -> permitReleaseScheduler, null),
            claimReschedules,
            outcomeRetries,
            Clock.systemUTC(),
            resolver,
            RatchetOptions.defaults());

    JobEntity job = createTestJob();
    job.setPayload(
        new JobPayload(
            JobTaskTest.class.getName(),
            "captureArg",
            "(Ljava/lang/String;)Ljava/lang/String;",
            true,
            List.of("original"),
            null));
    initJobTaskWithDefaultStubs(resolving, job);
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING);
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);
    when(resilienceStrategy.execute(anyString(), any(), any(Callable.class)))
        .thenAnswer(inv -> ((Callable<?>) inv.getArgument(2)).call());
    when(lifecycleFacade.completeSuccess(
            any(JobEntity.class),
            any(),
            any(),
            any(),
            any(),
            anyLong(),
            anyLong(),
            nullable(Integer.class)))
        .thenReturn(true);

    resolving.call();

    Assertions.assertEquals(
        "patched", OBSERVED_ARG.get(), "the resolver-patched argument must be dispatched");
  }

  private JobEntity createTestJob() {
    JobEntity job = new JobEntity();
    job.setClaimSeq(0L);
    job.setId(JOB_UUID);
    job.setJobType(JobExecutionType.SINGLE);
    job.setPriority(JobPriority.NORMAL);
    job.setMaxRetries(3);
    job.setPayload(
        new JobPayload(
            JobTaskTest.class.getName(),
            "testJobMethod",
            "()Ljava/lang/String;",
            true,
            List.of(),
            null));
    return job;
  }

  private static JobClaimDto claimForNode(String pickedBy) {
    return new JobClaimDto(
        JOB_UUID,
        JobStatus.RUNNING,
        JobExecutionType.SINGLE,
        JobPriority.NORMAL,
        Instant.EPOCH,
        1,
        30,
        pickedBy,
        Instant.EPOCH,
        null,
        0,
        3,
        null,
        null,
        0L);
  }

  private void initJobTaskWithDefaultStubs(JobEntity job) {
    initJobTaskWithDefaultStubs(jobTask, job);
  }

  private void initJobTaskFromClaimWithDefaultStubs(JobClaimDto claim, JobEntity job) {
    jobTask.initFromClaim(claim);
    when(jobStore.findById(claim.id())).thenReturn(Optional.of(job));
    lenient().when(jobStore.findByIdLatest(claim.id())).thenReturn(Optional.of(job));
    when(nodeIdProvider.getNodeId()).thenReturn("node-1");
  }

  private void initJobTaskWithDefaultStubs(JobTask task, JobEntity job) {
    task.init(job);
    lenient().when(jobStore.findByIdLatest(job.getId())).thenReturn(Optional.of(job));
    when(nodeIdProvider.getNodeId()).thenReturn("node-1");
    // Lenient: a job that stops at a pre-execution gate records no attempt and never calls these.
    lenient()
        .when(observabilityFacade.startExecution(any(UUID.class), anyInt(), anyString()))
        .thenReturn(JobExecutionEntity.start(job.getId(), 1, "node-1"));
    lenient()
        .when(observabilityFacade.startExecutionScope(any(JobEntity.class)))
        .thenReturn(TracingCollector.NoOpExecutionScope.INSTANCE);
  }

  /** A job that stops at a pre-execution gate never ran its body, so it records no attempt. */
  private void verifyNoAttemptRecorded() {
    verify(observabilityFacade, never()).recordJobStart(any());
    verify(observabilityFacade, never()).startExecution(any(), anyInt(), any());
    verify(observabilityFacade, never()).saveExecution(any());
    verify(observabilityFacade, never()).publishEvent(any(JobStartedEvent.class));
    verify(observabilityFacade, never()).startExecutionScope(any());
  }

  private JobTask newJobTaskWithTimeoutHandler(JobTimeoutHandler timeoutHandler) {
    ResultPersistenceStrategy resultPersistenceStrategy =
        (jobId, result) -> SerializedJobResult.empty();
    return new JobTask(
        jobStore,
        resourcePermitService,
        lifecycleFacade,
        nodeIdProvider,
        observabilityFacade,
        validationFacade,
        new JobPayloadInvoker(beanResolver, classPolicy),
        new JobTransaction(),
        new JobSuccessFinalizer(lifecycleFacade, observabilityFacade),
        retryPolicy,
        resilienceStrategy,
        errorSanitizer,
        context -> noopLogger(),
        resultPersistenceStrategy,
        null,
        new JsonbTestPayloadSerializer(),
        checkpointRegistrar,
        jobStateFlusher,
        timeoutHandler,
        new HeldKeyReleases(jobStore, () -> holdReleaseScheduler),
        new PermitReleases(resourcePermitService, () -> permitReleaseScheduler, null),
        claimReschedules,
        outcomeRetries,
        FIXED_CLOCK,
        null,
        RatchetOptions.defaults());
  }

  private JobTask newJobTaskWithClock(Clock taskClock) {
    ResultPersistenceStrategy resultPersistenceStrategy =
        (jobId, result) -> SerializedJobResult.empty();
    return new JobTask(
        jobStore,
        resourcePermitService,
        lifecycleFacade,
        nodeIdProvider,
        observabilityFacade,
        validationFacade,
        new JobPayloadInvoker(beanResolver, classPolicy),
        new JobTransaction(),
        new JobSuccessFinalizer(lifecycleFacade, observabilityFacade),
        retryPolicy,
        resilienceStrategy,
        errorSanitizer,
        context -> noopLogger(),
        resultPersistenceStrategy,
        null,
        null,
        mock(AfterCommitRegistrar.class),
        mock(JobStateFlusher.class),
        null,
        new HeldKeyReleases(jobStore, () -> holdReleaseScheduler),
        new PermitReleases(resourcePermitService, () -> permitReleaseScheduler, null),
        claimReschedules,
        outcomeRetries,
        taskClock,
        null,
        RatchetOptions.defaults());
  }

  /** Encrypts to a real frame but always fails to decrypt — a tampered/wrong-key analogue. */
  private static final class FailingDecryptEngine implements PayloadEncryption {
    static final String ALGORITHM_ID = "FAIL-DECRYPT";

    @Override
    public String algorithmId() {
      return ALGORITHM_ID;
    }

    @Override
    public byte[] encrypt(byte[] plaintext, EncryptionContext ctx) {
      return new byte[] {1, 2, 3};
    }

    @Override
    public byte[] decrypt(byte[] ciphertext, EncryptionContext ctx) {
      throw new PayloadDecryptionException("wrong key");
    }
  }

  public static class AnnotatedJobTarget {

    /** Returns the annotated job result. */
    @CircuitBreakerProtected(
        service = "external-api",
        recordExceptions = IllegalStateException.class,
        ignoreExceptions = IllegalArgumentException.class)
    public static String annotatedJobMethod() {
      return "done";
    }
  }

  public record CustomArgument(String reference, int attempt) implements Serializable {}

  @Test
  @SuppressWarnings("unchecked")
  void rejectedOwnerRetryUsesSequenceAndPublishesNoEventOrDlq() throws Exception {
    JobEntity job = createTestJob();
    job.setMaxRetries(3);
    job.setClaimSeq(37L);

    JobTimeoutHandler timeoutHandler =
        new JobTimeoutHandler(
            jobStore,
            jobStore,
            jobStore,
            lifecycleFacade,
            80,
            60L,
            FIXED_CLOCK,
            null,
            null,
            null,
            JobTimeoutHandler.DEFAULT_SIGNAL_TIMEOUT_BATCH_SIZE);

    JobTask task = newJobTaskWithTimeoutHandler(timeoutHandler);
    initJobTaskWithDefaultStubs(task, job);
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING);
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);
    var failure = new RuntimeException("stale body failed");
    when(resilienceStrategy.execute(anyString(), any(), any(Callable.class))).thenThrow(failure);
    when(validationFacade.shouldNotRetry(failure)).thenReturn(false);
    when(jobStore.incrementRetryAttempt(JOB_UUID, 37L, 0)).thenReturn(-1);

    JobAttemptControl attempt =
        new JobAttemptControl(JOB_UUID, FIXED_NOW.plusSeconds(30), 30, FIXED_NOW, 0, 37L, "node-1");
    task.attach(attempt);
    task.call();
    Assertions.assertTrue(attempt.isSuperseded());

    ArgumentCaptor<JobExecutionEntity> execution =
        ArgumentCaptor.forClass(JobExecutionEntity.class);
    verify(observabilityFacade).saveExecution(execution.capture());
    Assertions.assertEquals(ExecutionStatus.FAILED, execution.getValue().getStatus());
    Assertions.assertEquals(RuntimeException.class.getName(), execution.getValue().getErrorClass());
    Assertions.assertEquals(
        RuntimeException.class.getName(), execution.getValue().getErrorMessage());
    verify(jobStore).incrementRetryAttempt(JOB_UUID, 37L, 0);
    verify(jobStore, never()).scheduleJobRetry(any(), any(), any(), anyInt(), any());
    verify(lifecycleFacade, never())
        .completeFailure(any(), any(), anyBoolean(), nullable(Integer.class));
    verify(lifecycleFacade, never()).moveToDlq(any(), any());
    verify(observabilityFacade, never()).publishEvent(any(JobRetryingEvent.class));
  }

  @Test
  @SuppressWarnings("unchecked")
  void executionHistoryAndTracingUseSanitizedFailure() throws Exception {
    assertSanitizedFailure(false);
  }

  @Test
  @SuppressWarnings("unchecked")
  void failureSinksFallBackToClassNameWhenSanitizerThrows() throws Exception {
    assertSanitizedFailure(true);
  }

  @SuppressWarnings("unchecked")
  private void assertSanitizedFailure(boolean sanitizerThrows) throws Exception {
    JobEntity job = createTestJob();
    initJobTaskWithDefaultStubs(job);
    TracingCollector.ExecutionScope scope = mock(TracingCollector.ExecutionScope.class);
    when(observabilityFacade.startExecutionScope(job)).thenReturn(scope);
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING);
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);
    RuntimeException failure =
        new IllegalStateException("jdbc:mysql://user:hunter2@localhost/db person@example.com");
    doThrow(failure).when(resilienceStrategy).execute(anyString(), any(), any(Callable.class));
    when(validationFacade.shouldNotRetry(failure)).thenReturn(true);
    when(lifecycleFacade.completeFailure(
            eq(job), eq(JobStatus.RUNNING), eq(false), nullable(Integer.class)))
        .thenReturn(true);
    String expected;
    if (sanitizerThrows) {
      when(errorSanitizer.sanitize(failure)).thenThrow(new AssertionError("sanitizer failed"));
      expected = failure.getClass().getName();
    } else {
      expected = new DefaultErrorSanitizer().sanitize(failure);
      when(errorSanitizer.sanitize(failure)).thenReturn(expected);
    }

    jobTask.call();

    ArgumentCaptor<JobExecutionEntity> history = ArgumentCaptor.forClass(JobExecutionEntity.class);
    verify(observabilityFacade).saveExecution(history.capture());
    Assertions.assertEquals(failure.getClass().getName(), history.getValue().getErrorClass());
    Assertions.assertEquals(expected, history.getValue().getErrorMessage());
    Assertions.assertEquals(expected, job.getLastError());
    verify(scope).failure(failure.getClass().getName(), expected, job.getAttempts() + 1);
    verify(errorSanitizer, times(1)).sanitize(failure);
    if (!sanitizerThrows) {
      Assertions.assertTrue(expected.contains("***REDACTED***"));
    }
    Assertions.assertFalse(history.getValue().getErrorMessage().contains("hunter2"));
    Assertions.assertFalse(history.getValue().getErrorMessage().contains("person@example.com"));
  }

  @Test
  void userCancelledBodyStop_withCanceledStatus_endsCanceled() throws Exception {
    // A cooperative user stop closes the attempt as canceled without failure or timeout work.
    JobTimeoutHandler timeout = mock(JobTimeoutHandler.class);
    jobTask = newJobTaskWithTimeoutHandler(timeout);
    JobEntity job = cancelScenario(false);
    CallbackRecorder.reset();
    SerializableBiConsumer<JobContext, Throwable> callback = CallbackRecorder::failure;
    job.setOnFailurePayload(JobPayloadFactory.fromLambda(callback));
    JobAttemptControl attempt = cancelAttempt();
    jobTask.attach(attempt);
    attempt.requestUserCancel();
    failBody(() -> new CancellationRequestedException("user stop"));

    jobTask.call();

    verifyCanceledExecution(job);
    verify(jobStore, never()).incrementRetryAttempt(any(), anyLong(), any());
    verify(timeout, never()).processCooperativeTimeout(any());
    verify(jobStore, never()).scheduleJobRetry(any(), any(), any(), anyInt(), any());
    verify(lifecycleFacade, never())
        .completeFailure(any(), any(), anyBoolean(), nullable(Integer.class));
    verify(lifecycleFacade, never())
        .moveToDlqAndHandlePermanentFailure(any(), any(), nullable(Integer.class));
    verify(lifecycleFacade, never()).cancelChain(any());
    Assertions.assertNull(CallbackRecorder.context);
  }

  @Test
  void userCancelledBodyStop_withUnknownStatus_isNotHandledAsCancel() throws Exception {
    // An absent fresh status is insufficient to classify a cooperative failure as user cancel.
    jobTask = newJobTaskWithTimeoutHandler(mock(JobTimeoutHandler.class));
    JobEntity job = cancelScenario(false);
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING, (JobStatus) null);
    JobAttemptControl attempt = cancelAttempt();
    jobTask.attach(attempt);
    attempt.requestUserCancel();
    failBody(() -> new CancellationRequestedException("user stop"));

    jobTask.call();

    verify(observabilityFacade, never()).recordJobCancellation(any());
    verify(observabilityFacade, never())
        .saveExecution(argThat(execution -> execution.getStatus() == ExecutionStatus.CANCELED));
  }

  @Test
  void supersededAttemptFailure_withCanceledStatus_endsCanceled() throws Exception {
    // A canceled superseded attempt closes canceled rather than recording a deferred failure.
    JobEntity job = cancelScenario(false);
    JobAttemptControl attempt = cancelAttempt();
    jobTask.attach(attempt);
    failBody(
        () -> {
          attempt.markSuperseded();
          return new IllegalStateException("superseded");
        });
    jobTask.call();
    verifyCanceledExecution(job);
  }

  @Test
  void rejectedRetryIncrement_withCanceledStatus_endsCanceled() throws Exception {
    // A rejected retry increment consults the cancellation status before deferring failure.
    JobEntity job = cancelScenario(false);
    jobTask.attach(cancelAttempt());
    failBody(() -> new IllegalStateException("body failed"));
    when(jobStore.incrementRetryAttempt(JOB_UUID, 7L, 0)).thenReturn(-1);
    jobTask.call();
    verifyCanceledExecution(job);
  }

  @Test
  void terminalSkippedSuccess_withCanceledStatus_endsCanceled() throws Exception {
    // A success rejected by terminal state closes canceled rather than discarding the result.
    JobEntity job = cancelScenario(false);
    jobTask.attach(cancelAttempt());
    when(jobStore.getJobStatus(JOB_UUID))
        .thenReturn(JobStatus.RUNNING, JobStatus.RUNNING, JobStatus.CANCELED);
    succeedBody();
    jobTask.call();
    verify(lifecycleFacade)
        .completeSuccess(
            eq(job), any(), any(), any(), any(), anyLong(), anyLong(), nullable(Integer.class));
    verifyCanceledExecution(job);
  }

  @Test
  void stillInterruptedWorker_handsTheHoldReleaseOff() throws Exception {
    // The mocked body bypasses the invocation that clears a body's interrupt, so the flag is still
    // set at the release, as after an interrupt from outside Ratchet. The worker hands the release
    // to the scheduler and keeps its interrupt.
    JobEntity job = cancelScenario(true);
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING);
    JobAttemptControl attempt = cancelAttempt();
    jobTask.attach(attempt);
    failBody(
        () -> {
          attempt.markSuperseded();
          Thread.currentThread().interrupt();
          return new IllegalStateException("interrupted");
        });
    try {
      jobTask.call();
      verify(jobStore).getJobStatus(JOB_UUID);
      verify(jobStore, never()).releaseBusinessKeyHold(any(), anyLong());
      Assertions.assertTrue(Thread.currentThread().isInterrupted());
      ArgumentCaptor<Runnable> release = ArgumentCaptor.forClass(Runnable.class);
      verify(holdReleaseScheduler).schedule(release.capture(), eq(0L), eq(TimeUnit.MILLISECONDS));
      Thread.interrupted();
      release.getValue().run();
      verify(jobStore).releaseBusinessKeyHold(JOB_UUID, 7L);
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  void interruptedBody_releasesTheHoldWithTheFlagClearAndRestoresIt() throws Exception {
    // A body's interrupt is cleared before the worker releases the hold, so the release runs on
    // the worker and the interrupt comes back when the task ends.
    JobPayloadInvoker invoker = mock(JobPayloadInvoker.class);
    jobTask = callbackTask(invoker);
    JobEntity job = createTestJob();
    job.setBusinessKey("held");
    job.setClaimSeq(7L);
    initJobTaskWithDefaultStubs(job);
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING);
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);
    when(invoker.materializeArguments(any(), any())).thenAnswer(inv -> inv.getArgument(0));
    when(resilienceStrategy.execute(anyString(), any(), any(Callable.class)))
        .thenAnswer(inv -> ((Callable<?>) inv.getArgument(2)).call());
    JobAttemptControl attempt = cancelAttempt();
    jobTask.attach(attempt);
    when(invoker.invoke(any()))
        .thenAnswer(
            inv -> {
              attempt.markSuperseded();
              Thread.currentThread().interrupt();
              throw new IllegalStateException("interrupted");
            });
    AtomicBoolean interruptedAtRelease = new AtomicBoolean(true);
    when(jobStore.releaseBusinessKeyHold(JOB_UUID, 7L))
        .thenAnswer(
            inv -> {
              interruptedAtRelease.set(Thread.currentThread().isInterrupted());
              return true;
            });
    try {
      jobTask.call();
      verify(jobStore).releaseBusinessKeyHold(JOB_UUID, 7L);
      Assertions.assertFalse(interruptedAtRelease.get());
      verifyNoInteractions(holdReleaseScheduler);
      Assertions.assertTrue(Thread.currentThread().isInterrupted());
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  void failedPermitRelease_isRetriedForTheSameClaim() throws Exception {
    // A store failure in the attempt's one release must not leave the permit until the node
    // stops. The release is retried on the scheduler for the same resource, job and claim.
    JobEntity job = createTestJob();
    job.setResourceName("gpu");
    job.setClaimSeq(7L);
    initJobTaskWithDefaultStubs(job);
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING);
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);
    when(resourcePermitService.tryAcquire("gpu", JOB_UUID, 7L, "node-1"))
        .thenReturn(AcquireResult.ACQUIRED);
    succeedBody();
    when(lifecycleFacade.completeSuccess(
            any(JobEntity.class),
            any(),
            any(),
            any(),
            any(),
            anyLong(),
            anyLong(),
            nullable(Integer.class)))
        .thenReturn(true);
    doThrow(new RatchetTransientStoreException("lock wait timeout"))
        .doNothing()
        .when(resourcePermitService)
        .release("gpu", JOB_UUID, 7L);

    jobTask.call();

    ArgumentCaptor<Runnable> retry = ArgumentCaptor.forClass(Runnable.class);
    verify(permitReleaseScheduler).schedule(retry.capture(), eq(5_000L), eq(TimeUnit.MILLISECONDS));
    retry.getValue().run();
    verify(resourcePermitService, times(2)).release("gpu", JOB_UUID, 7L);
    verifyNoMoreInteractions(permitReleaseScheduler);
  }

  @Test
  void stillInterruptedWorker_handsThePermitReleaseOff() throws Exception {
    // A timeout or a shutdown supersedes the attempt and interrupts the worker. The mocked body
    // bypasses the invocation that clears a body's interrupt, so the flag is still set at the
    // release. The worker does not call the store on an interrupted thread: it hands the release
    // to the scheduler at once and keeps its interrupt.
    JobEntity job = createTestJob();
    job.setResourceName("gpu");
    job.setClaimSeq(7L);
    initJobTaskWithDefaultStubs(job);
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING);
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);
    when(resourcePermitService.tryAcquire("gpu", JOB_UUID, 7L, "node-1"))
        .thenReturn(AcquireResult.ACQUIRED);
    JobAttemptControl attempt = cancelAttempt();
    jobTask.attach(attempt);
    failBody(
        () -> {
          attempt.markSuperseded();
          Thread.currentThread().interrupt();
          return new IllegalStateException("interrupted");
        });
    try {
      jobTask.call();
      verify(resourcePermitService, never()).release(anyString(), any(UUID.class), anyLong());
      Assertions.assertTrue(Thread.currentThread().isInterrupted());
      ArgumentCaptor<Runnable> release = ArgumentCaptor.forClass(Runnable.class);
      verify(permitReleaseScheduler).schedule(release.capture(), eq(0L), eq(TimeUnit.MILLISECONDS));
      Thread.interrupted();
      release.getValue().run();
      verify(resourcePermitService).release("gpu", JOB_UUID, 7L);
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  void canceledKeyedJob_releasesItsHoldOnce() throws Exception {
    // A canceled keyed attempt releases its own claim's hold exactly once when its body ends.
    cancelScenario(true);
    jobTask.attach(cancelAttempt());
    succeedBody();
    jobTask.call();
    verify(jobStore).releaseBusinessKeyHold(JOB_UUID, 7L);
  }

  @Test
  void canceledJobWithoutKey_releasesNoHold() throws Exception {
    // A canceled job without a business key never calls the hold store.
    JobEntity job = cancelScenario(false);
    jobTask.attach(cancelAttempt());
    succeedBody();
    jobTask.call();
    verifyCanceledExecution(job);
    verify(jobStore, never()).releaseBusinessKeyHold(any(), anyLong());
  }

  @Test
  void succeededKeyedJob_releasesNoHold() throws Exception {
    // A successful keyed job leaves ordinary reservation cleanup to completion.
    JobEntity job = cancelScenario(true);
    jobTask.attach(cancelAttempt());
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING);
    when(lifecycleFacade.completeSuccess(
            eq(job), any(), any(), any(), any(), anyLong(), anyLong(), nullable(Integer.class)))
        .thenReturn(true);
    succeedBody();
    jobTask.call();
    verify(observabilityFacade).recordJobSuccess(eq(job), anyLong());
    verify(jobStore, never()).releaseBusinessKeyHold(any(), anyLong());
  }

  @Test
  void canceledBeforeStart_keyedJob_releasesHold() throws Exception {
    // An entity-initialized task canceled before its body releases the claim's held key.
    JobEntity job = createTestJob();
    job.setBusinessKey("held");
    job.setClaimSeq(7L);
    initJobTaskWithDefaultStubs(job);
    jobTask.attach(cancelAttempt());
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.CANCELED);
    jobTask.call();
    verify(jobStore).releaseBusinessKeyHold(JOB_UUID, 7L);
    verifyNoAttemptRecorded();
  }

  @Test
  void claimTaskWhoseRowIsCanceled_keyedJob_releasesHoldWithClaimSeq() throws Exception {
    // A canceled loaded row releases the original claim sequence before returning early.
    assertCanceledClaimHold(true);
    verify(jobStore).releaseBusinessKeyHold(JOB_UUID, 7L);
  }

  @Test
  void claimTaskWhoseRowIsCanceled_keylessJob_releasesNoHold() throws Exception {
    // A keyless loaded row returns early without any held-key cleanup.
    assertCanceledClaimHold(false);
    verify(jobStore, never()).releaseBusinessKeyHold(any(), anyLong());
  }

  private void assertCanceledClaimHold(boolean keyed) throws Exception {
    JobEntity job = createTestJob();
    job.setStatus(JobStatus.CANCELED);
    job.setBusinessKey(keyed ? "held" : null);
    job.setClaimSeq(99L);
    JobClaimDto claim =
        new JobClaimDto(
            JOB_UUID,
            JobStatus.RUNNING,
            JobExecutionType.SINGLE,
            JobPriority.NORMAL,
            Instant.EPOCH,
            1,
            30,
            "node-1",
            Instant.EPOCH,
            null,
            0,
            3,
            null,
            null,
            7L);
    initJobTaskFromClaimWithDefaultStubs(claim, job);
    jobTask.call();
    verify(resilienceStrategy, never()).execute(anyString(), any(), any(Callable.class));
    verifyNoAttemptRecorded();
  }

  @Test
  void supersededPermitAcquire_keyedJob_releasesHold() throws Exception {
    // A rejected reschedule after superseded permit acquisition releases that claim's held key.
    JobEntity job = createTestJob();
    job.setBusinessKey("held");
    job.setResourceName("gpu");
    job.setClaimSeq(7L);
    initJobTaskWithDefaultStubs(job);
    jobTask.attach(cancelAttempt());
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING);
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);
    when(resourcePermitService.tryAcquire("gpu", JOB_UUID, 7L, "node-1"))
        .thenReturn(AcquireResult.SUPERSEDED);
    when(resourcePermitService.getRetryDelay("gpu")).thenReturn(250);
    jobTask.call();
    verify(jobStore).releaseClaim(eq(JOB_UUID), anyString(), any(), eq(0), eq(7L));
    verify(jobStore).releaseBusinessKeyHold(JOB_UUID, 7L);
    verifyNoAttemptRecorded();
  }

  @Test
  void canceledKeyedJobWhoseSignalPayloadFails_releasesItsHold() throws Exception {
    // A cancel that wins while the signal payload fails to load still frees the claim's held key.
    JobEntity job = createTestJob();
    job.setClaimSeq(7L);
    job.setBusinessKey("held");
    job.setSignalPayloadType(DefaultJobSchedulerService.SIGNAL_PAYLOAD_TYPE_DECISION);
    job.setSignalOutcome("UNKNOWN");
    initJobTaskWithDefaultStubs(job);
    JobAttemptControl attempt = cancelAttempt();
    attempt.requestUserCancel();
    jobTask.attach(attempt);
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.CANCELED);

    jobTask.call();

    verify(observabilityFacade).recordJobCancellation(job);
    verify(jobStore).releaseBusinessKeyHold(JOB_UUID, 7L);
  }

  @Test
  void retryThatLosesItsWriteToACancel_endsCanceled() throws Exception {
    // A cancel that commits between the attempt increment and the retry write ends canceled, with
    // no failure metric and no failed tracing scope.
    JobEntity job = cancelScenario(true);
    TracingCollector.ExecutionScope scope = mock(TracingCollector.ExecutionScope.class);
    when(observabilityFacade.startExecutionScope(job)).thenReturn(scope);
    jobTask.attach(cancelAttempt());
    RuntimeException error = new IllegalStateException("retry me");
    failBody(() -> error);
    when(jobStore.incrementRetryAttempt(JOB_UUID, 7L, 0)).thenReturn(1);
    when(retryPolicy.shouldRetry(1, error)).thenReturn(true);
    when(retryPolicy.getDelay(1)).thenReturn(Duration.ofSeconds(5));

    AtomicReference<JobStatus> status = new AtomicReference<>(JobStatus.RUNNING);
    when(jobStore.getJobStatus(JOB_UUID)).thenAnswer(invocation -> status.get());
    when(jobStore.scheduleJobRetry(eq(JOB_UUID), any(), any(), anyInt(), eq(7L)))
        .thenAnswer(
            invocation -> {
              status.set(JobStatus.CANCELED);
              return false;
            });
    jobTask.call();
    verify(jobStore).scheduleJobRetry(eq(JOB_UUID), any(), any(), anyInt(), eq(7L));

    ArgumentCaptor<JobExecutionEntity> execution =
        ArgumentCaptor.forClass(JobExecutionEntity.class);
    verify(observabilityFacade, atLeastOnce()).saveExecution(execution.capture());
    Assertions.assertEquals(ExecutionStatus.CANCELED, execution.getValue().getStatus());
    verify(observabilityFacade).recordJobCancellation(job);
    verify(observabilityFacade, never()).recordJobFailure(any(), any(), anyInt());
    verify(scope, never()).failure(any(), any(), anyInt());
    verify(scope).close();
    verify(jobStore).releaseBusinessKeyHold(JOB_UUID, 7L);
  }

  @Test
  void dlqMoveThatLosesItsWriteToACancel_endsCanceled() throws Exception {
    // A cancel that commits between the attempt increment and the DLQ write ends canceled, with
    // no failure metric, no failed tracing scope and no onFailure callback.
    JobEntity job = cancelScenario(true);
    CallbackRecorder.reset();
    SerializableBiConsumer<JobContext, Throwable> callback = CallbackRecorder::failure;
    job.setOnFailurePayload(JobPayloadFactory.fromLambda(callback));
    TracingCollector.ExecutionScope scope = mock(TracingCollector.ExecutionScope.class);
    when(observabilityFacade.startExecutionScope(job)).thenReturn(scope);
    jobTask.attach(cancelAttempt());
    RuntimeException error = new IllegalStateException("out of retries");
    failBody(() -> error);
    when(jobStore.incrementRetryAttempt(JOB_UUID, 7L, 0)).thenReturn(1);
    when(retryPolicy.shouldRetry(1, error)).thenReturn(false);

    AtomicReference<JobStatus> status = new AtomicReference<>(JobStatus.RUNNING);
    when(jobStore.getJobStatus(JOB_UUID)).thenAnswer(invocation -> status.get());
    when(lifecycleFacade.completeFailure(
            eq(job), eq(JobStatus.RUNNING), eq(false), nullable(Integer.class)))
        .thenAnswer(
            invocation -> {
              status.set(JobStatus.CANCELED);
              return false;
            });
    jobTask.call();
    verify(lifecycleFacade)
        .completeFailure(eq(job), eq(JobStatus.RUNNING), eq(false), nullable(Integer.class));

    ArgumentCaptor<JobExecutionEntity> execution =
        ArgumentCaptor.forClass(JobExecutionEntity.class);
    verify(observabilityFacade, atLeastOnce()).saveExecution(execution.capture());
    Assertions.assertEquals(ExecutionStatus.CANCELED, execution.getValue().getStatus());
    verify(observabilityFacade).recordJobCancellation(job);
    verify(observabilityFacade, never()).recordJobFailure(any(), any(), anyInt());
    verify(scope, never()).failure(any(), any(), anyInt());
    verify(scope).close();
    verify(jobStore).releaseBusinessKeyHold(JOB_UUID, 7L);
    Assertions.assertNull(CallbackRecorder.context);
  }

  @Test
  void cooperativeTimeoutThatFindsTheJobCanceled_endsCanceled() throws Exception {
    // A cancel between the worker's post-body check and the timeout read ends canceled.
    // The worker releases the hold with its claim sequence.
    jobTask = newJobTaskWithTimeoutHandler(realTimeoutHandler());
    JobEntity job = cancelScenario(true);
    TracingCollector.ExecutionScope scope = mock(TracingCollector.ExecutionScope.class);
    when(observabilityFacade.startExecutionScope(job)).thenReturn(scope);
    JobAttemptControl attempt = cancelAttempt();
    jobTask.attach(attempt);
    attempt.requestCancellation();
    failBody(() -> new CancellationRequestedException("grace stop"));
    List<ExecutionStatus> saved = savedExecutionStatuses();
    AtomicReference<JobStatus> status = new AtomicReference<>(JobStatus.RUNNING);
    when(jobStore.getJobStatus(JOB_UUID)).thenAnswer(invocation -> status.get());
    when(jobStore.findById(JOB_UUID))
        .thenAnswer(
            invocation -> {
              status.set(JobStatus.CANCELED);
              return Optional.of(storedRow(JobStatus.CANCELED));
            });

    jobTask.call();

    verify(jobStore).findById(JOB_UUID);
    Assertions.assertEquals(List.of(ExecutionStatus.FAILED, ExecutionStatus.CANCELED), saved);
    verify(observabilityFacade).recordJobCancellation(job);
    verify(jobStore).releaseBusinessKeyHold(JOB_UUID, 7L);
    verify(observabilityFacade, never()).recordJobFailure(any(), any(), anyInt());
    verify(scope, never()).failure(any(), any(), anyInt());
    verify(scope).close();
    verify(jobStore, never()).incrementRetryAttempt(any(), any(), any());
    verify(jobStore, never()).scheduleJobRetry(any(), any(), any(), anyInt(), any());
    verify(lifecycleFacade, never())
        .completeTimeoutFailure(any(), any(), anyBoolean(), any(), any());
    Assertions.assertTrue(attempt.isSuperseded());
  }

  @Test
  void cooperativeTimeoutCanceledSaveThatFailsOnce_isSavedOnRetry() throws Exception {
    // A store blip on the canceled execution save is retried, so the history still ends canceled.
    jobTask = newJobTaskWithTimeoutHandler(realTimeoutHandler());
    JobEntity job = cancelScenario(true);
    TracingCollector.ExecutionScope scope = mock(TracingCollector.ExecutionScope.class);
    when(observabilityFacade.startExecutionScope(job)).thenReturn(scope);
    JobAttemptControl attempt = cancelAttempt();
    jobTask.attach(attempt);
    attempt.requestCancellation();
    failBody(() -> new CancellationRequestedException("grace stop"));
    List<ExecutionStatus> saved = new ArrayList<>();
    AtomicBoolean failedOnce = new AtomicBoolean();
    when(observabilityFacade.saveExecution(any(JobExecutionEntity.class)))
        .thenAnswer(
            invocation -> {
              JobExecutionEntity execution = invocation.getArgument(0);
              saved.add(execution.getStatus());
              if (execution.getStatus() == ExecutionStatus.CANCELED
                  && failedOnce.compareAndSet(false, true)) {
                throw new IllegalStateException("execution store blip");
              }
              return execution;
            });
    AtomicReference<JobStatus> status = new AtomicReference<>(JobStatus.RUNNING);
    when(jobStore.getJobStatus(JOB_UUID)).thenAnswer(invocation -> status.get());
    when(jobStore.findById(JOB_UUID))
        .thenAnswer(
            invocation -> {
              status.set(JobStatus.CANCELED);
              return Optional.of(storedRow(JobStatus.CANCELED));
            });

    jobTask.call();

    verify(observabilityFacade).recordJobCancellation(job);
    Assertions.assertEquals(
        List.of(ExecutionStatus.FAILED, ExecutionStatus.CANCELED, ExecutionStatus.CANCELED), saved);
    verify(jobStore).releaseBusinessKeyHold(JOB_UUID, 7L);
    verify(observabilityFacade, never()).recordJobFailure(any(), any(), anyInt());
    verify(lifecycleFacade, never())
        .completeFailure(any(), any(), anyBoolean(), nullable(Integer.class));
    Assertions.assertTrue(attempt.isSuperseded());
  }

  @Test
  void retryIncrementCanceledSaveThatKeepsFailing_stillRecordsTheCancel() throws Exception {
    // A canceled execution save that fails twice is logged. The cancel still counts, the hold is
    // released, and the job is not forced to FAILED.
    JobEntity job = cancelScenario(true);
    jobTask.attach(cancelAttempt());
    failBody(() -> new IllegalStateException("body failed"));
    when(jobStore.incrementRetryAttempt(JOB_UUID, 7L, 0)).thenReturn(-1);
    List<ExecutionStatus> saved = new ArrayList<>();
    when(observabilityFacade.saveExecution(any(JobExecutionEntity.class)))
        .thenAnswer(
            invocation -> {
              JobExecutionEntity execution = invocation.getArgument(0);
              saved.add(execution.getStatus());
              throw new IllegalStateException("execution store down");
            });

    jobTask.call();

    verify(observabilityFacade).recordJobCancellation(job);
    verify(lifecycleFacade, never())
        .completeFailure(any(), any(), anyBoolean(), nullable(Integer.class));
    Assertions.assertEquals(List.of(ExecutionStatus.CANCELED, ExecutionStatus.CANCELED), saved);
    verify(jobStore).releaseBusinessKeyHold(JOB_UUID, 7L);
    verify(observabilityFacade, never()).recordJobFailure(any(), any(), anyInt());
  }

  @Test
  void cooperativeTimeoutRetryThatLosesItsWriteToACancel_endsCanceled() throws Exception {
    // A cancel after the timeout handler's read makes its retry increment miss.
    // The attempt still ends canceled.
    jobTask = newJobTaskWithTimeoutHandler(realTimeoutHandler());
    JobEntity job = cancelScenario(true);
    TracingCollector.ExecutionScope scope = mock(TracingCollector.ExecutionScope.class);
    when(observabilityFacade.startExecutionScope(job)).thenReturn(scope);
    JobAttemptControl attempt = cancelAttempt();
    jobTask.attach(attempt);
    attempt.requestCancellation();
    failBody(() -> new CancellationRequestedException("grace stop"));
    List<ExecutionStatus> saved = savedExecutionStatuses();
    AtomicReference<JobStatus> status = new AtomicReference<>(JobStatus.RUNNING);
    when(jobStore.getJobStatus(JOB_UUID)).thenAnswer(invocation -> status.get());
    when(jobStore.findById(JOB_UUID)).thenReturn(Optional.of(storedRow(JobStatus.RUNNING)));
    when(jobStore.incrementRetryAttempt(JOB_UUID, 7L, 0))
        .thenAnswer(
            invocation -> {
              status.set(JobStatus.CANCELED);
              return -1;
            });

    jobTask.call();

    verify(jobStore).incrementRetryAttempt(JOB_UUID, 7L, 0);
    Assertions.assertEquals(List.of(ExecutionStatus.FAILED, ExecutionStatus.CANCELED), saved);
    verify(observabilityFacade).recordJobCancellation(job);
    verify(jobStore).releaseBusinessKeyHold(JOB_UUID, 7L);
    verify(observabilityFacade, never()).recordJobFailure(any(), any(), anyInt());
    verify(scope, never()).failure(any(), any(), anyInt());
    verify(scope).close();
    verify(jobStore, never()).scheduleJobRetry(any(), any(), any(), anyInt(), any());
    verify(lifecycleFacade, never())
        .completeTimeoutFailure(any(), any(), anyBoolean(), any(), any());
  }

  @Test
  void circuitBreakerRescheduleThatLosesItsWriteToACancel_endsCanceled() throws Exception {
    // A cancel before the reschedule after an open-circuit rejection ends canceled, not failed.
    JobEntity job = cancelScenario(true);
    TracingCollector.ExecutionScope scope = mock(TracingCollector.ExecutionScope.class);
    when(observabilityFacade.startExecutionScope(job)).thenReturn(scope);
    jobTask.attach(cancelAttempt());
    failBody(() -> new CircuitBreakerOpenException("Circuit breaker OPEN"));
    when(resilienceStrategy.getRetryDelay(anyString())).thenReturn(Duration.ofMillis(250));
    List<ExecutionStatus> saved = savedExecutionStatuses();
    AtomicReference<JobStatus> status = new AtomicReference<>(JobStatus.RUNNING);
    when(jobStore.getJobStatus(JOB_UUID)).thenAnswer(invocation -> status.get());
    when(jobStore.releaseClaim(eq(JOB_UUID), any(), any(), anyInt(), eq(7L)))
        .thenAnswer(
            invocation -> {
              status.set(JobStatus.CANCELED);
              return false;
            });

    jobTask.call();

    verify(jobStore).releaseClaim(eq(JOB_UUID), any(), any(), anyInt(), eq(7L));
    Assertions.assertEquals(List.of(ExecutionStatus.FAILED, ExecutionStatus.CANCELED), saved);
    verify(observabilityFacade).recordJobCancellation(job);
    verify(jobStore).releaseBusinessKeyHold(JOB_UUID, 7L);
    verify(observabilityFacade, never()).recordJobFailure(any(), any(), anyInt());
    verify(scope, never()).failure(any(), any(), anyInt());
    verify(scope).close();
    verify(jobStore, never()).incrementRetryAttempt(any(), any(), any());
  }

  @Test
  @SuppressWarnings("unchecked")
  void openCircuitRescheduleThatLosesItsWriteToACancel_recordsTheCancelWithoutAnAttempt()
      throws Exception {
    // When the circuit is open before the body starts and a cancel wins the reschedule,
    // the cancel is recorded and no attempt is.
    JobEntity job = cancelScenario(true);
    jobTask.attach(cancelAttempt());
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(false);
    when(resilienceStrategy.getRetryDelay(anyString())).thenReturn(Duration.ofMillis(250));
    AtomicReference<JobStatus> status = new AtomicReference<>(JobStatus.RUNNING);
    when(jobStore.getJobStatus(JOB_UUID)).thenAnswer(invocation -> status.get());
    when(jobStore.releaseClaim(eq(JOB_UUID), any(), any(), anyInt(), eq(7L)))
        .thenAnswer(
            invocation -> {
              status.set(JobStatus.CANCELED);
              return false;
            });

    jobTask.call();

    verify(jobStore).releaseClaim(eq(JOB_UUID), any(), any(), anyInt(), eq(7L));
    verify(observabilityFacade).recordJobCancellation(job);
    verify(jobStore).releaseBusinessKeyHold(JOB_UUID, 7L);
    verifyNoAttemptRecorded();
    verify(resilienceStrategy, never()).execute(anyString(), any(), any(Callable.class));
  }

  @Test
  @SuppressWarnings("unchecked")
  void unboundArgumentDlqMoveThatLosesItsWriteToACancel_endsCanceled() throws Exception {
    // A cancel before the dead-letter move of a job with an unbound argument ends canceled,
    // not failed.
    JobEntity job = cancelScenario(true);
    JobPayload original = job.getPayload();
    job.setPayload(
        new JobPayload(
            original.target(),
            original.method(),
            original.methodDescriptor(),
            original.isStatic(),
            Arrays.asList((Object) null),
            List.of(0)));
    TracingCollector.ExecutionScope scope = mock(TracingCollector.ExecutionScope.class);
    when(observabilityFacade.startExecutionScope(job)).thenReturn(scope);
    jobTask.attach(cancelAttempt());
    List<ExecutionStatus> saved = savedExecutionStatuses();
    AtomicReference<JobStatus> status = new AtomicReference<>(JobStatus.RUNNING);
    when(jobStore.getJobStatus(JOB_UUID)).thenAnswer(invocation -> status.get());
    when(lifecycleFacade.moveToDlqAndHandlePermanentFailure(
            eq(job), any(), nullable(Integer.class)))
        .thenAnswer(
            invocation -> {
              status.set(JobStatus.CANCELED);
              return false;
            });

    jobTask.call();

    verify(lifecycleFacade)
        .moveToDlqAndHandlePermanentFailure(eq(job), any(), nullable(Integer.class));
    Assertions.assertEquals(List.of(ExecutionStatus.FAILED, ExecutionStatus.CANCELED), saved);
    verify(observabilityFacade).recordJobCancellation(job);
    verify(jobStore).releaseBusinessKeyHold(JOB_UUID, 7L);
    verify(observabilityFacade, never()).recordJobFailure(any(), any(), anyInt());
    verify(scope, never()).failure(any(), any(), anyInt());
    verify(scope).close();
    verify(resilienceStrategy, never()).execute(anyString(), any(), any(Callable.class));
  }

  @Test
  void lastResortFailureThatLosesItsWriteToACancel_endsCanceled() throws Exception {
    // When failure handling fails and a cancel wins the last-resort failure write,
    // the attempt ends canceled and onFailure does not run.
    JobEntity job = cancelScenario(true);
    CallbackRecorder.reset();
    SerializableBiConsumer<JobContext, Throwable> callback = CallbackRecorder::failure;
    job.setOnFailurePayload(JobPayloadFactory.fromLambda(callback));
    TracingCollector.ExecutionScope scope = mock(TracingCollector.ExecutionScope.class);
    when(observabilityFacade.startExecutionScope(job)).thenReturn(scope);
    jobTask.attach(cancelAttempt());
    RuntimeException original = new RuntimeException("original");
    failBody(() -> original);
    // Break failure handling before any write, so the last-resort path runs.
    when(validationFacade.shouldNotRetry(original))
        .thenThrow(new IllegalStateException("validation failed"));
    List<ExecutionStatus> saved = savedExecutionStatuses();
    AtomicReference<JobStatus> status = new AtomicReference<>(JobStatus.RUNNING);
    when(jobStore.getJobStatus(JOB_UUID)).thenAnswer(invocation -> status.get());
    when(lifecycleFacade.completeFailure(
            eq(job), eq(JobStatus.RUNNING), eq(false), nullable(Integer.class)))
        .thenAnswer(
            invocation -> {
              status.set(JobStatus.CANCELED);
              return false;
            });

    jobTask.call();

    verify(lifecycleFacade)
        .completeFailure(eq(job), eq(JobStatus.RUNNING), eq(false), nullable(Integer.class));
    Assertions.assertEquals(List.of(ExecutionStatus.CANCELED), saved);
    verify(observabilityFacade).recordJobCancellation(job);
    verify(jobStore).releaseBusinessKeyHold(JOB_UUID, 7L);
    verify(observabilityFacade, never()).recordJobFailure(any(), any(), anyInt());
    verify(scope, never()).failure(any(), any(), anyInt());
    verify(scope).close();
    Assertions.assertNull(CallbackRecorder.context);
  }

  @Test
  void retryRecordsOneFailureAfterTheRetryWrite() throws Exception {
    // A retried failure counts one failure and fails the tracing scope after the retry write.
    JobEntity job = cancelScenario(false);
    TracingCollector.ExecutionScope scope = mock(TracingCollector.ExecutionScope.class);
    when(observabilityFacade.startExecutionScope(job)).thenReturn(scope);
    jobTask.attach(cancelAttempt());
    RuntimeException error = new IllegalStateException("retry me");
    failBody(() -> error);
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING);
    when(jobStore.incrementRetryAttempt(JOB_UUID, 7L, 0)).thenReturn(1);
    when(retryPolicy.shouldRetry(1, error)).thenReturn(true);
    when(retryPolicy.getDelay(1)).thenReturn(Duration.ofSeconds(5));
    when(jobStore.scheduleJobRetry(eq(JOB_UUID), any(), any(), eq(1), eq(7L))).thenReturn(true);

    jobTask.call();

    InOrder order = inOrder(jobStore, observabilityFacade, scope);
    order.verify(jobStore).scheduleJobRetry(eq(JOB_UUID), any(), any(), eq(1), eq(7L));
    order.verify(observabilityFacade).recordJobFailure(job, error, 1);
    order.verify(scope).failure(eq(error.getClass().getName()), any(), eq(1));
    verify(observabilityFacade, times(1)).recordJobFailure(any(), any(), anyInt());
    verify(observabilityFacade, never()).recordJobCancellation(any());
  }

  @Test
  void canceledAfterFailedExecutionRecord_usesAttemptStartTime() throws Exception {
    // Cancellation before an execution record exists uses the attempt's original start time.
    JobEntity job = cancelScenario(false);
    JobAttemptControl attempt = cancelAttempt();
    attempt.requestUserCancel();
    jobTask.attach(attempt);
    when(observabilityFacade.startExecution(JOB_UUID, 1, "node-1"))
        .thenThrow(new IllegalStateException("execution write failed"));
    jobTask.call();
    verify(observabilityFacade).recordJobCancellation(job);
    Assertions.assertEquals(attempt.executionStartTime(), job.getExecutionStartTime());
  }

  @Test
  void graceStopOfACanceledJob_endsCanceledWithoutTheTimeoutPath() throws Exception {
    // A stop after a watchdog grace request ends canceled when another node cancelled the job.
    JobTimeoutHandler timeout = mock(JobTimeoutHandler.class);
    jobTask = newJobTaskWithTimeoutHandler(timeout);
    JobEntity job = cancelScenario(false);
    JobAttemptControl attempt = cancelAttempt();
    jobTask.attach(attempt);
    attempt.requestCancellation();
    failBody(() -> new CancellationRequestedException("grace stop"));

    jobTask.call();

    verifyCanceledExecution(job);
    verify(timeout, never()).processCooperativeTimeout(any());
  }

  @Test
  void nonRetryableFailureOfACanceledJob_endsCanceled() throws Exception {
    // A non-retryable failure that loses its failure write to a cancel ends canceled.
    JobEntity job = cancelScenario(false);
    CallbackRecorder.reset();
    SerializableBiConsumer<JobContext, Throwable> callback = CallbackRecorder::failure;
    job.setOnFailurePayload(JobPayloadFactory.fromLambda(callback));
    jobTask.attach(cancelAttempt());
    RuntimeException error = new IllegalStateException("do not retry");
    failBody(() -> error);
    when(validationFacade.shouldNotRetry(error)).thenReturn(true);

    jobTask.call();

    ArgumentCaptor<JobExecutionEntity> execution =
        ArgumentCaptor.forClass(JobExecutionEntity.class);
    verify(observabilityFacade, atLeastOnce()).saveExecution(execution.capture());
    Assertions.assertEquals(ExecutionStatus.CANCELED, execution.getValue().getStatus());
    verify(observabilityFacade).recordJobCancellation(job);
    verify(observabilityFacade, never()).recordJobFailure(any(), any(), anyInt());
    Assertions.assertNull(CallbackRecorder.context);
  }

  @Test
  @SuppressWarnings("unchecked")
  void hardTimeoutThatFindsTheJobCanceledAfterTheWorkerEnded_endsCanceled() throws Exception {
    // The worker reads RUNNING and saves its execution FAILED. A cancel commits before the worker
    // ends, and the watchdog reads CANCELED after it. The watchdog finishes the cancel.
    JobTimeoutHandler handler = realTimeoutHandler();
    jobTask = newJobTaskWithTimeoutHandler(handler);
    JobEntity job = cancelScenario(true);
    CancelRace race = cancelRace();
    JobAttemptControl attempt = cancelAttempt();
    jobTask.attach(attempt);
    interruptBody();
    recordSaves(
        race,
        status -> {
          if (status == ExecutionStatus.FAILED) race.cancel();
        });
    when(jobStore.findById(JOB_UUID))
        .thenAnswer(
            invocation -> {
              jobTask.call();
              return Optional.of(storedRow(race.status().get()));
            });

    runWatchdog(handler, attempt, new FutureTask<>(() -> null));

    Assertions.assertEquals(
        List.of(ExecutionStatus.FAILED, ExecutionStatus.CANCELED), race.saved());
    verify(observabilityFacade).recordJobCancellation(job);
    verify(observabilityFacade, never()).recordJobFailure(any(), any(), anyInt());
    Assertions.assertFalse(race.held().get(), "the key must not stay held");
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  @SuppressWarnings("unchecked")
  void hardTimeoutThatFindsTheJobCanceledWhileTheWorkerRuns_endsCanceled(boolean recordFails)
      throws Exception {
    // The interrupted worker saves its execution FAILED, then a cancel commits, then the watchdog
    // reads CANCELED while the worker still runs. The worker finishes the cancel when it ends,
    // with its interrupt cleared for the store calls and restored afterwards. A record that
    // throws still lets the release run.
    JobTimeoutHandler handler = realTimeoutHandler();
    jobTask = newJobTaskWithTimeoutHandler(handler);
    JobEntity job = cancelScenario(true);
    CancelRace race = cancelRace();
    JobAttemptControl attempt = cancelAttempt();
    jobTask.attach(attempt);
    if (recordFails) {
      doThrow(new IllegalStateException("metrics down"))
          .when(observabilityFacade)
          .recordJobCancellation(job);
    }
    CountDownLatch interrupted = new CountDownLatch(1);
    CountDownLatch failedSaved = new CountDownLatch(1);
    CountDownLatch watchdogDone = new CountDownLatch(1);
    Future<?> future = mock(Future.class);
    when(future.cancel(anyBoolean()))
        .thenAnswer(
            invocation -> {
              interrupted.countDown();
              return true;
            });
    when(resilienceStrategy.execute(anyString(), any(), any(Callable.class)))
        .thenAnswer(
            invocation -> {
              awaitLatch(interrupted);
              Thread.currentThread().interrupt();
              throw new InterruptedException("watchdog interrupt");
            });
    List<Boolean> canceledSaveInterrupted = new ArrayList<>();
    recordSaves(
        race,
        status -> {
          if (status == ExecutionStatus.FAILED) {
            race.cancel();
            failedSaved.countDown();
            awaitLatch(watchdogDone);
          } else {
            canceledSaveInterrupted.add(Thread.currentThread().isInterrupted());
          }
        });
    when(jobStore.findById(JOB_UUID))
        .thenAnswer(
            invocation -> {
              awaitLatch(failedSaved);
              return Optional.of(storedRow(race.status().get()));
            });
    ExecutorService watchdog = Executors.newSingleThreadExecutor();
    try {
      Future<?> watchdogRun =
          watchdog.submit(
              () -> {
                try {
                  runWatchdog(handler, attempt, future);
                } finally {
                  watchdogDone.countDown();
                }
                return null;
              });

      jobTask.call();
      Assertions.assertTrue(Thread.interrupted(), "the worker's interrupt must be restored");
      watchdogRun.get(10, TimeUnit.SECONDS);
    } finally {
      Thread.interrupted();
      watchdog.shutdownNow();
    }

    Assertions.assertEquals(
        List.of(ExecutionStatus.FAILED, ExecutionStatus.CANCELED), race.saved());
    Assertions.assertEquals(List.of(false), canceledSaveInterrupted);
    verify(observabilityFacade).recordJobCancellation(job);
    verify(observabilityFacade, never()).recordJobFailure(any(), any(), anyInt());
    Assertions.assertFalse(race.held().get(), "the key must not stay held");
  }

  @Test
  void cancelAfterTheWorkerEndedBeforeTheWatchdogRead_releasesTheHold() throws Exception {
    // The worker ends while the row is RUNNING and releases nothing. A cancel then holds the key,
    // and the watchdog reads CANCELED. The watchdog releases the hold and finishes the cancel.
    JobTimeoutHandler handler = realTimeoutHandler();
    jobTask = newJobTaskWithTimeoutHandler(handler);
    JobEntity job = cancelScenario(true);
    CancelRace race = cancelRace();
    JobAttemptControl attempt = cancelAttempt();
    jobTask.attach(attempt);
    interruptBody();
    recordSaves(race, status -> {});
    when(jobStore.findById(JOB_UUID))
        .thenAnswer(
            invocation -> {
              jobTask.call();
              race.cancel();
              return Optional.of(storedRow(JobStatus.CANCELED));
            });

    runWatchdog(handler, attempt, new FutureTask<>(() -> null));

    Assertions.assertFalse(race.held().get(), "the key must not stay held");
    Assertions.assertEquals(
        List.of(ExecutionStatus.FAILED, ExecutionStatus.CANCELED), race.saved());
    verify(observabilityFacade).recordJobCancellation(job);
    verify(observabilityFacade, never()).recordJobFailure(any(), any(), anyInt());
  }

  @Test
  void cancelThatBeatsTheWatchdogsRetryWrite_releasesTheHold() throws Exception {
    // The watchdog reads RUNNING after the worker ended, then a cancel commits before its retry
    // increment. The increment misses, and the watchdog finishes the cancel.
    JobTimeoutHandler handler = realTimeoutHandler();
    jobTask = newJobTaskWithTimeoutHandler(handler);
    JobEntity job = cancelScenario(true);
    CancelRace race = cancelRace();
    JobAttemptControl attempt = cancelAttempt();
    jobTask.attach(attempt);
    interruptBody();
    recordSaves(race, status -> {});
    when(jobStore.findById(JOB_UUID))
        .thenAnswer(
            invocation -> {
              jobTask.call();
              return Optional.of(storedRow(JobStatus.RUNNING));
            });
    when(jobStore.incrementRetryAttempt(JOB_UUID, 7L, 0))
        .thenAnswer(
            invocation -> {
              race.cancel();
              return -1;
            });

    runWatchdog(handler, attempt, new FutureTask<>(() -> null));

    Assertions.assertFalse(race.held().get(), "the key must not stay held");
    Assertions.assertEquals(
        List.of(ExecutionStatus.FAILED, ExecutionStatus.CANCELED), race.saved());
    verify(observabilityFacade).recordJobCancellation(job);
    verify(observabilityFacade, never()).recordJobFailure(any(), any(), anyInt());
    verify(jobStore, never()).scheduleJobRetry(any(), any(), any(), anyInt(), any());
  }

  @Test
  void cooperativeTimeoutHandedBackAndThenCanceled_releasesTheHold() throws Exception {
    // The worker's cooperative timeout fails on a store error, so it hands the timeout back to
    // the watchdog and ends. A cancel then holds the key, and the watchdog reads CANCELED.
    JobTimeoutHandler handler = realTimeoutHandler();
    jobTask = newJobTaskWithTimeoutHandler(handler);
    JobEntity job = cancelScenario(true);
    CancelRace race = cancelRace();
    JobAttemptControl attempt = cancelAttempt();
    jobTask.attach(attempt);
    attempt.requestCancellation();
    failBody(() -> new CancellationRequestedException("grace stop"));
    recordSaves(race, status -> {});
    AtomicInteger reads = new AtomicInteger();
    when(jobStore.findById(JOB_UUID))
        .thenAnswer(
            invocation -> {
              if (reads.getAndIncrement() == 0) throw new IllegalStateException("store down");
              return Optional.of(storedRow(race.status().get()));
            });

    jobTask.call();
    Assertions.assertTrue(attempt.isTimeoutHandedBack());
    race.cancel();
    FutureTask<Void> ended = new FutureTask<>(() -> null);
    ended.run();
    runWatchdog(handler, attempt, ended);

    Assertions.assertFalse(race.held().get(), "the key must not stay held");
    Assertions.assertEquals(
        List.of(ExecutionStatus.FAILED, ExecutionStatus.CANCELED), race.saved());
    verify(observabilityFacade).recordJobCancellation(job);
    verify(observabilityFacade, never()).recordJobFailure(any(), any(), anyInt());
  }

  @Test
  void cancelTheWorkerAlreadyFinished_isNotFinishedAgainByTheWatchdog() throws Exception {
    // The cancel commits before the worker's read, so the worker ends the attempt canceled. The
    // watchdog then reads CANCELED and leaves the finished cancel alone.
    JobTimeoutHandler handler = realTimeoutHandler();
    jobTask = newJobTaskWithTimeoutHandler(handler);
    JobEntity job = cancelScenario(true);
    CancelRace race = cancelRace();
    JobAttemptControl attempt = cancelAttempt();
    jobTask.attach(attempt);
    when(resilienceStrategy.execute(anyString(), any(), any(Callable.class)))
        .thenAnswer(
            invocation -> {
              race.cancel();
              throw new InterruptedException("watchdog interrupt");
            });
    recordSaves(race, status -> {});
    when(jobStore.findById(JOB_UUID))
        .thenAnswer(
            invocation -> {
              jobTask.call();
              return Optional.of(storedRow(JobStatus.CANCELED));
            });

    runWatchdog(handler, attempt, new FutureTask<>(() -> null));

    Assertions.assertEquals(List.of(ExecutionStatus.CANCELED), race.saved());
    verify(observabilityFacade, times(1)).recordJobCancellation(job);
    Assertions.assertFalse(race.held().get(), "the key must not stay held");
  }

  @Test
  @SuppressWarnings("unchecked")
  void hardTimeoutOfACanceledBodyThatStillRuns_leavesTheKeyHeldUntilTheBodyEnds() throws Exception {
    // A body that ignores both the stop request and the interrupt still runs when the watchdog
    // reads CANCELED. The key stays held, and the worker finishes the cancel when the body ends.
    JobTimeoutHandler handler = realTimeoutHandler();
    jobTask = newJobTaskWithTimeoutHandler(handler);
    JobEntity job = cancelScenario(true);
    CancelRace race = cancelRace();
    JobAttemptControl attempt = cancelAttempt();
    jobTask.attach(attempt);
    CountDownLatch bodyRunning = new CountDownLatch(1);
    CountDownLatch bodyMayEnd = new CountDownLatch(1);
    when(resilienceStrategy.execute(anyString(), any(), any(Callable.class)))
        .thenAnswer(
            invocation -> {
              race.cancel();
              bodyRunning.countDown();
              awaitLatch(bodyMayEnd);
              throw new IllegalStateException("stopped late");
            });
    recordSaves(race, status -> {});
    when(jobStore.findById(JOB_UUID))
        .thenAnswer(invocation -> Optional.of(storedRow(JobStatus.CANCELED)));
    ExecutorService worker = Executors.newSingleThreadExecutor();
    try {
      Future<Void> workerRun = worker.submit(jobTask);
      awaitLatch(bodyRunning);

      runWatchdog(handler, attempt, mock(Future.class));

      Assertions.assertTrue(race.held().get(), "the key stays held while the body runs");
      Assertions.assertEquals(List.of(), race.saved());
      verify(observabilityFacade, never()).recordJobCancellation(any());
      bodyMayEnd.countDown();
      workerRun.get(10, TimeUnit.SECONDS);
    } finally {
      worker.shutdownNow();
    }

    Assertions.assertEquals(List.of(ExecutionStatus.CANCELED), race.saved());
    verify(observabilityFacade, times(1)).recordJobCancellation(job);
    Assertions.assertFalse(race.held().get(), "the worker releases the key when the body ends");
  }

  @Test
  void cancelFoundWhileSuccessFinalizationIsStuck_endsTheExecutionCanceled() throws Exception {
    // The body returned in time, but its success write is stuck on a store blip. The watchdog
    // revokes the attempt, a cancel commits, and the watchdog reads CANCELED. The worker stops
    // retrying without an outcome, so the execution ends CANCELED.
    JobTimeoutHandler handler = realTimeoutHandler();
    jobTask = newJobTaskWithTimeoutHandler(handler);
    JobEntity job = cancelScenario(true);
    CancelRace race = cancelRace();
    JobAttemptControl attempt = cancelAttempt();
    jobTask.attach(attempt);
    jobTask.setTerminalRetryDelays(1, 2);
    succeedBody();
    when(lifecycleFacade.completeSuccess(
            any(), any(), any(), any(), any(), anyLong(), anyLong(), nullable(Integer.class)))
        .thenThrow(new RatchetTransientStoreException("busy"));
    CountDownLatch stuck = new CountDownLatch(1);
    when(lifecycleFacade.completeSuccessMinimal(
            any(), any(), any(), anyLong(), anyLong(), nullable(Integer.class)))
        .thenAnswer(
            invocation -> {
              stuck.countDown();
              throw new RatchetTransientStoreException("busy");
            });
    recordSaves(race, status -> {});
    when(jobStore.findById(JOB_UUID))
        .thenAnswer(invocation -> Optional.of(storedRow(race.status().get())));
    ExecutorService worker = Executors.newSingleThreadExecutor();
    try {
      Future<Void> workerRun = worker.submit(jobTask);
      awaitLatch(stuck);
      race.cancel();

      runWatchdog(handler, attempt, new FutureTask<>(() -> null));

      workerRun.get(10, TimeUnit.SECONDS);
    } finally {
      worker.shutdownNow();
    }

    Assertions.assertEquals(List.of(ExecutionStatus.CANCELED), race.saved());
    verify(observabilityFacade).recordJobCancellation(job);
    verify(observabilityFacade, never()).recordJobSuccess(any(), anyLong());
    Assertions.assertFalse(race.held().get(), "the key must not stay held");
  }

  @Test
  void missedRetryWriteWhoseCancelReadFailed_isFinishedCanceledByTheWatchdog() throws Exception {
    // A cancel beats the worker's retry write, and the worker's read of the cancel fails. The
    // worker leaves its execution FAILED, and the watchdog later reads CANCELED and finishes it.
    JobTimeoutHandler handler = realTimeoutHandler();
    jobTask = newJobTaskWithTimeoutHandler(handler);
    JobEntity job = cancelScenario(true);
    CancelRace race = cancelRace();
    JobAttemptControl attempt = cancelAttempt();
    jobTask.attach(attempt);
    retryAfterAnOrdinaryFailure();
    when(jobStore.getJobStatus(JOB_UUID))
        .thenAnswer(
            invocation -> {
              if (race.status().get() == JobStatus.CANCELED) {
                throw new IllegalStateException("store down");
              }
              return JobStatus.RUNNING;
            });
    when(jobStore.scheduleJobRetry(eq(JOB_UUID), anyString(), any(), eq(1), eq(7L)))
        .thenAnswer(
            invocation -> {
              race.cancel();
              return false;
            });
    recordSaves(race, status -> {});
    when(jobStore.findById(JOB_UUID))
        .thenAnswer(invocation -> Optional.of(storedRow(race.status().get())));

    jobTask.call();
    Assertions.assertTrue(attempt.isTimeoutHandedBack());
    Assertions.assertEquals(List.of(ExecutionStatus.FAILED), race.saved());
    runWatchdog(handler, attempt, new FutureTask<>(() -> null));

    Assertions.assertEquals(
        List.of(ExecutionStatus.FAILED, ExecutionStatus.CANCELED), race.saved());
    verify(observabilityFacade).recordJobCancellation(job);
    Assertions.assertFalse(race.held().get(), "the key must not stay held");
  }

  @Test
  void watchdogPassedBeforeMissedRetryWrite_workerRechecksCancel() throws Exception {
    JobEntity job = cancelScenario(true);
    CancelRace race = cancelRace();
    JobAttemptControl attempt = cancelAttempt();
    jobTask.attach(attempt);
    retryAfterAnOrdinaryFailure();
    AtomicReference<Boolean> watchdogClaimed = new AtomicReference<>();
    when(jobStore.incrementRetryAttempt(JOB_UUID, 7L, 0))
        .thenAnswer(
            invocation -> {
              boolean claimed = attempt.claimTimeoutForWatchdog();
              watchdogClaimed.set(claimed);
              Assertions.assertFalse(claimed);
              return 1;
            });
    AtomicInteger cancelReads = new AtomicInteger();
    when(jobStore.getJobStatus(JOB_UUID))
        .thenAnswer(
            invocation -> {
              if (race.status().get() != JobStatus.CANCELED) return JobStatus.RUNNING;
              if (cancelReads.getAndIncrement() == 0) throw new IllegalStateException("store down");
              return JobStatus.CANCELED;
            });
    when(jobStore.scheduleJobRetry(eq(JOB_UUID), anyString(), any(), eq(1), eq(7L)))
        .thenAnswer(
            invocation -> {
              race.cancel();
              return false;
            });
    recordSaves(race, status -> {});

    jobTask.call();

    Assertions.assertEquals(Boolean.FALSE, watchdogClaimed.get());
    Assertions.assertEquals(2, cancelReads.get());
    Assertions.assertEquals(
        List.of(ExecutionStatus.FAILED, ExecutionStatus.CANCELED), race.saved());
    verify(observabilityFacade).recordJobCancellation(job);
    Assertions.assertFalse(attempt.isTimeoutHandedBack());
    verify(lifecycleFacade, never())
        .completeFailure(any(), any(), anyBoolean(), nullable(Integer.class));
    verify(lifecycleFacade, never())
        .completeTimeoutFailure(any(), any(), anyBoolean(), any(), any());
  }

  @Test
  void watchdogPassedRecheckClearsInterrupt() throws Exception {
    JobEntity job = cancelScenario(true);
    CancelRace race = cancelRace();
    JobAttemptControl attempt = cancelAttempt();
    jobTask.attach(attempt);
    retryAfterAnOrdinaryFailure();
    AtomicReference<Boolean> watchdogClaimed = new AtomicReference<>();
    when(jobStore.incrementRetryAttempt(JOB_UUID, 7L, 0))
        .thenAnswer(
            invocation -> {
              boolean claimed = attempt.claimTimeoutForWatchdog();
              watchdogClaimed.set(claimed);
              Assertions.assertFalse(claimed);
              return 1;
            });
    AtomicReference<Boolean> recheckInterrupted = new AtomicReference<>();
    when(jobStore.getJobStatus(JOB_UUID))
        .thenAnswer(
            invocation -> {
              if (race.status().get() != JobStatus.CANCELED) return JobStatus.RUNNING;
              recheckInterrupted.set(Thread.currentThread().isInterrupted());
              return JobStatus.CANCELED;
            });
    when(jobStore.scheduleJobRetry(eq(JOB_UUID), anyString(), any(), eq(1), eq(7L)))
        .thenAnswer(
            invocation -> {
              race.cancel();
              Thread.currentThread().interrupt();
              return false;
            });
    recordSaves(race, status -> {});

    try {
      jobTask.call();

      Assertions.assertEquals(Boolean.FALSE, watchdogClaimed.get());
      Assertions.assertEquals(Boolean.FALSE, recheckInterrupted.get());
      Assertions.assertTrue(Thread.currentThread().isInterrupted());
      Assertions.assertEquals(
          List.of(ExecutionStatus.FAILED, ExecutionStatus.CANCELED), race.saved());
      verify(observabilityFacade).recordJobCancellation(job);
      Assertions.assertFalse(attempt.isTimeoutHandedBack());
      verify(lifecycleFacade, never())
          .completeFailure(any(), any(), anyBoolean(), nullable(Integer.class));
      verify(lifecycleFacade, never())
          .completeTimeoutFailure(any(), any(), anyBoolean(), any(), any());
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  void watchdogUnknownRescheduleThenCancelDoesNotRecordInterruptedWorkerCancel() throws Exception {
    JobTimeoutHandler handler = realTimeoutHandler();
    jobTask = newJobTaskWithTimeoutHandler(handler);
    JobEntity job = cancelScenario(true);
    CancelRace race = cancelRace();
    JobAttemptControl attempt = cancelAttempt();
    jobTask.attach(attempt);
    recordSaves(race, status -> {});
    // The body's own throws are failure-handled, so record the flag here and assert it after
    // call().
    AtomicBoolean markedBeforeCancel = new AtomicBoolean();
    when(jobStore.findById(JOB_UUID))
        .thenAnswer(invocation -> Optional.of(storedRow(race.status().get())));
    when(jobStore.incrementRetryAttempt(JOB_UUID, 7L, 0)).thenReturn(1);
    when(jobStore.scheduleJobRetry(eq(JOB_UUID), anyString(), any(), eq(1), eq(7L)))
        .thenAnswer(
            invocation -> {
              race.status().set(JobStatus.PENDING);
              throw new IllegalStateException("committed then threw");
            });
    when(resilienceStrategy.execute(anyString(), any(), any(Callable.class)))
        .thenAnswer(
            invocation -> {
              runWatchdog(handler, attempt, mock(Future.class));
              markedBeforeCancel.set(attempt.timeoutOutcomeUnknown());
              race.cancel();
              attempt.finishCancelFoundByTimeout();
              throw new InterruptedException("watchdog interrupt");
            });

    jobTask.call();

    Assertions.assertTrue(markedBeforeCancel.get(), "the watchdog marks its reschedule unknown");
    Assertions.assertTrue(attempt.isSuperseded());
    Assertions.assertFalse(race.saved().contains(ExecutionStatus.CANCELED));
    Assertions.assertEquals(List.of(ExecutionStatus.FAILED), race.saved());
    verify(observabilityFacade, never()).recordJobCancellation(any());
    // The superseded worker and the cancel finish both release the hold. The first release deletes
    // it, so the second finds none and changes nothing.
    verify(jobStore, atLeastOnce()).releaseBusinessKeyHold(JOB_UUID, 7L);
    Assertions.assertFalse(race.held().get());
  }

  @Test
  void committedRetryWriteThatThrowsDoesNotRecordLaterPendingCancel() throws Exception {
    assertUnknownOutcomeDoesNotRecordCancel("retry");
  }

  @Test
  void dlqWriteThatThrowsThenCancelRecordsTheCancel() throws Exception {
    RuntimeException failure = new IllegalStateException("body");
    var scenario =
        hydratedClaimScenario(
            () -> {
              throw failure;
            });
    CancelRace race = cancelRace();
    recordSaves(race, status -> {});
    when(jobStore.incrementRetryAttempt(JOB_UUID, 7L, 2)).thenReturn(3);
    when(retryPolicy.shouldRetry(3, failure)).thenReturn(false);
    AtomicReference<JobStatus> statusBeforeCancel = new AtomicReference<>();
    when(lifecycleFacade.completeFailure(scenario.job(), JobStatus.RUNNING, false, 3))
        .thenAnswer(
            invocation -> {
              statusBeforeCancel.set(race.status().get());
              race.cancel();
              throw new IllegalStateException("DLQ write did not commit");
            })
        .thenReturn(false);

    jobTask.call();

    Assertions.assertEquals(JobStatus.RUNNING, statusBeforeCancel.get());
    Assertions.assertEquals(JobStatus.CANCELED, race.status().get());
    Assertions.assertEquals(ExecutionStatus.CANCELED, scenario.execution().getStatus());
    verify(lifecycleFacade, times(2)).completeFailure(scenario.job(), JobStatus.RUNNING, false, 3);
    verify(observabilityFacade, times(1)).recordJobCancellation(scenario.job());
  }

  @Test
  void committedContinuationWriteThatThrowsDoesNotRecordLaterPendingCancel() throws Exception {
    assertUnknownOutcomeDoesNotRecordCancel("continuation");
  }

  private void assertUnknownOutcomeDoesNotRecordCancel(String write) throws Exception {
    RuntimeException bodyFailure = new IllegalStateException("body");
    var scenario =
        hydratedClaimScenario(
            () -> {
              if (write.equals("continuation")) JobContext.current().requestContinuation();
              else throw bodyFailure;
            });
    CancelRace race = cancelRace();
    recordSaves(race, status -> {});
    RuntimeException uncertain = new IllegalStateException("committed then threw");
    if (write.equals("continuation")) {
      when(scenario.store().initState(eq(JOB_UUID), eq(CONTINUATION_NS), anyString(), eq(7L)))
          .thenReturn(StateWriteResult.UPDATED);
      when(jobStore.releaseClaim(eq(JOB_UUID), isNull(), any(), eq(2), eq(7L)))
          .thenAnswer(
              invocation -> {
                race.status().set(JobStatus.PENDING);
                throw uncertain;
              });
      when(validationFacade.shouldNotRetry(uncertain))
          .thenThrow(new IllegalStateException("policy"));
    } else {
      when(jobStore.incrementRetryAttempt(JOB_UUID, 7L, 2)).thenReturn(3);
      when(retryPolicy.shouldRetry(3, bodyFailure)).thenReturn(write.equals("retry"));
      when(retryPolicy.getDelay(3)).thenReturn(Duration.ofSeconds(1));
      when(jobStore.scheduleJobRetry(eq(JOB_UUID), any(), any(), eq(3), eq(7L)))
          .thenAnswer(
              invocation -> {
                race.status().set(JobStatus.PENDING);
                throw uncertain;
              });
    }
    // All forced writes miss because the uncertain write already ended this claim.
    when(lifecycleFacade.completeFailure(any(), eq(JobStatus.RUNNING), eq(false), anyInt()))
        .thenReturn(false);

    jobTask.call();

    Assertions.assertEquals(JobStatus.PENDING, race.status().get());
    race.cancel();
    Assertions.assertFalse(scenario.attempt().isTimeoutHandedBack());
    runWatchdog(realTimeoutHandler(), scenario.attempt(), new FutureTask<>(() -> null));
    // Exercise the cancel-finish coordination even if a watchdog had already claimed the cancel.
    scenario.attempt().finishCancelFoundByTimeout();
    Assertions.assertEquals(ExecutionStatus.FAILED, scenario.execution().getStatus());
    Assertions.assertFalse(race.saved().contains(ExecutionStatus.CANCELED));
    verify(observabilityFacade, never()).recordJobCancellation(any());
  }

  @Test
  void forcedFailureMissWithUnconfirmedCancel_handsTimeoutBack() throws Exception {
    JobTimeoutHandler handler = realTimeoutHandler();
    jobTask = newJobTaskWithTimeoutHandler(handler);
    JobEntity job = cancelScenario(true);
    CancelRace race = cancelRace();
    JobAttemptControl attempt = cancelAttempt();
    jobTask.attach(attempt);
    failBody(() -> new IllegalStateException("body"));
    when(jobStore.incrementRetryAttempt(JOB_UUID, 7L, 0))
        .thenAnswer(
            invocation -> {
              race.cancel();
              throw new IllegalStateException("increment failed");
            });
    when(lifecycleFacade.completeFailure(job, JobStatus.RUNNING, false, null)).thenReturn(false);
    when(jobStore.getJobStatus(JOB_UUID))
        .thenAnswer(
            invocation -> {
              if (race.status().get() == JobStatus.CANCELED) {
                throw new IllegalStateException("store down");
              }
              return JobStatus.RUNNING;
            });
    recordSaves(race, status -> {});
    when(jobStore.findById(JOB_UUID))
        .thenAnswer(invocation -> Optional.of(storedRow(race.status().get())));

    jobTask.call();

    verify(lifecycleFacade).completeFailure(job, JobStatus.RUNNING, false, null);
    Assertions.assertTrue(attempt.isTimeoutHandedBack());
    Assertions.assertEquals(List.of(ExecutionStatus.FAILED), race.saved());
    runWatchdog(handler, attempt, new FutureTask<>(() -> null));

    Assertions.assertEquals(
        List.of(ExecutionStatus.FAILED, ExecutionStatus.CANCELED), race.saved());
    verify(observabilityFacade).recordJobCancellation(job);
  }

  @Test
  void forcedFailureMissAfterWatchdogPassed_workerRechecksCancel() throws Exception {
    JobEntity job = cancelScenario(true);
    CancelRace race = cancelRace();
    JobAttemptControl attempt = cancelAttempt();
    jobTask.attach(attempt);
    failBody(() -> new IllegalStateException("body"));
    AtomicReference<Boolean> watchdogClaimed = new AtomicReference<>();
    when(jobStore.incrementRetryAttempt(JOB_UUID, 7L, 0))
        .thenAnswer(
            invocation -> {
              boolean claimed = attempt.claimTimeoutForWatchdog();
              watchdogClaimed.set(claimed);
              Assertions.assertFalse(claimed);
              race.cancel();
              throw new IllegalStateException("increment failed");
            });
    when(lifecycleFacade.completeFailure(job, JobStatus.RUNNING, false, null)).thenReturn(false);
    AtomicInteger cancelReads = new AtomicInteger();
    when(jobStore.getJobStatus(JOB_UUID))
        .thenAnswer(
            invocation -> {
              if (race.status().get() != JobStatus.CANCELED) return JobStatus.RUNNING;
              if (cancelReads.getAndIncrement() == 0) throw new IllegalStateException("store down");
              return JobStatus.CANCELED;
            });
    recordSaves(race, status -> {});

    jobTask.call();

    Assertions.assertEquals(Boolean.FALSE, watchdogClaimed.get());
    Assertions.assertEquals(2, cancelReads.get());
    verify(lifecycleFacade).completeFailure(job, JobStatus.RUNNING, false, null);
    Assertions.assertEquals(
        List.of(ExecutionStatus.FAILED, ExecutionStatus.CANCELED), race.saved());
    verify(observabilityFacade).recordJobCancellation(job);
    Assertions.assertFalse(attempt.isTimeoutHandedBack());
    verify(lifecycleFacade, never())
        .completeTimeoutFailure(any(), any(), anyBoolean(), any(), any());
  }

  @Test
  void supersededFailureHandlingErrorWithUnconfirmedCancel_handsTimeoutBack() throws Exception {
    JobEntity job = cancelScenario(true);
    CancelRace race = cancelRace();
    JobAttemptControl attempt = cancelAttempt();
    jobTask.attach(attempt);
    retryAfterAnOrdinaryFailure();
    when(jobStore.scheduleJobRetry(eq(JOB_UUID), anyString(), any(), eq(1), eq(7L)))
        .thenAnswer(
            invocation -> {
              race.cancel();
              return false;
            });
    when(jobStore.getJobStatus(JOB_UUID))
        .thenAnswer(
            invocation -> {
              if (race.status().get() == JobStatus.CANCELED) {
                throw new IllegalStateException("store down");
              }
              return JobStatus.RUNNING;
            });
    doThrow(new IllegalStateException("observer failed"))
        .when(observabilityFacade)
        .recordJobFailure(eq(job), any(), eq(1));
    recordSaves(race, status -> {});

    jobTask.call();

    Assertions.assertTrue(attempt.isSuperseded());
    Assertions.assertTrue(attempt.isTimeoutHandedBack());
    Assertions.assertEquals(List.of(ExecutionStatus.FAILED), race.saved());
    verify(observabilityFacade).recordJobFailure(eq(job), any(), eq(1));
    verify(observabilityFacade, never()).recordJobCancellation(any());
    verify(lifecycleFacade, never())
        .completeFailure(any(), any(), anyBoolean(), nullable(Integer.class));
  }

  @Test
  void retryWrittenAndThenCanceledFromPending_keepsTheExecutionFailed() throws Exception {
    // The worker's retry write commits, so this attempt failed. A later cancel of the PENDING job
    // is not this attempt's cancel. The worker keeps the timeout, so the watchdog skips the store.
    JobTimeoutHandler handler = realTimeoutHandler();
    jobTask = newJobTaskWithTimeoutHandler(handler);
    cancelScenario(true);
    CancelRace race = cancelRace();
    JobAttemptControl attempt = cancelAttempt();
    jobTask.attach(attempt);
    retryAfterAnOrdinaryFailure();
    when(jobStore.scheduleJobRetry(eq(JOB_UUID), anyString(), any(), eq(1), eq(7L)))
        .thenReturn(true);
    recordSaves(race, status -> {});

    jobTask.call();
    Assertions.assertFalse(attempt.isTimeoutHandedBack());
    race.status().set(JobStatus.CANCELED);
    runWatchdog(handler, attempt, new FutureTask<>(() -> null));

    verify(jobStore, never()).findById(JOB_UUID);
    Assertions.assertEquals(List.of(ExecutionStatus.FAILED), race.saved());
    verify(observabilityFacade, never()).recordJobCancellation(any());
  }

  private enum NoChargeReleaseSite {
    TRANSIENT_PERMIT,
    SUPERSEDED_PERMIT,
    OPEN_CIRCUIT,
    UPGRADE,
    BATCH_CHECK
  }

  @ParameterizedTest
  @EnumSource(NoChargeReleaseSite.class)
  void noChargeReleaseWrittenAndThenCanceledFromPending_recordsNoCancel(NoChargeReleaseSite site)
      throws Exception {
    assertNoChargeReleaseThenCanceled(site, true);
  }

  @Test
  void noChargeReleaseAfterUncertainPermitAcquireThenCanceled_recordsNothing() throws Exception {
    assertNoChargeReleaseThenCanceled(NoChargeReleaseSite.TRANSIENT_PERMIT, false);
  }

  private void assertNoChargeReleaseThenCanceled(NoChargeReleaseSite site, boolean released)
      throws Exception {
    JobTimeoutHandler handler = realTimeoutHandler();
    jobTask = newJobTaskWithTimeoutHandler(handler);
    JobEntity job = cancelScenario(true, 2);
    CancelRace race = cancelRace();
    JobAttemptControl attempt = cancelAttempt();
    jobTask.attach(attempt);
    if (site == NoChargeReleaseSite.UPGRADE || site == NoChargeReleaseSite.BATCH_CHECK) {
      // These gates return before service availability is checked.
      lenient().when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);
    }
    switch (site) {
      case TRANSIENT_PERMIT, SUPERSEDED_PERMIT -> {
        job.setResourceName("gpu");
        if (site == NoChargeReleaseSite.TRANSIENT_PERMIT) {
          when(resourcePermitService.tryAcquire("gpu", JOB_UUID, 7L, "node-1"))
              .thenThrow(new RatchetTransientStoreException("acquire deadlock"));
        } else {
          when(resourcePermitService.tryAcquire("gpu", JOB_UUID, 7L, "node-1"))
              .thenReturn(AcquireResult.SUPERSEDED);
        }
        when(resourcePermitService.getRetryDelay("gpu")).thenReturn(250);
      }
      case OPEN_CIRCUIT -> {
        failBody(() -> new CircuitBreakerOpenException("Circuit breaker OPEN"));
        when(resilienceStrategy.getRetryDelay(anyString())).thenReturn(Duration.ofSeconds(1));
      }
      case UPGRADE -> {
        job = spy(job);
        jobTask.init(job);
        UnsupportedEnvelopeVersionException skew = new UnsupportedEnvelopeVersionException(2, 1);
        doThrow(skew).when(job).getSignalPayload();
        when(errorSanitizer.sanitize(skew)).thenReturn("sanitized skew");
      }
      case BATCH_CHECK -> {
        UUID parentId = UUID.randomUUID();
        job.setJobType(JobExecutionType.BATCH_CHILD);
        job.setDependsOn(parentId);
        IllegalStateException unavailable = new IllegalStateException("parent unavailable");
        when(jobStore.getJobStatus(parentId)).thenThrow(unavailable);
        when(errorSanitizer.sanitize(unavailable)).thenReturn("sanitized parent failure");
      }
    }
    when(jobStore.releaseClaim(eq(JOB_UUID), anyString(), any(), anyInt(), eq(7L)))
        .thenReturn(released);
    recordSaves(race, status -> {});
    when(jobStore.findById(JOB_UUID))
        .thenAnswer(invocation -> Optional.of(storedRow(race.status().get())));

    jobTask.call();
    race.status().set(JobStatus.CANCELED);
    runWatchdog(handler, attempt, new FutureTask<>(() -> null));

    verify(jobStore).releaseClaim(eq(JOB_UUID), anyString(), any(), eq(2), eq(7L));
    verify(jobStore, never()).scheduleJobRetry(any(), anyString(), any(), anyInt(), anyLong());
    verify(jobStore, never()).incrementRetryAttempt(any(), anyLong(), any());
    if (released || site == NoChargeReleaseSite.TRANSIENT_PERMIT) {
      verify(observabilityFacade, never()).recordJobCancellation(any());
      Assertions.assertFalse(race.saved().contains(ExecutionStatus.CANCELED));
    } else {
      verify(observabilityFacade).recordJobCancellation(job);
    }
    Assertions.assertEquals(
        site == NoChargeReleaseSite.OPEN_CIRCUIT ? List.of(ExecutionStatus.FAILED) : List.of(),
        race.saved());
  }

  @Test
  void watchdogInterruptOfARunningBody_endsTheCancelOnceEndToEnd() throws Exception {
    // A real FutureTask runs the worker, and a cancel commits while the body runs. The watchdog
    // cancels the future, which interrupts the body, and reads CANCELED. Whichever side ends
    // last finishes the cancel, once.
    JobTimeoutHandler handler = realTimeoutHandler();
    jobTask = newJobTaskWithTimeoutHandler(handler);
    JobEntity job = cancelScenario(true);
    job.setPayload(
        new JobPayload(
            JobTaskTest.class.getName(),
            "waitForTheWatchdog",
            "()Ljava/lang/String;",
            true,
            List.of(),
            null));
    CancelRace race = cancelRace();
    JobAttemptControl attempt = cancelAttempt();
    jobTask.attach(attempt);
    succeedBody();
    // The watchdog reads only after the worker saved, so its interrupt marker is still set.
    CountDownLatch workerSaved = new CountDownLatch(1);
    recordSaves(race, status -> workerSaved.countDown());
    when(jobStore.findById(JOB_UUID))
        .thenAnswer(
            invocation -> {
              awaitLatch(workerSaved);
              return Optional.of(storedRow(race.status().get()));
            });
    CountDownLatch entered = new CountDownLatch(1);
    BODY_ENTERED.set(entered);
    FutureTask<Void> future = new FutureTask<>(jobTask);
    ExecutorService worker = Executors.newSingleThreadExecutor();
    try {
      worker.execute(future);
      awaitLatch(entered);
      race.cancel();

      runWatchdog(handler, attempt, future);

      Assertions.assertTrue(future.isCancelled());
    } finally {
      worker.shutdown();
      Assertions.assertTrue(worker.awaitTermination(10, TimeUnit.SECONDS));
      BODY_ENTERED.set(null);
    }

    Assertions.assertEquals(ExecutionStatus.CANCELED, race.saved().get(race.saved().size() - 1));
    Assertions.assertEquals(1, Collections.frequency(race.saved(), ExecutionStatus.CANCELED));
    verify(observabilityFacade).recordJobCancellation(job);
    verify(observabilityFacade, never()).recordJobFailure(any(), any(), anyInt());
    verify(jobStore, never()).incrementRetryAttempt(any(), any(), any());
    Assertions.assertFalse(race.held().get(), "the key must not stay held");
  }

  @Test
  @SuppressWarnings("unchecked")
  void watchdogThatReleasedBeforeTheWorkerStarted_leavesTheLateWorkerToEndCanceled()
      throws Exception {
    // The watchdog finds the cancel before any worker starts, so it releases the hold. A late
    // worker can no longer start the body, reads CANCELED, and records the cancel once.
    JobTimeoutHandler handler = realTimeoutHandler();
    jobTask = newJobTaskWithTimeoutHandler(handler);
    JobEntity job = createTestJob();
    job.setClaimSeq(7L);
    job.setBusinessKey("held");
    initJobTaskWithDefaultStubs(job);
    CancelRace race = cancelRace();
    JobAttemptControl attempt = cancelAttempt();
    jobTask.attach(attempt);
    when(jobStore.findById(JOB_UUID))
        .thenAnswer(invocation -> Optional.of(storedRow(race.status().get())));
    race.cancel();

    runWatchdog(handler, attempt, new FutureTask<>(() -> null));
    Assertions.assertFalse(race.held().get(), "the watchdog releases the hold");
    Assertions.assertFalse(attempt.markBodyStarted(), "no body may start after the release");

    jobTask.call();

    verify(resilienceStrategy, never()).execute(anyString(), any(), any(Callable.class));
    verify(observabilityFacade, times(1)).recordJobCancellation(job);
    verify(observabilityFacade, never()).saveExecution(any());
  }

  /** The body fails with an ordinary error, and its first retry is allowed. */
  private void retryAfterAnOrdinaryFailure() throws Exception {
    failBody(() -> new IllegalStateException("boom"));
    when(jobStore.incrementRetryAttempt(JOB_UUID, 7L, 0)).thenReturn(1);
    when(retryPolicy.shouldRetry(eq(1), any())).thenReturn(true);
    when(retryPolicy.getDelay(1)).thenReturn(Duration.ofSeconds(5));
  }

  /**
   * The store side of a cancel race: the job's status, the key hold that a running cancel takes and
   * a release with claim sequence 7 removes, and the status of each saved execution.
   */
  private record CancelRace(
      AtomicReference<JobStatus> status, AtomicBoolean held, List<ExecutionStatus> saved) {
    void cancel() {
      status.set(JobStatus.CANCELED);
      held.set(true);
    }
  }

  private CancelRace cancelRace() {
    CancelRace race =
        new CancelRace(
            new AtomicReference<>(JobStatus.RUNNING), new AtomicBoolean(), new ArrayList<>());
    lenient().when(jobStore.getJobStatus(JOB_UUID)).thenAnswer(invocation -> race.status().get());
    lenient()
        .when(jobStore.releaseBusinessKeyHold(JOB_UUID, 7L))
        .thenAnswer(invocation -> race.held().getAndSet(false));
    return race;
  }

  private void recordSaves(CancelRace race, Consumer<ExecutionStatus> onSave) {
    // Pre-execution releases legitimately save no execution; still capture an unexpected save.
    lenient()
        .when(observabilityFacade.saveExecution(any(JobExecutionEntity.class)))
        .thenAnswer(
            invocation -> {
              JobExecutionEntity execution = invocation.getArgument(0);
              race.saved().add(execution.getStatus());
              onSave.accept(execution.getStatus());
              return execution;
            });
  }

  /** The body throws the interrupt that the watchdog gives a running body. */
  @SuppressWarnings("unchecked")
  private void interruptBody() throws Exception {
    when(resilienceStrategy.execute(anyString(), any(), any(Callable.class)))
        .thenAnswer(
            invocation -> {
              throw new InterruptedException("watchdog interrupt");
            });
  }

  private static void runWatchdog(
      JobTimeoutHandler handler, JobAttemptControl attempt, Future<?> future) throws Exception {
    Method hardTimeout =
        JobTimeoutHandler.class.getDeclaredMethod(
            "handleHardTimeoutById",
            JobAttemptControl.class,
            Future.class,
            OutcomeWriteRetries.class);
    hardTimeout.setAccessible(true);
    hardTimeout.invoke(handler, attempt, future, null);
  }

  /** Waits like a store call that ignores interrupts, and keeps the caller's interrupt. */
  private static void awaitLatch(CountDownLatch latch) {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    boolean interrupted = false;
    try {
      while (true) {
        try {
          Assertions.assertTrue(
              latch.await(deadline - System.nanoTime(), TimeUnit.NANOSECONDS),
              "timed out waiting for the race");
          return;
        } catch (InterruptedException ignored) {
          interrupted = true;
        }
      }
    } finally {
      if (interrupted) Thread.currentThread().interrupt();
    }
  }

  /** A real timeout handler over the store mock, whose transition wrapper runs the transition. */
  @SuppressWarnings("unchecked")
  private JobTimeoutHandler realTimeoutHandler() {
    lenient()
        .when(lifecycleFacade.handleTimeoutTransition(any(), anyBoolean(), any(Supplier.class)))
        .thenAnswer(
            invocation ->
                ((Optional<?>) invocation.getArgument(2, Supplier.class).get()).isPresent());
    return new JobTimeoutHandler(
        jobStore,
        jobStore,
        jobStore,
        lifecycleFacade,
        80,
        60L,
        FIXED_CLOCK,
        null,
        null,
        null,
        JobTimeoutHandler.DEFAULT_SIGNAL_TIMEOUT_BATCH_SIZE);
  }

  /** Records each saved execution's status at the moment it is saved. */
  private List<ExecutionStatus> savedExecutionStatuses() {
    List<ExecutionStatus> statuses = new ArrayList<>();
    when(observabilityFacade.saveExecution(any(JobExecutionEntity.class)))
        .thenAnswer(
            invocation -> {
              JobExecutionEntity execution = invocation.getArgument(0);
              statuses.add(execution.getStatus());
              return execution;
            });
    return statuses;
  }

  private JobEntity storedRow(JobStatus status) {
    JobEntity row = createTestJob();
    row.setStatus(status);
    row.setClaimSeq(7L);
    row.setBusinessKey("held");
    return row;
  }

  private JobAttemptControl cancelAttempt() {
    return new JobAttemptControl(
        JOB_UUID, FIXED_NOW.plusSeconds(30), 30, FIXED_NOW.minusSeconds(10), 0, 7L, "node-1");
  }

  private JobEntity cancelScenario(boolean keyed) {
    return cancelScenario(keyed, 0);
  }

  private JobEntity cancelScenario(boolean keyed, int attempts) {
    JobEntity job = createTestJob();
    job.setClaimSeq(7L);
    job.setAttempts(attempts);
    if (keyed) job.setBusinessKey("held");
    initJobTaskWithDefaultStubs(job);
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING, JobStatus.CANCELED);
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);
    return job;
  }

  @SuppressWarnings("unchecked")
  private void failBody(Supplier<RuntimeException> failure) throws Exception {
    when(resilienceStrategy.execute(anyString(), any(), any(Callable.class)))
        .thenAnswer(
            inv -> {
              throw failure.get();
            });
  }

  @SuppressWarnings("unchecked")
  private void succeedBody() throws Exception {
    when(resilienceStrategy.execute(anyString(), any(), any(Callable.class)))
        .thenAnswer(inv -> inv.<Callable<?>>getArgument(2).call());
  }

  private void verifyCanceledExecution(JobEntity job) {
    ArgumentCaptor<JobExecutionEntity> execution =
        ArgumentCaptor.forClass(JobExecutionEntity.class);
    verify(observabilityFacade).saveExecution(execution.capture());
    Assertions.assertEquals(ExecutionStatus.CANCELED, execution.getValue().getStatus());
    verify(observabilityFacade).recordJobCancellation(job);
  }
}
