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
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
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
import run.ratchet.store.util.EncryptionTarget;
import run.ratchet.store.util.PayloadEncryptor;

@ExtendWith(MockitoExtension.class)
class JobTaskTest {

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

  @SuppressWarnings("unchecked")
  private ContinuationScenario continuationScenario(Runnable payload, int limit) throws Exception {
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
    when(observabilityFacade.startExecutionScope(any(JobEntity.class)))
        .thenReturn(TracingCollector.NoOpExecutionScope.INSTANCE);
    jobTask.attach(attempt);
    JobExecutionEntity execution = JobExecutionEntity.start(JOB_UUID, 1, "node-1");
    when(observabilityFacade.startExecution(any(UUID.class), anyInt(), anyString()))
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

  @Test
  void continuationPreservesAttemptsClearsErrorAndSkipsSuccessProcessing() throws Exception {
    var scenario =
        continuationScenario(
            () -> JobContext.current().requestContinuation(Duration.ofSeconds(2)), 1000);
    when(scenario.store().initState(eq(JOB_UUID), eq(CONTINUATION_NS), anyString(), eq(7L)))
        .thenReturn(StateWriteResult.UPDATED);
    when(jobStore.scheduleJobRetry(JOB_UUID, null, FIXED_NOW.plusSeconds(2), 0, 7L))
        .thenReturn(true);
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
    order.verify(jobStore).scheduleJobRetry(JOB_UUID, null, FIXED_NOW.plusSeconds(2), 0, 7L);
    order.verify(observabilityFacade).saveExecution(scenario.execution());
    Assertions.assertEquals(JobStatus.PENDING, scenario.job().getStatus());
    Assertions.assertNull(scenario.job().getLastError());
    Assertions.assertEquals(0, scenario.job().getAttempts());
    Assertions.assertEquals(ExecutionStatus.SUCCEEDED, scenario.execution().getStatus());
    verify(scenario.results(), never()).serialize(any(), any());
    verify(lifecycleFacade, never())
        .completeSuccess(any(), any(), any(), any(), any(), anyLong(), anyLong());
    verify(observabilityFacade, never()).recordJobSuccess(any(), anyLong());
    verify(observabilityFacade, never()).publishEvent(any(JobRetryingEvent.class));
    verify(jobStore, never()).incrementRetryAttempt(any(), any());
  }

  @Test
  void existingCounterAdvancesAcrossRetriesWithoutChangingAttempts() throws Exception {
    var scenario = continuationScenario(() -> JobContext.current().requestContinuation(), 1000);
    scenario.job().setAttempts(2);
    when(scenario.store().getState(JOB_UUID, CONTINUATION_NS))
        .thenReturn(Optional.of(new JobStateStore.VersionedState("{\"count\":4}", 9)));
    when(scenario.store().updateState(JOB_UUID, CONTINUATION_NS, "{\"count\":5}", 9, 7L))
        .thenReturn(StateWriteResult.UPDATED);
    when(jobStore.scheduleJobRetry(JOB_UUID, null, FIXED_NOW, 2, 7L)).thenReturn(true);
    TracingCollector.ExecutionScope scope = mock(TracingCollector.ExecutionScope.class);
    when(observabilityFacade.startExecutionScope(scenario.job())).thenReturn(scope);
    jobTask.call();
    verify(scenario.store()).updateState(JOB_UUID, CONTINUATION_NS, "{\"count\":5}", 9, 7L);
    verify(jobStore).scheduleJobRetry(JOB_UUID, null, FIXED_NOW, 2, 7L);
    Assertions.assertEquals(2, scenario.job().getAttempts());
    verify(scope).success(0L);
    verify(scope).close();
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
    when(jobStore.incrementRetryAttempt(JOB_UUID, 7L)).thenReturn(1);
    jobTask.call();
    verify(jobStore).incrementRetryAttempt(JOB_UUID, 7L);
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
    when(lifecycleFacade.completeSuccess(any(), any(), any(), any(), any(), anyLong(), anyLong()))
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
    when(lifecycleFacade.completeSuccess(any(), any(), any(), any(), any(), anyLong(), anyLong()))
        .thenReturn(true);
    jobTask.call();
    Assertions.assertInstanceOf(UnsupportedOperationException.class, rejected.get());
    verify(jobStore, never()).scheduleJobRetry(any(), any(), any(), anyInt(), any());
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
    when(jobStore.scheduleJobRetry(JOB_UUID, null, FIXED_NOW, 0, 7L)).thenReturn(true);
    jobTask.call();
    InOrder order = inOrder(scenario.store(), jobStore);
    order.verify(scenario.store()).getState(JOB_UUID, CONTINUATION_NS);
    order.verify(scenario.store()).updateState(JOB_UUID, CONTINUATION_NS, "{\"count\":5}", 9, 7L);
    order.verify(scenario.store()).getState(JOB_UUID, CONTINUATION_NS);
    order.verify(scenario.store()).updateState(JOB_UUID, CONTINUATION_NS, "{\"count\":1}", 10, 7L);
    order.verify(jobStore).scheduleJobRetry(JOB_UUID, null, FIXED_NOW, 0, 7L);
    Assertions.assertFalse(scenario.attempt().isSuperseded());
    Assertions.assertEquals(JobStatus.PENDING, scenario.job().getStatus());
    Assertions.assertEquals(ExecutionStatus.SUCCEEDED, scenario.execution().getStatus());
    verify(jobStore, never()).incrementRetryAttempt(any(), any());
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
    when(jobStore.scheduleJobRetry(JOB_UUID, null, FIXED_NOW, 0, 7L)).thenReturn(true);
    jobTask.call();
    verify(scenario.store(), times(2)).getState(JOB_UUID, CONTINUATION_NS);
    verify(scenario.store()).updateState(JOB_UUID, CONTINUATION_NS, "{\"count\":2}", 0, 7L);
    Assertions.assertFalse(scenario.attempt().isSuperseded());
    Assertions.assertEquals(JobStatus.PENDING, scenario.job().getStatus());
    verify(jobStore, never()).incrementRetryAttempt(any(), any());
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
    when(lifecycleFacade.completeFailure(scenario.job(), JobStatus.RUNNING, false))
        .thenReturn(true);
    jobTask.call();
    verify(scenario.store(), times(2)).getState(JOB_UUID, CONTINUATION_NS);
    verify(scenario.store()).updateState(JOB_UUID, CONTINUATION_NS, "{\"count\":0}", 10);
    Assertions.assertFalse(scenario.attempt().isSuperseded());
    Assertions.assertEquals(JobStatus.FAILED, scenario.job().getStatus());
    Assertions.assertEquals(ExecutionStatus.FAILED, scenario.execution().getStatus());
    verify(jobStore, never()).incrementRetryAttempt(any(), any());
    verify(jobStore, never()).scheduleJobRetry(any(), any(), any(), anyInt(), any());
  }

  @Test
  void exhaustedCounterWriteConflictsUseFailureHandling() throws Exception {
    var scenario = continuationScenario(() -> JobContext.current().requestContinuation(), 1000);
    when(scenario.store().getState(JOB_UUID, CONTINUATION_NS))
        .thenReturn(Optional.of(new JobStateStore.VersionedState("{\"count\":4}", 9)));
    when(scenario.store().updateState(JOB_UUID, CONTINUATION_NS, "{\"count\":5}", 9, 7L))
        .thenReturn(StateWriteResult.VERSION_CONFLICT);
    when(jobStore.incrementRetryAttempt(JOB_UUID, 7L)).thenReturn(1);
    when(lifecycleFacade.completeFailure(scenario.job(), JobStatus.RUNNING, false))
        .thenReturn(true);
    jobTask.call();
    verify(scenario.store(), times(3)).getState(JOB_UUID, CONTINUATION_NS);
    verify(scenario.store(), times(3))
        .updateState(JOB_UUID, CONTINUATION_NS, "{\"count\":5}", 9, 7L);
    verify(jobStore).incrementRetryAttempt(JOB_UUID, 7L);
    Assertions.assertFalse(scenario.attempt().isSuperseded());
    Assertions.assertEquals(1, scenario.job().getAttempts());
    Assertions.assertEquals(JobStatus.FAILED, scenario.job().getStatus());
    Assertions.assertEquals(ExecutionStatus.FAILED, scenario.execution().getStatus());
    Assertions.assertEquals(
        IllegalStateException.class.getName(), scenario.execution().getErrorClass());
    verify(jobStore, never()).scheduleJobRetry(any(), any(), any(), anyInt(), any());
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
  }

  @Test
  void pausedTransitionKeepsSpentCounterAndDoesNotFail() throws Exception {
    var scenario = continuationScenario(() -> JobContext.current().requestContinuation(), 1000);
    TracingCollector.ExecutionScope scope = mock(TracingCollector.ExecutionScope.class);
    when(observabilityFacade.startExecutionScope(scenario.job())).thenReturn(scope);
    when(scenario.store().initState(eq(JOB_UUID), eq(CONTINUATION_NS), anyString(), eq(7L)))
        .thenReturn(StateWriteResult.UPDATED);
    when(jobStore.scheduleJobRetry(JOB_UUID, null, FIXED_NOW, 0, 7L))
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
    verify(jobStore, never()).incrementRetryAttempt(any(), any());
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
    when(jobStore.scheduleJobRetry(JOB_UUID, null, FIXED_NOW, 0, 7L)).thenReturn(true);
    Assertions.assertDoesNotThrow(() -> jobTask.call());
    verify(scenario.store()).updateState(JOB_UUID, CONTINUATION_NS, repaired, 9, 7L);
    Assertions.assertEquals(JobStatus.PENDING, scenario.job().getStatus());
    Assertions.assertEquals(ExecutionStatus.SUCCEEDED, scenario.execution().getStatus());
    verify(jobStore, never()).incrementRetryAttempt(any(), any());
    verify(observabilityFacade, never()).recordJobFailure(any(), any(), anyInt());
    verify(lifecycleFacade, never()).completeFailure(any(), any(), anyBoolean());
  }

  @Test
  void counterExceptionUsesFailureHandling() throws Exception {
    var scenario = continuationScenario(() -> JobContext.current().requestContinuation(), 1000);
    when(scenario.store().initState(eq(JOB_UUID), eq(CONTINUATION_NS), anyString(), eq(7L)))
        .thenThrow(new IllegalStateException("counter unavailable"));
    when(jobStore.incrementRetryAttempt(JOB_UUID, 7L)).thenReturn(1);
    jobTask.call();
    verify(jobStore).incrementRetryAttempt(JOB_UUID, 7L);
  }

  @Test
  void transitionExceptionUsesFailureHandlingAfterSpendingCounter() throws Exception {
    var scenario = continuationScenario(() -> JobContext.current().requestContinuation(), 1000);
    when(scenario.store().initState(eq(JOB_UUID), eq(CONTINUATION_NS), anyString(), eq(7L)))
        .thenReturn(StateWriteResult.UPDATED);
    when(jobStore.scheduleJobRetry(JOB_UUID, null, FIXED_NOW, 0, 7L))
        .thenThrow(new IllegalStateException("transition unavailable"));
    when(jobStore.incrementRetryAttempt(JOB_UUID, 7L)).thenReturn(1);
    jobTask.call();
    verify(scenario.store()).initState(JOB_UUID, CONTINUATION_NS, "{\"count\":1}", 7L);
    verify(jobStore).incrementRetryAttempt(JOB_UUID, 7L);
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
  void resetFailureDoesNotUndoAcceptedTerminalFailure() throws Exception {
    var scenario = continuationScenario(() -> JobContext.current().requestContinuation(), 2);
    when(scenario.store().getState(JOB_UUID, CONTINUATION_NS))
        .thenReturn(Optional.of(new JobStateStore.VersionedState("{\"count\":2}", 5)));
    when(lifecycleFacade.completeFailure(scenario.job(), JobStatus.RUNNING, false))
        .thenReturn(true);
    when(scenario.store().updateState(JOB_UUID, CONTINUATION_NS, "{\"count\":0}", 5))
        .thenThrow(new IllegalStateException("reset unavailable"));
    jobTask.call();
    Assertions.assertEquals(JobStatus.FAILED, scenario.job().getStatus());
    verify(jobStore, never()).incrementRetryAttempt(any(), any());
  }

  private void assertContinuationLimit(boolean accepted) throws Exception {
    var scenario = continuationScenario(() -> JobContext.current().requestContinuation(), 2);
    when(scenario.store().getState(JOB_UUID, CONTINUATION_NS))
        .thenReturn(Optional.of(new JobStateStore.VersionedState("{\"count\":2}", 5)));
    when(lifecycleFacade.completeFailure(scenario.job(), JobStatus.RUNNING, false))
        .thenReturn(accepted);
    when(errorSanitizer.sanitize(any()))
        .thenAnswer(inv -> ((Throwable) inv.getArgument(0)).getMessage());
    jobTask.call();
    Assertions.assertTrue(scenario.job().getLastError().contains("limit 2"));
    Assertions.assertTrue(
        scenario.job().getLastError().contains("ratchet.execution.max-continuations"));
    if (accepted) {
      InOrder order = inOrder(lifecycleFacade, scenario.store());
      order.verify(lifecycleFacade).completeFailure(scenario.job(), JobStatus.RUNNING, false);
      order.verify(scenario.store()).updateState(JOB_UUID, CONTINUATION_NS, "{\"count\":0}", 5);
    } else {
      verify(scenario.store(), never()).updateState(any(), anyString(), anyString(), anyInt());
    }
    verify(jobStore, never()).incrementRetryAttempt(any(), any());
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
      when(lifecycleFacade.completeFailure(job, JobStatus.RUNNING, false)).thenReturn(true);
    } else {
      when(lifecycleFacade.completeSuccess(
              any(JobEntity.class), any(), any(), any(), any(), anyLong(), anyLong()))
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
      if (failure) order.verify(lifecycleFacade).completeFailure(job, JobStatus.RUNNING, false);
      else
        order
            .verify(lifecycleFacade)
            .completeSuccess(
                any(JobEntity.class), any(), any(), any(), any(), anyLong(), anyLong());
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
            any(JobEntity.class), any(), any(), any(), any(), anyLong(), anyLong()))
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
    when(lifecycleFacade.completeSuccess(any(), any(), any(), any(), any(), anyLong(), anyLong()))
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
              any(JobEntity.class), any(), any(), any(), any(), anyLong(), anyLong()))
          .thenReturn(true);
    } else if (!expectCooperative) {
      when(jobStore.incrementRetryAttempt(JOB_UUID, 0L)).thenReturn(1);
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
        verify(jobStore).incrementRetryAttempt(JOB_UUID, 0L);
      } else {
        verify(jobStore, never()).incrementRetryAttempt(any(UUID.class), eq(0L));
      }
      if (expectCooperative) {
        verify(lifecycleFacade, never()).completeFailure(any(JobEntity.class), any(), anyBoolean());
        verify(observabilityFacade, never()).publishEvent(any(JobDlqEvent.class));
      }
      if (failure == null) {
        verify(lifecycleFacade)
            .completeSuccess(eq(job), any(), any(), any(), any(), anyLong(), anyLong());
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
  private JobTask jobTask;

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
              any(JobEntity.class), any(), any(), any(), any(), anyLong(), anyLong()))
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
      when(lifecycleFacade.completeFailure(job, JobStatus.RUNNING, false)).thenReturn(true);

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
    CallbackRecorder.reset();
    OBSERVED_SIGNAL_DECISION.remove();
    OBSERVED_CUSTOM_ARGUMENT.remove();
    JsonbTestPayloadSerializer serializer = new JsonbTestPayloadSerializer();
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
            context -> new JBossLoggingJobLogger(context.jobId(), null),
            new DefaultResultPersistenceStrategy(RatchetOptions.defaults(), serializer, null),
            null,
            serializer,
            checkpointRegistrar,
            jobStateFlusher,
            null,
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
            any(JobEntity.class), any(), any(), any(), any(), anyLong(), anyLong()))
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
            any(JobEntity.class), any(), any(), any(), any(), anyLong(), anyLong()))
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
    when(jobStore.incrementRetryAttempt(JOB_UUID, 0L)).thenReturn(1);
    when(retryPolicy.shouldRetry(1, auditError)).thenReturn(false);
    when(lifecycleFacade.completeFailure(any(JobEntity.class), eq(JobStatus.RUNNING), eq(false)))
        .thenReturn(true);

    jobTask.call();

    // The payload never ran, and the job was failed cleanly through the terminal DLQ path.
    verify(resilienceStrategy, never()).execute(anyString(), any(), any(Callable.class));
    verify(lifecycleFacade).completeFailure(eq(job), eq(JobStatus.RUNNING), eq(false));
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
            any(JobEntity.class), any(), any(), any(), any(), anyLong(), anyLong()))
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
            any(JobEntity.class), any(), any(), any(), any(), anyLong(), anyLong()))
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
            any(JobEntity.class), any(), any(), any(), any(), anyLong(), anyLong()))
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
    verify(jobStore).scheduleJobRetry(eq(JOB_UUID), anyString(), any(), anyInt(), eq(0L));
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
        .scheduleJobRetry(
            eq(JOB_UUID),
            eq("Circuit breaker OPEN for service: " + serviceName),
            any(),
            eq(2),
            eq(0L));
    verify(observabilityFacade).saveExecution(any(JobExecutionEntity.class));
    verify(jobStore, never()).incrementRetryAttempt(any(UUID.class), eq(0L));
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
    when(jobStore.incrementRetryAttempt(JOB_UUID, 0L)).thenReturn(1);
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
    when(jobStore.incrementRetryAttempt(JOB_UUID, 0L)).thenReturn(1);
    when(retryPolicy.shouldRetry(1, error)).thenReturn(true);
    if (parentStatus != JobStatus.CANCELED) {
      when(retryPolicy.getDelay(1)).thenReturn(Duration.ofSeconds(5));
      when(jobStore.scheduleJobRetry(any(UUID.class), anyString(), any(), anyInt(), eq(0L)))
          .thenReturn(true);
    } else {
      when(lifecycleFacade.completeFailure(any(JobEntity.class), eq(JobStatus.RUNNING), eq(false)))
          .thenReturn(true);
    }

    jobTask.call();

    verify(jobStore, times(2)).getJobStatus(parentId);
    if (parentStatus != JobStatus.CANCELED) {
      verify(jobStore).scheduleJobRetry(eq(JOB_UUID), anyString(), any(), eq(1), eq(0L));
    } else {
      verify(jobStore, never()).scheduleJobRetry(any(), any(), any(), anyInt(), any());
      verify(lifecycleFacade).completeFailure(eq(job), eq(JobStatus.RUNNING), eq(false));
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
    when(jobStore.incrementRetryAttempt(JOB_UUID, 0L)).thenReturn(1);
    when(retryPolicy.shouldRetry(1, error)).thenReturn(false);
    when(lifecycleFacade.completeFailure(any(JobEntity.class), eq(JobStatus.RUNNING), eq(false)))
        .thenReturn(true);

    jobTask.call();

    verify(lifecycleFacade).completeFailure(eq(job), eq(JobStatus.RUNNING), eq(false));
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
            any(JobEntity.class), any(), any(), any(), any(), anyLong(), anyLong()))
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
    verify(jobStore, never()).incrementRetryAttempt(any(), anyLong());
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
    lenient().when(lifecycleFacade.completeFailure(job, JobStatus.RUNNING, false)).thenReturn(true);
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
    verify(jobStore, never()).incrementRetryAttempt(any(), anyLong());
    verify(jobStore, never()).scheduleJobRetry(any(), any(), any(), anyInt(), any());
    verify(lifecycleFacade, never()).completeFailure(any(), any(), anyBoolean());
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

    verify(jobStore, never()).incrementRetryAttempt(any(), anyLong());
    verify(lifecycleFacade, never()).completeFailure(any(), any(), anyBoolean());
    verify(observabilityFacade, never()).recordJobFailure(any(), any(), anyInt());
    ArgumentCaptor<JobExecutionEntity> execution =
        ArgumentCaptor.forClass(JobExecutionEntity.class);
    verify(observabilityFacade).saveExecution(execution.capture());
    Assertions.assertEquals(
        JobExecutionEntity.ExecutionStatus.FAILED, execution.getValue().getStatus());
  }

  @Test
  @SuppressWarnings("unchecked")
  void supersededDuringFailureHandlingDoesNotForceAnOutcome() throws Exception {
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
              Assertions.assertTrue(attempt.claimTimeoutForWatchdog());
              throw new IllegalStateException("failure classification failed");
            });

    Assertions.assertDoesNotThrow(jobTask::call);

    verify(jobStore, never()).incrementRetryAttempt(any(), anyLong());
    verify(lifecycleFacade, never()).completeFailure(any(), any(), anyBoolean());
    verify(observabilityFacade, never()).recordJobFailure(any(), any(), anyInt());
    ArgumentCaptor<JobExecutionEntity> execution =
        ArgumentCaptor.forClass(JobExecutionEntity.class);
    verify(observabilityFacade).saveExecution(execution.capture());
    Assertions.assertEquals(
        JobExecutionEntity.ExecutionStatus.FAILED, execution.getValue().getStatus());
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
    verify(jobStore, never()).incrementRetryAttempt(any(), anyLong());
    verify(timeoutHandler, never()).processCooperativeTimeout(any());
    verify(lifecycleFacade, never()).completeFailure(any(), any(), anyBoolean());
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
    when(jobStore.incrementRetryAttempt(JOB_UUID, 0L))
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
            "handleHardTimeoutById", JobAttemptControl.class, Future.class);
    handleHard.setAccessible(true);
    handleHard.invoke(
        timeoutHandler,
        timeoutHandler.newAttempt(JOB_UUID, 30, FIXED_NOW, 0, 0L, "node-1"),
        future);

    Assertions.assertEquals(
        1, attempts.get(), "a single hard timeout must consume exactly one attempt");
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
    when(jobStore.incrementRetryAttempt(JOB_UUID, 0L)).thenReturn(1);
    when(retryPolicy.shouldRetry(1, interrupt)).thenReturn(false);
    when(lifecycleFacade.completeFailure(any(JobEntity.class), eq(JobStatus.RUNNING), eq(false)))
        .thenReturn(true);

    task.call();

    verify(jobStore, times(1)).incrementRetryAttempt(JOB_UUID, 0L);
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
    when(lifecycleFacade.moveToDlqAndHandlePermanentFailure(any(), any())).thenReturn(true);
    jobTask.call();
    ArgumentCaptor<Throwable> failure = ArgumentCaptor.forClass(Throwable.class);
    verify(lifecycleFacade).moveToDlqAndHandlePermanentFailure(eq(job), failure.capture());
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
            any(JobEntity.class), any(PayloadDecryptionException.class)))
        .thenReturn(true);

    jobTask.call();

    ArgumentCaptor<JobEntity> dlqJob = ArgumentCaptor.forClass(JobEntity.class);
    verify(lifecycleFacade)
        .moveToDlqAndHandlePermanentFailure(
            dlqJob.capture(), any(PayloadDecryptionException.class));
    Assertions.assertEquals("poisoned-job", dlqJob.getValue().getBusinessKey());
    Assertions.assertEquals(JobExecutionType.SINGLE, dlqJob.getValue().getJobType());
    Assertions.assertEquals(JobPriority.HIGH, dlqJob.getValue().getPriority());
    Assertions.assertEquals("node-1", dlqJob.getValue().getPickedBy());
    Assertions.assertEquals(2, dlqJob.getValue().getAttempts());
    Assertions.assertEquals(3, dlqJob.getValue().getMaxRetries());
    Assertions.assertEquals(JobStatus.RUNNING, dlqJob.getValue().getStatus());
    verify(lifecycleFacade, never())
        .completeFailure(any(JobEntity.class), eq(JobStatus.RUNNING), eq(false));
    verify(observabilityFacade, never()).publishEvent(any(JobFailedEvent.class));
    verify(observabilityFacade, never()).publishEvent(any(JobDlqEvent.class));
    // Non-retryable: never rescheduled, never increments the attempt counter.
    verify(jobStore, never())
        .scheduleJobRetry(any(UUID.class), anyString(), any(), anyInt(), eq(0L));
    verify(jobStore, never()).incrementRetryAttempt(any(UUID.class), eq(0L));
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
            any(JobEntity.class), any(PayloadDecryptionException.class));
    verify(lifecycleFacade, never())
        .completeFailure(any(JobEntity.class), eq(JobStatus.RUNNING), eq(false));
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
            any(JobEntity.class), any(PayloadDecryptionException.class)))
        .thenReturn(true);

    jobTask.call();

    ArgumentCaptor<JobEntity> failedChild = ArgumentCaptor.forClass(JobEntity.class);
    verify(lifecycleFacade)
        .moveToDlqAndHandlePermanentFailure(
            failedChild.capture(), any(PayloadDecryptionException.class));
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
            any(JobEntity.class), any(PayloadDecryptionException.class));

    jobTask.call();

    verify(lifecycleFacade, never())
        .completeFailure(any(JobEntity.class), eq(JobStatus.RUNNING), eq(false));
    verify(jobStore, never())
        .scheduleJobRetry(any(UUID.class), anyString(), any(), anyInt(), eq(0L));
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
    when(jobStore.scheduleJobRetry(any(UUID.class), anyString(), any(), anyInt(), eq(0L)))
        .thenReturn(true);

    jobTask.call();

    // Released for an upgraded peer (attempt count preserved), with a skew metric.
    verify(jobStore)
        .scheduleJobRetry(eq(JOB_UUID), eq("sanitized skew"), any(), eq(claim.attempts()), eq(0L));
    verify(observabilityFacade).recordEnvelopeVersionSkew(JOB_UUID, 2, 1);
    // Not poison: never dead-lettered.
    verify(lifecycleFacade, never())
        .completeFailure(any(JobEntity.class), eq(JobStatus.RUNNING), eq(false));
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
    when(jobStore.scheduleJobRetry(any(UUID.class), any(), any(), anyInt(), eq(0L)))
        .thenReturn(true);

    Method requeue =
        JobTask.class.getDeclaredMethod(
            "requeueForUpgrade", UUID.class, UnsupportedEnvelopeVersionException.class);
    requeue.setAccessible(true);
    requeue.invoke(jobTask, JOB_UUID, new UnsupportedEnvelopeVersionException(2, 1));

    verify(jobStore).scheduleJobRetry(eq(JOB_UUID), any(), any(), eq(2), eq(0L));
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
    when(jobStore.incrementRetryAttempt(JOB_UUID, 0L)).thenReturn(1);
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
    when(lifecycleFacade.completeFailure(any(JobEntity.class), eq(JobStatus.RUNNING), eq(false)))
        .thenReturn(true);

    jobTask.call();

    verify(jobStore, never()).incrementRetryAttempt(any(UUID.class), eq(0L));
    verify(retryPolicy, never()).shouldRetry(anyInt(), any());
    verify(jobStore, never())
        .scheduleJobRetry(any(UUID.class), anyString(), any(), anyInt(), eq(0L));
    // The stored count stays at 2; observers see the third attempt, the one that failed.
    verify(observabilityFacade).recordJobFailure(job, error, 3);
    Assertions.assertEquals(2, job.getAttempts());
    verify(errorSanitizer, times(1)).sanitize(error);
    Assertions.assertEquals("safe do not retry", job.getLastError());
    verify(lifecycleFacade).completeFailure(eq(job), eq(JobStatus.RUNNING), eq(false));
    verify(lifecycleFacade).completeFailure(eq(job), eq(JobStatus.RUNNING), eq(false));
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
    when(lifecycleFacade.completeFailure(job, JobStatus.RUNNING, false)).thenReturn(false);

    jobTask.call();

    verify(lifecycleFacade).completeFailure(job, JobStatus.RUNNING, false);
    verify(payloadInvoker, never()).invoke(callback);
  }

  private JobTask callbackTask(JobPayloadInvoker invoker) {
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
        null,
        mock(AfterCommitRegistrar.class),
        mock(JobStateFlusher.class),
        null,
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
    doThrow(new IllegalStateException("observer failed"))
        .when(observabilityFacade)
        .recordJobFailure(job, original, job.getAttempts() + 1);
    when(lifecycleFacade.completeFailure(job, JobStatus.RUNNING, false)).thenReturn(true);
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
    doThrow(new IllegalStateException("observer failed"))
        .when(observabilityFacade)
        .recordJobFailure(job, original, job.getAttempts() + 1);
    when(lifecycleFacade.completeFailure(job, JobStatus.RUNNING, false)).thenReturn(false);

    task.call();

    verify(invoker, never()).invoke(any());
    verify(lifecycleFacade).completeFailure(job, JobStatus.RUNNING, false);
  }

  @Test
  void lastResortCallbackErrorDoesNotEscape() throws Exception {
    JobPayloadInvoker invoker = mock(JobPayloadInvoker.class);
    JobTask task = callbackTask(invoker);
    JobEntity job = jobWithFailureCallback();
    RuntimeException original = new RuntimeException("original");
    stubWorkerFailure(task, job, original);
    doThrow(new IllegalStateException("observer failed"))
        .when(observabilityFacade)
        .recordJobFailure(job, original, job.getAttempts() + 1);
    when(lifecycleFacade.completeFailure(job, JobStatus.RUNNING, false)).thenReturn(true);
    when(invoker.materializeArguments(any(), any())).thenAnswer(inv -> inv.getArgument(0));
    doThrow(new LinkageError("boom")).when(invoker).invoke(any());

    Assertions.assertDoesNotThrow(task::call);

    verify(invoker).invoke(any());
    Assertions.assertEquals(JobStatus.FAILED, job.getStatus());
  }

  @Test
  void onFailureCallbackErrorIsContainedOnNormalDlqPath() throws Exception {
    JobPayloadInvoker invoker = mock(JobPayloadInvoker.class);
    JobTask task = callbackTask(invoker);
    JobEntity job = jobWithFailureCallback();
    RuntimeException original = new RuntimeException("original");
    stubWorkerFailure(task, job, original);
    when(lifecycleFacade.completeFailure(job, JobStatus.RUNNING, false)).thenReturn(true);
    when(invoker.materializeArguments(any(), any())).thenAnswer(inv -> inv.getArgument(0));
    LinkageError callbackError = new LinkageError("boom");
    doThrow(callbackError).when(invoker).invoke(any());

    Assertions.assertDoesNotThrow(task::call);

    verify(invoker).invoke(any());
    verify(lifecycleFacade).completeFailure(job, JobStatus.RUNNING, false);
    verify(observabilityFacade).recordCallbackFailure(job, callbackError, 1);
  }

  @Test
  void hydrationPoisonDoesNotRunOnFailure() throws Exception {
    JobPayloadInvoker invoker = mock(JobPayloadInvoker.class);
    JobTask task = callbackTask(invoker);
    task.initFromClaim(claimForNode("node-1"));
    when(jobStore.findById(JOB_UUID))
        .thenThrow(new PayloadDecryptionException("ciphertext failed authentication"));
    when(lifecycleFacade.moveToDlqAndHandlePermanentFailure(any(), any())).thenReturn(true);

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
    verify(lifecycleFacade, never()).completeFailure(any(), any(), anyBoolean());
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
    when(validationFacade.shouldNotRetry(any())).thenReturn(true);
    doThrow(new IllegalStateException("observer failed"))
        .when(observabilityFacade)
        .recordJobFailure(eq(job), any(), eq(1));
    when(lifecycleFacade.completeFailure(job, JobStatus.RUNNING, false)).thenReturn(true);
    when(invoker.materializeArguments(any(), any())).thenAnswer(inv -> inv.getArgument(0));
    JobMdcContext.clear();

    task.call();

    ArgumentCaptor<JobPayload> invoked = ArgumentCaptor.forClass(JobPayload.class);
    verify(invoker).invoke(invoked.capture());
    Assertions.assertTrue(invoked.getValue().args().get(1) instanceof PayloadDecryptionException);
    Assertions.assertNotNull(invoked.getValue().args().get(0));
    ArgumentCaptor<Throwable> original = ArgumentCaptor.forClass(Throwable.class);
    verify(observabilityFacade).recordJobFailure(eq(job), original.capture(), eq(1));
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
    when(jobStore.incrementRetryAttempt(JOB_UUID, 0L)).thenReturn(1);
    doThrow(new IllegalStateException("observer failed"))
        .when(observabilityFacade)
        .recordJobFailure(job, error, 1);
    when(errorSanitizer.sanitize(error)).thenReturn("safe original");
    when(lifecycleFacade.completeFailure(any(JobEntity.class), eq(JobStatus.RUNNING), eq(false)))
        .thenReturn(true);

    jobTask.call();

    verify(observabilityFacade, never()).publishEvent(any(JobFailedEvent.class));
    verify(observabilityFacade, never()).publishEvent(any(JobDlqEvent.class));
    verify(lifecycleFacade).completeFailure(eq(job), eq(JobStatus.RUNNING), eq(false));
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
    when(lifecycleFacade.completeFailure(any(JobEntity.class), eq(JobStatus.RUNNING), eq(false)))
        .thenReturn(true);

    jobTask.call();

    verify(lifecycleFacade).completeFailure(eq(job), eq(JobStatus.RUNNING), eq(false));
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
    JobEntity job = createTestJob();
    initJobTaskWithDefaultStubs(job);
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING, JobStatus.CANCELED);
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);
    when(resilienceStrategy.execute(anyString(), any(), any(Callable.class)))
        .thenAnswer(inv -> ((Callable<?>) inv.getArgument(2)).call());

    jobTask.call();

    verify(lifecycleFacade, never())
        .completeSuccess(any(JobEntity.class), any(), any(), any(), any(), anyLong(), anyLong());
    verify(observabilityFacade).recordJobCancellation(job);
    verify(observabilityFacade, never()).publishEvent(any(JobCompletedEvent.class));
    verify(observabilityFacade, never()).recordJobSuccess(any(), anyLong());
    verify(lifecycleFacade).cancelChain(job);
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
            any(JobEntity.class), any(), any(), any(), any(), anyLong(), anyLong()))
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
            any(JobEntity.class), any(), any(), any(), any(), anyLong(), anyLong()))
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
        .completeSuccess(eq(job), any(), any(), eq(FIXED_NOW), eq(FIXED_NOW), eq(0L), anyLong());
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
            any(JobEntity.class), any(), any(), any(), any(), anyLong(), anyLong()))
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
            any(JobEntity.class), any(), any(), any(), any(), anyLong(), anyLong()))
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
    when(jobStore.scheduleJobRetry(eq(JOB_UUID), anyString(), any(), anyInt(), eq(0L)))
        .thenAnswer(inv -> queueStatus.get() == JobStatus.RUNNING);

    jobTask.call();

    verify(resourcePermitService, times(2)).tryAcquire("gpu", JOB_UUID, 0L, "node-1");
    verifyNoInteractions(lifecycleFacade);
    verify(resilienceStrategy, never()).execute(anyString(), any(), any(Callable.class));
    verify(jobStore).scheduleJobRetry(eq(JOB_UUID), anyString(), any(), anyInt(), eq(0L));
    verify(jobStore, never()).incrementRetryAttempt(any(), any());
    verifyNoAttemptRecorded();
  }

  @Test
  void call_exhaustedTransientAcquireUsesExistingFailurePath() throws Exception {
    assertAcquireFailure(false);
  }

  @Test
  void call_interruptedAcquireBackoffStopsAndRestoresInterrupt() throws Exception {
    try {
      assertAcquireFailure(true);
      Assertions.assertTrue(Thread.currentThread().isInterrupted());
    } finally {
      Thread.interrupted();
    }
  }

  private void assertAcquireFailure(boolean interrupt) throws Exception {
    JobEntity job = createTestJob();
    job.setResourceName("gpu");
    initJobTaskWithDefaultStubs(job);
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING);
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);
    var failure = new RatchetTransientStoreException("deadlock");
    when(resourcePermitService.tryAcquire("gpu", JOB_UUID, 0L, "node-1"))
        .thenAnswer(
            inv -> {
              if (interrupt) {
                Thread.currentThread().interrupt();
              }
              throw failure;
            });
    when(jobStore.incrementRetryAttempt(JOB_UUID, 0L)).thenReturn(1);
    when(lifecycleFacade.completeFailure(job, JobStatus.RUNNING, false)).thenReturn(true);

    jobTask.call();

    verify(resourcePermitService, times(interrupt ? 1 : JobTask.PERMIT_ACQUIRE_TRIES))
        .tryAcquire("gpu", JOB_UUID, 0L, "node-1");
    verify(jobStore).incrementRetryAttempt(JOB_UUID, 0L);
    verify(lifecycleFacade).completeFailure(job, JobStatus.RUNNING, false);
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
            any(JobEntity.class), any(), any(), any(), any(), anyLong(), anyLong()))
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
            any(JobEntity.class), any(), any(), any(), any(), anyLong(), anyLong()))
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
    when(lifecycleFacade.completeFailure(job, JobStatus.RUNNING, false)).thenReturn(true);

    jobTask.call();

    InOrder order = inOrder(resourcePermitService, observabilityFacade, lifecycleFacade);
    order.verify(resourcePermitService).tryAcquire("gpu", JOB_UUID, 7L, "node-1");
    order.verify(observabilityFacade).startExecution(JOB_UUID, 1, "node-1");
    order.verify(lifecycleFacade).completeFailure(job, JobStatus.RUNNING, false);
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
    when(lifecycleFacade.completeFailure(job, JobStatus.RUNNING, false)).thenReturn(true);

    jobTask.call();

    verify(lifecycleFacade).completeFailure(job, JobStatus.RUNNING, false);
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
    when(lifecycleFacade.completeFailure(job, JobStatus.RUNNING, false)).thenReturn(true);

    jobTask.call();

    verify(lifecycleFacade).completeFailure(job, JobStatus.RUNNING, false);
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
    when(jobStore.scheduleJobRetry(eq(JOB_UUID), anyString(), any(), eq(0), eq(0L)))
        .thenReturn(true);

    jobTask.call();

    verify(jobStore)
        .scheduleJobRetry(
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
    JobEntity job = createTestJob();
    job.setResourceName("gpu");
    initJobTaskWithDefaultStubs(job);
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.CANCELED);

    jobTask.call();

    verify(lifecycleFacade).cancelChain(job);
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
    when(jobStore.scheduleJobRetry(eq(JOB_UUID), anyString(), any(), eq(2), eq(7L)))
        .thenReturn(rescheduled);

    jobTask.call();

    verify(jobStore)
        .scheduleJobRetry(eq(JOB_UUID), eq("Waiting for resource: gpu"), any(), eq(2), eq(7L));
    verify(resilienceStrategy, never()).execute(anyString(), any(), any(Callable.class));
    verify(resourcePermitService, never()).release(anyString(), any(UUID.class), anyLong());
    // The superseded attempt never ran its body; the claim that replaced it records its own.
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
  }

  @Test
  void claimedBatchChildNeverStartsWhenParentCheckFails() throws Exception {
    when(jobStore.scheduleJobRetry(
            eq(JOB_UUID), any(), eq(FIXED_NOW.plusSeconds(30)), eq(2), eq(7L)))
        .thenReturn(true);
    assertBatchChildNeverStarts(
        parentId ->
            when(jobStore.getJobStatus(parentId))
                .thenThrow(new IllegalStateException("store unavailable")),
        false);
    verify(jobStore)
        .scheduleJobRetry(eq(JOB_UUID), any(), eq(FIXED_NOW.plusSeconds(30)), eq(2), eq(7L));
  }

  @Test
  void parentCheckFailureLogsClaimReleaseFailureWithoutRethrowing() throws Exception {
    IllegalStateException releaseFailure = new IllegalStateException("release unavailable");
    when(jobStore.scheduleJobRetry(
            eq(JOB_UUID), any(), eq(FIXED_NOW.plusSeconds(30)), eq(2), eq(7L)))
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
    verify(lifecycleFacade, never()).completeFailure(any(), any(), anyBoolean());
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
            any(JobEntity.class), any(), any(), any(), any(), anyLong(), anyLong()))
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
            any(JobEntity.class), any(), any(), any(), any(), anyLong(), anyLong()))
        .thenReturn(transitionWins);

    jobTask.call();

    verify(resilienceStrategy, times(1)).execute(anyString(), any(), any(Callable.class));
    verify(lifecycleFacade, times(1))
        .completeSuccess(any(JobEntity.class), any(), any(), any(), any(), anyLong(), anyLong());
    verify(jobStore, never()).incrementRetryAttempt(any(UUID.class), eq(0L));
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
            any(JobEntity.class), any(), any(), any(), any(), anyLong(), anyLong()))
        .thenThrow(new RatchetTransientStoreException("deadlock"))
        .thenReturn(true);

    jobTask.call();

    verify(lifecycleFacade, times(2))
        .completeSuccess(any(JobEntity.class), any(), any(), any(), any(), anyLong(), anyLong());
    verify(lifecycleFacade, never())
        .completeSuccessMinimal(any(JobEntity.class), any(), any(), anyLong(), anyLong());
    verify(jobStore, never()).incrementRetryAttempt(any(UUID.class), eq(0L));
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
            any(JobEntity.class), any(), any(), any(), any(), anyLong(), anyLong()))
        .thenThrow(new RatchetTransientStoreException("deadlock"));
    when(lifecycleFacade.completeSuccessMinimal(
            any(JobEntity.class), any(), any(), anyLong(), anyLong()))
        .thenReturn(true);

    jobTask.call();

    verify(lifecycleFacade, times(5))
        .completeSuccess(any(JobEntity.class), any(), any(), any(), any(), anyLong(), anyLong());
    verify(lifecycleFacade)
        .completeSuccessMinimal(any(JobEntity.class), any(), any(), anyLong(), anyLong());
    verify(jobStore, never()).incrementRetryAttempt(any(UUID.class), eq(0L));
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
            any(JobEntity.class), any(), any(), any(), any(), anyLong(), anyLong()))
        .thenThrow(new RatchetTransientStoreException("deadlock"));
    when(lifecycleFacade.completeSuccessMinimal(
            any(JobEntity.class), any(), any(), anyLong(), anyLong()))
        .thenThrow(new RatchetTransientStoreException("deadlock"));

    jobTask.call();

    verify(lifecycleFacade, times(5))
        .completeSuccess(any(JobEntity.class), any(), any(), any(), any(), anyLong(), anyLong());
    verify(lifecycleFacade)
        .completeSuccessMinimal(any(JobEntity.class), any(), any(), anyLong(), anyLong());
    verify(jobStore, never()).incrementRetryAttempt(any(UUID.class), eq(0L));
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
    when(lifecycleFacade.completeSuccess(any(), any(), any(), any(), any(), anyLong(), anyLong()))
        .thenThrow(new RatchetTransientStoreException("busy"));
    return scenario;
  }

  @Test
  void handleSuccess_stuckFinalizationDoesNotMoveSuccessfulJobToFailurePath() throws Exception {
    AtomicInteger bodies = new AtomicInteger();
    var scenario = stuckSuccessScenario(bodies);
    when(lifecycleFacade.completeSuccessMinimal(any(), any(), any(), anyLong(), anyLong()))
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
        .completeSuccessMinimal(any(), any(), any(), anyLong(), anyLong());
    verify(observabilityFacade).recordJobSuccess(eq(scenario.job()), anyLong());
    verify(observabilityFacade).saveExecution(scenario.execution());
    verify(observabilityFacade).recordSuccessFinalizationStuck(scenario.job());
    verify(observabilityFacade).recordSuccessFinalizationMinimal(scenario.job());
    verify(jobStore, never()).incrementRetryAttempt(any(), any());
    verify(lifecycleFacade, never()).completeFailure(any(), any(), anyBoolean());
    verify(lifecycleFacade, never()).moveToDlq(any(), any());
  }

  @Test
  void handleSuccess_interruptedSlowFinalizationContinuesAndRestoresInterrupt() throws Exception {
    AtomicInteger bodies = new AtomicInteger();
    var scenario = stuckSuccessScenario(bodies);
    AtomicInteger writes = new AtomicInteger();
    when(lifecycleFacade.completeSuccessMinimal(any(), any(), any(), anyLong(), anyLong()))
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
    verify(jobStore, never()).incrementRetryAttempt(any(), any());
    verify(lifecycleFacade, never()).completeFailure(any(), any(), anyBoolean());
  }

  @Test
  void handleSuccess_stuckThenTerminalSkippedDiscardsExecution() throws Exception {
    AtomicInteger bodies = new AtomicInteger();
    var scenario = stuckSuccessScenario(bodies);
    when(lifecycleFacade.completeSuccessMinimal(any(), any(), any(), anyLong(), anyLong()))
        .thenThrow(new RatchetTransientStoreException("busy"))
        .thenReturn(false);

    jobTask.call();

    Assertions.assertEquals(1, bodies.get());
    Assertions.assertTrue(scenario.attempt().isSuperseded());
    Assertions.assertEquals(ExecutionStatus.FAILED, scenario.execution().getStatus());
    verify(observabilityFacade).saveExecution(scenario.execution());
    verify(observabilityFacade, never()).recordJobSuccess(any(), anyLong());
    verify(jobStore, never()).incrementRetryAttempt(any(), any());
    verify(lifecycleFacade, never()).completeFailure(any(), any(), anyBoolean());
  }

  @Test
  void handleSuccess_fenceBounceDuringSlowFinalizationStillDiscardsExecution() throws Exception {
    AtomicInteger bodies = new AtomicInteger();
    var scenario = stuckSuccessScenario(bodies);
    AtomicInteger writes = new AtomicInteger();
    when(lifecycleFacade.completeSuccessMinimal(any(), any(), any(), anyLong(), anyLong()))
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
    verify(jobStore, never()).incrementRetryAttempt(any(), any());
    verify(lifecycleFacade, never()).completeFailure(any(), any(), anyBoolean());
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
    when(lifecycleFacade.completeSuccessMinimal(any(), any(), any(), anyLong(), anyLong()))
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
    verify(jobStore, never()).incrementRetryAttempt(any(), any());
    verify(lifecycleFacade, never()).completeFailure(any(), any(), anyBoolean());
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
            any(JobEntity.class), any(), any(), any(), any(), anyLong(), anyLong()))
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
    when(lifecycleFacade.completeFailure(any(JobEntity.class), eq(JobStatus.RUNNING), eq(false)))
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
    verify(jobStore, never()).incrementRetryAttempt(any(UUID.class), eq(0L));
    verify(lifecycleFacade).completeFailure(eq(job), eq(JobStatus.RUNNING), eq(false));
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
            any(JobEntity.class), any(), any(), any(), any(), anyLong(), anyLong()))
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
    when(lifecycleFacade.completeFailure(any(JobEntity.class), eq(JobStatus.RUNNING), eq(false)))
        .thenReturn(true);

    jobTask.call();

    verify(lifecycleFacade).completeFailure(any(JobEntity.class), eq(JobStatus.RUNNING), eq(false));
    // Poison surfaces with its true type now (no IllegalArgumentException wrap); it is still DLQ'd.
    verify(observabilityFacade)
        .recordJobFailure(eq(job), any(PayloadDecryptionException.class), eq(1));
    verify(observabilityFacade, never()).startExecution(any(UUID.class), anyInt(), anyString());
    verify(resilienceStrategy, never()).execute(anyString(), any(), any(Callable.class));
    verify(lifecycleFacade).completeFailure(eq(job), eq(JobStatus.RUNNING), eq(false));
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
            any(JobEntity.class), any(), any(), any(), any(), anyLong(), anyLong()))
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
    when(nodeIdProvider.getNodeId()).thenReturn("node-1");
  }

  private void initJobTaskWithDefaultStubs(JobTask task, JobEntity job) {
    task.init(job);
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
    when(jobStore.incrementRetryAttempt(JOB_UUID, 37L)).thenReturn(-1);

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
    verify(jobStore).incrementRetryAttempt(JOB_UUID, 37L);
    verify(jobStore, never()).scheduleJobRetry(any(), any(), any(), anyInt(), any());
    verify(lifecycleFacade, never()).completeFailure(any(), any(), anyBoolean());
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
    when(lifecycleFacade.completeFailure(job, JobStatus.RUNNING, false)).thenReturn(true);
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
}
