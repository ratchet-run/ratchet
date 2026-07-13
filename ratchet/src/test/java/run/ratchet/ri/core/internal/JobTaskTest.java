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
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.Serializable;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.jboss.logging.MDC;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import run.ratchet.api.CircuitBreakerProtected;
import run.ratchet.api.JobContext;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobStatus;
import run.ratchet.api.RatchetOptions;
import run.ratchet.api.SerializableCheckedRunnable;
import run.ratchet.api.SignalDecision;
import run.ratchet.api.event.JobCallbackFailedEvent;
import run.ratchet.api.event.JobCompletedEvent;
import run.ratchet.api.event.JobDlqEvent;
import run.ratchet.api.event.JobFailedEvent;
import run.ratchet.api.event.JobStartedEvent;
import run.ratchet.api.exception.CircuitBreakerOpenException;
import run.ratchet.api.exception.KeyProviderUnavailableException;
import run.ratchet.api.exception.PayloadDecryptionException;
import run.ratchet.api.exception.RatchetTransientStoreException;
import run.ratchet.api.exception.SignalOutcomeHydrationException;
import run.ratchet.api.exception.UnsupportedEnvelopeVersionException;
import run.ratchet.ri.core.DefaultJobSchedulerService;
import run.ratchet.ri.core.DefaultResultPersistenceStrategy;
import run.ratchet.ri.core.JBossLoggingJobLogger;
import run.ratchet.ri.core.ResourcePermitService;
import run.ratchet.ri.payload.JobPayloadFactory;
import run.ratchet.ri.testsupport.EncryptionTestKit;
import run.ratchet.ri.testutil.JsonbTestPayloadSerializer;
import run.ratchet.spi.BeanResolver;
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
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.entity.JobPayload;
import run.ratchet.store.spi.JobStore;
import run.ratchet.store.util.EncryptionTarget;
import run.ratchet.store.util.PayloadEncryptor;

@ExtendWith(MockitoExtension.class)
class JobTaskTest {

  private static final UUID JOB_UUID = new UUID(0L, 42L);
  private static final Instant FIXED_NOW = Instant.parse("2026-05-05T12:00:00Z");
  private static final Clock FIXED_CLOCK = Clock.fixed(FIXED_NOW, java.time.ZoneOffset.UTC);
  private static final ThreadLocal<SignalDecision> OBSERVED_SIGNAL_DECISION = new ThreadLocal<>();
  private static final ThreadLocal<String> OBSERVED_SIGNAL_STRING = new ThreadLocal<>();
  private static final ThreadLocal<CustomArgument> OBSERVED_CUSTOM_ARGUMENT = new ThreadLocal<>();

  private final ClassPolicy classPolicy = className -> true;
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
                new JobSuccessFinalizer(jobStore, observabilityFacade),
                retryPolicy,
                resilienceStrategy,
                errorSanitizer,
                context -> null,
                null,
                null,
                null,
                null,
                null,
                null));
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
            new JobSuccessFinalizer(jobStore, observabilityFacade),
            retryPolicy,
            resilienceStrategy,
            errorSanitizer,
            context -> new JBossLoggingJobLogger(context.jobId(), null),
            new DefaultResultPersistenceStrategy(RatchetOptions.defaults(), serializer, null),
            null,
            serializer,
            null,
            Clock.systemUTC(),
            null);
  }

  @AfterEach
  void tearDown() {
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
    when(resilienceStrategy.execute(anyString(), any(Callable.class)))
        .thenAnswer(invocation -> ((Callable<?>) invocation.getArgument(1)).call());
    when(jobStore.markJobSucceeded(
            any(UUID.class), any(), any(), any(), any(), anyLong(), anyLong()))
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
    when(resilienceStrategy.execute(anyString(), any(Callable.class)))
        .thenAnswer(inv -> ((Callable<?>) inv.getArgument(1)).call());
    when(jobStore.markJobSucceeded(
            any(UUID.class), any(), any(), any(), any(), anyLong(), anyLong()))
        .thenReturn(true);

    jobTask.call();

    verify(resilienceStrategy)
        .execute(eq(JobTaskTest.class.getSimpleName() + ".testJobMethod"), any(Callable.class));
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
    when(observabilityFacade.startExecution(any(UUID.class), anyInt(), anyString()))
        .thenThrow(auditError);
    when(validationFacade.shouldNotRetry(auditError)).thenReturn(false);
    when(jobStore.incrementRetryAttempt(JOB_UUID)).thenReturn(1);
    when(retryPolicy.shouldRetry(1, auditError)).thenReturn(false);
    when(jobStore.compareAndSwapStatus(
            eq(JOB_UUID), eq(JobStatus.RUNNING), eq(JobStatus.FAILED), any()))
        .thenReturn(true);

    jobTask.call();

    // The payload never ran, and the job was failed cleanly through the terminal DLQ path.
    verify(resilienceStrategy, never()).execute(anyString(), any(Callable.class));
    verify(lifecycleFacade).moveToDlq(eq(job), eq(auditError));
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
    when(resilienceStrategy.execute(anyString(), any(Callable.class)))
        .thenAnswer(inv -> ((Callable<?>) inv.getArgument(1)).call());
    when(jobStore.markJobSucceeded(
            any(UUID.class), any(), any(), any(), any(), anyLong(), anyLong()))
        .thenReturn(true);

    jobTask.call();

    ArgumentCaptor<String> serviceNameCaptor = ArgumentCaptor.forClass(String.class);
    verify(resilienceStrategy).execute(serviceNameCaptor.capture(), any(Callable.class));
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
            List.of()));
    initJobTaskWithDefaultStubs(job);
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING);
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);
    when(resilienceStrategy.execute(anyString(), any(Callable.class)))
        .thenAnswer(inv -> ((Callable<?>) inv.getArgument(1)).call());
    when(jobStore.markJobSucceeded(
            any(UUID.class), any(), any(), any(), any(), anyLong(), anyLong()))
        .thenReturn(true);

    jobTask.call();

    verify(resilienceStrategy).execute(eq("external-api"), any(Callable.class));
  }

  @Test
  void call_checksServiceAvailableBeforeExecution() throws Exception {
    JobEntity job = createTestJob();
    initJobTaskWithDefaultStubs(job);
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING);
    when(resilienceStrategy.isServiceAvailable(
            JobTaskTest.class.getSimpleName() + ".testJobMethod"))
        .thenReturn(true);
    when(resilienceStrategy.execute(anyString(), any(Callable.class)))
        .thenAnswer(inv -> ((Callable<?>) inv.getArgument(1)).call());
    when(jobStore.markJobSucceeded(
            any(UUID.class), any(), any(), any(), any(), anyLong(), anyLong()))
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

    verify(resilienceStrategy, never()).execute(anyString(), any(Callable.class));
    verify(jobStore).scheduleJobRetry(eq(JOB_UUID), anyString(), any(), anyInt());
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
    when(resilienceStrategy.execute(eq(serviceName), any(Callable.class))).thenThrow(rejection);
    when(resilienceStrategy.getRetryDelay(serviceName)).thenReturn(Duration.ofMillis(250));

    jobTask.call();

    verify(resilienceStrategy).getRetryDelay(serviceName);
    verify(jobStore)
        .scheduleJobRetry(
            eq(JOB_UUID), eq("Circuit breaker OPEN for service: " + serviceName), any(), eq(2));
    verify(observabilityFacade).saveExecution(any(JobExecutionEntity.class));
    verify(jobStore, never()).incrementRetryAttempt(any(UUID.class));
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
    when(resilienceStrategy.execute(anyString(), any(Callable.class))).thenThrow(error);
    when(validationFacade.shouldNotRetry(error)).thenReturn(false);
    when(jobStore.incrementRetryAttempt(JOB_UUID)).thenReturn(1);
    when(retryPolicy.shouldRetry(1, error)).thenReturn(true);
    when(retryPolicy.getDelay(1)).thenReturn(Duration.ofSeconds(5));
    when(errorSanitizer.sanitize(error)).thenReturn("safe boom");
    when(jobStore.scheduleJobRetry(any(UUID.class), anyString(), any(), anyInt())).thenReturn(true);

    jobTask.call();

    verify(retryPolicy).shouldRetry(1, error);
    verify(errorSanitizer, times(1)).sanitize(error);
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
    when(resilienceStrategy.execute(anyString(), any(Callable.class))).thenThrow(error);
    when(validationFacade.shouldNotRetry(error)).thenReturn(false);
    when(jobStore.incrementRetryAttempt(JOB_UUID)).thenReturn(1);
    when(retryPolicy.shouldRetry(1, error)).thenReturn(false);
    when(jobStore.compareAndSwapStatus(
            eq(JOB_UUID), eq(JobStatus.RUNNING), eq(JobStatus.FAILED), any()))
        .thenReturn(true);

    jobTask.call();

    verify(lifecycleFacade).moveToDlq(eq(job), eq(error));
  }

  @Test
  @SuppressWarnings("unchecked")
  void hardTimeoutAndInterruptedWorker_consumeExactlyOneAttempt() throws Exception {
    // A hard timeout interrupts the worker. The watchdog (processHardTimeout) and the interrupted
    // worker (handleFailure) both run while the row is still RUNNING. Without coordination both
    // increment the attempt and one timeout burns two attempts, dead-lettering at half maxRetries.
    JobEntity job = createTestJob();
    job.setMaxRetries(3);

    AtomicInteger attempts = new AtomicInteger(0);
    // Mirror the store: incrementRetryAttempt only matches a RUNNING/WAITING row. The watchdog's
    // reschedule moves the row off RUNNING, so a later increment returns -1.
    AtomicInteger running = new AtomicInteger(1);
    when(jobStore.incrementRetryAttempt(JOB_UUID))
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
    when(resilienceStrategy.execute(anyString(), any(Callable.class))).thenThrow(interrupt);
    // The deferring worker never reaches shouldNotRetry, so keep this lenient.
    lenient().when(validationFacade.shouldNotRetry(interrupt)).thenReturn(false);

    // Reproduce the both-increment-before-either-CAS window: the worker's handleFailure runs while
    // the watchdog still holds the marker and the row is still RUNNING — i.e. exactly when the
    // watchdog is mid-reschedule. scheduleJobRetry runs the worker first, THEN flips the row off
    // RUNNING, just as the real CAS would. With the fix the worker defers (no second increment).
    when(jobStore.scheduleJobRetry(eq(JOB_UUID), any(), any(), anyInt()))
        .thenAnswer(
            inv -> {
              task.call();
              running.set(0);
              return true;
            });

    java.util.concurrent.FutureTask<Void> future =
        new java.util.concurrent.FutureTask<>(() -> null);
    java.lang.reflect.Method handleHard =
        JobTimeoutHandler.class.getDeclaredMethod(
            "handleHardTimeoutById",
            UUID.class,
            java.util.concurrent.Future.class,
            Instant.class,
            long.class);
    handleHard.setAccessible(true);
    handleHard.invoke(timeoutHandler, JOB_UUID, future, FIXED_NOW, 30L);

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
    when(resilienceStrategy.execute(anyString(), any(Callable.class))).thenThrow(interrupt);
    when(validationFacade.shouldNotRetry(interrupt)).thenReturn(false);
    when(jobStore.incrementRetryAttempt(JOB_UUID)).thenReturn(1);
    when(retryPolicy.shouldRetry(1, interrupt)).thenReturn(false);
    when(jobStore.compareAndSwapStatus(
            eq(JOB_UUID), eq(JobStatus.RUNNING), eq(JobStatus.FAILED), any()))
        .thenReturn(true);

    task.call();

    verify(jobStore, times(1)).incrementRetryAttempt(JOB_UUID);
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
            null);
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
    verify(jobStore, never())
        .compareAndSwapStatus(eq(JOB_UUID), eq(JobStatus.RUNNING), eq(JobStatus.FAILED), any());
    verify(observabilityFacade, never()).publishEvent(any(JobFailedEvent.class));
    verify(observabilityFacade, never()).publishEvent(any(JobDlqEvent.class));
    // Non-retryable: never rescheduled, never increments the attempt counter.
    verify(jobStore, never()).scheduleJobRetry(any(UUID.class), anyString(), any(), anyInt());
    verify(jobStore, never()).incrementRetryAttempt(any(UUID.class));
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
    verify(jobStore, never())
        .compareAndSwapStatus(eq(JOB_UUID), eq(JobStatus.RUNNING), eq(JobStatus.FAILED), any());
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
            parentId);
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

    verify(jobStore, never())
        .compareAndSwapStatus(eq(JOB_UUID), eq(JobStatus.RUNNING), eq(JobStatus.FAILED), any());
    verify(jobStore, never()).scheduleJobRetry(any(UUID.class), anyString(), any(), anyInt());
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
    when(jobStore.findById(JOB_UUID)).thenThrow(new UnsupportedEnvelopeVersionException(2, 1));
    when(jobStore.scheduleJobRetry(any(UUID.class), anyString(), any(), anyInt())).thenReturn(true);

    jobTask.call();

    // Released for an upgraded peer (attempt count preserved), with a skew metric.
    verify(jobStore).scheduleJobRetry(eq(JOB_UUID), anyString(), any(), eq(claim.attempts()));
    verify(observabilityFacade).recordEnvelopeVersionSkew(JOB_UUID, 2, 1);
    // Not poison: never dead-lettered.
    verify(jobStore, never())
        .compareAndSwapStatus(eq(JOB_UUID), eq(JobStatus.RUNNING), eq(JobStatus.FAILED), any());
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
    when(jobStore.scheduleJobRetry(any(UUID.class), any(), any(), anyInt())).thenReturn(true);

    java.lang.reflect.Method requeue =
        JobTask.class.getDeclaredMethod(
            "requeueForUpgrade", UUID.class, UnsupportedEnvelopeVersionException.class);
    requeue.setAccessible(true);
    requeue.invoke(jobTask, JOB_UUID, new UnsupportedEnvelopeVersionException(2, 1));

    verify(jobStore).scheduleJobRetry(eq(JOB_UUID), any(), any(), eq(2));
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
    when(jobStore.incrementRetryAttempt(JOB_UUID)).thenReturn(1);
    when(retryPolicy.shouldRetry(eq(1), any())).thenReturn(true);
    when(retryPolicy.getDelay(1)).thenReturn(Duration.ofSeconds(5));
    when(errorSanitizer.sanitize(any())).thenReturn("safe transient failure");
    when(jobStore.scheduleJobRetry(any(UUID.class), anyString(), any(), anyInt())).thenReturn(true);

    jobTask.call();

    // Transient outage is retryable: the job is rescheduled, never dead-lettered.
    verify(jobStore).scheduleJobRetry(any(UUID.class), anyString(), any(), anyInt());
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
    when(resilienceStrategy.execute(anyString(), any(Callable.class))).thenThrow(error);
    when(validationFacade.shouldNotRetry(error)).thenReturn(true);
    when(errorSanitizer.sanitize(error)).thenReturn("safe do not retry");
    when(jobStore.compareAndSwapStatus(
            eq(JOB_UUID), eq(JobStatus.RUNNING), eq(JobStatus.FAILED), eq("safe do not retry")))
        .thenReturn(true);

    jobTask.call();

    verify(jobStore, never()).incrementRetryAttempt(any(UUID.class));
    verify(retryPolicy, never()).shouldRetry(anyInt(), any());
    verify(jobStore, never()).scheduleJobRetry(any(UUID.class), anyString(), any(), anyInt());
    verify(observabilityFacade).recordJobFailure(job, error, 2);
    verify(errorSanitizer, times(1)).sanitize(error);
    Assertions.assertEquals("safe do not retry", job.getLastError());
    verify(lifecycleFacade).moveToDlq(eq(job), eq(error));
    verify(lifecycleFacade).scheduleNext(job);
  }

  @Test
  @SuppressWarnings("unchecked")
  void handleFailureFallbackPublishesFailureEventAndDelegatesDlqEvent() throws Exception {
    JobEntity job = createTestJob();
    initJobTaskWithDefaultStubs(job);
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING);
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);

    RuntimeException error = new RuntimeException("original");
    when(resilienceStrategy.execute(anyString(), any(Callable.class))).thenThrow(error);
    when(validationFacade.shouldNotRetry(error)).thenReturn(false);
    when(jobStore.incrementRetryAttempt(JOB_UUID)).thenReturn(1);
    doThrow(new IllegalStateException("observer failed"))
        .when(observabilityFacade)
        .recordJobFailure(job, error, 1);
    when(errorSanitizer.sanitize(error)).thenReturn("safe original");
    when(jobStore.compareAndSwapStatus(
            eq(JOB_UUID), eq(JobStatus.RUNNING), eq(JobStatus.FAILED), eq("safe original")))
        .thenReturn(true);

    jobTask.call();

    verify(observabilityFacade).publishEvent(any(JobFailedEvent.class));
    verify(observabilityFacade, never()).publishEvent(any(JobDlqEvent.class));
    verify(lifecycleFacade).moveToDlq(job, error);
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
    when(resilienceStrategy.execute(anyString(), any(Callable.class))).thenThrow(error);
    when(validationFacade.shouldNotRetry(error)).thenReturn(true);
    when(jobStore.compareAndSwapStatus(
            eq(JOB_UUID), eq(JobStatus.RUNNING), eq(JobStatus.FAILED), any()))
        .thenReturn(true);

    jobTask.call();

    verify(lifecycleFacade).markBatchChildFailed(job);
    verify(lifecycleFacade, never()).scheduleNext(job);
  }

  @Test
  @SuppressWarnings("unchecked")
  void handleCanceledDuringExecution_batchChild_marksBatchChildFailedInsteadOfCancelingChain()
      throws Exception {
    JobEntity job = createTestJob();
    job.setJobType(JobExecutionType.BATCH_CHILD);
    initJobTaskWithDefaultStubs(job);
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING, JobStatus.CANCELED);
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);
    when(resilienceStrategy.execute(anyString(), any(Callable.class)))
        .thenAnswer(inv -> ((Callable<?>) inv.getArgument(1)).call());

    jobTask.call();

    verify(lifecycleFacade).markBatchChildFailed(job);
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
    when(resilienceStrategy.execute(anyString(), any(Callable.class)))
        .thenAnswer(inv -> ((Callable<?>) inv.getArgument(1)).call());

    jobTask.call();

    verify(jobStore, never())
        .markJobSucceeded(any(UUID.class), any(), any(), any(), any(), anyLong(), anyLong());
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
    when(resilienceStrategy.execute(anyString(), any(Callable.class)))
        .thenAnswer(inv -> ((Callable<?>) inv.getArgument(1)).call());
    when(jobStore.markJobSucceeded(
            any(UUID.class), any(), any(), any(), any(), anyLong(), anyLong()))
        .thenReturn(true);

    jobTask.call();

    verify(observabilityFacade).publishEvent(any(JobCompletedEvent.class));
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
        new JobPayload(JobTaskTest.class.getName(), "failingCallback", "()V", true, List.of()));
    initJobTaskWithDefaultStubs(fixedClockTask, job);
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING);
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);
    when(resilienceStrategy.execute(anyString(), any(Callable.class)))
        .thenAnswer(inv -> ((Callable<?>) inv.getArgument(1)).call());
    when(jobStore.markJobSucceeded(
            any(UUID.class), any(), any(), any(), any(), anyLong(), anyLong()))
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
    JobCompletedEvent completedEvent =
        eventCaptor.getAllValues().stream()
            .filter(JobCompletedEvent.class::isInstance)
            .map(JobCompletedEvent.class::cast)
            .findFirst()
            .orElseThrow();
    JobCallbackFailedEvent callbackEvent =
        eventCaptor.getAllValues().stream()
            .filter(JobCallbackFailedEvent.class::isInstance)
            .map(JobCallbackFailedEvent.class::cast)
            .findFirst()
            .orElseThrow();

    Assertions.assertEquals(recurringMasterId, startedEvent.getRecurringMasterId());
    Assertions.assertEquals(recurringMasterId, completedEvent.getRecurringMasterId());
    Assertions.assertEquals(recurringMasterId, callbackEvent.getRecurringMasterId());
    Assertions.assertEquals(FIXED_NOW, completedEvent.getTimestamp());
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
    when(resourcePermitService.tryAcquire("api-gateway", JOB_UUID, "node-1")).thenReturn(true);
    when(resilienceStrategy.execute(anyString(), any(Callable.class)))
        .thenAnswer(inv -> ((Callable<?>) inv.getArgument(1)).call());
    when(jobStore.markJobSucceeded(
            any(UUID.class), any(), any(), any(), any(), anyLong(), anyLong()))
        .thenReturn(true);

    jobTask.call();

    verify(resourcePermitService).release("api-gateway", JOB_UUID);
  }

  @Test
  @SuppressWarnings("unchecked")
  void call_resourcePermitUnavailable_reschedulesWithoutExecutingPayload() throws Exception {
    JobEntity job = createTestJob();
    job.setResourceName("gpu");
    job.setAttempts(2);
    initJobTaskWithDefaultStubs(job);
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING);
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);
    when(resourcePermitService.tryAcquire("gpu", JOB_UUID, "node-1")).thenReturn(false);
    when(resourcePermitService.getRetryDelay("gpu")).thenReturn(250);
    when(jobStore.scheduleJobRetry(eq(JOB_UUID), anyString(), any(), eq(2))).thenReturn(true);

    jobTask.call();

    verify(jobStore).scheduleJobRetry(eq(JOB_UUID), eq("Waiting for resource: gpu"), any(), eq(2));
    verify(resilienceStrategy, never()).execute(anyString(), any(Callable.class));
    verify(resourcePermitService, never()).release(anyString(), any(UUID.class));
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
    verify(resilienceStrategy, never()).execute(anyString(), any(Callable.class));
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
    verify(resilienceStrategy, never()).execute(anyString(), any(Callable.class));
    verify(jobStore, never()).getJobStatus(any(UUID.class));
  }

  @Test
  @SuppressWarnings("unchecked")
  void handleSuccess_retriesTransientFinalizationWithoutFailingJob() throws Exception {
    JobEntity job = createTestJob();
    initJobTaskWithDefaultStubs(job);
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING);
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);
    when(resilienceStrategy.execute(anyString(), any(Callable.class)))
        .thenAnswer(inv -> ((Callable<?>) inv.getArgument(1)).call());
    when(jobStore.markJobSucceeded(
            any(UUID.class), any(), any(), any(), any(), anyLong(), anyLong()))
        .thenThrow(new RatchetTransientStoreException("deadlock"))
        .thenReturn(true);

    jobTask.call();

    verify(jobStore, times(2))
        .markJobSucceeded(any(UUID.class), any(), any(), any(), any(), anyLong(), anyLong());
    verify(jobStore, never())
        .markJobSucceededMinimal(any(UUID.class), any(), any(), anyLong(), anyLong());
    verify(jobStore, never()).incrementRetryAttempt(any(UUID.class));
    verify(lifecycleFacade, never()).moveToDlq(any(), any());
    verify(observabilityFacade).recordSuccessFinalizationRetry(job);
    verify(observabilityFacade, never()).recordSuccessFinalizationMinimal(any());
    verify(observabilityFacade, never()).recordSuccessFinalizationStuck(any());
    verify(observabilityFacade).publishEvent(any(JobCompletedEvent.class));
  }

  @Test
  @SuppressWarnings("unchecked")
  void handleSuccess_fallsBackToMinimalSuccessAfterTransientFinalizationExhaustion()
      throws Exception {
    JobEntity job = createTestJob();
    initJobTaskWithDefaultStubs(job);
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING);
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);
    when(resilienceStrategy.execute(anyString(), any(Callable.class)))
        .thenAnswer(inv -> ((Callable<?>) inv.getArgument(1)).call());
    when(jobStore.markJobSucceeded(
            any(UUID.class), any(), any(), any(), any(), anyLong(), anyLong()))
        .thenThrow(new RatchetTransientStoreException("deadlock"));
    when(jobStore.markJobSucceededMinimal(any(UUID.class), any(), any(), anyLong(), anyLong()))
        .thenReturn(true);

    jobTask.call();

    verify(jobStore, times(5))
        .markJobSucceeded(any(UUID.class), any(), any(), any(), any(), anyLong(), anyLong());
    verify(jobStore).markJobSucceededMinimal(any(UUID.class), any(), any(), anyLong(), anyLong());
    verify(jobStore, never()).incrementRetryAttempt(any(UUID.class));
    verify(lifecycleFacade, never()).moveToDlq(any(), any());
    verify(observabilityFacade, times(5)).recordSuccessFinalizationRetry(job);
    verify(observabilityFacade).recordSuccessFinalizationMinimal(job);
    verify(observabilityFacade, never()).recordSuccessFinalizationStuck(any());
    verify(observabilityFacade).publishEvent(any(JobCompletedEvent.class));
  }

  @Test
  @SuppressWarnings("unchecked")
  void handleSuccess_stuckFinalizationDoesNotMoveSuccessfulJobToFailurePath() throws Exception {
    JobEntity job = createTestJob();
    initJobTaskWithDefaultStubs(job);
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING);
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);
    when(resilienceStrategy.execute(anyString(), any(Callable.class)))
        .thenAnswer(inv -> ((Callable<?>) inv.getArgument(1)).call());
    when(jobStore.markJobSucceeded(
            any(UUID.class), any(), any(), any(), any(), anyLong(), anyLong()))
        .thenThrow(new RatchetTransientStoreException("deadlock"));
    when(jobStore.markJobSucceededMinimal(any(UUID.class), any(), any(), anyLong(), anyLong()))
        .thenThrow(new RatchetTransientStoreException("deadlock"));

    jobTask.call();

    verify(jobStore, times(5))
        .markJobSucceeded(any(UUID.class), any(), any(), any(), any(), anyLong(), anyLong());
    verify(jobStore).markJobSucceededMinimal(any(UUID.class), any(), any(), anyLong(), anyLong());
    verify(jobStore, never()).incrementRetryAttempt(any(UUID.class));
    verify(lifecycleFacade, never()).moveToDlq(any(), any());
    verify(observabilityFacade, times(5)).recordSuccessFinalizationRetry(job);
    verify(observabilityFacade).recordSuccessFinalizationStuck(job);
    verify(observabilityFacade, never()).publishEvent(any(JobCompletedEvent.class));
    verify(observabilityFacade, never()).recordJobSuccess(any(), anyLong());
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
            new JobSuccessFinalizer(jobStore, observabilityFacade),
            retryPolicy,
            resilienceStrategy,
            errorSanitizer,
            context -> noopLogger(),
            resultPersistenceStrategy,
            null,
            signalSerializer,
            null,
            Clock.systemUTC(),
            null);
    JobEntity job = createTestJob();
    job.setPayload(
        new JobPayload(
            JobTaskTest.class.getName(),
            "captureSignalDecision",
            "()Ljava/lang/String;",
            true,
            List.of()));
    job.setSignalPayload("\"payload\"");
    job.setSignalPayloadType(DefaultJobSchedulerService.SIGNAL_PAYLOAD_TYPE_DECISION);
    job.setSignalOutcome("REJECTED");
    job.setSignalRejectionReason("denied");
    initJobTaskWithDefaultStubs(signalTask, job);
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING);
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);
    when(resilienceStrategy.execute(anyString(), any(Callable.class)))
        .thenAnswer(inv -> ((Callable<?>) inv.getArgument(1)).call());
    when(jobStore.markJobSucceeded(
            any(UUID.class), any(), any(), any(), any(), anyLong(), anyLong()))
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
    when(jobStore.compareAndSwapStatus(
            eq(JOB_UUID), eq(JobStatus.RUNNING), eq(JobStatus.FAILED), any()))
        .thenReturn(true);

    jobTask.call();

    ArgumentCaptor<Throwable> failure = ArgumentCaptor.forClass(Throwable.class);
    verify(observabilityFacade).recordJobFailure(eq(job), failure.capture(), eq(0));
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
    verify(jobStore, never()).incrementRetryAttempt(any(UUID.class));
    verify(lifecycleFacade).moveToDlq(eq(job), eq(exception));
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
            new JobSuccessFinalizer(jobStore, observabilityFacade),
            retryPolicy,
            resilienceStrategy,
            errorSanitizer,
            context -> noopLogger(),
            resultPersistenceStrategy,
            null,
            signalSerializer,
            null,
            Clock.systemUTC(),
            null);
    JobEntity job = createTestJob();
    job.setPayload(
        new JobPayload(
            JobTaskTest.class.getName(),
            "captureSignalString",
            "()Ljava/lang/String;",
            true,
            List.of()));
    job.setSignalPayload(signalSerializer.serialize("hello"));
    // Any non-DECISION marker drives the raw-Serializable branch.
    job.setSignalPayloadType("RAW");
    initJobTaskWithDefaultStubs(signalTask, job);
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING);
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);
    when(resilienceStrategy.execute(anyString(), any(Callable.class)))
        .thenAnswer(inv -> ((Callable<?>) inv.getArgument(1)).call());
    when(jobStore.markJobSucceeded(
            any(UUID.class), any(), any(), any(), any(), anyLong(), anyLong()))
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
    when(jobStore.compareAndSwapStatus(
            eq(JOB_UUID), eq(JobStatus.RUNNING), eq(JobStatus.FAILED), any()))
        .thenReturn(true);

    jobTask.call();

    verify(jobStore)
        .compareAndSwapStatus(eq(JOB_UUID), eq(JobStatus.RUNNING), eq(JobStatus.FAILED), any());
    // Poison surfaces with its true type now (no IllegalArgumentException wrap); it is still DLQ'd.
    verify(observabilityFacade)
        .recordJobFailure(eq(job), any(PayloadDecryptionException.class), eq(0));
    verify(observabilityFacade, never()).startExecution(any(UUID.class), anyInt(), anyString());
    verify(resilienceStrategy, never()).execute(anyString(), any(Callable.class));
    verify(lifecycleFacade).moveToDlq(eq(job), any(PayloadDecryptionException.class));
  }

  private static final java.util.concurrent.atomic.AtomicReference<String> OBSERVED_ARG =
      new java.util.concurrent.atomic.AtomicReference<>();

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
                List.of("patched"));
    JobTask resolving =
        new JobTask(
            jobStore,
            resourcePermitService,
            lifecycleFacade,
            nodeIdProvider,
            observabilityFacade,
            validationFacade,
            new JobPayloadInvoker(beanResolver, classPolicy),
            new JobSuccessFinalizer(jobStore, observabilityFacade),
            retryPolicy,
            resilienceStrategy,
            errorSanitizer,
            context -> new JBossLoggingJobLogger(context.jobId(), null),
            new DefaultResultPersistenceStrategy(RatchetOptions.defaults(), serializer, null),
            null,
            serializer,
            null,
            Clock.systemUTC(),
            resolver);

    JobEntity job = createTestJob();
    job.setPayload(
        new JobPayload(
            JobTaskTest.class.getName(),
            "captureArg",
            "(Ljava/lang/String;)Ljava/lang/String;",
            true,
            List.of("original")));
    initJobTaskWithDefaultStubs(resolving, job);
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING);
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);
    when(resilienceStrategy.execute(anyString(), any(Callable.class)))
        .thenAnswer(inv -> ((Callable<?>) inv.getArgument(1)).call());
    when(jobStore.markJobSucceeded(
            any(UUID.class), any(), any(), any(), any(), anyLong(), anyLong()))
        .thenReturn(true);

    resolving.call();

    Assertions.assertEquals(
        "patched", OBSERVED_ARG.get(), "the resolver-patched argument must be dispatched");
  }

  private JobEntity createTestJob() {
    JobEntity job = new JobEntity();
    job.setId(JOB_UUID);
    job.setJobType(JobExecutionType.SINGLE);
    job.setPriority(JobPriority.NORMAL);
    job.setMaxRetries(3);
    job.setPayload(
        new JobPayload(
            JobTaskTest.class.getName(), "testJobMethod", "()Ljava/lang/String;", true, List.of()));
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
        null);
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
    when(observabilityFacade.startExecution(any(UUID.class), anyInt(), anyString()))
        .thenReturn(JobExecutionEntity.start(job.getId(), 1, "node-1"));
    when(observabilityFacade.startExecutionScope(any(JobEntity.class)))
        .thenReturn(TracingCollector.NoOpExecutionScope.INSTANCE);
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
        new JobSuccessFinalizer(jobStore, observabilityFacade),
        retryPolicy,
        resilienceStrategy,
        errorSanitizer,
        context -> noopLogger(),
        resultPersistenceStrategy,
        null,
        null,
        timeoutHandler,
        FIXED_CLOCK,
        null);
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
        new JobSuccessFinalizer(jobStore, observabilityFacade),
        retryPolicy,
        resilienceStrategy,
        errorSanitizer,
        context -> noopLogger(),
        resultPersistenceStrategy,
        null,
        null,
        null,
        taskClock,
        null);
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

    @CircuitBreakerProtected(service = "external-api")
    public static String annotatedJobMethod() {
      return "done";
    }
  }

  public record CustomArgument(String reference, int attempt) implements Serializable {}
}
