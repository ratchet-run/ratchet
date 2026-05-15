package run.ratchet.ri.core;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;
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
import run.ratchet.api.SignalDecision;
import run.ratchet.api.event.JobCancelledEvent;
import run.ratchet.api.event.JobCompletedEvent;
import run.ratchet.api.exception.CircuitBreakerOpenException;
import run.ratchet.api.exception.RatchetTransientStoreException;
import run.ratchet.spi.BeanResolver;
import run.ratchet.spi.ClassPolicy;
import run.ratchet.spi.ErrorSanitizer;
import run.ratchet.spi.JobLogger;
import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.spi.PayloadSerializer;
import run.ratchet.spi.ResilienceStrategy;
import run.ratchet.spi.ResultPersistenceStrategy;
import run.ratchet.spi.RetryPolicy;
import run.ratchet.spi.SerializedJobResult;
import run.ratchet.spi.TracingCollector;
import run.ratchet.store.dto.JobClaimDto;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.entity.JobPayload;
import run.ratchet.store.spi.JobStore;

@ExtendWith(MockitoExtension.class)
class JobTaskTest {

  private static final UUID JOB_UUID = new UUID(0L, 42L);
  private static final ThreadLocal<SignalDecision> OBSERVED_SIGNAL_DECISION = new ThreadLocal<>();

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
                beanResolver,
                retryPolicy,
                resilienceStrategy,
                errorSanitizer,
                classPolicy,
                context -> null,
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
    jobTask =
        new JobTask(
            jobStore,
            resourcePermitService,
            lifecycleFacade,
            nodeIdProvider,
            observabilityFacade,
            validationFacade,
            beanResolver,
            retryPolicy,
            resilienceStrategy,
            errorSanitizer,
            classPolicy);
  }

  @AfterEach
  void tearDown() {
    OBSERVED_SIGNAL_DECISION.remove();
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
    when(jobStore.compareAndSwapStatus(
            eq(JOB_UUID), eq(JobStatus.RUNNING), eq(JobStatus.FAILED), any()))
        .thenReturn(true);

    jobTask.call();

    verify(jobStore, never()).incrementRetryAttempt(any(UUID.class));
    verify(retryPolicy, never()).shouldRetry(anyInt(), any());
    verify(jobStore, never()).scheduleJobRetry(any(UUID.class), anyString(), any(), anyInt());
    verify(observabilityFacade).recordJobFailure(job, error, 2);
    verify(lifecycleFacade).moveToDlq(eq(job), eq(error));
    verify(lifecycleFacade).scheduleNext(job);
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
    verify(observabilityFacade).publishEvent(any(JobCancelledEvent.class));
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
            beanResolver,
            retryPolicy,
            resilienceStrategy,
            errorSanitizer,
            classPolicy,
            context -> noopLogger(),
            resultPersistenceStrategy,
            null,
            signalSerializer,
            Clock.systemUTC());
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
        3);
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

  public static class AnnotatedJobTarget {

    @CircuitBreakerProtected(service = "external-api")
    public static String annotatedJobMethod() {
      return "done";
    }
  }
}
