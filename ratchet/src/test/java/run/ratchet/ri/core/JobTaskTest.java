package run.ratchet.ri.core;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import run.ratchet.api.CircuitBreakerProtected;
import run.ratchet.api.JobPriority;
import run.ratchet.spi.BeanResolver;
import run.ratchet.spi.ErrorSanitizer;
import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.spi.ResilienceStrategy;
import run.ratchet.spi.RetryPolicy;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.entity.JobPayload;
import run.ratchet.store.entity.JobStatus;
import run.ratchet.store.spi.JobStore;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Callable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class JobTaskTest {

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

  /**
   * Public static method used as the job payload target in tests. Must be public for
   * reflection-based invocation.
   */
  public static String testJobMethod() {
    return "done";
  }

  public static class AnnotatedJobTarget {

    @CircuitBreakerProtected(service = "external-api")
    public static String annotatedJobMethod() {
      return "done";
    }
  }

  @BeforeEach
  void setUp() {
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
            errorSanitizer);
  }

  // ── Helpers ────────────────────────────────────────────────────────────

  private JobEntity createTestJob() {
    JobEntity job = new JobEntity();
    job.setId(42L);
    job.setJobType(JobExecutionType.SINGLE);
    job.setPriority(JobPriority.NORMAL);
    job.setMaxRetries(3);
    job.setPayload(
        new JobPayload(
            JobTaskTest.class.getName(), "testJobMethod", "()Ljava/lang/String;", true, List.of()));
    return job;
  }

  private void initJobTaskWithDefaultStubs(JobEntity job) {
    jobTask.init(job);
    when(nodeIdProvider.getNodeId()).thenReturn("node-1");
    when(observabilityFacade.startExecution(anyLong(), anyInt(), anyString()))
        .thenReturn(JobExecutionEntity.start(job.getId(), 1, "node-1"));
  }

  // ── ResilienceStrategy.execute() is called ─────────────────────────────

  @Test
  @SuppressWarnings("unchecked")
  void call_invokesResilienceStrategyExecute() throws Exception {
    JobEntity job = createTestJob();
    initJobTaskWithDefaultStubs(job);
    when(jobStore.getJobStatus(42L)).thenReturn(JobStatus.RUNNING);
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);
    when(resilienceStrategy.execute(anyString(), any(Callable.class)))
        .thenAnswer(inv -> ((Callable<?>) inv.getArgument(1)).call());
    when(jobStore.markJobSucceeded(anyLong(), any(), any(), any(), any(), anyLong(), anyLong()))
        .thenReturn(true);

    jobTask.call();

    verify(resilienceStrategy)
        .execute(eq(JobTaskTest.class.getSimpleName() + ".testJobMethod"), any(Callable.class));
  }

  // ── Service name passed to resilience strategy ─────────────────────────

  @Test
  @SuppressWarnings("unchecked")
  void call_passesMethodScopedServiceName() throws Exception {
    JobEntity job = createTestJob();
    initJobTaskWithDefaultStubs(job);
    when(jobStore.getJobStatus(42L)).thenReturn(JobStatus.RUNNING);
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);
    when(resilienceStrategy.execute(anyString(), any(Callable.class)))
        .thenAnswer(inv -> ((Callable<?>) inv.getArgument(1)).call());
    when(jobStore.markJobSucceeded(anyLong(), any(), any(), any(), any(), anyLong(), anyLong()))
        .thenReturn(true);

    jobTask.call();

    ArgumentCaptor<String> serviceNameCaptor = ArgumentCaptor.forClass(String.class);
    verify(resilienceStrategy).execute(serviceNameCaptor.capture(), any(Callable.class));
    org.junit.jupiter.api.Assertions.assertEquals(
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
    when(jobStore.getJobStatus(42L)).thenReturn(JobStatus.RUNNING);
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);
    when(resilienceStrategy.execute(anyString(), any(Callable.class)))
        .thenAnswer(inv -> ((Callable<?>) inv.getArgument(1)).call());
    when(jobStore.markJobSucceeded(anyLong(), any(), any(), any(), any(), anyLong(), anyLong()))
        .thenReturn(true);

    jobTask.call();

    verify(resilienceStrategy).execute(eq("external-api"), any(Callable.class));
  }

  // ── isServiceAvailable checked before execution ────────────────────────

  @Test
  void call_checksServiceAvailableBeforeExecution() throws Exception {
    JobEntity job = createTestJob();
    initJobTaskWithDefaultStubs(job);
    when(jobStore.getJobStatus(42L)).thenReturn(JobStatus.RUNNING);
    when(resilienceStrategy.isServiceAvailable(
            JobTaskTest.class.getSimpleName() + ".testJobMethod"))
        .thenReturn(true);
    when(resilienceStrategy.execute(anyString(), any(Callable.class)))
        .thenAnswer(inv -> ((Callable<?>) inv.getArgument(1)).call());
    when(jobStore.markJobSucceeded(anyLong(), any(), any(), any(), any(), anyLong(), anyLong()))
        .thenReturn(true);

    jobTask.call();

    verify(resilienceStrategy)
        .isServiceAvailable(JobTaskTest.class.getSimpleName() + ".testJobMethod");
  }

  // ── Service unavailable → skip execution ───────────────────────────────

  @Test
  @SuppressWarnings("unchecked")
  void call_serviceUnavailable_skipsExecution() throws Exception {
    JobEntity job = createTestJob();
    initJobTaskWithDefaultStubs(job);
    when(jobStore.getJobStatus(42L)).thenReturn(JobStatus.RUNNING);
    when(resilienceStrategy.isServiceAvailable(
            JobTaskTest.class.getSimpleName() + ".testJobMethod"))
        .thenReturn(false);

    jobTask.call();

    verify(resilienceStrategy, never()).execute(anyString(), any(Callable.class));
    // Job should be rescheduled via scheduleJobRetry
    verify(jobStore).scheduleJobRetry(eq(42L), anyString(), any(), anyInt());
  }

  // ── handleFailure consults RetryPolicy ─────────────────────────────────

  @Test
  @SuppressWarnings("unchecked")
  void handleFailure_consultsRetryPolicy() throws Exception {
    JobEntity job = createTestJob();
    job.setMaxRetries(3);
    initJobTaskWithDefaultStubs(job);
    when(jobStore.getJobStatus(42L)).thenReturn(JobStatus.RUNNING);
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);

    RuntimeException error = new RuntimeException("boom");
    when(resilienceStrategy.execute(anyString(), any(Callable.class))).thenThrow(error);
    when(validationFacade.shouldNotRetry(error)).thenReturn(false);
    when(jobStore.incrementRetryAttempt(42L)).thenReturn(1);
    when(retryPolicy.shouldRetry(1, error)).thenReturn(true);
    when(retryPolicy.getDelay(1)).thenReturn(Duration.ofSeconds(5));
    when(jobStore.scheduleJobRetry(anyLong(), anyString(), any(), anyInt())).thenReturn(true);

    jobTask.call();

    verify(retryPolicy).shouldRetry(1, error);
  }

  // ── RetryPolicy denies → FAILED ────────────────────────────────────────

  @Test
  @SuppressWarnings("unchecked")
  void handleFailure_retryPolicyDenies_movesToFailed() throws Exception {
    JobEntity job = createTestJob();
    job.setMaxRetries(3);
    initJobTaskWithDefaultStubs(job);
    when(jobStore.getJobStatus(42L)).thenReturn(JobStatus.RUNNING);
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);

    RuntimeException error = new RuntimeException("permanent");
    when(resilienceStrategy.execute(anyString(), any(Callable.class))).thenThrow(error);
    when(validationFacade.shouldNotRetry(error)).thenReturn(false);
    when(jobStore.incrementRetryAttempt(42L)).thenReturn(1);
    when(retryPolicy.shouldRetry(1, error)).thenReturn(false);
    when(jobStore.compareAndSwapStatus(eq(42L), eq(JobStatus.RUNNING), eq(JobStatus.FAILED), any()))
        .thenReturn(true);

    jobTask.call();

    verify(lifecycleFacade).moveToDlq(eq(job), eq(error));
  }

  // ── handleSuccess publishes completed event ────────────────────────────

  @Test
  @SuppressWarnings("unchecked")
  void handleSuccess_publishesCompletedEvent() throws Exception {
    JobEntity job = createTestJob();
    initJobTaskWithDefaultStubs(job);
    when(jobStore.getJobStatus(42L)).thenReturn(JobStatus.RUNNING);
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);
    when(resilienceStrategy.execute(anyString(), any(Callable.class)))
        .thenAnswer(inv -> ((Callable<?>) inv.getArgument(1)).call());
    when(jobStore.markJobSucceeded(anyLong(), any(), any(), any(), any(), anyLong(), anyLong()))
        .thenReturn(true);

    jobTask.call();

    verify(observabilityFacade)
        .publishEvent(any(run.ratchet.api.event.JobCompletedEvent.class));
  }

  // ── Resource permit released on success ────────────────────────────────

  @Test
  @SuppressWarnings("unchecked")
  void call_releasesResourcePermit_onSuccess() throws Exception {
    JobEntity job = createTestJob();
    job.setResourceName("api-gateway");
    initJobTaskWithDefaultStubs(job);
    when(jobStore.getJobStatus(42L)).thenReturn(JobStatus.RUNNING);
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);
    when(resourcePermitService.tryAcquire("api-gateway", 42L, "node-1")).thenReturn(true);
    when(resilienceStrategy.execute(anyString(), any(Callable.class)))
        .thenAnswer(inv -> ((Callable<?>) inv.getArgument(1)).call());
    when(jobStore.markJobSucceeded(anyLong(), any(), any(), any(), any(), anyLong(), anyLong()))
        .thenReturn(true);

    jobTask.call();

    verify(resourcePermitService).release("api-gateway", 42L);
  }
}
