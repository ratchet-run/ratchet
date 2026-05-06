package run.ratchet.ri.core;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobStatus;
import run.ratchet.api.exception.JobAuthorizationException;
import run.ratchet.spi.BeanResolver;
import run.ratchet.spi.ClassPolicy;
import run.ratchet.spi.ErrorSanitizer;
import run.ratchet.spi.JobAuthorizationPolicy;
import run.ratchet.spi.JobLoggerFactory;
import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.spi.ResilienceStrategy;
import run.ratchet.spi.ResultPersistenceStrategy;
import run.ratchet.spi.RetryPolicy;
import run.ratchet.spi.SerializedJobResult;
import run.ratchet.spi.TracingCollector;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.entity.JobPayload;
import run.ratchet.store.spi.JobStore;

@ExtendWith(MockitoExtension.class)
class JobTaskAuthorizationTest {

  private static final UUID JOB_UUID = new UUID(0L, 99L);
  private static final String OWNER_PRINCIPAL = "alice";

  private final ClassPolicy classPolicy = className -> true;
  private final JobLoggerFactory loggerFactory =
      ctx -> new JBossLoggingJobLogger(ctx.jobId(), null);

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
  @Mock private ResultPersistenceStrategy resultPersistenceStrategy;
  @Mock private JobAuthorizationPolicy authorizationPolicy;

  private JobTask jobTask;

  public static String targetMethod() {
    return "done";
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
            errorSanitizer,
            classPolicy,
            loggerFactory,
            resultPersistenceStrategy,
            authorizationPolicy,
            null,
            Clock.systemUTC());
  }

  @Test
  @SuppressWarnings("unchecked")
  void checkExecute_isCalledBeforeResilienceStrategy() throws Exception {
    JobEntity job = jobWithPrincipal();
    initCore(job);
    stubSuccessPath();

    jobTask.call();

    InOrder order = inOrder(authorizationPolicy, resilienceStrategy);
    order.verify(authorizationPolicy).checkExecute(eq(JOB_UUID), eq(OWNER_PRINCIPAL));
    order.verify(resilienceStrategy).execute(anyString(), any(Callable.class));
  }

  @Test
  @SuppressWarnings("unchecked")
  void checkExecute_receivesJobIdAndOwnerPrincipal() throws Exception {
    JobEntity job = jobWithPrincipal();
    initCore(job);
    stubSuccessPath();

    jobTask.call();

    ArgumentCaptor<UUID> idCaptor = ArgumentCaptor.forClass(UUID.class);
    ArgumentCaptor<String> principalCaptor = ArgumentCaptor.forClass(String.class);
    verify(authorizationPolicy).checkExecute(idCaptor.capture(), principalCaptor.capture());
    assert JOB_UUID.equals(idCaptor.getValue()) : "checkExecute must receive the job's UUID";
    assert OWNER_PRINCIPAL.equals(principalCaptor.getValue())
        : "checkExecute must receive the entity's callerPrincipal";
  }

  @Test
  void checkExecute_denial_movesJobToDlq_withoutCallingResilienceStrategy() throws Exception {
    JobEntity job = jobWithPrincipal();
    initCore(job);
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING);
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);

    JobAuthorizationException denial =
        new JobAuthorizationException(JOB_UUID, "execute", OWNER_PRINCIPAL, "denied");
    doThrow(denial).when(authorizationPolicy).checkExecute(any(UUID.class), anyString());
    when(validationFacade.shouldNotRetry(denial)).thenReturn(true);
    when(jobStore.compareAndSwapStatus(
            eq(JOB_UUID), eq(JobStatus.RUNNING), eq(JobStatus.FAILED), any()))
        .thenReturn(true);

    jobTask.call();

    verify(resilienceStrategy, never()).execute(anyString(), any());
    verify(lifecycleFacade).moveToDlq(eq(job), eq(denial));
  }

  @Test
  @SuppressWarnings("unchecked")
  void checkExecute_nullPrincipalOnEntity_passedThroughAsNull() throws Exception {
    // createBaseJob() leaves callerPrincipal null
    JobEntity job = createBaseJob();
    initCore(job);
    stubSuccessPath();

    jobTask.call();

    verify(authorizationPolicy).checkExecute(eq(JOB_UUID), isNull());
  }

  @Test
  @SuppressWarnings("unchecked")
  void checkExecute_skippedWhenPolicyIsNull() throws Exception {
    // 11-param constructor sets authorizationPolicy = null
    JobTask nullPolicyTask =
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

    JobEntity job = createBaseJob();
    nullPolicyTask.init(job);
    when(nodeIdProvider.getNodeId()).thenReturn("node-1");
    when(observabilityFacade.startExecution(any(), anyInt(), anyString()))
        .thenReturn(JobExecutionEntity.start(JOB_UUID, 1, "node-1"));
    when(observabilityFacade.startExecutionScope(any()))
        .thenReturn(TracingCollector.NoOpExecutionScope.INSTANCE);
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING);
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);
    when(resilienceStrategy.execute(anyString(), any(Callable.class)))
        .thenAnswer(inv -> ((Callable<?>) inv.getArgument(1)).call());
    // 11-param constructor uses DefaultResultPersistenceStrategy (real object, handles exceptions)
    when(jobStore.markJobSucceeded(any(), any(), any(), any(), any(), anyLong(), anyLong()))
        .thenReturn(true);

    nullPolicyTask.call();

    verify(resilienceStrategy).execute(anyString(), any(Callable.class));
  }

  // ---- helpers ----

  /** Job entity with OWNER_PRINCIPAL already stamped. */
  private JobEntity jobWithPrincipal() {
    JobEntity job = createBaseJob();
    job.setCallerPrincipal(OWNER_PRINCIPAL);
    return job;
  }

  /** Minimal job entity — callerPrincipal is null (set explicitly by tests that need it). */
  private JobEntity createBaseJob() {
    JobEntity job = new JobEntity();
    job.setId(JOB_UUID);
    job.setJobType(JobExecutionType.SINGLE);
    job.setPriority(JobPriority.NORMAL);
    job.setMaxRetries(3);
    job.setPayload(
        new JobPayload(
            JobTaskAuthorizationTest.class.getName(),
            "targetMethod",
            "()Ljava/lang/String;",
            true,
            List.of()));
    return job;
  }

  /** Stubs required by every code path (observability + node identity). */
  private void initCore(JobEntity job) {
    jobTask.init(job);
    when(nodeIdProvider.getNodeId()).thenReturn("node-1");
    when(observabilityFacade.startExecution(any(), anyInt(), anyString()))
        .thenReturn(JobExecutionEntity.start(JOB_UUID, 1, "node-1"));
    when(observabilityFacade.startExecutionScope(any()))
        .thenReturn(TracingCollector.NoOpExecutionScope.INSTANCE);
  }

  /** Stubs required only by the success execution path. */
  @SuppressWarnings("unchecked")
  private void stubSuccessPath() throws Exception {
    when(jobStore.getJobStatus(JOB_UUID)).thenReturn(JobStatus.RUNNING);
    when(resilienceStrategy.isServiceAvailable(anyString())).thenReturn(true);
    when(resilienceStrategy.execute(anyString(), any(Callable.class)))
        .thenAnswer(
            inv -> {
              try {
                return ((Callable<?>) inv.getArgument(1)).call();
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });
    when(resultPersistenceStrategy.serialize(any(), any()))
        .thenReturn(new SerializedJobResult(null, null));
    when(jobStore.markJobSucceeded(any(), any(), any(), any(), any(), anyLong(), anyLong()))
        .thenReturn(true);
  }
}
