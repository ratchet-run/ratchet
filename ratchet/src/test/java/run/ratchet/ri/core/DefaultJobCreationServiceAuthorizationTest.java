package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;
import run.ratchet.api.exception.JobAuthorizationException;
import run.ratchet.ri.payload.DefaultJobInvocationResolver;
import run.ratchet.ri.security.CallerPrincipalProvider;
import run.ratchet.ri.security.JobPayloadInputValidator;
import run.ratchet.spi.JobAuthorizationPolicy;
import run.ratchet.spi.JobInvocationResolver;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.spi.TracingCollector;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.spi.BatchStore;
import run.ratchet.store.spi.JobBatchStatusStore;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.JobTerminalStore;
import run.ratchet.store.spi.TagStore;
import run.ratchet.store.spi.WorkflowConditionStore;

@ExtendWith(MockitoExtension.class)
class DefaultJobCreationServiceAuthorizationTest {

  private static final String CAPTURED_PRINCIPAL = "bob";

  @Mock private JobBatchStatusStore jobBatchStatusStore;
  @Mock private JobTerminalStore jobTerminalStore;
  @Mock private JobCrudStore jobCrudStore;
  @Mock private BatchStore batchStore;
  @Mock private TagStore tagStore;
  @Mock private WorkflowConditionStore workflowConditionStore;
  @Mock private JobWakeupService wakeupService;
  @Mock private RecurringScheduler recurringScheduler;
  @Mock private TracingCollector tracingCollector;
  @Mock private JobAuthorizationPolicy authorizationPolicy;
  @Mock private InternalEventPublisher eventPublisher;
  @Mock private MetricsCollector metricsCollector;

  private DefaultJobCreationService service;

  public static void noopTask() {}

  public static void consumeString(String s) {}

  private static JobEntity savedEntity() {
    JobEntity e = new JobEntity();
    e.setId(UUID.randomUUID());
    e.setJobType(JobExecutionType.SINGLE);
    e.setPriority(JobPriority.NORMAL);
    return e;
  }

  @BeforeEach
  void setUp() {
    CallerPrincipalProvider principalProvider =
        new CallerPrincipalProvider(null) {
          @Override
          public Optional<String> currentPrincipal() {
            return Optional.of(CAPTURED_PRINCIPAL);
          }
        };

    JobInvocationResolver resolver = new DefaultJobInvocationResolver();

    service =
        new DefaultJobCreationService(
            jobBatchStatusStore,
            jobTerminalStore,
            jobCrudStore,
            batchStore,
            tagStore,
            workflowConditionStore,
            wakeupService,
            recurringScheduler,
            resolver,
            new JobPayloadInputValidator(),
            principalProvider,
            tracingCollector,
            authorizationPolicy,
            eventPublisher,
            metricsCollector,
            Clock.systemUTC());
  }

  @Test
  void checkCreate_isCalledAfterPrincipalCapture_withCorrectArgs() {
    JobEntity saved = savedEntity();
    when(jobCrudStore.findByIdempotencyKey(anyString())).thenReturn(Optional.empty());
    when(jobCrudStore.create(any(JobEntity.class))).thenReturn(saved);

    DefaultJobBuilder builder =
        (DefaultJobBuilder)
            DefaultJobBuilder.create(
                service, DefaultJobCreationServiceAuthorizationTest::noopTask, Duration.ZERO);

    service.submit(builder);

    ArgumentCaptor<UUID> idCaptor = ArgumentCaptor.forClass(UUID.class);
    ArgumentCaptor<String> principalCaptor = ArgumentCaptor.forClass(String.class);
    verify(authorizationPolicy).checkCreate(idCaptor.capture(), principalCaptor.capture());
    assertNotNull(idCaptor.getValue(), "checkCreate must receive a non-null job ID");
    assertEquals(
        CAPTURED_PRINCIPAL,
        principalCaptor.getValue(),
        "checkCreate must receive the stamped caller principal");
  }

  @Test
  void checkCreate_denial_preventsCreate() {
    when(jobCrudStore.findByIdempotencyKey(anyString())).thenReturn(Optional.empty());
    doThrow(new JobAuthorizationException(null, "create", CAPTURED_PRINCIPAL, "denied"))
        .when(authorizationPolicy)
        .checkCreate(any(), anyString());

    DefaultJobBuilder builder =
        (DefaultJobBuilder)
            DefaultJobBuilder.create(
                service, DefaultJobCreationServiceAuthorizationTest::noopTask, Duration.ZERO);

    assertThrows(JobAuthorizationException.class, () -> service.submit(builder));
    verify(jobCrudStore, never()).create(any());
  }

  @Test
  void checkCreate_calledBeforeSave() {
    JobEntity saved = savedEntity();
    when(jobCrudStore.findByIdempotencyKey(anyString())).thenReturn(Optional.empty());
    when(jobCrudStore.create(any(JobEntity.class))).thenReturn(saved);

    DefaultJobBuilder builder =
        (DefaultJobBuilder)
            DefaultJobBuilder.create(
                service, DefaultJobCreationServiceAuthorizationTest::noopTask, Duration.ZERO);

    service.submit(builder);

    InOrder order = Mockito.inOrder(authorizationPolicy, jobCrudStore);
    order.verify(authorizationPolicy).checkCreate(any(UUID.class), anyString());
    order.verify(jobCrudStore).create(any(JobEntity.class));
  }

  @Test
  void checkCreate_nullPolicyIsToleratedWithoutException() {
    // Use the 8-param constructor which sets authorizationPolicy = null
    DefaultJobCreationService nullPolicyService =
        new DefaultJobCreationService(
            jobBatchStatusStore,
            jobTerminalStore,
            jobCrudStore,
            batchStore,
            tagStore,
            workflowConditionStore,
            wakeupService,
            recurringScheduler);

    JobEntity saved = savedEntity();
    when(jobCrudStore.findByIdempotencyKey(anyString())).thenReturn(Optional.empty());
    when(jobCrudStore.create(any(JobEntity.class))).thenReturn(saved);

    DefaultJobBuilder builder =
        (DefaultJobBuilder)
            DefaultJobBuilder.create(
                nullPolicyService,
                DefaultJobCreationServiceAuthorizationTest::noopTask,
                Duration.ZERO);

    JobHandle handle = nullPolicyService.submit(builder);
    assertNotNull(handle, "Null policy must not throw — permit-all by default");
  }

  @Test
  void checkCreate_systemJob_nullPrincipalPassedThrough() {
    // Provider returns empty = system-initiated job
    DefaultJobCreationService systemService =
        new DefaultJobCreationService(
            jobBatchStatusStore,
            jobTerminalStore,
            jobCrudStore,
            batchStore,
            tagStore,
            workflowConditionStore,
            wakeupService,
            recurringScheduler,
            new DefaultJobInvocationResolver(),
            new JobPayloadInputValidator(),
            null, // no CallerPrincipalProvider
            tracingCollector,
            authorizationPolicy,
            null,
            null,
            Clock.systemUTC());

    JobEntity saved = savedEntity();
    when(jobCrudStore.findByIdempotencyKey(anyString())).thenReturn(Optional.empty());
    when(jobCrudStore.create(any(JobEntity.class))).thenReturn(saved);

    DefaultJobBuilder builder =
        (DefaultJobBuilder)
            DefaultJobBuilder.create(
                systemService, DefaultJobCreationServiceAuthorizationTest::noopTask, Duration.ZERO);

    systemService.submit(builder);

    verify(authorizationPolicy).checkCreate(any(UUID.class), isNull());
  }

  // ---- recurring job ----

  @Test
  void signalWaitingJobPublishesMetric() {
    JobEntity saved = savedEntity();
    when(jobCrudStore.findByIdempotencyKey(anyString())).thenReturn(Optional.empty());
    when(jobCrudStore.create(any(JobEntity.class))).thenReturn(saved);

    DefaultJobBuilder builder =
        (DefaultJobBuilder)
            DefaultJobBuilder.create(
                    service, DefaultJobCreationServiceAuthorizationTest::noopTask, Duration.ZERO)
                .awaitSignal("approval", Duration.ofSeconds(30));

    service.submit(builder);

    verify(metricsCollector).signalWaiting(saved.getId(), JobType.SINGLE, "approval");
  }

  // ---- streaming batch parent ----

  @Test
  void signalDeadlineIsComputedAtSubmitTimeWithInjectedClock() {
    Instant fixedNow = Instant.parse("2026-05-06T10:15:30Z");
    DefaultJobCreationService fixedClockService =
        new DefaultJobCreationService(
            jobBatchStatusStore,
            jobTerminalStore,
            jobCrudStore,
            batchStore,
            tagStore,
            workflowConditionStore,
            wakeupService,
            recurringScheduler,
            new DefaultJobInvocationResolver(),
            new JobPayloadInputValidator(),
            null,
            tracingCollector,
            authorizationPolicy,
            eventPublisher,
            metricsCollector,
            Clock.fixed(fixedNow, ZoneOffset.UTC));
    JobEntity saved = savedEntity();
    when(jobCrudStore.findByIdempotencyKey(anyString())).thenReturn(Optional.empty());
    when(jobCrudStore.create(any(JobEntity.class))).thenReturn(saved);
    DefaultJobBuilder builder =
        (DefaultJobBuilder)
            DefaultJobBuilder.create(
                    fixedClockService,
                    DefaultJobCreationServiceAuthorizationTest::noopTask,
                    Duration.ZERO)
                .awaitSignal("approval", Duration.ofSeconds(30));
    ArgumentCaptor<JobEntity> jobCaptor = ArgumentCaptor.forClass(JobEntity.class);

    fixedClockService.submit(builder);

    verify(jobCrudStore).create(jobCaptor.capture());
    assertEquals(fixedNow.plusSeconds(30), jobCaptor.getValue().getSignalTimeout());
  }

  // ---- chain steps ----

  @Test
  void checkCreate_calledForRecurringJob() {
    when(jobCrudStore.create(any())).thenReturn(savedEntity());

    DefaultRecurringJobBuilder builder =
        new DefaultRecurringJobBuilder(
            "0 0 * * * ?",
            ZoneId.of("UTC"),
            DefaultJobCreationServiceAuthorizationTest::noopTask,
            service);

    service.submit(builder);

    verify(authorizationPolicy).checkCreate(any(UUID.class), anyString());
  }

  // ---- workflow branch ----

  @Test
  void checkCreate_calledForStreamingBatchParent() {
    when(jobCrudStore.create(any())).thenAnswer(inv -> savedEntity());

    DefaultStreamingBatchBuilder<String> builder =
        new DefaultStreamingBatchBuilder<>("test-batch", service);
    builder.fromStream(Stream.of("item"));
    builder.process(DefaultJobCreationServiceAuthorizationTest::consumeString);

    service.submit(builder);

    verify(authorizationPolicy).checkCreate(any(UUID.class), anyString());
  }

  @Test
  void checkCreate_calledForEachChainStep() {
    when(jobCrudStore.findByIdempotencyKey(anyString())).thenReturn(Optional.empty());
    when(jobCrudStore.create(any())).thenAnswer(inv -> savedEntity());

    DefaultJobBuilder builder =
        (DefaultJobBuilder)
            DefaultJobBuilder.create(
                    service, DefaultJobCreationServiceAuthorizationTest::noopTask, Duration.ZERO)
                .then(DefaultJobCreationServiceAuthorizationTest::noopTask)
                .then(DefaultJobCreationServiceAuthorizationTest::noopTask);

    service.submit(builder);

    // 1 parent + 2 chain steps
    verify(authorizationPolicy, times(3)).checkCreate(any(UUID.class), anyString());
  }

  @Test
  void checkCreate_calledForWorkflowBranch() {
    when(jobCrudStore.findByIdempotencyKey(anyString())).thenReturn(Optional.empty());
    when(jobCrudStore.create(any())).thenAnswer(inv -> savedEntity());

    DefaultJobBuilder builder =
        (DefaultJobBuilder)
            DefaultJobBuilder.create(
                    service, DefaultJobCreationServiceAuthorizationTest::noopTask, Duration.ZERO)
                .thenOnSuccess(DefaultJobCreationServiceAuthorizationTest::noopTask);

    service.submit(builder);

    // 1 parent + 1 workflow branch
    verify(authorizationPolicy, times(2)).checkCreate(any(UUID.class), anyString());
  }
}
