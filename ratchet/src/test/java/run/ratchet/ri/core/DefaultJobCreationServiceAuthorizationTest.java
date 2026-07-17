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
package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import jakarta.transaction.Status;
import jakarta.transaction.Synchronization;
import jakarta.transaction.TransactionSynchronizationRegistry;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
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
import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;
import run.ratchet.api.event.JobSignalWaitingEvent;
import run.ratchet.api.exception.JobAuthorizationException;
import run.ratchet.ri.core.internal.InternalEventPublisher;
import run.ratchet.ri.core.internal.JobWakeupService;
import run.ratchet.ri.payload.DefaultJobInvocationResolver;
import run.ratchet.ri.security.CallerPrincipalProvider;
import run.ratchet.ri.security.JobPayloadInputValidator;
import run.ratchet.spi.CallerPrincipalResolver;
import run.ratchet.spi.JobAuthorizationPolicy;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.spi.TracingCollector;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.spi.BatchStore;
import run.ratchet.store.spi.JobBatchStatusStore;
import run.ratchet.store.spi.JobBulkStore;
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
  @Mock private JobBulkStore jobBulkStore;
  @Mock private BatchStore batchStore;
  @Mock private TagStore tagStore;
  @Mock private WorkflowConditionStore workflowConditionStore;
  @Mock private run.ratchet.store.spi.RecurringJobStore recurringJobStore;
  @Mock private RecurringScheduler recurringScheduler;
  @Mock private TracingCollector tracingCollector;
  @Mock private JobAuthorizationPolicy authorizationPolicy;
  @Mock private InternalEventPublisher eventPublisher;
  @Mock private MetricsCollector metricsCollector;
  @Mock private TransactionSynchronizationRegistry txRegistry;

  private DefaultJobCreationService service;
  private JobWakeupService wakeupService;

  private static class NoopJobWakeupService extends JobWakeupService {
    @Override
    public void notify(JobPriority priority, boolean immediate, String executionTarget) {}

    @Override
    public void notifyIfNeeded(
        JobExecutionType jobType, JobPriority priority, Duration delay, String executionTarget) {}
  }

  public static void noopTask() {}

  public static void consumeString(String s) {}

  private static JobEntity savedEntity() {
    JobEntity e = new JobEntity();
    e.setId(UUID.randomUUID());
    e.setJobType(JobExecutionType.SINGLE);
    e.setPriority(JobPriority.NORMAL);
    return e;
  }

  private static CallerPrincipalProvider principalProviderReturning(String principal) {
    return new CallerPrincipalProvider(null) {
      @Override
      public Optional<String> currentPrincipal() {
        return Optional.of(principal);
      }
    };
  }

  private static CallerPrincipalProvider principalProviderReturningEmpty() {
    return new CallerPrincipalProvider(null) {
      @Override
      public Optional<String> currentPrincipal() {
        return Optional.empty();
      }
    };
  }

  private DefaultJobCreationService serviceWith(
      CallerPrincipalProvider principalProvider,
      JobAuthorizationPolicy authorizationPolicy,
      InternalEventPublisher eventPublisher,
      MetricsCollector metricsCollector,
      Clock clock) {
    return new DefaultJobCreationService(
        jobBatchStatusStore,
        jobTerminalStore,
        jobCrudStore,
        jobBulkStore,
        batchStore,
        tagStore,
        workflowConditionStore,
        recurringJobStore,
        wakeupService,
        recurringScheduler,
        new DefaultJobInvocationResolver(),
        new JobPayloadInputValidator(),
        principalProvider,
        tracingCollector,
        authorizationPolicy,
        null,
        eventPublisher,
        metricsCollector,
        clock);
  }

  private DefaultJobCreationService serviceWithResolver(
      CallerPrincipalProvider principalProvider, CallerPrincipalResolver callerPrincipalResolver) {
    return new DefaultJobCreationService(
        jobBatchStatusStore,
        jobTerminalStore,
        jobCrudStore,
        jobBulkStore,
        batchStore,
        tagStore,
        workflowConditionStore,
        recurringJobStore,
        wakeupService,
        recurringScheduler,
        new DefaultJobInvocationResolver(),
        new JobPayloadInputValidator(),
        principalProvider,
        tracingCollector,
        authorizationPolicy,
        null,
        eventPublisher,
        metricsCollector,
        Clock.systemUTC(),
        callerPrincipalResolver);
  }

  private DefaultJobCreationService serviceWithoutAuthorizationPolicy() {
    // null jobBulkStore reproduces the legacy convenience-ctor behaviour where the bulk store was
    // omitted; batch submissions on this service exercise the "no bulk store" fallback path.
    return new DefaultJobCreationService(
        jobBatchStatusStore,
        jobTerminalStore,
        jobCrudStore,
        null,
        batchStore,
        tagStore,
        workflowConditionStore,
        recurringJobStore,
        wakeupService,
        recurringScheduler,
        new DefaultJobInvocationResolver(),
        new JobPayloadInputValidator(),
        null,
        null,
        null,
        null,
        null,
        null,
        Clock.systemUTC());
  }

  @BeforeEach
  void setUp() {
    wakeupService = new NoopJobWakeupService();
    service =
        serviceWith(
            principalProviderReturning(CAPTURED_PRINCIPAL),
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
    DefaultJobCreationService nullPolicyService = serviceWithoutAuthorizationPolicy();

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
  void checkCreate_nullCallerPrincipalProvider_passesNullPrincipal() {
    // No CallerPrincipalProvider is configured.
    DefaultJobCreationService systemService =
        serviceWith(
            null, // no CallerPrincipalProvider
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

  @Test
  void checkCreate_emptyPrincipalProvider_passesNullPrincipalAndDoesNotStampEntity() {
    DefaultJobCreationService anonymousService =
        serviceWith(
            principalProviderReturningEmpty(), authorizationPolicy, null, null, Clock.systemUTC());

    JobEntity saved = savedEntity();
    when(jobCrudStore.findByIdempotencyKey(anyString())).thenReturn(Optional.empty());
    when(jobCrudStore.create(any(JobEntity.class))).thenReturn(saved);

    DefaultJobBuilder builder =
        (DefaultJobBuilder)
            DefaultJobBuilder.create(
                anonymousService,
                DefaultJobCreationServiceAuthorizationTest::noopTask,
                Duration.ZERO);

    anonymousService.submit(builder);

    ArgumentCaptor<JobEntity> jobCaptor = ArgumentCaptor.forClass(JobEntity.class);
    verify(jobCrudStore).create(jobCaptor.capture());
    verify(authorizationPolicy).checkCreate(any(UUID.class), isNull());
    assertNull(jobCaptor.getValue().getCallerPrincipal());
  }

  @Test
  void checkCreate_configuredResolverTakesPrecedenceOverProvider() {
    DefaultJobCreationService resolverService =
        serviceWithResolver(
            principalProviderReturning("provider-principal"),
            () -> Optional.of("resolver-principal"));

    JobEntity saved = savedEntity();
    when(jobCrudStore.findByIdempotencyKey(anyString())).thenReturn(Optional.empty());
    when(jobCrudStore.create(any(JobEntity.class))).thenReturn(saved);

    DefaultJobBuilder builder =
        (DefaultJobBuilder)
            DefaultJobBuilder.create(
                resolverService,
                DefaultJobCreationServiceAuthorizationTest::noopTask,
                Duration.ZERO);

    resolverService.submit(builder);

    ArgumentCaptor<JobEntity> jobCaptor = ArgumentCaptor.forClass(JobEntity.class);
    verify(jobCrudStore).create(jobCaptor.capture());
    assertEquals("resolver-principal", jobCaptor.getValue().getCallerPrincipal());
    verify(authorizationPolicy).checkCreate(any(UUID.class), Mockito.eq("resolver-principal"));
  }

  @Test
  void checkCreate_throwingResolverDegradesToNullPrincipal_doesNotFailSubmission() {
    DefaultJobCreationService resolverService =
        serviceWithResolver(
            principalProviderReturning("provider-principal"),
            () -> {
              throw new IllegalStateException("ContextNotActiveException-like failure");
            });

    JobEntity saved = savedEntity();
    when(jobCrudStore.findByIdempotencyKey(anyString())).thenReturn(Optional.empty());
    when(jobCrudStore.create(any(JobEntity.class))).thenReturn(saved);

    DefaultJobBuilder builder =
        (DefaultJobBuilder)
            DefaultJobBuilder.create(
                resolverService,
                DefaultJobCreationServiceAuthorizationTest::noopTask,
                Duration.ZERO);

    JobHandle handle = resolverService.submit(builder);

    assertNotNull(handle, "A throwing resolver must not fail submission");
    ArgumentCaptor<JobEntity> jobCaptor = ArgumentCaptor.forClass(JobEntity.class);
    verify(jobCrudStore).create(jobCaptor.capture());
    assertNull(jobCaptor.getValue().getCallerPrincipal());
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
    verify(eventPublisher).publish(any(JobSignalWaitingEvent.class));
  }

  @Test
  void signalWaitingEventPublishesAfterCommit() {
    service.setTxRegistryForTesting(txRegistry);
    when(txRegistry.getTransactionStatus()).thenReturn(Status.STATUS_ACTIVE);
    ArgumentCaptor<Synchronization> synchronizationCaptor =
        ArgumentCaptor.forClass(Synchronization.class);
    JobEntity saved = savedEntity();
    when(jobCrudStore.findByIdempotencyKey(anyString())).thenReturn(Optional.empty());
    when(jobCrudStore.create(any(JobEntity.class))).thenReturn(saved);
    DefaultJobBuilder builder =
        (DefaultJobBuilder)
            DefaultJobBuilder.create(
                    service, DefaultJobCreationServiceAuthorizationTest::noopTask, Duration.ZERO)
                .awaitSignal("approval", Duration.ofSeconds(30));

    service.submit(builder);

    verify(txRegistry).registerInterposedSynchronization(synchronizationCaptor.capture());
    verify(eventPublisher, never()).publish(any(JobSignalWaitingEvent.class));

    synchronizationCaptor.getValue().afterCompletion(Status.STATUS_COMMITTED);

    verify(eventPublisher).publish(any(JobSignalWaitingEvent.class));
  }

  @Test
  void signalWaitingEventIsSuppressedWhenTransactionRollsBack() {
    service.setTxRegistryForTesting(txRegistry);
    when(txRegistry.getTransactionStatus()).thenReturn(Status.STATUS_ACTIVE);
    ArgumentCaptor<Synchronization> synchronizationCaptor =
        ArgumentCaptor.forClass(Synchronization.class);
    JobEntity saved = savedEntity();
    when(jobCrudStore.findByIdempotencyKey(anyString())).thenReturn(Optional.empty());
    when(jobCrudStore.create(any(JobEntity.class))).thenReturn(saved);
    DefaultJobBuilder builder =
        (DefaultJobBuilder)
            DefaultJobBuilder.create(
                    service, DefaultJobCreationServiceAuthorizationTest::noopTask, Duration.ZERO)
                .awaitSignal("approval", Duration.ofSeconds(30));

    service.submit(builder);
    verify(txRegistry).registerInterposedSynchronization(synchronizationCaptor.capture());

    synchronizationCaptor.getValue().afterCompletion(Status.STATUS_ROLLEDBACK);

    verify(eventPublisher, never()).publish(any(JobSignalWaitingEvent.class));
  }

  @Test
  void signalWaitingEventIsSuppressedWhenAfterCommitRegistrationFails() {
    service.setTxRegistryForTesting(txRegistry);
    when(txRegistry.getTransactionStatus()).thenReturn(Status.STATUS_ACTIVE);
    doThrow(new IllegalStateException("boom"))
        .when(txRegistry)
        .registerInterposedSynchronization(any());
    JobEntity saved = savedEntity();
    when(jobCrudStore.findByIdempotencyKey(anyString())).thenReturn(Optional.empty());
    when(jobCrudStore.create(any(JobEntity.class))).thenReturn(saved);
    DefaultJobBuilder builder =
        (DefaultJobBuilder)
            DefaultJobBuilder.create(
                    service, DefaultJobCreationServiceAuthorizationTest::noopTask, Duration.ZERO)
                .awaitSignal("approval", Duration.ofSeconds(30));

    service.submit(builder);

    verify(eventPublisher, never()).publish(any(JobSignalWaitingEvent.class));
  }

  // ---- streaming batch parent ----

  @Test
  void signalDeadlineIsComputedAtSubmitTimeWithInjectedClock() {
    Instant fixedNow = Instant.parse("2026-05-06T10:15:30Z");
    DefaultJobCreationService fixedClockService =
        serviceWith(
            null,
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
    when(recurringJobStore.createRecurring(any())).thenAnswer(inv -> UUID.randomUUID());

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

    verify(authorizationPolicy, times(2)).checkCreate(any(UUID.class), anyString());
  }

  @Test
  void checkCreate_calledForEachBatchChild() {
    when(jobCrudStore.create(any())).thenAnswer(inv -> savedEntity());

    DefaultBatchBuilder builder = new DefaultBatchBuilder("test-batch", service);
    builder.forEach(
        List.of("one", "two", "three"), DefaultJobCreationServiceAuthorizationTest::consumeString);

    service.submit(builder);

    // 1 batch parent + 3 child jobs
    verify(authorizationPolicy, times(4)).checkCreate(any(UUID.class), anyString());
  }

  @Test
  @SuppressWarnings("unchecked")
  void batchSubmit_bulkInsertsChildren() {
    when(jobCrudStore.create(any())).thenAnswer(inv -> savedEntity());

    DefaultBatchBuilder builder = new DefaultBatchBuilder("test-batch", service);
    builder.forEach(
        List.of("one", "two", "three"), DefaultJobCreationServiceAuthorizationTest::consumeString);

    service.submit(builder);

    ArgumentCaptor<List<JobEntity>> childrenCaptor = ArgumentCaptor.forClass(List.class);
    verify(jobBulkStore).bulkInsert(childrenCaptor.capture());
    assertEquals(3, childrenCaptor.getValue().size());
    verify(jobCrudStore, times(1)).create(any(JobEntity.class));
  }

  @Test
  @SuppressWarnings("unchecked")
  void batchSubmit_persistsChildRetryOptionsConfiguredAfterForEach() {
    when(jobCrudStore.create(any())).thenAnswer(inv -> savedEntity());

    DefaultBatchBuilder builder = new DefaultBatchBuilder("test-batch", service);
    builder.forEach(List.of("one"), DefaultJobCreationServiceAuthorizationTest::consumeString);
    builder.withMaxRetries(3).withBackoff(BackoffPolicy.EXPONENTIAL, Duration.ofSeconds(2));

    service.submit(builder);

    ArgumentCaptor<List<JobEntity>> childrenCaptor = ArgumentCaptor.forClass(List.class);
    verify(jobBulkStore).bulkInsert(childrenCaptor.capture());
    JobEntity child = childrenCaptor.getValue().get(0);
    assertEquals(3, child.getMaxRetries());
    assertEquals(BackoffPolicy.EXPONENTIAL, child.getBackoffPolicy());
    assertEquals(2_000, child.getBackoffParamMs());
  }

  @Test
  @SuppressWarnings("unchecked")
  void batchSubmit_doesNotApplyChildRetriesToParentOrBranches() {
    when(jobCrudStore.create(any())).thenAnswer(inv -> savedEntity());

    DefaultBatchBuilder builder = new DefaultBatchBuilder("test-batch", service);
    builder.forEach(List.of("one"), DefaultJobCreationServiceAuthorizationTest::consumeString);
    builder
        .withMaxRetries(3)
        .withBackoff(BackoffPolicy.FIXED, Duration.ofSeconds(1))
        .thenOnBatchSuccess(DefaultJobCreationServiceAuthorizationTest::noopTask);

    service.submit(builder);

    ArgumentCaptor<JobEntity> createdCaptor = ArgumentCaptor.forClass(JobEntity.class);
    verify(jobCrudStore, times(2)).create(createdCaptor.capture());
    List<JobEntity> created = createdCaptor.getAllValues();
    assertEquals(0, created.get(0).getMaxRetries(), "batch parent must not inherit child retries");
    assertEquals(
        0, created.get(1).getMaxRetries(), "workflow branch must not inherit child retries");

    ArgumentCaptor<List<JobEntity>> childrenCaptor = ArgumentCaptor.forClass(List.class);
    verify(jobBulkStore).bulkInsert(childrenCaptor.capture());
    assertEquals(3, childrenCaptor.getValue().get(0).getMaxRetries());
  }

  @Test
  @SuppressWarnings("unchecked")
  void batchSubmit_defaultsChildrenToNoRetries() {
    when(jobCrudStore.create(any())).thenAnswer(inv -> savedEntity());

    DefaultBatchBuilder defaults = new DefaultBatchBuilder("default-batch", service);
    defaults.forEach(List.of("two"), DefaultJobCreationServiceAuthorizationTest::consumeString);
    service.submit(defaults);

    ArgumentCaptor<List<JobEntity>> childrenCaptor = ArgumentCaptor.forClass(List.class);
    verify(jobBulkStore).bulkInsert(childrenCaptor.capture());
    assertEquals(0, childrenCaptor.getValue().get(0).getMaxRetries());
  }

  @Test
  void batchSubmit_withoutBulkStoreThrowsClearException() {
    DefaultJobCreationService serviceWithoutBulkStore = serviceWithoutAuthorizationPolicy();
    when(jobCrudStore.create(any())).thenAnswer(inv -> savedEntity());

    DefaultBatchBuilder builder = new DefaultBatchBuilder("test-batch", serviceWithoutBulkStore);
    builder.forEach(List.of("one"), DefaultJobCreationServiceAuthorizationTest::consumeString);

    IllegalStateException exception =
        assertThrows(IllegalStateException.class, () -> serviceWithoutBulkStore.submit(builder));

    assertEquals(
        "Batch submission requires a JobBulkStore; use the CDI constructor or pass a store that"
            + " implements JobBulkStore.",
        exception.getMessage());
  }

  @Test
  @SuppressWarnings("unchecked")
  void streamingBatchSubmit_bulkInsertsEachChunk() {
    when(jobCrudStore.create(any())).thenAnswer(inv -> savedEntity());

    DefaultStreamingBatchBuilder<String> builder =
        new DefaultStreamingBatchBuilder<>("test-batch", service);
    builder.fromStream(Stream.of("one", "two", "three"));
    builder.withChunkSize(2);
    builder.process(DefaultJobCreationServiceAuthorizationTest::consumeString);

    service.submit(builder);

    ArgumentCaptor<List<JobEntity>> childrenCaptor = ArgumentCaptor.forClass(List.class);
    verify(jobBulkStore, times(2)).bulkInsert(childrenCaptor.capture());
    assertEquals(2, childrenCaptor.getAllValues().get(0).size());
    assertEquals(1, childrenCaptor.getAllValues().get(1).size());
  }

  @Test
  @SuppressWarnings("unchecked")
  void streamingBatchSubmit_persistsChildRetryOptionsConfiguredAfterProcess() {
    when(jobCrudStore.create(any())).thenAnswer(inv -> savedEntity());

    DefaultStreamingBatchBuilder<String> builder =
        new DefaultStreamingBatchBuilder<>("test-batch", service);
    builder.fromStream(Stream.of("one"));
    builder.process(DefaultJobCreationServiceAuthorizationTest::consumeString);
    builder.withMaxRetries(2).withBackoff(BackoffPolicy.FIXED, Duration.ofMillis(750));

    service.submit(builder);

    ArgumentCaptor<List<JobEntity>> childrenCaptor = ArgumentCaptor.forClass(List.class);
    verify(jobBulkStore).bulkInsert(childrenCaptor.capture());
    JobEntity child = childrenCaptor.getValue().get(0);
    assertEquals(2, child.getMaxRetries());
    assertEquals(BackoffPolicy.FIXED, child.getBackoffPolicy());
    assertEquals(750, child.getBackoffParamMs());
  }

  @Test
  void batchBuilders_reuseJobOptionsValidationForChildRetries() {
    DefaultJobBuilder job =
        (DefaultJobBuilder)
            DefaultJobBuilder.create(
                service, DefaultJobCreationServiceAuthorizationTest::noopTask, Duration.ZERO);
    DefaultBatchBuilder batch = new DefaultBatchBuilder("batch", service);
    DefaultStreamingBatchBuilder<String> streaming =
        new DefaultStreamingBatchBuilder<>("streaming", service);

    IllegalArgumentException retriesError =
        assertThrows(IllegalArgumentException.class, () -> job.withMaxRetries(-1));
    assertEquals(
        retriesError.getMessage(),
        assertThrows(IllegalArgumentException.class, () -> batch.withMaxRetries(-1)).getMessage());
    assertEquals(
        retriesError.getMessage(),
        assertThrows(IllegalArgumentException.class, () -> streaming.withMaxRetries(-1))
            .getMessage());

    NullPointerException policyError =
        assertThrows(NullPointerException.class, () -> job.withBackoff(null, Duration.ZERO));
    assertEquals(
        policyError.getMessage(),
        assertThrows(NullPointerException.class, () -> batch.withBackoff(null, Duration.ZERO))
            .getMessage());

    NullPointerException paramError =
        assertThrows(NullPointerException.class, () -> job.withBackoff(BackoffPolicy.FIXED, null));
    assertEquals(
        paramError.getMessage(),
        assertThrows(
                NullPointerException.class, () -> streaming.withBackoff(BackoffPolicy.FIXED, null))
            .getMessage());
  }

  @Test
  void streamingBatchSubmit_savesBatchBeforeChildrenAndUpdatesFinalTotal() {
    JobEntity saved = savedEntity();
    when(jobCrudStore.create(any())).thenReturn(saved);

    DefaultStreamingBatchBuilder<String> builder =
        new DefaultStreamingBatchBuilder<>("test-batch", service);
    builder.fromStream(Stream.of("one", "two", "three"));
    builder.withChunkSize(2);
    builder.process(DefaultJobCreationServiceAuthorizationTest::consumeString);

    service.submit(builder);

    InOrder inOrder = Mockito.inOrder(batchStore, jobBulkStore);
    inOrder.verify(batchStore).saveBatch(any());
    inOrder.verify(jobBulkStore, times(2)).bulkInsert(any());
    inOrder.verify(batchStore).updateBatchTotalItems(saved.getId(), 3);
  }

  @Test
  void emptyBatchSubmit_marksBatchCompleteOnlyAfterSyntheticPickupWins() {
    JobEntity saved = savedEntity();
    when(jobCrudStore.create(any())).thenReturn(saved);
    when(jobBatchStatusStore.tryPickUpJob(
            saved.getId(), DefaultBatchBuilder.BATCH_LIFECYCLE_NODE_ID))
        .thenReturn(false);

    DefaultBatchBuilder builder = new DefaultBatchBuilder("empty-batch", service);

    service.submit(builder);

    verify(jobTerminalStore, never()).markJobSucceededMinimal(any(), any(), any(), any(), any());
    verify(batchStore, never()).markBatchCompleteIfReady(saved.getId());
  }

  @Test
  void checkCreate_skippedForDuplicateIdempotencyKey() {
    UUID existingId = UUID.randomUUID();
    JobEntity existing = savedEntity();
    existing.setId(existingId);
    when(jobCrudStore.findByIdempotencyKey(anyString())).thenReturn(Optional.of(existing));

    DefaultJobBuilder builder =
        (DefaultJobBuilder)
            DefaultJobBuilder.create(
                service, DefaultJobCreationServiceAuthorizationTest::noopTask, Duration.ZERO);

    JobHandle handle = service.submit(builder);

    assertEquals(existingId, handle.id());
    verify(authorizationPolicy, never()).checkCreate(any(), any());
    verify(jobCrudStore, never()).create(any());
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
