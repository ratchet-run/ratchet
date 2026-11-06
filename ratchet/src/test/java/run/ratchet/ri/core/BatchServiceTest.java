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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import jakarta.transaction.Status;
import jakarta.transaction.Synchronization;
import jakarta.transaction.TransactionSynchronizationRegistry;
import jakarta.transaction.Transactional;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import run.ratchet.api.BatchContext;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobStatus;
import run.ratchet.api.JobType;
import run.ratchet.api.RatchetOptions;
import run.ratchet.api.SerializableConsumer;
import run.ratchet.api.event.BatchCompletedEvent;
import run.ratchet.api.event.BatchCompletingEvent;
import run.ratchet.api.event.JobCompletedEvent;
import run.ratchet.api.event.JobDlqEvent;
import run.ratchet.api.event.JobFailedEvent;
import run.ratchet.ri.core.internal.DeadLetterService;
import run.ratchet.ri.core.internal.DurableTerminalEvents;
import run.ratchet.ri.core.internal.DurableTerminalEvents.PreparedDlqEvents;
import run.ratchet.ri.core.internal.InternalEventPublisher;
import run.ratchet.ri.core.internal.JakartaAfterCommitRegistrar;
import run.ratchet.ri.core.internal.WorkflowCompletionPlan;
import run.ratchet.ri.core.internal.WorkflowScheduler;
import run.ratchet.ri.payload.JobPayloadFactory;
import run.ratchet.spi.AfterCommitRegistrar;
import run.ratchet.spi.BeanResolver;
import run.ratchet.spi.ClassPolicy;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.store.dto.BatchProgress;
import run.ratchet.store.dto.JobCompletionPlan;
import run.ratchet.store.dto.JobCompletionResult;
import run.ratchet.store.entity.BatchEntity;
import run.ratchet.store.entity.BatchMetricsEntity;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.entity.JobPayload;
import run.ratchet.store.spi.BatchStore;
import run.ratchet.store.spi.JobBatchStatusStore;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.JobStore;
import run.ratchet.store.spi.JobTerminalStore;

@ExtendWith(MockitoExtension.class)
class BatchServiceTest {
  public static final class Hooks {
    static Object album;
    static BatchContext context;

    public static void record(long albumId, BatchContext ctx) {
      album = albumId;
      context = ctx;
    }

    public static void onProgress(BatchContext ctx) {
      context = ctx;
    }
  }

  @Test
  void progressHookBindsContextBesideCapturedValueAndSupportsMethodReference() throws Exception {
    when(classPolicy.isAllowed(Hooks.class.getName())).thenReturn(true);
    BatchContext ctx = new BatchContext(UUID.randomUUID(), 3, 1, 0);
    Method execute =
        BatchService.class.getDeclaredMethod(
            "executeProgressHook", JobPayload.class, BatchContext.class);
    execute.setAccessible(true);
    long albumId = Long.parseLong("42");
    JobPayload indexed =
        JobPayloadFactory.fromLambda(
            (SerializableConsumer<BatchContext>) value -> Hooks.record(albumId, value));
    execute.invoke(batchService, indexed, ctx);
    assertEquals(albumId, Hooks.album);
    assertSame(ctx, Hooks.context);
    Hooks.context = null;
    execute.invoke(
        batchService,
        JobPayloadFactory.fromLambda((SerializableConsumer<BatchContext>) Hooks::onProgress),
        ctx);
    assertSame(ctx, Hooks.context);
    Hooks.context = null;
    execute.invoke(
        batchService,
        new JobPayload(
            Hooks.class.getName(),
            "onProgress",
            "(Lrun/ratchet/api/BatchContext;)V",
            true,
            List.of(),
            null),
        ctx);
    assertSame(ctx, Hooks.context);
  }

  private static final Instant FIXED_NOW = Instant.parse("2026-05-12T12:00:00Z");
  private static final Clock FIXED_CLOCK = Clock.fixed(FIXED_NOW, ZoneOffset.UTC);

  @Mock private BatchStore batchStore;
  @Mock private JobCrudStore jobCrudStore;
  @Mock private JobBatchStatusStore jobBatchStatusStore;
  @Mock private JobTerminalStore jobTerminalStore;
  @Mock private MetricsCollector metricsCollector;
  @Mock private InternalEventPublisher eventPublisher;
  @Mock private DeadLetterService deadLetterService;
  @Mock private WorkflowScheduler workflowScheduler;
  @Mock private ClassPolicy classPolicy;
  @Mock private BeanResolver beanResolver;
  @Mock private TransactionSynchronizationRegistry txRegistry;
  @Spy private BatchCompletionTransaction batchCompletionTransaction;

  private BatchService batchService;

  @BeforeEach
  void setUp() {
    lenient().when(txRegistry.getTransactionStatus()).thenReturn(Status.STATUS_NO_TRANSACTION);
    lenient()
        .when(workflowScheduler.planCompletion(any(), ArgumentMatchers.eq(false)))
        .thenReturn(WorkflowCompletionPlan.empty());
    lenient()
        .when(jobTerminalStore.commitCompletion(any()))
        .thenReturn(new JobCompletionResult(true, null));
    batchService =
        new BatchService(
            batchStore,
            jobCrudStore,
            jobBatchStatusStore,
            jobTerminalStore,
            metricsCollector,
            eventPublisher,
            deadLetterService,
            workflowScheduler,
            classPolicy,
            beanResolver,
            FIXED_CLOCK,
            new JakartaAfterCommitRegistrar(txRegistry),
            batchCompletionTransaction,
            null);
  }

  @Test
  void hookCachesAreScopedToTheBeanInstance() throws NoSuchFieldException {
    Field hookCache = BatchService.class.getDeclaredField("hookMethodCache");
    Field classCache = BatchService.class.getDeclaredField("classCache");

    assertFalse(Modifier.isStatic(hookCache.getModifiers()));
    assertFalse(Modifier.isStatic(classCache.getModifiers()));
  }

  @Test
  void recoverySweepDelegatesEachBatchToASeparateTransaction() throws NoSuchMethodException {
    Transactional sweep =
        BatchService.class.getMethod("recoverStuckBatches").getAnnotation(Transactional.class);
    Transactional unit =
        BatchCompletionTransaction.class
            .getMethod("complete", Supplier.class)
            .getAnnotation(Transactional.class);

    assertNotNull(sweep);
    assertNotNull(unit);
    assertEquals(Transactional.TxType.NOT_SUPPORTED, sweep.value());
    assertEquals(Transactional.TxType.REQUIRES_NEW, unit.value());
  }

  @Test
  void emptyBatchCompletionUsesRequiredTransaction() throws NoSuchMethodException {
    Method completion =
        BatchService.class.getMethod("completeEmptyBatch", UUID.class, BatchEntity.class);
    Transactional transaction = completion.getAnnotation(Transactional.class);
    if (transaction == null) {
      transaction = BatchService.class.getAnnotation(Transactional.class);
    }
    assertNotNull(transaction);
    assertEquals(Transactional.TxType.REQUIRED, transaction.value());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void emptyBatchCompletesInCallerTransactionAndPublishesAfterCommit(boolean newWorkAvailable) {
    when(txRegistry.getTransactionStatus()).thenReturn(Status.STATUS_ACTIVE);
    UUID parentId = UUID.randomUUID();
    JobEntity parent = parent(parentId);
    WorkflowCompletionPlan workflow =
        new WorkflowCompletionPlan(List.of(), List.of(), newWorkAvailable);
    when(jobCrudStore.findById(parentId)).thenReturn(Optional.of(parent));
    when(workflowScheduler.planCompletion(parent, false)).thenReturn(workflow);

    assertEquals(
        newWorkAvailable, batchService.completeEmptyBatch(parentId, batch(parentId, 0, 0, 0)));

    ArgumentCaptor<JobCompletionPlan> plan = ArgumentCaptor.forClass(JobCompletionPlan.class);
    verify(jobTerminalStore).commitCompletion(plan.capture());
    assertEquals(parentId, plan.getValue().jobId());
    assertEquals(JobStatus.PENDING, plan.getValue().expectedStatus());
    assertEquals(JobStatus.SUCCEEDED, plan.getValue().terminalStatus());
    assertEquals(new JobCompletionPlan.BatchCompletion(0, 0, 0), plan.getValue().completedBatch());
    assertEquals(JobStatus.SUCCEEDED, parent.getStatus());
    verify(metricsCollector).jobCompleted(parentId, JobType.BATCH, 0L);
    verify(workflowScheduler).publishCompletion(workflow);
    verifyNoInteractions(batchCompletionTransaction);
    verify(batchStore, never()).markBatchCompleteIfReady(any());

    ArgumentCaptor<Synchronization> synchronization =
        ArgumentCaptor.forClass(Synchronization.class);
    verify(txRegistry).registerInterposedSynchronization(synchronization.capture());
    verifyNoInteractions(eventPublisher);

    synchronization.getValue().afterCompletion(Status.STATUS_COMMITTED);

    InOrder events = inOrder(eventPublisher);
    events.verify(eventPublisher).publish(any(BatchCompletingEvent.class));
    events.verify(eventPublisher).publish(any(BatchCompletedEvent.class));
    ArgumentCaptor<JobCompletedEvent> completed = ArgumentCaptor.forClass(JobCompletedEvent.class);
    events.verify(eventPublisher).publish(completed.capture());
    assertEquals(parentId, completed.getValue().getJobId());
    assertEquals(0L, completed.getValue().getExecutionTimeMs());
  }

  @Test
  void emptyBatchWithMetricsWithoutDurationRecordsZeroDuration() {
    UUID parentId = UUID.randomUUID();
    when(jobCrudStore.findById(parentId)).thenReturn(Optional.of(parent(parentId)));
    when(batchStore.findBatchMetrics(parentId)).thenReturn(Optional.of(new BatchMetricsEntity()));

    batchService.completeEmptyBatch(parentId, batch(parentId, 0, 0, 0));

    verify(metricsCollector).jobCompleted(parentId, JobType.BATCH, 0L);
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 2})
  void durableParentStoresKnownEmptyDurationAndPublishesCommittedDuration(int total) {
    AfterCommitRegistrar registrar =
        action -> {
          action.run();
          return AfterCommitRegistrar.Result.REGISTERED;
        };
    DurableTerminalEvents durable =
        new DurableTerminalEvents(
            RatchetOptions.builder().events(e -> e.durable(true)).build(),
            mock(JobStore.class),
            FIXED_CLOCK,
            eventPublisher,
            registrar,
            null);
    batchService =
        new BatchService(
            batchStore,
            jobCrudStore,
            jobBatchStatusStore,
            jobTerminalStore,
            metricsCollector,
            eventPublisher,
            deadLetterService,
            workflowScheduler,
            classPolicy,
            beanResolver,
            FIXED_CLOCK,
            new JakartaAfterCommitRegistrar(txRegistry),
            batchCompletionTransaction,
            null,
            durable);
    UUID parentId = UUID.randomUUID();
    BatchMetricsEntity metrics = new BatchMetricsEntity();
    metrics.setTotalDurationMs(total == 0 ? null : 7L);
    when(jobCrudStore.findById(parentId)).thenReturn(Optional.of(parent(parentId)));
    when(batchStore.findBatchMetrics(parentId)).thenReturn(Optional.of(metrics));
    if (total == 0) batchService.completeEmptyBatch(parentId, batch(parentId, 0, 0, 0));
    else {
      when(batchStore.findBatchById(parentId))
          .thenReturn(Optional.of(batch(parentId, total, total, 0)));
      batchService.afterChildCompletion(new BatchProgress(parentId, total, total, 0, null));
    }
    ArgumentCaptor<JobCompletionPlan> plan = ArgumentCaptor.forClass(JobCompletionPlan.class);
    verify(jobTerminalStore).commitCompletion(plan.capture());
    assertEquals(1, plan.getValue().outboxEvents().size());
    if (total == 0) assertEquals(0L, plan.getValue().outboxEvents().get(0).executionTimeMs());
    else assertNull(plan.getValue().outboxEvents().get(0).executionTimeMs());
    ArgumentCaptor<JobCompletedEvent> completed = ArgumentCaptor.forClass(JobCompletedEvent.class);
    verify(eventPublisher).publish(completed.capture());
    assertEquals(total == 0 ? 0L : 7L, completed.getValue().getExecutionTimeMs());
    assertEquals(
        plan.getValue().outboxEvents().get(0).eventId(), completed.getValue().getEventId());
  }

  @Test
  void durableFailedParentPlansAndPublishesFailureAndDlqWithMatchingIds() {
    AfterCommitRegistrar registrar =
        action -> {
          action.run();
          return AfterCommitRegistrar.Result.REGISTERED;
        };
    DurableTerminalEvents durable =
        new DurableTerminalEvents(
            RatchetOptions.builder().events(e -> e.durable(true)).build(),
            mock(JobStore.class),
            FIXED_CLOCK,
            eventPublisher,
            registrar,
            null);
    batchService =
        new BatchService(
            batchStore,
            jobCrudStore,
            jobBatchStatusStore,
            jobTerminalStore,
            metricsCollector,
            eventPublisher,
            deadLetterService,
            workflowScheduler,
            classPolicy,
            beanResolver,
            FIXED_CLOCK,
            new JakartaAfterCommitRegistrar(txRegistry),
            batchCompletionTransaction,
            null,
            durable);
    UUID parentId = UUID.randomUUID();
    JobEntity parent = parent(parentId);
    when(jobCrudStore.findById(parentId)).thenReturn(Optional.of(parent));
    when(batchStore.findBatchById(parentId)).thenReturn(Optional.of(batch(parentId, 2, 1, 1)));
    when(deadLetterService.prepareDlqEvent(any(), any()))
        .thenAnswer(
            invocation ->
                new JobDlqEvent(
                    parentId,
                    parent.getBusinessKey(),
                    parent.getRecurringMasterId(),
                    parent.getPublicJobType(),
                    parent.getPriority(),
                    parent.getPickedBy(),
                    FIXED_NOW,
                    parent.getLastError(),
                    0));
    doAnswer(
            invocation -> {
              PreparedDlqEvents prepared = invocation.getArgument(1);
              durable.publishNow(prepared.events());
              return null;
            })
        .when(deadLetterService)
        .recordPreparedBatchDlqTransition(any(), any());
    batchService.afterChildCompletion(new BatchProgress(parentId, 2, 1, 1, null));
    ArgumentCaptor<JobCompletionPlan> plan = ArgumentCaptor.forClass(JobCompletionPlan.class);
    verify(jobTerminalStore).commitCompletion(plan.capture());
    assertEquals(2, plan.getValue().outboxEvents().size());
    ArgumentCaptor<JobFailedEvent> failed = ArgumentCaptor.forClass(JobFailedEvent.class);
    ArgumentCaptor<JobDlqEvent> dlq = ArgumentCaptor.forClass(JobDlqEvent.class);
    InOrder order = inOrder(eventPublisher);
    order.verify(eventPublisher).publish(any(BatchCompletingEvent.class));
    order.verify(eventPublisher).publish(any(BatchCompletedEvent.class));
    order.verify(eventPublisher).publish(failed.capture());
    order.verify(eventPublisher).publish(dlq.capture());
    assertEquals(plan.getValue().outboxEvents().get(0).eventId(), failed.getValue().getEventId());
    assertEquals(plan.getValue().outboxEvents().get(1).eventId(), dlq.getValue().getEventId());
  }

  @Test
  void emptyBatchPreservesExistingMetricsDuration() {
    UUID parentId = UUID.randomUUID();
    BatchMetricsEntity metrics = new BatchMetricsEntity();
    metrics.setTotalDurationMs(7L);
    when(jobCrudStore.findById(parentId)).thenReturn(Optional.of(parent(parentId)));
    when(batchStore.findBatchMetrics(parentId)).thenReturn(Optional.of(metrics));

    batchService.completeEmptyBatch(parentId, batch(parentId, 0, 0, 0));

    verify(metricsCollector).jobCompleted(parentId, JobType.BATCH, 7L);
  }

  @Test
  void nonEmptyBatchWithoutMetricsDurationStillDoesNotRecordCompletionMetric() {
    UUID parentId = UUID.randomUUID();
    JobEntity child = new JobEntity();
    child.setDependsOn(parentId);
    when(batchStore.incrementCompletedAtomic(parentId))
        .thenReturn(new BatchProgress(parentId, 1, 1, 0, null));
    when(batchStore.findBatchById(parentId)).thenReturn(Optional.of(batch(parentId, 1, 1, 0)));
    when(jobCrudStore.findById(parentId)).thenReturn(Optional.of(parent(parentId)));

    batchService.markChildSucceeded(child);

    verifyNoInteractions(metricsCollector);
  }

  @ParameterizedTest
  @EnumSource(value = JobStatus.class, names = "PENDING", mode = EnumSource.Mode.EXCLUDE)
  void emptyBatchDoesNothingUnlessParentIsPending(JobStatus status) {
    UUID parentId = UUID.randomUUID();
    JobEntity parent = parent(parentId);
    parent.setStatus(status);
    when(jobCrudStore.findById(parentId)).thenReturn(Optional.of(parent));

    assertFalse(batchService.completeEmptyBatch(parentId, batch(parentId, 0, 0, 0)));

    assertEquals(status, parent.getStatus());
    verifyNoInteractions(
        jobTerminalStore,
        batchStore,
        workflowScheduler,
        metricsCollector,
        eventPublisher,
        batchCompletionTransaction);
  }

  @Test
  void emptyBatchLostCompletionDoesNotPublishOrRecordMetrics() {
    UUID parentId = UUID.randomUUID();
    JobEntity parent = parent(parentId);
    when(jobCrudStore.findById(parentId)).thenReturn(Optional.of(parent));
    when(jobTerminalStore.commitCompletion(any())).thenReturn(JobCompletionResult.notCommitted());

    assertFalse(batchService.completeEmptyBatch(parentId, batch(parentId, 0, 0, 0)));

    assertEquals(JobStatus.PENDING, parent.getStatus());
    verifyNoInteractions(batchStore, metricsCollector, eventPublisher);
    verify(workflowScheduler, never()).publishCompletion(any());
  }

  @Test
  void emptyBatchRollbackSuppressesCompletionEvents() {
    when(txRegistry.getTransactionStatus()).thenReturn(Status.STATUS_ACTIVE);
    UUID parentId = UUID.randomUUID();
    when(jobCrudStore.findById(parentId)).thenReturn(Optional.of(parent(parentId)));

    batchService.completeEmptyBatch(parentId, batch(parentId, 0, 0, 0));

    ArgumentCaptor<Synchronization> synchronization =
        ArgumentCaptor.forClass(Synchronization.class);
    verify(txRegistry).registerInterposedSynchronization(synchronization.capture());
    synchronization.getValue().afterCompletion(Status.STATUS_ROLLEDBACK);

    verifyNoInteractions(eventPublisher);
  }

  @Test
  void completedBatchEventUsesParentMetadata() {
    UUID parentId = UUID.randomUUID();
    JobEntity child = new JobEntity();
    child.setDependsOn(parentId);

    BatchProgress progress = new BatchProgress(parentId, 3, 3, 0, null);
    JobEntity parent = new JobEntity();
    parent.setId(parentId);
    parent.setStatus(JobStatus.PENDING);
    parent.setJobType(JobExecutionType.BATCH_PARENT);
    parent.setBusinessKey("invoice-run");
    parent.setPriority(JobPriority.HIGH);
    parent.setPickedBy("batch-node-1");

    when(batchStore.incrementCompletedAtomic(parentId)).thenReturn(progress);
    when(batchStore.findBatchById(parentId)).thenReturn(Optional.of(batch(parentId, 3, 3, 0)));
    when(jobCrudStore.findById(parentId)).thenReturn(Optional.of(parent));
    when(jobTerminalStore.commitCompletion(
            ArgumentMatchers.argThat(
                plan ->
                    plan != null
                        && parentId.equals(plan.jobId())
                        && plan.terminalStatus() == JobStatus.SUCCEEDED)))
        .thenReturn(new JobCompletionResult(true, null));
    when(batchStore.findBatchMetrics(parentId)).thenReturn(Optional.empty());

    batchService.markChildSucceeded(child);

    ArgumentCaptor<Object> event = ArgumentCaptor.forClass(Object.class);
    verify(eventPublisher, times(3)).publish(event.capture());
    BatchCompletingEvent completingEvent =
        event.getAllValues().stream()
            .filter(BatchCompletingEvent.class::isInstance)
            .map(BatchCompletingEvent.class::cast)
            .findFirst()
            .orElseThrow();
    assertEquals(parentId, completingEvent.getJobId());
    assertEquals("invoice-run", completingEvent.getBusinessKey());
    assertEquals(JobType.BATCH, completingEvent.getJobType());
    assertEquals(JobPriority.HIGH, completingEvent.getPriority());
    assertEquals("batch-node-1", completingEvent.getNodeId());
    assertEquals(3, completingEvent.getTotalItems());
    assertEquals(3, completingEvent.getCompletedItems());
    assertEquals(0, completingEvent.getFailedItems());
    BatchCompletedEvent completedEvent =
        event.getAllValues().stream()
            .filter(BatchCompletedEvent.class::isInstance)
            .map(BatchCompletedEvent.class::cast)
            .findFirst()
            .orElseThrow();
    assertEquals(parentId, completedEvent.getJobId());
    assertEquals(3, completedEvent.getTotalItems());
    assertEquals(3, completedEvent.getCompletedItems());
    assertEquals(0, completedEvent.getFailedItems());
    JobCompletedEvent jobCompletedEvent =
        event.getAllValues().stream()
            .filter(JobCompletedEvent.class::isInstance)
            .map(JobCompletedEvent.class::cast)
            .findFirst()
            .orElseThrow();
    assertEquals(parentId, jobCompletedEvent.getJobId());
    verify(batchStore).findBatchById(parentId);
    verify(jobTerminalStore)
        .commitCompletion(
            ArgumentMatchers.argThat(
                plan ->
                    plan != null
                        && parentId.equals(plan.jobId())
                        && plan.terminalStatus() == JobStatus.SUCCEEDED));
  }

  @Test
  void completedBatchReloadsCountersBeforeFinalizingParent() {
    UUID parentId = UUID.randomUUID();
    JobEntity child = new JobEntity();
    child.setDependsOn(parentId);

    BatchProgress staleProgress = new BatchProgress(parentId, 3, 2, 0, null);
    BatchEntity currentBatch = batch(parentId, 3, 2, 1);
    JobEntity parent = new JobEntity();
    parent.setId(parentId);
    parent.setStatus(JobStatus.PENDING);
    parent.setJobType(JobExecutionType.BATCH_PARENT);
    parent.setPriority(JobPriority.NORMAL);

    when(batchStore.incrementCompletedAtomic(parentId)).thenReturn(staleProgress);
    when(batchStore.findBatchById(parentId)).thenReturn(Optional.of(currentBatch));
    when(jobCrudStore.findById(parentId)).thenReturn(Optional.of(parent));
    when(jobTerminalStore.commitCompletion(
            ArgumentMatchers.argThat(
                plan ->
                    plan != null
                        && parentId.equals(plan.jobId())
                        && plan.terminalStatus() == JobStatus.FAILED)))
        .thenReturn(new JobCompletionResult(true, null));
    when(batchStore.findBatchMetrics(parentId)).thenReturn(Optional.empty());

    batchService.markChildSucceeded(child);

    assertEquals(JobStatus.FAILED, parent.getStatus());
    verify(jobTerminalStore, never()).markJobSucceededMinimal(any(), any(), any(), any(), any());
    verify(jobTerminalStore)
        .commitCompletion(
            ArgumentMatchers.argThat(
                plan ->
                    plan != null
                        && parentId.equals(plan.jobId())
                        && plan.terminalStatus() == JobStatus.FAILED));

    ArgumentCaptor<Object> event = ArgumentCaptor.forClass(Object.class);
    verify(eventPublisher, times(3)).publish(event.capture());
    BatchCompletingEvent completingEvent =
        event.getAllValues().stream()
            .filter(BatchCompletingEvent.class::isInstance)
            .map(BatchCompletingEvent.class::cast)
            .findFirst()
            .orElseThrow();
    assertEquals(3, completingEvent.getTotalItems());
    assertEquals(2, completingEvent.getCompletedItems());
    assertEquals(1, completingEvent.getFailedItems());
    JobFailedEvent failedEvent =
        event.getAllValues().stream()
            .filter(JobFailedEvent.class::isInstance)
            .map(JobFailedEvent.class::cast)
            .findFirst()
            .orElseThrow();
    assertEquals(parentId, failedEvent.getJobId());
    assertEquals("Batch completed with 1 failed children", failedEvent.getErrorMessage());
    assertEquals(0, failedEvent.getRetryAttempt());
    assertEquals("Batch completed with 1 failed children", parent.getLastError());
    assertEquals(0, parent.getAttempts());

    InOrder terminalOrder = inOrder(eventPublisher, deadLetterService);
    terminalOrder.verify(eventPublisher).publish(any(BatchCompletingEvent.class));
    terminalOrder.verify(eventPublisher).publish(any(BatchCompletedEvent.class));
    terminalOrder.verify(eventPublisher).publish(any(JobFailedEvent.class));
    terminalOrder.verify(deadLetterService).recordDlqTransition(parent, null);
  }

  @Test
  void completedBatchEventPublishesAfterCommit() {
    when(txRegistry.getTransactionStatus()).thenReturn(Status.STATUS_ACTIVE);
    ArgumentCaptor<Synchronization> synchronizationCaptor =
        ArgumentCaptor.forClass(Synchronization.class);
    UUID parentId = UUID.randomUUID();
    JobEntity child = new JobEntity();
    child.setDependsOn(parentId);
    BatchProgress progress = new BatchProgress(parentId, 1, 1, 0, null);
    JobEntity parent = new JobEntity();
    parent.setId(parentId);
    parent.setStatus(JobStatus.PENDING);
    parent.setJobType(JobExecutionType.BATCH_PARENT);
    parent.setPriority(JobPriority.NORMAL);

    when(batchStore.incrementCompletedAtomic(parentId)).thenReturn(progress);
    when(batchStore.findBatchById(parentId)).thenReturn(Optional.of(batch(parentId, 1, 1, 0)));
    when(jobCrudStore.findById(parentId)).thenReturn(Optional.of(parent));
    when(jobTerminalStore.commitCompletion(
            ArgumentMatchers.argThat(
                plan ->
                    plan != null
                        && parentId.equals(plan.jobId())
                        && plan.terminalStatus() == JobStatus.SUCCEEDED)))
        .thenReturn(new JobCompletionResult(true, null));
    when(batchStore.findBatchMetrics(parentId)).thenReturn(Optional.empty());

    batchService.markChildSucceeded(child);

    verify(txRegistry).registerInterposedSynchronization(synchronizationCaptor.capture());
    verify(eventPublisher, never()).publish(any());

    synchronizationCaptor.getValue().afterCompletion(Status.STATUS_COMMITTED);

    verify(eventPublisher).publish(any(BatchCompletingEvent.class));
  }

  @Test
  void failedBatchDelegatesDlqOnlyAfterCommitAndAfterJobFailed() {
    when(txRegistry.getTransactionStatus()).thenReturn(Status.STATUS_ACTIVE);
    ArgumentCaptor<Synchronization> synchronizationCaptor =
        ArgumentCaptor.forClass(Synchronization.class);
    UUID parentId = UUID.randomUUID();
    JobEntity child = new JobEntity();
    child.setDependsOn(parentId);
    JobEntity parent = parent(parentId);

    when(batchStore.incrementFailedAtomic(parentId))
        .thenReturn(new BatchProgress(parentId, 1, 0, 1, null));
    when(batchStore.findBatchById(parentId)).thenReturn(Optional.of(batch(parentId, 1, 0, 1)));
    when(jobCrudStore.findById(parentId)).thenReturn(Optional.of(parent));
    when(jobTerminalStore.commitCompletion(
            ArgumentMatchers.argThat(
                plan ->
                    plan != null
                        && parentId.equals(plan.jobId())
                        && plan.terminalStatus() == JobStatus.FAILED)))
        .thenReturn(new JobCompletionResult(true, null));
    when(batchStore.findBatchMetrics(parentId)).thenReturn(Optional.empty());

    batchService.markChildFailed(child);

    verify(txRegistry).registerInterposedSynchronization(synchronizationCaptor.capture());
    verify(eventPublisher, never()).publish(any());
    verify(deadLetterService, never()).recordDlqTransition(any(), any());

    synchronizationCaptor.getValue().afterCompletion(Status.STATUS_COMMITTED);

    InOrder terminalOrder = inOrder(eventPublisher, deadLetterService);
    terminalOrder.verify(eventPublisher).publish(any(BatchCompletingEvent.class));
    terminalOrder.verify(eventPublisher).publish(any(BatchCompletedEvent.class));
    terminalOrder.verify(eventPublisher).publish(any(JobFailedEvent.class));
    terminalOrder.verify(deadLetterService).recordDlqTransition(parent, null);
  }

  @Test
  void failedBatchRollbackSuppressesFailureAndDlqEvents() {
    when(txRegistry.getTransactionStatus()).thenReturn(Status.STATUS_ACTIVE);
    ArgumentCaptor<Synchronization> synchronizationCaptor =
        ArgumentCaptor.forClass(Synchronization.class);
    UUID parentId = UUID.randomUUID();
    JobEntity child = new JobEntity();
    child.setDependsOn(parentId);
    JobEntity parent = parent(parentId);

    when(batchStore.incrementFailedAtomic(parentId))
        .thenReturn(new BatchProgress(parentId, 1, 0, 1, null));
    when(batchStore.findBatchById(parentId)).thenReturn(Optional.of(batch(parentId, 1, 0, 1)));
    when(jobCrudStore.findById(parentId)).thenReturn(Optional.of(parent));
    when(jobTerminalStore.commitCompletion(
            ArgumentMatchers.argThat(
                plan ->
                    plan != null
                        && parentId.equals(plan.jobId())
                        && plan.terminalStatus() == JobStatus.FAILED)))
        .thenReturn(new JobCompletionResult(true, null));
    when(batchStore.findBatchMetrics(parentId)).thenReturn(Optional.empty());

    batchService.markChildFailed(child);

    verify(txRegistry).registerInterposedSynchronization(synchronizationCaptor.capture());
    synchronizationCaptor.getValue().afterCompletion(Status.STATUS_ROLLEDBACK);

    verify(eventPublisher, never()).publish(any());
    verify(deadLetterService, never()).recordDlqTransition(any(), any());
  }

  @Test
  void completedBatchUsesFreshBatchSnapshotAfterReadinessRace() {
    UUID parentId = UUID.randomUUID();
    JobEntity child = new JobEntity();
    child.setDependsOn(parentId);

    BatchProgress staleSuccessProgress = new BatchProgress(parentId, 3, 2, 0, null);
    BatchEntity freshCompletedBatch = batch(parentId, 3, 2, 1);
    JobEntity parent = parent(parentId);

    when(batchStore.incrementCompletedAtomic(parentId)).thenReturn(staleSuccessProgress);
    when(batchStore.findBatchById(parentId)).thenReturn(Optional.of(freshCompletedBatch));
    when(jobCrudStore.findById(parentId)).thenReturn(Optional.of(parent));
    when(jobTerminalStore.commitCompletion(
            ArgumentMatchers.argThat(
                plan ->
                    plan != null
                        && parentId.equals(plan.jobId())
                        && plan.terminalStatus() == JobStatus.FAILED)))
        .thenReturn(new JobCompletionResult(true, null));
    when(batchStore.findBatchMetrics(parentId)).thenReturn(Optional.empty());

    batchService.markChildSucceeded(child);

    assertEquals(JobStatus.FAILED, parent.getStatus());
    verify(jobTerminalStore, never()).markJobSucceededMinimal(any(), any(), any(), any(), any());
    verify(jobTerminalStore)
        .commitCompletion(
            ArgumentMatchers.argThat(
                plan ->
                    plan != null
                        && parentId.equals(plan.jobId())
                        && plan.terminalStatus() == JobStatus.FAILED));

    ArgumentCaptor<Object> event = ArgumentCaptor.forClass(Object.class);
    verify(eventPublisher, times(3)).publish(event.capture());
    BatchCompletingEvent completingEvent =
        event.getAllValues().stream()
            .filter(BatchCompletingEvent.class::isInstance)
            .map(BatchCompletingEvent.class::cast)
            .findFirst()
            .orElseThrow();
    assertEquals(3, completingEvent.getTotalItems());
    assertEquals(2, completingEvent.getCompletedItems());
    assertEquals(1, completingEvent.getFailedItems());
    verify(deadLetterService).recordDlqTransition(parent, null);
  }

  @Test
  void completedBatchDoesNotFinalizeOrPublishWhenSyntheticPickupLosesRace() {
    when(jobTerminalStore.commitCompletion(any())).thenReturn(JobCompletionResult.notCommitted());
    UUID parentId = UUID.randomUUID();
    JobEntity child = new JobEntity();
    child.setDependsOn(parentId);

    BatchProgress progress = new BatchProgress(parentId, 1, 1, 0, null);
    JobEntity parent = new JobEntity();
    parent.setId(parentId);
    parent.setStatus(JobStatus.PENDING);
    parent.setJobType(JobExecutionType.BATCH_PARENT);

    when(batchStore.incrementCompletedAtomic(parentId)).thenReturn(progress);
    when(batchStore.findBatchById(parentId)).thenReturn(Optional.of(batch(parentId, 1, 1, 0)));
    when(jobCrudStore.findById(parentId)).thenReturn(Optional.of(parent));

    batchService.markChildSucceeded(child);

    verify(jobTerminalStore, never()).markJobSucceededMinimal(any(), any(), any(), any(), any());
    verify(jobTerminalStore, never()).markJobFailedTerminal(any(), any(), anyInt(), isNull());
    verify(batchStore, never()).finalizeBatchMetrics(any());
    verify(eventPublisher, never()).publish(any());
    verify(deadLetterService, never()).recordDlqTransition(any(), any());
    verify(workflowScheduler, never()).scheduleNext(any());
    verify(batchStore).findBatchById(parentId);
  }

  @Test
  void completedBatchResetsSyntheticPickupWhenTerminalTransitionDoesNotApply() {
    when(jobTerminalStore.commitCompletion(any())).thenReturn(JobCompletionResult.notCommitted());
    UUID parentId = UUID.randomUUID();
    JobEntity child = new JobEntity();
    child.setDependsOn(parentId);

    BatchProgress progress = new BatchProgress(parentId, 1, 1, 0, null);
    JobEntity parent = new JobEntity();
    parent.setId(parentId);
    parent.setStatus(JobStatus.PENDING);
    parent.setJobType(JobExecutionType.BATCH_PARENT);

    when(batchStore.incrementCompletedAtomic(parentId)).thenReturn(progress);
    when(batchStore.findBatchById(parentId)).thenReturn(Optional.of(batch(parentId, 1, 1, 0)));
    when(jobCrudStore.findById(parentId)).thenReturn(Optional.of(parent));
    when(jobTerminalStore.commitCompletion(
            ArgumentMatchers.argThat(
                plan ->
                    plan != null
                        && parentId.equals(plan.jobId())
                        && plan.terminalStatus() == JobStatus.SUCCEEDED)))
        .thenReturn(new JobCompletionResult(false, null));

    batchService.markChildSucceeded(child);

    assertEquals(JobStatus.PENDING, parent.getStatus());
    verify(jobBatchStatusStore, never())
        .resetRunningJob(parentId, DefaultBatchBuilder.BATCH_LIFECYCLE_NODE_ID, 0L);
    verify(batchStore, never()).finalizeBatchMetrics(any());
    verify(eventPublisher, never()).publish(any());
    verify(workflowScheduler, never()).scheduleNext(any());
  }

  @Test
  void recoverStuckBatchesBulkLoadsCompletedParents() {
    UUID firstId = new UUID(0L, 101L);
    UUID secondId = new UUID(0L, 102L);
    BatchEntity firstBatch = batch(firstId, 2, 2, 0);
    BatchEntity secondBatch = batch(secondId, 3, 3, 0);
    JobEntity firstParent = parent(firstId);
    JobEntity secondParent = parent(secondId);

    when(batchStore.findRecoverableBatchIds(100)).thenReturn(List.of(firstId, secondId));
    when(batchStore.findBatchesByIds(List.of(firstId, secondId)))
        .thenReturn(List.of(firstBatch, secondBatch));
    when(jobCrudStore.findByIds(List.of(firstId, secondId)))
        .thenReturn(List.of(firstParent, secondParent));
    when(jobTerminalStore.commitCompletion(
            ArgumentMatchers.argThat(
                plan ->
                    plan != null
                        && firstId.equals(plan.jobId())
                        && plan.terminalStatus() == JobStatus.SUCCEEDED)))
        .thenReturn(new JobCompletionResult(true, null));
    when(jobTerminalStore.commitCompletion(
            ArgumentMatchers.argThat(
                plan ->
                    plan != null
                        && secondId.equals(plan.jobId())
                        && plan.terminalStatus() == JobStatus.SUCCEEDED)))
        .thenReturn(new JobCompletionResult(true, null));

    assertEquals(2, batchService.recoverStuckBatches());

    verify(jobCrudStore).findByIds(List.of(firstId, secondId));
    verify(jobCrudStore, never()).findById(firstId);
    verify(jobCrudStore, never()).findById(secondId);
    verify(batchCompletionTransaction, times(2)).complete(any());
  }

  @Test
  void recoverStuckBatchesContinuesAfterOneBatchCompletionFails() {
    UUID failingId = new UUID(0L, 201L);
    UUID succeedingId = new UUID(0L, 202L);
    BatchEntity failingBatch = batch(failingId, 1, 1, 0);
    BatchEntity succeedingBatch = batch(succeedingId, 1, 1, 0);
    JobEntity failingParent = parent(failingId);
    JobEntity succeedingParent = parent(succeedingId);

    when(batchStore.findRecoverableBatchIds(100)).thenReturn(List.of(failingId, succeedingId));
    when(batchStore.findBatchesByIds(List.of(failingId, succeedingId)))
        .thenReturn(List.of(failingBatch, succeedingBatch));
    when(jobCrudStore.findByIds(List.of(failingId, succeedingId)))
        .thenReturn(List.of(failingParent, succeedingParent));
    when(jobTerminalStore.commitCompletion(
            ArgumentMatchers.argThat(
                plan ->
                    plan != null
                        && failingId.equals(plan.jobId())
                        && plan.terminalStatus() == JobStatus.SUCCEEDED)))
        .thenThrow(new IllegalStateException("store down"));
    when(jobTerminalStore.commitCompletion(
            ArgumentMatchers.argThat(
                plan ->
                    plan != null
                        && succeedingId.equals(plan.jobId())
                        && plan.terminalStatus() == JobStatus.SUCCEEDED)))
        .thenReturn(new JobCompletionResult(true, null));

    assertEquals(1, batchService.recoverStuckBatches());

    verify(jobTerminalStore)
        .commitCompletion(
            ArgumentMatchers.argThat(
                plan ->
                    plan != null
                        && failingId.equals(plan.jobId())
                        && plan.terminalStatus() == JobStatus.SUCCEEDED));
    verify(jobTerminalStore)
        .commitCompletion(
            ArgumentMatchers.argThat(
                plan ->
                    plan != null
                        && succeedingId.equals(plan.jobId())
                        && plan.terminalStatus() == JobStatus.SUCCEEDED));
  }

  private static BatchEntity batch(UUID id, int total, int completed, int failed) {
    BatchEntity batch = new BatchEntity();
    batch.setId(id);
    batch.setTotalItems(total);
    batch.setCompletedItems(completed);
    batch.setFailedItems(failed);
    return batch;
  }

  private static JobEntity parent(UUID id) {
    JobEntity parent = new JobEntity();
    parent.setId(id);
    parent.setStatus(JobStatus.PENDING);
    parent.setJobType(JobExecutionType.BATCH_PARENT);
    parent.setPriority(JobPriority.NORMAL);
    return parent;
  }
}
