package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobStatus;
import run.ratchet.api.JobType;
import run.ratchet.api.event.BatchCompletingEvent;
import run.ratchet.spi.BeanResolver;
import run.ratchet.spi.ClassPolicy;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.store.dto.BatchProgress;
import run.ratchet.store.entity.BatchEntity;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.spi.BatchMetricsStore;
import run.ratchet.store.spi.BatchStore;
import run.ratchet.store.spi.JobBatchStatusStore;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.JobTerminalStore;

@ExtendWith(MockitoExtension.class)
class BatchServiceTest {

  private static final Instant FIXED_NOW = Instant.parse("2026-05-12T12:00:00Z");
  private static final Clock FIXED_CLOCK = Clock.fixed(FIXED_NOW, ZoneOffset.UTC);

  @Mock private BatchStore batchStore;
  @Mock private JobCrudStore jobCrudStore;
  @Mock private JobBatchStatusStore jobBatchStatusStore;
  @Mock private JobTerminalStore jobTerminalStore;
  @Mock private BatchMetricsStore metricsStore;
  @Mock private MetricsCollector metricsCollector;
  @Mock private InternalEventPublisher eventPublisher;
  @Mock private WorkflowScheduler workflowScheduler;
  @Mock private ClassPolicy classPolicy;
  @Mock private BeanResolver beanResolver;

  private BatchService batchService;

  @BeforeEach
  void setUp() {
    batchService =
        new BatchService(
            batchStore,
            jobCrudStore,
            jobBatchStatusStore,
            jobTerminalStore,
            metricsStore,
            metricsCollector,
            eventPublisher,
            workflowScheduler,
            classPolicy,
            beanResolver,
            FIXED_CLOCK);
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
    when(batchStore.markBatchCompleteIfReady(parentId)).thenReturn(true);
    when(jobCrudStore.findById(parentId)).thenReturn(Optional.of(parent));
    when(jobBatchStatusStore.tryPickUpJob(parentId, DefaultBatchBuilder.BATCH_LIFECYCLE_NODE_ID))
        .thenReturn(true);
    when(jobTerminalStore.markJobSucceededMinimal(parentId, FIXED_NOW, FIXED_NOW, 0L, 0L))
        .thenReturn(true);
    when(metricsStore.findBatchMetrics(parentId)).thenReturn(Optional.empty());
    when(workflowScheduler.scheduleNext(any(JobEntity.class))).thenReturn(false);

    batchService.markChildSucceeded(child);

    ArgumentCaptor<Object> event = ArgumentCaptor.forClass(Object.class);
    verify(eventPublisher).publish(event.capture());
    BatchCompletingEvent completingEvent = (BatchCompletingEvent) event.getValue();
    assertEquals(parentId, completingEvent.getJobId());
    assertEquals("invoice-run", completingEvent.getBusinessKey());
    assertEquals(JobType.BATCH, completingEvent.getJobType());
    assertEquals(JobPriority.HIGH, completingEvent.getPriority());
    assertEquals("batch-node-1", completingEvent.getNodeId());
    assertEquals(3, completingEvent.getTotalItems());
    assertEquals(3, completingEvent.getCompletedItems());
    assertEquals(0, completingEvent.getFailedItems());
    verify(batchStore, never()).findBatchById(parentId);
    verify(jobTerminalStore).markJobSucceededMinimal(parentId, FIXED_NOW, FIXED_NOW, 0L, 0L);
  }

  @Test
  void completedBatchDoesNotFinalizeOrPublishWhenSyntheticPickupLosesRace() {
    UUID parentId = UUID.randomUUID();
    JobEntity child = new JobEntity();
    child.setDependsOn(parentId);

    BatchProgress progress = new BatchProgress(parentId, 1, 1, 0, null);
    JobEntity parent = new JobEntity();
    parent.setId(parentId);
    parent.setStatus(JobStatus.PENDING);
    parent.setJobType(JobExecutionType.BATCH_PARENT);

    when(batchStore.incrementCompletedAtomic(parentId)).thenReturn(progress);
    when(batchStore.markBatchCompleteIfReady(parentId)).thenReturn(true);
    when(jobCrudStore.findById(parentId)).thenReturn(Optional.of(parent));
    when(jobBatchStatusStore.tryPickUpJob(parentId, DefaultBatchBuilder.BATCH_LIFECYCLE_NODE_ID))
        .thenReturn(false);

    batchService.markChildSucceeded(child);

    verify(jobTerminalStore, never()).markJobSucceededMinimal(any(), any(), any(), any(), any());
    verify(jobTerminalStore, never()).markJobFailedTerminal(any(), any(), anyInt());
    verify(metricsStore, never()).finalizeBatchMetrics(any());
    verify(eventPublisher, never()).publish(any());
    verify(workflowScheduler, never()).scheduleNext(any());
    verify(batchStore, never()).findBatchById(parentId);
  }

  @Test
  void completedBatchResetsSyntheticPickupWhenTerminalTransitionDoesNotApply() {
    UUID parentId = UUID.randomUUID();
    JobEntity child = new JobEntity();
    child.setDependsOn(parentId);

    BatchProgress progress = new BatchProgress(parentId, 1, 1, 0, null);
    JobEntity parent = new JobEntity();
    parent.setId(parentId);
    parent.setStatus(JobStatus.PENDING);
    parent.setJobType(JobExecutionType.BATCH_PARENT);

    when(batchStore.incrementCompletedAtomic(parentId)).thenReturn(progress);
    when(batchStore.markBatchCompleteIfReady(parentId)).thenReturn(true);
    when(jobCrudStore.findById(parentId)).thenReturn(Optional.of(parent));
    when(jobBatchStatusStore.tryPickUpJob(parentId, DefaultBatchBuilder.BATCH_LIFECYCLE_NODE_ID))
        .thenReturn(true);
    when(jobTerminalStore.markJobSucceededMinimal(parentId, FIXED_NOW, FIXED_NOW, 0L, 0L))
        .thenReturn(false);
    when(jobBatchStatusStore.resetRunningJob(parentId, DefaultBatchBuilder.BATCH_LIFECYCLE_NODE_ID))
        .thenReturn(true);

    batchService.markChildSucceeded(child);

    assertEquals(JobStatus.PENDING, parent.getStatus());
    verify(jobBatchStatusStore)
        .resetRunningJob(parentId, DefaultBatchBuilder.BATCH_LIFECYCLE_NODE_ID);
    verify(metricsStore, never()).finalizeBatchMetrics(any());
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
    when(batchStore.markBatchCompleteIfReady(firstId)).thenReturn(true);
    when(batchStore.markBatchCompleteIfReady(secondId)).thenReturn(true);
    when(jobCrudStore.findByIds(List.of(firstId, secondId)))
        .thenReturn(List.of(firstParent, secondParent));
    when(jobBatchStatusStore.tryPickUpJob(eq(firstId), any())).thenReturn(true);
    when(jobBatchStatusStore.tryPickUpJob(eq(secondId), any())).thenReturn(true);
    when(jobTerminalStore.markJobSucceededMinimal(eq(firstId), any(), any(), eq(0L), eq(0L)))
        .thenReturn(true);
    when(jobTerminalStore.markJobSucceededMinimal(eq(secondId), any(), any(), eq(0L), eq(0L)))
        .thenReturn(true);

    assertEquals(2, batchService.recoverStuckBatches());

    verify(jobCrudStore).findByIds(List.of(firstId, secondId));
    verify(jobCrudStore, never()).findById(firstId);
    verify(jobCrudStore, never()).findById(secondId);
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
