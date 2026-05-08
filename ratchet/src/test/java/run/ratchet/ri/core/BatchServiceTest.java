package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
            beanResolver);
  }

  @Test
  void completedBatchEventUsesParentMetadata() {
    UUID parentId = UUID.randomUUID();
    JobEntity child = new JobEntity();
    child.setDependsOn(parentId);

    BatchProgress progress = new BatchProgress(parentId, 3, 3, 0, null);
    BatchEntity batch = new BatchEntity();
    batch.setId(parentId);
    batch.setTotalItems(3);
    batch.setCompletedItems(3);
    batch.setFailedItems(0);

    JobEntity parent = new JobEntity();
    parent.setId(parentId);
    parent.setStatus(JobStatus.PENDING);
    parent.setJobType(JobExecutionType.BATCH_PARENT);
    parent.setBusinessKey("invoice-run");
    parent.setPriority(JobPriority.HIGH);
    parent.setPickedBy("batch-node-1");

    when(batchStore.incrementCompletedAtomic(parentId)).thenReturn(progress);
    when(batchStore.markBatchCompleteIfReady(parentId)).thenReturn(true);
    when(batchStore.findBatchById(parentId)).thenReturn(Optional.of(batch));
    when(jobCrudStore.findById(parentId)).thenReturn(Optional.of(parent));
    when(jobBatchStatusStore.tryPickUpJob(parentId, DefaultBatchBuilder.BATCH_LIFECYCLE_NODE_ID))
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
  }

  @Test
  void completedBatchDoesNotFinalizeOrPublishWhenSyntheticPickupLosesRace() {
    UUID parentId = UUID.randomUUID();
    JobEntity child = new JobEntity();
    child.setDependsOn(parentId);

    BatchProgress progress = new BatchProgress(parentId, 1, 1, 0, null);
    BatchEntity batch = new BatchEntity();
    batch.setId(parentId);
    batch.setTotalItems(1);
    batch.setCompletedItems(1);
    batch.setFailedItems(0);

    JobEntity parent = new JobEntity();
    parent.setId(parentId);
    parent.setStatus(JobStatus.PENDING);
    parent.setJobType(JobExecutionType.BATCH_PARENT);

    when(batchStore.incrementCompletedAtomic(parentId)).thenReturn(progress);
    when(batchStore.markBatchCompleteIfReady(parentId)).thenReturn(true);
    when(batchStore.findBatchById(parentId)).thenReturn(Optional.of(batch));
    when(jobCrudStore.findById(parentId)).thenReturn(Optional.of(parent));
    when(jobBatchStatusStore.tryPickUpJob(parentId, DefaultBatchBuilder.BATCH_LIFECYCLE_NODE_ID))
        .thenReturn(false);

    batchService.markChildSucceeded(child);

    verify(jobTerminalStore, never()).markJobSucceededMinimal(any(), any(), any(), any(), any());
    verify(jobTerminalStore, never()).markJobFailedTerminal(any(), any(), anyInt());
    verify(metricsStore, never()).finalizeBatchMetrics(any());
    verify(eventPublisher, never()).publish(any());
    verify(workflowScheduler, never()).scheduleNext(any());
  }
}
