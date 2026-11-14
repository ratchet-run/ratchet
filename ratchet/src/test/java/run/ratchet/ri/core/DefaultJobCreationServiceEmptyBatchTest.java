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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import run.ratchet.api.ExecutorTargets;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobPriority;
import run.ratchet.ri.core.internal.JobWakeupService;
import run.ratchet.ri.payload.DefaultJobInvocationResolver;
import run.ratchet.ri.security.JobPayloadInputValidator;
import run.ratchet.store.entity.BatchEntity;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.entity.WorkflowConditionEntity;
import run.ratchet.store.spi.BatchStore;
import run.ratchet.store.spi.JobStore;
import run.ratchet.store.spi.WorkflowConditionStore;

@ExtendWith(MockitoExtension.class)
class DefaultJobCreationServiceEmptyBatchTest {

  @Mock private JobStore store;
  @Mock private BatchStore batchStore;
  @Mock private WorkflowConditionStore conditionStore;
  @Mock private BatchService batchService;
  @Mock private JobWakeupService wakeupService;

  private DefaultJobCreationService service;

  public static void consumeString(String item) {}

  public static void callback() {}

  @BeforeEach
  void setUp() {
    service =
        new DefaultJobCreationService(
            batchService,
            store,
            store,
            batchStore,
            store,
            conditionStore,
            null,
            wakeupService,
            null,
            new DefaultJobInvocationResolver(),
            new JobPayloadInputValidator(),
            null,
            null,
            null,
            null,
            null,
            null,
            Clock.systemUTC(),
            true,
            null,
            null,
            null,
            null);
    when(store.create(any()))
        .thenAnswer(
            invocation -> {
              JobEntity job = invocation.getArgument(0);
              if (job.getId() == null) {
                job.setId(UUID.randomUUID());
              }
              return job;
            });
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void emptyBatchCreatesBranchesBeforeCompletionAndWakesOnlyForNewWork(boolean newWorkAvailable) {
    when(batchService.completeEmptyBatch(any(), any())).thenReturn(newWorkAvailable);
    DefaultBatchBuilder builder = new DefaultBatchBuilder("empty-batch", service);
    builder.forEach(List.<String>of(), DefaultJobCreationServiceEmptyBatchTest::consumeString);
    builder.virtual();
    builder.thenOnBatchSuccess(DefaultJobCreationServiceEmptyBatchTest::callback);

    JobHandle handle = service.submit(builder);

    verifyEmptySubmission(handle, false, newWorkAvailable);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void emptyStreamCreatesBranchesBeforeCompletionAndWakesOnlyForNewWork(boolean newWorkAvailable) {
    when(batchService.completeEmptyBatch(any(), any())).thenReturn(newWorkAvailable);
    DefaultStreamingBatchBuilder<String> builder =
        new DefaultStreamingBatchBuilder<>("empty-stream", service);
    builder.fromStream(Stream.empty());
    builder.process(DefaultJobCreationServiceEmptyBatchTest::consumeString);
    builder.virtual();
    builder.thenOnBatchSuccess(DefaultJobCreationServiceEmptyBatchTest::callback);

    JobHandle handle = service.submit(builder);

    verifyEmptySubmission(handle, true, newWorkAvailable);
  }

  private void verifyEmptySubmission(
      JobHandle handle, boolean streaming, boolean newWorkAvailable) {
    ArgumentCaptor<JobEntity> parent = ArgumentCaptor.forClass(JobEntity.class);
    ArgumentCaptor<JobEntity> branch = ArgumentCaptor.forClass(JobEntity.class);
    ArgumentCaptor<BatchEntity> batch = ArgumentCaptor.forClass(BatchEntity.class);
    ArgumentCaptor<WorkflowConditionEntity> condition =
        ArgumentCaptor.forClass(WorkflowConditionEntity.class);
    InOrder order = inOrder(store, batchStore, conditionStore, batchService, wakeupService);
    order.verify(store).create(parent.capture());
    order.verify(batchStore).saveBatch(batch.capture());
    order.verify(store).create(branch.capture());
    order.verify(conditionStore).saveCondition(condition.capture());
    if (streaming) {
      order.verify(batchStore).updateBatchTotalItems(handle.id(), 0);
    }
    order.verify(batchService).completeEmptyBatch(handle.id(), batch.getValue());

    assertEquals(JobExecutionType.BATCH_PARENT, parent.getValue().getJobType());
    assertEquals(handle.id(), parent.getValue().getId());
    assertEquals(ExecutorTargets.VIRTUAL, parent.getValue().getExecutionTarget());
    assertEquals(JobExecutionType.WORKFLOW_BRANCH, branch.getValue().getJobType());
    assertEquals(handle.id(), branch.getValue().getDependsOn());
    assertEquals(ExecutorTargets.VIRTUAL, branch.getValue().getExecutionTarget());
    assertEquals(handle.id(), condition.getValue().getParentJobId());
    assertEquals(branch.getValue().getId(), condition.getValue().getChildJobId());
    assertEquals(handle.id(), batch.getValue().getId());
    assertEquals(0, batch.getValue().getTotalItems());
    assertEquals(0, batch.getValue().getCompletedItems());
    assertEquals(0, batch.getValue().getFailedItems());
    verify(store, never()).bulkInsert(any());
    verify(store, never()).tryPickUpJob(any(), any());
    verify(store, never()).markJobSucceededMinimal(any(), any(), any(), any(), any());
    verify(batchStore, never()).markBatchCompleteIfReady(any());
    if (newWorkAvailable) {
      order.verify(wakeupService).notify(JobPriority.NORMAL, true, ExecutorTargets.VIRTUAL);
      verifyNoMoreInteractions(wakeupService);
    } else {
      verifyNoInteractions(wakeupService);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void callbacksPrecedeChildrenForPlainAndStreamingBatches(boolean streaming) {
    if (streaming) {
      DefaultStreamingBatchBuilder<String> builder =
          new DefaultStreamingBatchBuilder<>("stream", service);
      builder.fromStream(Stream.of("one"));
      builder.process(DefaultJobCreationServiceEmptyBatchTest::consumeString);
      builder.thenOnBatchSuccess(DefaultJobCreationServiceEmptyBatchTest::callback);
      service.submit(builder);
    } else {
      DefaultBatchBuilder builder = new DefaultBatchBuilder("plain", service);
      builder.forEach(List.of("one"), DefaultJobCreationServiceEmptyBatchTest::consumeString);
      builder.thenOnBatchSuccess(DefaultJobCreationServiceEmptyBatchTest::callback);
      service.submit(builder);
    }
    InOrder order = inOrder(store, conditionStore);
    order.verify(store, times(2)).create(any());
    order.verify(conditionStore).saveCondition(any());
    order.verify(store).bulkInsert(any());
  }
}
