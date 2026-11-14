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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import run.ratchet.api.JobStatus;
import run.ratchet.api.KeyRange;
import run.ratchet.api.exception.DuplicateIdempotencyKeyException;
import run.ratchet.api.exception.JobAuthorizationException;
import run.ratchet.ri.core.internal.JakartaAfterCommitRegistrar;
import run.ratchet.ri.core.internal.JobWakeupService;
import run.ratchet.ri.payload.DefaultJobInvocationResolver;
import run.ratchet.ri.security.DefaultErrorSanitizer;
import run.ratchet.ri.security.JobPayloadInputValidator;
import run.ratchet.spi.JobAuthorizationPolicy;
import run.ratchet.store.dto.PartitionGenerationWrite;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.PartitionEntity;
import run.ratchet.store.entity.PartitionOutcome;
import run.ratchet.store.entity.PartitionSetEntity;
import run.ratchet.store.entity.WorkflowConditionEntity;
import run.ratchet.store.spi.BatchStore;
import run.ratchet.store.spi.JobBulkStore;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.PartitionStore;
import run.ratchet.store.spi.TagStore;
import run.ratchet.store.spi.WorkflowConditionStore;

class PartitionSubmissionTest {
  private final PartitionStore partitions = mock(PartitionStore.class);
  private final JobCrudStore jobs = mock(JobCrudStore.class);
  private final JobBulkStore bulk = mock(JobBulkStore.class);
  private final JobAuthorizationPolicy authorization = mock(JobAuthorizationPolicy.class);
  private final PartitionService partitionService = mock(PartitionService.class);
  private final List<KeyRange> ranges = List.of(new KeyRange(0, 10), new KeyRange(10, 20));
  private WorkflowConditionStore conditions = mock(WorkflowConditionStore.class);
  private final BatchService batchService = mock(BatchService.class);
  private final BatchCompletionTransaction conflicts = mock(BatchCompletionTransaction.class);
  private DefaultJobCreationService service;

  public static void process(KeyRange range) {}

  public static void callback() {}

  @BeforeEach
  void setup() {
    service =
        new DefaultJobCreationService(
            batchService,
            jobs,
            bulk,
            mock(BatchStore.class),
            mock(TagStore.class),
            conditions,
            null,
            mock(JobWakeupService.class),
            null,
            new DefaultJobInvocationResolver(),
            new JobPayloadInputValidator(),
            null,
            null,
            authorization,
            null,
            null,
            null,
            Clock.fixed(Instant.parse("2026-11-11T12:00:00Z"), ZoneOffset.UTC),
            true,
            true,
            null,
            new JakartaAfterCommitRegistrar(),
            new DefaultErrorSanitizer(),
            conflicts);
    when(partitionService.store()).thenReturn(partitions);
    when(partitionService.maxPartitions()).thenReturn(10000);
    service.configurePartitions(partitionService);
    when(partitionService.sealForSubmission(any()))
        .thenAnswer(
            call -> new PartitionService.SubmissionSnapshot(call.getArgument(0), List.of()));
    when(partitions.createOrAdvance(any())).thenReturn(true);
  }

  @Test
  void parentKeyRace_resolvesOriginalWithoutBranchesOrChildren() {
    UUID original = UUID.randomUUID();
    var conflict = new DuplicateIdempotencyKeyException("partition-race");
    when(jobs.findOriginalJobIdByIdempotencyKey("partition-race"))
        .thenReturn(Optional.empty(), Optional.of(original));
    when(conflicts.complete(any())).thenAnswer(call -> call.<Supplier<?>>getArgument(0).get());
    when(partitions.createOrAdvance(any())).thenThrow(conflict);
    var builder = builder();
    builder.withIdempotencyKey("partition-race");
    builder.thenOnBatchSuccess(PartitionSubmissionTest::callback);

    assertEquals(original, builder.submit().id());

    verify(conflicts).complete(any());
    verifyNoInteractions(bulk, conditions, batchService);
    verify(jobs, never()).create(any());
  }

  @Test
  void parentKeyRace_absentOriginalRethrowsWithoutBranchesOrChildren() {
    var conflict = new DuplicateIdempotencyKeyException("partition-race");
    when(conflicts.complete(any())).thenAnswer(call -> call.<Supplier<?>>getArgument(0).get());
    when(partitions.createOrAdvance(any())).thenThrow(conflict);
    var builder = builder();
    builder.withIdempotencyKey("partition-race");
    builder.thenOnBatchSuccess(PartitionSubmissionTest::callback);

    assertSame(conflict, assertThrows(DuplicateIdempotencyKeyException.class, builder::submit));

    verify(conflicts).complete(any());
    verifyNoInteractions(bulk, conditions, batchService);
    verify(jobs, never()).create(any());
  }

  private DefaultBatchBuilder builder() {
    var builder = new DefaultBatchBuilder("partitions", service);
    builder.forEachPartition("set", ranges, PartitionSubmissionTest::process);
    return builder;
  }

  private PartitionSetEntity existing(boolean sealed) {
    var set = new PartitionSetEntity();
    set.setSetId(UUID.randomUUID());
    set.setSetKey("set");
    set.setName("partitions");
    set.setPartitionCount(2);
    set.setLatestGeneration(1);
    set.setSealedGeneration(sealed ? 1 : 0);
    set.setLatestBatchId(UUID.randomUUID());
    set.setOwnerPrincipal("owner");
    set.setVersion(5);
    when(partitions.findPartitionSet("set")).thenReturn(Optional.of(set));
    List<PartitionEntity> rows = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      var row = new PartitionEntity();
      row.setSetId(set.getSetId());
      row.setPartitionIndex(i);
      row.setRangeFrom(ranges.get(i).from());
      row.setRangeTo(ranges.get(i).to());
      row.setGeneration(1);
      row.setChildJobId(UUID.randomUUID());
      row.setCheckpoint("seed-" + i);
      row.setOutcome(i == 0 ? PartitionOutcome.SUCCEEDED : PartitionOutcome.FAILED);
      rows.add(row);
    }
    when(partitions.findPartitions(set.getSetId(), -1, 500)).thenReturn(rows);
    return set;
  }

  private PartitionGenerationWrite submitted() {
    var capture = ArgumentCaptor.forClass(PartitionGenerationWrite.class);
    verify(partitions).createOrAdvance(capture.capture());
    return capture.getValue();
  }

  @Test
  void deniedChildDoesNotAdvanceOrInsertJobs() {
    doNothing()
        .doThrow(new JobAuthorizationException(null, "create", null, "denied"))
        .when(authorization)
        .checkCreate(any(), isNull());
    assertThrows(JobAuthorizationException.class, () -> builder().submit());
    verify(partitions, never()).createOrAdvance(any());
    verify(jobs, never()).create(any());
    verifyNoInteractions(bulk, conditions);
  }

  @Test
  void deniedCallbackDoesNotAdvanceOrInsertJobs() {
    doNothing()
        .doNothing()
        .doNothing()
        .doThrow(new JobAuthorizationException(null, "create", null, "denied"))
        .when(authorization)
        .checkCreate(any(), isNull());
    var builder = builder();
    builder.thenOnBatchSuccess(PartitionSubmissionTest::callback);
    assertThrows(JobAuthorizationException.class, builder::submit);
    verify(partitions, never()).createOrAdvance(any());
    verify(jobs, never()).create(any());
    verifyNoInteractions(bulk, conditions);
  }

  @Test
  void preparedCallbackKeepsItsIdAndIsPersistedBeforeChildren() {
    var builder = builder();
    builder.thenOnBatchSuccess(PartitionSubmissionTest::callback);
    UUID parentId = builder.submit().id();
    var branch = ArgumentCaptor.forClass(JobEntity.class);
    var condition = ArgumentCaptor.forClass(WorkflowConditionEntity.class);
    var order = inOrder(partitions, bulk, jobs, conditions);
    order.verify(partitions).createOrAdvance(any());
    order.verify(jobs).create(branch.capture());
    order.verify(conditions).saveCondition(condition.capture());
    order.verify(bulk).bulkInsert(anyList());
    assertNotNull(branch.getValue().getId());
    assertEquals(parentId, condition.getValue().getParentJobId());
    assertEquals(branch.getValue().getId(), condition.getValue().getChildJobId());
    verify(authorization).checkCreate(branch.getValue().getId(), null);
  }

  @Test
  void canceledParentCascadesAfterAllBranchesAndStillReturnsHandle() {
    when(jobs.getJobStatus(any())).thenReturn(JobStatus.CANCELED);
    var builder = builder();
    builder.thenOnBatchSuccess(PartitionSubmissionTest::callback);
    builder.thenOnBatchFailure(PartitionSubmissionTest::callback);
    UUID parentId = builder.submit().id();
    var write = submitted();
    assertEquals(write.parent().getId(), parentId);
    var order = inOrder(partitions, bulk, jobs, conditions, batchService);
    order.verify(partitions).createOrAdvance(any());
    order.verify(jobs).create(any());
    order.verify(conditions).saveCondition(any());
    order.verify(jobs).create(any());
    order.verify(conditions).saveCondition(any());
    order.verify(bulk).bulkInsert(anyList());
    order.verify(jobs).getJobStatus(parentId);
    order.verify(batchService).cancelBatch(write.parent());
    verify(batchService, times(1)).cancelBatch(any());
  }

  @Test
  void pendingParentDoesNotCascade() {
    when(jobs.getJobStatus(any())).thenReturn(JobStatus.PENDING);
    var builder = builder();
    builder.thenOnBatchSuccess(PartitionSubmissionTest::callback);
    UUID parentId = builder.submit().id();
    verify(jobs).getJobStatus(parentId);
    verifyNoInteractions(batchService);
  }

  @Test
  void missingCallbackCapabilityDoesNotAdvanceOrInsertJobs() {
    conditions = null;
    setup();
    var builder = builder();
    builder.thenOnBatchSuccess(PartitionSubmissionTest::callback);
    assertThrows(UnsupportedOperationException.class, builder::submit);
    verify(partitions, never()).createOrAdvance(any());
    verify(jobs, never()).create(any());
    verifyNoInteractions(bulk);
  }

  @Test
  void lazySealReloadsOutcomesBeforeSelectingTargets() {
    var set = existing(false);
    var postSeal = partitions.findPartitions(set.getSetId(), -1, 500);
    List<PartitionEntity> preSeal = new ArrayList<>();
    for (PartitionEntity row : postSeal) {
      PartitionEntity before = new PartitionEntity();
      before.setPartitionIndex(row.getPartitionIndex());
      before.setRangeFrom(row.getRangeFrom());
      before.setRangeTo(row.getRangeTo());
      before.setOutcome(PartitionOutcome.SUCCEEDED);
      preSeal.add(before);
    }
    when(partitions.findPartitions(set.getSetId(), -1, 500)).thenReturn(preSeal, postSeal);
    doAnswer(
            ignored -> {
              set.setSealedGeneration(1);
              return new PartitionService.SubmissionSnapshot(set, postSeal);
            })
        .when(partitionService)
        .sealForSubmission(set);
    builder().submit();
    var write = submitted();
    assertEquals(2, write.set().getLatestGeneration());
    assertEquals(1, write.rows().size());
    assertEquals(1, write.rows().get(0).getPartitionIndex());
    verify(partitionService).sealForSubmission(set);
  }

  @Test
  void allSucceededReturnsLatestWithoutWriting() {
    var set = existing(true);
    var rows = partitions.findPartitions(set.getSetId(), -1, 500);
    rows.forEach(row -> row.setOutcome(PartitionOutcome.SUCCEEDED));
    assertEquals(set.getLatestBatchId(), builder().submit().id());
    verify(partitions, never()).createOrAdvance(any());
    verifyNoInteractions(bulk);
  }

  @Test
  void configuredMaximumIsEnforcedBeforeWriting() {
    when(partitionService.maxPartitions()).thenReturn(1);
    assertThrows(IllegalArgumentException.class, () -> builder().submit());
    verify(partitions, never()).createOrAdvance(any());
  }

  @Test
  void firstGenerationUsesPresetAuthorizedIds() {
    UUID id = builder().submit().id();
    var write = submitted();
    assertEquals(id, write.parent().getId());
    assertEquals(id, write.set().getLatestBatchId());
    assertEquals(write.set().getSetId(), write.batch().getPartitionSetId());
    assertEquals(1, write.batch().getGeneration());
    assertEquals(2, write.batch().getTotalItems());
    assertNull(write.expectedVersion());
    assertEquals(2, write.rows().size());
    ArgumentCaptor<List<JobEntity>> children = ArgumentCaptor.forClass(List.class);
    verify(bulk).bulkInsert(children.capture());
    for (int i = 0; i < 2; i++) {
      UUID childId = write.rows().get(i).getChildJobId();
      assertEquals(childId, children.getValue().get(i).getId());
      verify(authorization).checkCreate(childId, null);
      assertNull(write.rows().get(i).getCheckpoint());
    }
    verify(authorization).checkCreate(id, null);
    verify(jobs, never()).create(any());
  }

  @Test
  void dedupPrecedesPartitionAccess() {
    UUID id = UUID.randomUUID();
    when(jobs.findOriginalJobIdByIdempotencyKey("request")).thenReturn(Optional.of(id));
    var builder = builder();
    builder.withIdempotencyKey("request");
    assertEquals(id, builder.submit().id());
    verify(partitionService).maxPartitions();
    verifyNoMoreInteractions(partitionService);
    verifyNoInteractions(partitions, bulk);
  }

  @Test
  void resubmitTargetsUnfinishedAndReopenedClearingOnlyReopenedSeeds() {
    existing(true);
    var builder = builder();
    builder.withIdempotencyKey("request");
    builder.reopen(List.of(0));
    builder.submit();
    var write = submitted();
    assertEquals(5, write.expectedVersion());
    assertEquals(2, write.set().getLatestGeneration());
    assertEquals(2, write.rows().size());
    assertNull(write.rows().get(0).getCheckpoint());
    assertEquals("seed-1", write.rows().get(1).getCheckpoint());
    verify(authorization).checkRetry(any(), eq("owner"), isNull());
  }

  @Test
  void deniedCallerIsRejectedBeforeRangesAreCompared() {
    existing(true);
    doThrow(new JobAuthorizationException(null, "retry", null, "denied"))
        .when(authorization)
        .checkRetry(any(), eq("owner"), isNull());
    var builder = new DefaultBatchBuilder("partitions", service);
    builder.forEachPartition("set", List.of(new KeyRange(0, 99)), PartitionSubmissionTest::process);
    assertThrows(JobAuthorizationException.class, builder::submit);
    verify(partitions, never()).findPartitions(any(), anyInt(), anyInt());
  }

  @Test
  void resubmitTargetsOnlyFailed() {
    existing(true);
    builder().submit();
    var write = submitted();
    assertEquals(1, write.rows().size());
    assertEquals(1, write.rows().get(0).getPartitionIndex());
    assertEquals("seed-1", write.rows().get(0).getCheckpoint());
  }

  @Test
  void openGenerationReturnsExistingAndRejectsReopen() {
    var set = existing(false);
    assertEquals(set.getLatestBatchId(), builder().submit().id());
    verify(partitions, never()).createOrAdvance(any());
    verifyNoInteractions(bulk);
    var builder = builder();
    builder.withIdempotencyKey("request");
    builder.reopen(List.of(0));
    assertThrows(IllegalStateException.class, builder::submit);
  }

  @Test
  void rejectsReopenWithoutRequestKeyAndNewSetReopen() {
    var builder = builder();
    builder.reopen(List.of(0));
    assertThrows(IllegalStateException.class, builder::submit);
    builder.withIdempotencyKey("request");
    assertThrows(IllegalArgumentException.class, builder::submit);
    verify(partitions, never()).createOrAdvance(any());
  }

  @Test
  void rejectsChangedRangesAndConcurrentAdvanceWithoutChildren() {
    var set = existing(true);
    when(partitions.findPartitions(set.getSetId(), -1, 500)).thenReturn(List.of());
    assertThrows(IllegalArgumentException.class, () -> builder().submit());
    verifyNoInteractions(bulk);
    when(partitions.findPartitionSet("set")).thenReturn(Optional.empty());
    when(partitions.createOrAdvance(any())).thenReturn(false);
    assertThrows(IllegalStateException.class, () -> builder().submit());
    verifyNoInteractions(bulk);
  }

  @Test
  void absentCapabilityThrowsAndInvalidNamesNeverWrite() {
    service.configurePartitions(null);
    assertThrows(UnsupportedOperationException.class, () -> builder().submit());
    service.configurePartitions(partitionService);
    var builder = new DefaultBatchBuilder("x".repeat(256), service);
    builder.forEachPartition("set", ranges, PartitionSubmissionTest::process);
    assertThrows(IllegalArgumentException.class, builder::submit);
    verify(partitions, never()).createOrAdvance(any());
  }

  @Test
  void statusReadFailureKeepsCompleteGeneration() {
    IllegalStateException failure = new IllegalStateException("status read");
    when(jobs.getJobStatus(any())).thenThrow(failure);

    assertSame(failure, assertThrows(IllegalStateException.class, () -> builder().submit()));

    var write = submitted();
    ArgumentCaptor<List<JobEntity>> children = ArgumentCaptor.forClass(List.class);
    verify(bulk).bulkInsert(children.capture());
    assertEquals(write.rows().size(), children.getValue().size());
    assertEquals(2, children.getValue().size());
    verify(jobs).getJobStatus(write.parent().getId());
    verify(partitionService, never()).cancelGeneration(any());
    verifyNoInteractions(batchService);
  }

  @Test
  void childrenFailureCancelsGenerationAndPreservesOriginal() {
    RuntimeException failure = new IllegalStateException("children");
    doThrow(failure).when(bulk).bulkInsert(anyList());
    assertSame(failure, assertThrows(IllegalStateException.class, () -> builder().submit()));
    UUID parentId = submitted().parent().getId();
    verify(partitionService).cancelGeneration(parentId);
  }

  @Test
  void cancellationFailureIsSuppressed() {
    RuntimeException failure = new IllegalStateException("children");
    RuntimeException cancellation = new IllegalArgumentException("cancel");
    doThrow(failure).when(bulk).bulkInsert(anyList());
    doThrow(cancellation).when(partitionService).cancelGeneration(any());
    assertSame(failure, assertThrows(IllegalStateException.class, () -> builder().submit()));
    assertArrayEquals(new Throwable[] {cancellation}, failure.getSuppressed());
  }

  @Test
  void branchFailureCancelsGenerationBeforeChildren() {
    RuntimeException failure = new IllegalStateException("branch");
    doThrow(failure).when(jobs).create(any());
    var builder = builder();
    builder.thenOnBatchSuccess(PartitionSubmissionTest::callback);
    assertSame(failure, assertThrows(IllegalStateException.class, builder::submit));
    UUID parentId = submitted().parent().getId();
    verify(partitionService).cancelGeneration(parentId);
    verifyNoInteractions(bulk);
  }

  @Test
  void callbackAndConditionAreWrittenBeforeChildren() {
    when(jobs.create(any())).thenAnswer(call -> call.getArgument(0));
    var builder = builder();
    builder.thenOnBatchSuccess(PartitionSubmissionTest::callback);
    builder.submit();
    var order = inOrder(jobs, conditions, bulk);
    order.verify(jobs).create(any());
    order.verify(conditions).saveCondition(any());
    order.verify(bulk).bulkInsert(anyList());
  }

  @Test
  void excessiveLazyRangesAreRejectedBeforeReadingElements() {
    when(partitionService.maxPartitions()).thenReturn(2);
    var builder = new DefaultBatchBuilder("partitions", service);
    List<KeyRange> ranges =
        new AbstractList<>() {
          @Override
          public KeyRange get(int index) {
            throw new AssertionError("Over-limit input must not be read");
          }

          @Override
          public int size() {
            return 100_000_000;
          }
        };
    assertThrows(
        IllegalArgumentException.class,
        () -> builder.forEachPartition("set", ranges, PartitionSubmissionTest::process));
    verifyNoInteractions(bulk, partitions);
  }
}
