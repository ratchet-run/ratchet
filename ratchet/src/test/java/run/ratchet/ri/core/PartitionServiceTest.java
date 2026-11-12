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
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobStatus;
import run.ratchet.api.PartitionStatus;
import run.ratchet.api.RatchetOptions;
import run.ratchet.api.exception.JobAuthorizationException;
import run.ratchet.api.exception.PayloadDecryptionException;
import run.ratchet.ri.core.internal.JobCheckpointWriter;
import run.ratchet.ri.core.internal.JobProgressWriter;
import run.ratchet.ri.testsupport.EncryptionTestKit;
import run.ratchet.spi.ErrorSanitizer;
import run.ratchet.spi.JobAuthorizationPolicy;
import run.ratchet.spi.PayloadSerializer;
import run.ratchet.store.converter.EncryptionHolder;
import run.ratchet.store.entity.BatchEntity;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.PartitionEntity;
import run.ratchet.store.entity.PartitionOutcome;
import run.ratchet.store.entity.PartitionSetEntity;
import run.ratchet.store.spi.BatchStore;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.JobStateStore;
import run.ratchet.store.spi.JobStateStore.VersionedState;
import run.ratchet.store.spi.PartitionStore;
import run.ratchet.store.util.EncryptionTarget;
import run.ratchet.store.util.PayloadEncryptor;

class PartitionServiceTest {
  private final PartitionStore store = mock(PartitionStore.class);
  private final JobCrudStore jobs = mock(JobCrudStore.class);
  private final JobStateStore states = mock(JobStateStore.class);
  private final BatchStore batches = mock(BatchStore.class);
  private final PayloadSerializer serializer = mock(PayloadSerializer.class);
  private final ErrorSanitizer sanitizer = mock(ErrorSanitizer.class);
  private final JobAuthorizationPolicy policy = mock(JobAuthorizationPolicy.class);
  private final Instant now = Instant.parse("2026-11-11T12:00:00Z");
  private final PartitionService service =
      new PartitionService(
          store,
          jobs,
          states,
          batches,
          sanitizer,
          serializer,
          Clock.fixed(now, ZoneOffset.UTC),
          RatchetOptions.builder().build(),
          new BatchCompletionTransaction(),
          policy,
          null);
  private PartitionSetEntity set;
  private JobEntity parent;

  @BeforeEach
  void setup() {
    set = new PartitionSetEntity();
    set.setSetId(UUID.randomUUID());
    set.setSetKey("set");
    set.setName("name");
    set.setPartitionCount(4);
    set.setLatestGeneration(1);
    set.setLatestBatchId(UUID.randomUUID());
    set.setGenerationStartedAt(now);
    parent = new JobEntity();
    parent.setId(set.getLatestBatchId());
    parent.setStatus(JobStatus.FAILED);
    when(jobs.findById(parent.getId())).thenReturn(Optional.of(parent));
    when(store.findPartitionSet("set")).thenReturn(Optional.of(set));
    when(sanitizer.sanitize(any())).thenReturn("safe");
  }

  private PartitionEntity row(int index, JobStatus status, boolean exists) {
    PartitionEntity row = new PartitionEntity();
    row.setSetId(set.getSetId());
    row.setPartitionIndex(index);
    row.setRangeFrom(index);
    row.setRangeTo(index + 1);
    row.setGeneration(1);
    row.setChildJobId(UUID.randomUUID());
    row.setCheckpoint("seed");
    if (exists) {
      JobEntity child = new JobEntity();
      child.setId(row.getChildJobId());
      child.setStatus(status);
      child.setLastError("secret");
      when(jobs.findById(child.getId())).thenReturn(Optional.of(child));
    }
    return row;
  }

  @AfterEach
  void clearEncryption() {
    EncryptionHolder.disable();
  }

  @Test
  void absentPartitionCapabilityIsANoop() {
    var absent =
        new PartitionService(
            null,
            jobs,
            states,
            batches,
            sanitizer,
            serializer,
            Clock.fixed(now, ZoneOffset.UTC),
            RatchetOptions.builder().build(),
            new BatchCompletionTransaction(),
            policy,
            null);
    assertTrue(absent.getPartitionSet("set").isEmpty());
    assertEquals(0, absent.getBatchPartitions("set", 10, 0).totalCount());
    assertFalse(absent.deletePartitionSet("set"));
    assertEquals(0, absent.sweep());
    verify(store, never()).findPartitionSet(anyString());
  }

  @Test
  void sealedCheckpointEncryptionBindsSetAndIndex() {
    EncryptionTestKit.install(true);
    var row = row(0, JobStatus.FAILED, true);
    String document = "{\"value\":\"checkpoint\"}";
    when(states.getState(row.getChildJobId(), JobCheckpointWriter.NAMESPACE))
        .thenReturn(Optional.of(new VersionedState(document, 0)));
    when(store.findGenerationPartitions(set.getSetId(), 1, -1, 500)).thenReturn(List.of(row));
    assertTrue(service.trySeal(set));
    assertNotEquals(document, row.getCheckpoint());
    assertEquals(
        document,
        PayloadEncryptor.decryptJsonColumn(
            row.getCheckpoint(), EncryptionTarget.partitionCheckpoint(set.getSetId(), 0)));
    assertThrows(
        PayloadDecryptionException.class,
        () ->
            PayloadEncryptor.decryptJsonColumn(
                row.getCheckpoint(), EncryptionTarget.partitionCheckpoint(set.getSetId(), 1)));
    assertThrows(
        PayloadDecryptionException.class,
        () ->
            PayloadEncryptor.decryptJsonColumn(
                row.getCheckpoint(), EncryptionTarget.partitionCheckpoint(UUID.randomUUID(), 0)));
  }

  @Test
  void waitsForTerminalParentAndChildren() {
    parent.setStatus(JobStatus.PAUSED);
    assertFalse(service.trySeal(set));
    verify(store, never()).sealPartitions(any(), anyInt(), anyList());
    parent.setStatus(JobStatus.FAILED);
    when(store.countUnfinishedBatchChildren(parent.getId())).thenReturn(1L);
    assertFalse(service.trySeal(set));
    verify(store, never()).markSealed(any(), anyInt());
  }

  @Test
  void sealsOutcomesAndRetainsOrCopiesSeeds() {
    var success = row(0, JobStatus.SUCCEEDED, true);
    var own = row(1, JobStatus.FAILED, true);
    var seed = row(2, JobStatus.FAILED, true);
    var absent = row(3, JobStatus.FAILED, false);
    when(states.getState(own.getChildJobId(), JobCheckpointWriter.NAMESPACE))
        .thenReturn(Optional.of(new VersionedState("own-json", 0)));
    when(store.findGenerationPartitions(set.getSetId(), 1, -1, 500))
        .thenReturn(List.of(success, own, seed, absent));
    assertTrue(service.trySeal(set));
    assertEquals(PartitionOutcome.SUCCEEDED, success.getOutcome());
    assertNull(success.getCheckpoint());
    assertEquals(PartitionOutcome.FAILED, own.getOutcome());
    assertEquals("own-json", own.getCheckpoint());
    assertEquals("safe", own.getLastError());
    assertEquals("seed", seed.getCheckpoint());
    assertEquals(PartitionOutcome.ABANDONED, absent.getOutcome());
    assertEquals("seed", absent.getCheckpoint());
    var ordered = inOrder(store);
    ordered.verify(store).sealPartitions(eq(set.getSetId()), eq(1), anyList());
    ordered.verify(store).markSealed(set.getSetId(), 1);
  }

  @Test
  void missingParentPreservesEverySeed() {
    when(jobs.findById(parent.getId())).thenReturn(Optional.empty());
    var row = row(0, JobStatus.SUCCEEDED, true);
    when(store.findGenerationPartitions(set.getSetId(), 1, -1, 500)).thenReturn(List.of(row));
    assertTrue(service.trySeal(set));
    assertEquals("seed", row.getCheckpoint());
    verifyNoInteractions(states);
  }

  @Test
  void sweepCancelsOldIncompleteNonTerminalGenerations() {
    Consumer<UUID> cancel = mock(Consumer.class);
    service.configureCancellation(cancel);
    parent.setStatus(JobStatus.PENDING);
    when(store.findUnsealedSets(null, 100)).thenReturn(List.of(set));
    BatchEntity batch = new BatchEntity();
    batch.setTotalItems(4);
    when(batches.findBatchById(parent.getId())).thenReturn(Optional.of(batch));
    service.sweep();
    verifyNoInteractions(cancel);
    set.setGenerationStartedAt(now.minus(Duration.ofMinutes(31)));
    when(store.countBatchChildren(parent.getId())).thenReturn(4L);
    service.sweep();
    verifyNoInteractions(cancel);
    when(store.countBatchChildren(parent.getId())).thenReturn(3L);
    service.sweep();
    verify(cancel).accept(parent.getId());
    clearInvocations(cancel);
    parent.setStatus(JobStatus.RUNNING);
    service.sweep();
    verify(cancel).accept(parent.getId());
    clearInvocations(cancel);
    parent.setStatus(JobStatus.PAUSED);
    service.sweep();
    verify(cancel).accept(parent.getId());
    clearInvocations(cancel);
    parent.setStatus(JobStatus.WAITING);
    service.sweep();
    verify(cancel).accept(parent.getId());
    clearInvocations(cancel);
    when(store.countUnfinishedBatchChildren(parent.getId())).thenReturn(1L);
    for (JobStatus status : List.of(JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.CANCELED)) {
      parent.setStatus(status);
      service.sweep();
    }
    verifyNoInteractions(cancel);
  }

  @Test
  void sweepCascadesCanceledParentBeforeSealingOnEveryTick() {
    Consumer<JobEntity> cascade = mock(Consumer.class);
    var recovery = spy(service);
    recovery.configureBatchCancellation(cascade);
    parent.setStatus(JobStatus.CANCELED);
    when(store.findUnsealedSets(null, 100)).thenReturn(List.of(set));
    doReturn(false).when(recovery).trySeal(set);
    assertEquals(0, recovery.sweep());
    var order = inOrder(cascade, recovery);
    order.verify(cascade).accept(parent);
    order.verify(recovery).trySeal(set);
    doReturn(true).when(recovery).trySeal(set);
    assertEquals(1, recovery.sweep());
    order.verify(cascade).accept(parent);
    order.verify(recovery).trySeal(set);
    verify(cascade, times(2)).accept(parent);
  }

  @Test
  void sweepDoesNotCascadeForOtherParentStatuses() {
    Consumer<JobEntity> cascade = mock(Consumer.class);
    service.configureBatchCancellation(cascade);
    when(store.findUnsealedSets(null, 100)).thenReturn(List.of(set));
    for (JobStatus status :
        List.of(
            JobStatus.PENDING,
            JobStatus.RUNNING,
            JobStatus.PAUSED,
            JobStatus.WAITING,
            JobStatus.SUCCEEDED,
            JobStatus.FAILED)) {
      parent.setStatus(status);
      service.sweep();
    }
    verifyNoInteractions(cascade);
  }

  @Test
  void sweepPagesPastOldUnsealedSetsAndContinuesAfterSetFailure() {
    var recovery = spy(service);
    List<PartitionSetEntity> first = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      var older = new PartitionSetEntity();
      older.setSetId(UUID.randomUUID());
      older.setLatestBatchId(UUID.randomUUID());
      first.add(older);
      doReturn(false).when(recovery).trySeal(older);
    }
    doThrow(new IllegalStateException("unavailable")).when(recovery).trySeal(first.get(0));
    UUID cursor = first.get(99).getSetId();
    when(store.findUnsealedSets(null, 100)).thenReturn(first);
    when(store.findUnsealedSets(cursor, 100)).thenReturn(List.of(set));
    when(store.findUnsealedSets(set.getSetId(), 100)).thenReturn(List.of());
    assertEquals(1, recovery.sweep());
    verify(store).markSealed(set.getSetId(), 1);
    verify(store).findUnsealedSets(set.getSetId(), 100);
  }

  @Test
  void deleteSealsFinishedGenerationAfterAuthorizationAndReloads() {
    doAnswer(
            invocation -> {
              set.setSealedGeneration(1);
              return true;
            })
        .when(store)
        .markSealed(set.getSetId(), 1);
    when(store.deletePartitionSet(set.getSetId())).thenReturn(true);
    assertTrue(service.deletePartitionSet("set"));
    var order = inOrder(policy, store);
    order.verify(store).findPartitionSet("set");
    order.verify(policy).checkCancel(parent.getId(), null, null);
    order.verify(store).markSealed(set.getSetId(), 1);
    order.verify(store).findPartitionSet("set");
    order.verify(store).deletePartitionSet(set.getSetId());
  }

  @Test
  void readsLiveAndSealedRowsWithoutWriting() {
    var live = row(0, JobStatus.RUNNING, true);
    var sealed = row(1, JobStatus.SUCCEEDED, false);
    sealed.setOutcome(PartitionOutcome.SUCCEEDED);
    var pending = row(2, JobStatus.PENDING, false);
    when(store.findPartitions(set.getSetId(), -1, 10)).thenReturn(List.of(live, sealed, pending));
    when(states.getState(live.getChildJobId(), JobProgressWriter.NAMESPACE))
        .thenReturn(Optional.of(new VersionedState("progress-json", 0)));
    when(serializer.deserialize("progress-json", Map.class))
        .thenReturn(Map.of("done", 1, "total", 2, "reportedAt", now.toString()));
    var page = service.getBatchPartitions("set", 10, 0);
    assertEquals(4, page.totalCount());
    assertEquals(
        List.of(PartitionStatus.RUNNING, PartitionStatus.SUCCEEDED, PartitionStatus.PENDING),
        page.items().stream().map(p -> p.status()).toList());
    assertNotNull(page.items().get(0).progress());
    verify(policy).checkRead(parent.getId(), null);
    verify(store, never()).sealPartitions(any(), anyInt(), anyList());
    assertTrue(service.getPartitionSet("set").orElseThrow().open());
    set.setSealedGeneration(1);
    assertFalse(service.getPartitionSet("set").orElseThrow().open());
  }

  @Test
  void unknownQueriesAndDeleteAreEmptyAndUnsealedDeleteThrows() {
    assertTrue(service.getPartitionSet("unknown").isEmpty());
    assertEquals(0, service.getBatchPartitions("unknown", 10, 0).totalCount());
    assertFalse(service.deletePartitionSet("unknown"));
    assertThrows(IllegalStateException.class, () -> service.deletePartitionSet("set"));
    set.setSealedGeneration(1);
    when(store.deletePartitionSet(set.getSetId())).thenReturn(true);
    assertTrue(service.deletePartitionSet("set"));
    verify(policy, times(2)).checkCancel(parent.getId(), null, null);
  }

  @Test
  void deniedReadsMatchMissingKeys() {
    var missingSet = service.getPartitionSet("missing");
    var missingPage = service.getBatchPartitions("missing", 10, 0);
    doThrow(new JobAuthorizationException(set.getLatestBatchId(), "read", null, "denied"))
        .when(policy)
        .checkRead(any(), any());
    assertEquals(missingSet, service.getPartitionSet("set"));
    assertEquals(missingPage, service.getBatchPartitions("set", 10, 0));
  }
}
