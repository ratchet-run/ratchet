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
package run.ratchet.tck.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobStatus;
import run.ratchet.store.dto.PartitionGenerationWrite;
import run.ratchet.store.entity.BatchEntity;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.entity.PartitionEntity;
import run.ratchet.store.entity.PartitionOutcome;
import run.ratchet.store.entity.PartitionSetEntity;

/** Contract for atomic partition generations, durable seeds, and fenced sealing. */
public abstract class AbstractPartitionStoreContract implements JobStoreContractFixture {
  @BeforeEach
  @AfterEach
  void cleanupPartitionFixture() {
    cleanupStore();
  }

  static PartitionGenerationWrite generation(
      JobStoreContractFixture fixture, String key, int count) {
    JobEntity parent = fixture.newBatchParentJob();
    parent.setId(UUID.randomUUID());
    PartitionSetEntity set = new PartitionSetEntity();
    Instant now = Instant.now().truncatedTo(ChronoUnit.MILLIS);
    set.setSetId(UUID.randomUUID());
    set.setSetKey(key);
    set.setName("partition contract");
    set.setPartitionCount(count);
    set.setLatestGeneration(1);
    set.setLatestBatchId(parent.getId());
    set.setGenerationStartedAt(now);
    set.setOwnerPrincipal("partition-owner");
    set.setCreatedAt(now);
    set.setUpdatedAt(now);
    List<PartitionEntity> rows = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      PartitionEntity row = new PartitionEntity();
      row.setSetId(set.getSetId());
      row.setPartitionIndex(i);
      row.setRangeFrom(i * 100L);
      row.setRangeTo((i + 1) * 100L);
      row.setGeneration(1);
      row.setChildJobId(UUID.randomUUID());
      rows.add(row);
    }
    BatchEntity batch = new BatchEntity();
    batch.setId(parent.getId());
    batch.setTotalItems(count);
    batch.setPartitionSetId(set.getSetId());
    batch.setGeneration(1);
    return new PartitionGenerationWrite(set, null, rows, parent, batch);
  }

  private PartitionGenerationWrite create(int count) {
    PartitionGenerationWrite write = generation(this, UUID.randomUUID().toString(), count);
    assertTrue(partitionStore().createOrAdvance(write));
    return write;
  }

  private PartitionGenerationWrite next(PartitionGenerationWrite previous, int expectedVersion) {
    PartitionGenerationWrite fresh = generation(this, previous.set().getSetKey(), 0);
    PartitionSetEntity set =
        partitionStore().findPartitionSetById(previous.set().getSetId()).orElseThrow();
    set.setLatestGeneration(set.getLatestGeneration() + 1);
    set.setLatestBatchId(fresh.parent().getId());
    set.setUpdatedAt(Instant.now().truncatedTo(ChronoUnit.MILLIS));
    set.setGenerationStartedAt(set.getUpdatedAt());
    fresh.batch().setPartitionSetId(set.getSetId());
    fresh.batch().setGeneration(set.getLatestGeneration());
    List<PartitionEntity> rows = partitionStore().findPartitions(set.getSetId(), -1, 100);
    for (PartitionEntity row : rows) {
      row.setGeneration(set.getLatestGeneration());
      row.setChildJobId(UUID.randomUUID());
    }
    fresh.batch().setTotalItems(rows.size());
    return new PartitionGenerationWrite(set, expectedVersion, rows, fresh.parent(), fresh.batch());
  }

  @Test
  void createAndFindByKey() {
    assertTrue(partitionStore().findPartitionSet("missing").isEmpty());
    var write = create(2);
    var found = partitionStore().findPartitionSet(write.set().getSetKey()).orElseThrow();
    assertEquals(write.set().getSetId(), found.getSetId());
    assertEquals("partition contract", found.getName());
    assertEquals("partition-owner", found.getOwnerPrincipal());
    assertEquals(2, found.getPartitionCount());
    assertEquals(0, found.getVersion());
    assertEquals(0, found.getSealedGeneration());
    assertEquals(write.set().getCreatedAt(), found.getCreatedAt());
    assertEquals(write.set().getGenerationStartedAt(), found.getGenerationStartedAt());
  }

  @Test
  void findByIdAndPersistPresetParentAndBatchColumns() {
    assertTrue(partitionStore().findPartitionSetById(UUID.randomUUID()).isEmpty());
    var write = generation(this, UUID.randomUUID().toString(), 2);
    UUID preset = write.parent().getId();
    write.parent().setTags(List.of("partition-tag"));
    write.parent().setBusinessKey("partition-business");
    write.parent().setIdempotencyKey(UUID.randomUUID().toString());
    assertTrue(partitionStore().createOrAdvance(write));
    assertEquals(preset, write.parent().getId());
    var parent = store().findById(preset).orElseThrow();
    assertEquals(preset, parent.getId());
    assertEquals(List.of("partition-tag"), parent.getTags());
    assertEquals(write.parent().getIdempotencyKey(), parent.getIdempotencyKey());
    assertEquals("partition-business", parent.getBusinessKey());
    assertEquals(
        preset,
        partitionStore()
            .findPartitionSetById(write.set().getSetId())
            .orElseThrow()
            .getLatestBatchId());
    var batch = batchStore().findBatchById(preset).orElseThrow();
    assertEquals(write.set().getSetId(), batch.getPartitionSetId());
    assertEquals(1, batch.getGeneration());
    batch = batchStore().findBatchesByIds(List.of(preset)).get(0);
    assertEquals(write.set().getSetId(), batch.getPartitionSetId());
    assertEquals(1, batch.getGeneration());
    batchStore().saveBatch(batch);
    assertEquals(
        write.set().getSetId(),
        batchStore().findBatchById(preset).orElseThrow().getPartitionSetId());
  }

  @Test
  void duplicateKeyWritesNothing() {
    var original = create(2);
    var duplicate = generation(this, original.set().getSetKey(), 3);
    assertFalse(partitionStore().createOrAdvance(duplicate));
    assertTrue(partitionStore().findPartitionSetById(duplicate.set().getSetId()).isEmpty());
    assertTrue(partitionStore().findPartitions(duplicate.set().getSetId(), -1, 10).isEmpty());
    assertTrue(store().findById(duplicate.parent().getId()).isEmpty());
    assertTrue(batchStore().findBatchById(duplicate.batch().getId()).isEmpty());
    assertEquals(2, partitionStore().findPartitions(original.set().getSetId(), -1, 10).size());
  }

  @Test
  void advanceFencesVersionAndKeepsOrClearsSeeds() {
    var first = create(2);
    for (PartitionEntity row : first.rows()) {
      row.setCheckpoint("seed");
      row.setOutcome(PartitionOutcome.FAILED);
      row.setLastError("old error");
    }
    partitionStore().sealPartitions(first.set().getSetId(), 1, first.rows());
    var advance = next(first, 0);
    advance.rows().get(1).setCheckpoint(null);
    assertTrue(partitionStore().createOrAdvance(advance));
    var rows = partitionStore().findPartitions(first.set().getSetId(), -1, 10);
    assertEquals("seed", rows.get(0).getCheckpoint());
    assertNull(rows.get(1).getCheckpoint());
    assertNull(rows.get(0).getOutcome());
    assertNull(rows.get(0).getLastError());
    var current = partitionStore().findPartitionSetById(first.set().getSetId()).orElseThrow();
    assertEquals(1, current.getVersion());
    assertEquals(2, current.getLatestGeneration());
    assertEquals(advance.parent().getId(), current.getLatestBatchId());
    var stale = next(advance, 0);
    stale.rows().get(0).setCheckpoint("must not write");
    assertFalse(partitionStore().createOrAdvance(stale));
    assertTrue(store().findById(stale.parent().getId()).isEmpty());
    assertTrue(batchStore().findBatchById(stale.batch().getId()).isEmpty());
    current = partitionStore().findPartitionSetById(first.set().getSetId()).orElseThrow();
    assertEquals(2, current.getLatestGeneration());
    assertEquals(1, current.getVersion());
    assertEquals(advance.parent().getId(), current.getLatestBatchId());
    rows = partitionStore().findPartitions(first.set().getSetId(), -1, 10);
    assertEquals("seed", rows.get(0).getCheckpoint());
    assertEquals(advance.rows().get(0).getChildJobId(), rows.get(0).getChildJobId());
    assertEquals(2, rows.get(0).getGeneration());
  }

  @Test
  void createRollsBackSetRowsWhenParentCreationFails() {
    var existing = persist(newPendingJob());
    var write = generation(this, UUID.randomUUID().toString(), 2);
    write.parent().setIdempotencyKey(existing.getIdempotencyKey());
    assertThrows(RuntimeException.class, () -> partitionStore().createOrAdvance(write));
    assertTrue(partitionStore().findPartitionSetById(write.set().getSetId()).isEmpty());
    assertTrue(partitionStore().findPartitions(write.set().getSetId(), -1, 10).isEmpty());
    assertTrue(store().findById(write.parent().getId()).isEmpty());
    assertTrue(batchStore().findBatchById(write.batch().getId()).isEmpty());
  }

  @Test
  void advanceRollsBackVersionAndRowsWhenParentCreationFails() {
    var original = create(1);
    var next = next(original, 0);
    next.parent().setIdempotencyKey(original.parent().getIdempotencyKey());
    next.rows().get(0).setCheckpoint("must roll back");
    assertThrows(RuntimeException.class, () -> partitionStore().createOrAdvance(next));
    var set = partitionStore().findPartitionSetById(original.set().getSetId()).orElseThrow();
    assertEquals(0, set.getVersion());
    assertEquals(1, set.getLatestGeneration());
    assertEquals(original.parent().getId(), set.getLatestBatchId());
    var row = partitionStore().findPartitions(set.getSetId(), -1, 10).get(0);
    assertEquals(original.rows().get(0).getChildJobId(), row.getChildJobId());
    assertEquals(1, row.getGeneration());
    assertNull(row.getCheckpoint());
    assertTrue(store().findById(next.parent().getId()).isEmpty());
    assertTrue(batchStore().findBatchById(next.batch().getId()).isEmpty());
  }

  @Test
  void pagesPartitionsByExclusiveIndex() {
    var write = create(5);
    var first = partitionStore().findPartitions(write.set().getSetId(), -1, 2);
    assertEquals(List.of(0, 1), first.stream().map(PartitionEntity::getPartitionIndex).toList());
    var second = partitionStore().findPartitions(write.set().getSetId(), 1, 2);
    assertEquals(List.of(2, 3), second.stream().map(PartitionEntity::getPartitionIndex).toList());
    assertEquals(200L, second.get(0).getRangeFrom());
    assertEquals(300L, second.get(0).getRangeTo());
    assertTrue(partitionStore().findPartitions(write.set().getSetId(), 4, 2).isEmpty());
  }

  @Test
  void pagesOnlyGenerationPartitions() {
    var write = create(4);
    var advance = next(write, 0);
    advance =
        new PartitionGenerationWrite(
            advance.set(), 0, advance.rows().subList(1, 3), advance.parent(), advance.batch());
    assertTrue(partitionStore().createOrAdvance(advance));
    assertEquals(
        List.of(0, 3),
        partitionStore().findGenerationPartitions(write.set().getSetId(), 1, -1, 10).stream()
            .map(PartitionEntity::getPartitionIndex)
            .toList());
    assertEquals(
        List.of(1),
        partitionStore().findGenerationPartitions(write.set().getSetId(), 2, -1, 1).stream()
            .map(PartitionEntity::getPartitionIndex)
            .toList());
    assertEquals(
        List.of(2),
        partitionStore().findGenerationPartitions(write.set().getSetId(), 2, 1, 10).stream()
            .map(PartitionEntity::getPartitionIndex)
            .toList());
  }

  @Test
  void sealsOnlyMatchingGenerationAndChildIdIdempotently() {
    var write = create(4);
    for (int i = 0; i < 4; i++) {
      write.rows().get(i).setOutcome(PartitionOutcome.values()[i]);
      write.rows().get(i).setLastError("error-" + i);
      write.rows().get(i).setCheckpoint("checkpoint-" + i);
    }
    assertEquals(0, partitionStore().sealPartitions(write.set().getSetId(), 2, write.rows()));
    var wrong = new PartitionEntity();
    wrong.setPartitionIndex(0);
    wrong.setChildJobId(UUID.randomUUID());
    wrong.setOutcome(PartitionOutcome.SUCCEEDED);
    assertEquals(0, partitionStore().sealPartitions(write.set().getSetId(), 1, List.of(wrong)));
    assertNull(partitionStore().findPartitions(write.set().getSetId(), -1, 10).get(0).getOutcome());
    assertEquals(4, partitionStore().sealPartitions(write.set().getSetId(), 1, write.rows()));
    partitionStore().sealPartitions(write.set().getSetId(), 1, write.rows());
    var rows = partitionStore().findPartitions(write.set().getSetId(), -1, 10);
    for (int i = 0; i < 4; i++) {
      assertEquals(PartitionOutcome.values()[i], rows.get(i).getOutcome());
      assertEquals("error-" + i, rows.get(i).getLastError());
      assertEquals("checkpoint-" + i, rows.get(i).getCheckpoint());
    }
  }

  @Test
  void markSealedOnlyMovesForward() {
    var write = create(1);
    assertTrue(partitionStore().markSealed(write.set().getSetId(), 1));
    assertFalse(partitionStore().markSealed(write.set().getSetId(), 1));
    assertFalse(partitionStore().markSealed(write.set().getSetId(), 0));
    assertEquals(
        1,
        partitionStore()
            .findPartitionSetById(write.set().getSetId())
            .orElseThrow()
            .getSealedGeneration());
    assertFalse(partitionStore().markSealed(UUID.randomUUID(), 1));
  }

  @Test
  void findsUnsealedSetsWithinLimit() {
    var first = create(1);
    var second = create(1);
    assertEquals(1, partitionStore().findUnsealedSets(null, 1).size());
    assertEquals(2, partitionStore().findUnsealedSets(null, 10).size());
    partitionStore().markSealed(first.set().getSetId(), 1);
    assertEquals(
        List.of(second.set().getSetId()),
        partitionStore().findUnsealedSets(null, 10).stream()
            .map(PartitionSetEntity::getSetId)
            .toList());
  }

  @Test
  void pagesEveryUnsealedSetExactlyOnce() {
    Set<UUID> expected = new HashSet<>();
    for (int i = 0; i < 8; i++) {
      var write = create(1);
      if (i % 3 == 0) partitionStore().markSealed(write.set().getSetId(), 1);
      else expected.add(write.set().getSetId());
    }
    Set<UUID> seen = new HashSet<>();
    UUID cursor = null;
    int pages = 0;
    while (true) {
      var page = partitionStore().findUnsealedSets(cursor, 2);
      if (page.isEmpty()) break;
      assertTrue(++pages <= expected.size());
      assertTrue(page.size() <= 2);
      for (var set : page) {
        assertTrue(expected.contains(set.getSetId()), "sealed set returned");
        assertTrue(seen.add(set.getSetId()), "duplicate set returned");
      }
      cursor = page.get(page.size() - 1).getSetId();
    }
    assertTrue(pages > 1);
    assertEquals(expected, seen);
  }

  @Test
  void findsPartitionByChildAndRoundTripsLargeMultibyteCheckpoint() {
    var write = generation(this, UUID.randomUUID().toString(), 2);
    String checkpoint = "界".repeat(40000);
    write.rows().get(0).setCheckpoint(checkpoint);
    assertTrue(partitionStore().createOrAdvance(write));
    UUID child = write.rows().get(0).getChildJobId();
    var found = partitionStore().findPartitionByChild(child).orElseThrow();
    assertEquals(write.set().getSetId(), found.getSetId());
    assertEquals(0, found.getPartitionIndex());
    assertEquals(checkpoint, found.getCheckpoint());
    assertNull(
        partitionStore()
            .findPartitionByChild(write.rows().get(1).getChildJobId())
            .orElseThrow()
            .getCheckpoint());
    assertTrue(partitionStore().findPartitionByChild(UUID.randomUUID()).isEmpty());
    var row = write.rows().get(0);
    row.setOutcome(PartitionOutcome.FAILED);
    row.setCheckpoint(checkpoint + "続");
    partitionStore().sealPartitions(write.set().getSetId(), 1, List.of(row));
    assertEquals(
        checkpoint + "続",
        partitionStore().findPartitionByChild(child).orElseThrow().getCheckpoint());
    var advance = next(write, 0);
    assertTrue(partitionStore().createOrAdvance(advance));
    assertEquals(
        checkpoint + "続",
        partitionStore().findPartitions(write.set().getSetId(), -1, 10).get(0).getCheckpoint());
    assertTrue(partitionStore().findPartitionByChild(child).isEmpty());
  }

  @Test
  void countsOnlyBatchChildren() {
    var write = create(1);
    for (JobExecutionType type :
        List.of(JobExecutionType.BATCH_CHILD, JobExecutionType.WORKFLOW_BRANCH)) {
      for (JobStatus status :
          List.of(JobStatus.PENDING, JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.CANCELED)) {
        var job = newPendingJob();
        job.setDependsOn(write.parent().getId());
        job.setJobType(type);
        job = persist(job);
        if (status == JobStatus.CANCELED) assertTrue(store().cancelJob(job.getId()));
        else if (status != JobStatus.PENDING) {
          assertTrue(store().tryPickUpJob(job.getId(), "partition-test"));
          if (status == JobStatus.SUCCEEDED)
            assertTrue(
                store()
                    .markJobSucceeded(
                        job.getId(),
                        null,
                        null,
                        Instant.EPOCH,
                        Instant.EPOCH.plusSeconds(1),
                        0L,
                        0L));
          else assertTrue(store().markJobFailedTerminal(job.getId(), "failed", 1, null));
        }
      }
    }
    assertEquals(4, partitionStore().countBatchChildren(write.parent().getId()));
    assertEquals(1, partitionStore().countUnfinishedBatchChildren(write.parent().getId()));
    assertEquals(0, partitionStore().countBatchChildren(UUID.randomUUID()));
  }

  @Test
  void unfinishedChildCountIncludesAllActiveStates() {
    var write = create(1);
    for (JobStatus status :
        List.of(JobStatus.PENDING, JobStatus.RUNNING, JobStatus.PAUSED, JobStatus.WAITING)) {
      var child = newPendingJob();
      child.setJobType(JobExecutionType.BATCH_CHILD);
      child.setDependsOn(write.parent().getId());
      child.setStatus(status);
      persist(child);
    }
    assertEquals(4, partitionStore().countUnfinishedBatchChildren(write.parent().getId()));
  }

  @Test
  void deleteRefusesUnsealedAndCascadesOnlyPartitionRows() {
    var write = create(2);
    assertFalse(partitionStore().deletePartitionSet(write.set().getSetId()));
    assertEquals(2, partitionStore().findPartitions(write.set().getSetId(), -1, 10).size());
    partitionStore().markSealed(write.set().getSetId(), 1);
    assertTrue(partitionStore().deletePartitionSet(write.set().getSetId()));
    assertTrue(partitionStore().findPartitionSetById(write.set().getSetId()).isEmpty());
    assertTrue(partitionStore().findPartitions(write.set().getSetId(), -1, 10).isEmpty());
    assertTrue(store().findById(write.parent().getId()).isPresent());
    assertFalse(partitionStore().deletePartitionSet(write.set().getSetId()));
  }
}
