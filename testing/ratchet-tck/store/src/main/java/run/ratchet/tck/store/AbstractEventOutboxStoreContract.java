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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static run.ratchet.tck.store.AbstractJobTerminalStoreContract.completionPlan;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobStatus;
import run.ratchet.api.JobType;
import run.ratchet.store.dto.JobCompletionPlan.BatchCompletion;
import run.ratchet.store.dto.JobCompletionPlan.DependencyTransition;
import run.ratchet.store.dto.OutboxEvent;
import run.ratchet.store.dto.OutboxEventRead;
import run.ratchet.store.dto.OutboxEventType;
import run.ratchet.store.entity.JobEntity;

/** Contract for atomic terminal event persistence and bounded recovery/delete operations. */
public abstract class AbstractEventOutboxStoreContract implements JobStoreContractFixture {
  private static final Instant CREATED = Instant.parse("2026-01-01T00:00:00Z");
  private static final Instant CUTOFF = CREATED.plusSeconds(10);

  @BeforeEach
  @AfterEach
  void cleanupOutboxFixture() {
    eventOutboxStore();
    cleanupStore();
  }

  @Test
  void committedCompletionRoundTripsNullableAndFullSnapshots() {
    JobEntity job = runningJob();
    OutboxEvent sparse = event(job.getId(), UUID.randomUUID(), CREATED);
    OutboxEvent full =
        new OutboxEvent(
            UUID.randomUUID(),
            OutboxEventType.JOB_DLQ,
            job.getId(),
            "business-key",
            UUID.randomUUID(),
            JobType.SINGLE,
            JobPriority.HIGH,
            "node-1",
            CREATED.minusSeconds(5),
            123L,
            "é".repeat(128 * 1024),
            7,
            CREATED.plusSeconds(1));
    assertTrue(
        store()
            .commitCompletion(
                completionPlan(
                    job,
                    JobStatus.RUNNING,
                    JobStatus.FAILED,
                    null,
                    List.of(),
                    List.of(sparse, full)))
            .committed());
    assertEquals(
        List.of(OutboxEventRead.decoded(sparse), OutboxEventRead.decoded(full)),
        eventOutboxStore().findEventsCreatedBefore(CUTOFF, 10));
  }

  @Test
  void lostPrimaryCasDoesNotInsertEvents() {
    JobEntity job = persist(newPendingJob());
    assertFalse(
        store()
            .commitCompletion(
                completionPlan(
                    job,
                    JobStatus.RUNNING,
                    JobStatus.SUCCEEDED,
                    null,
                    List.of(),
                    List.of(event(job.getId(), UUID.randomUUID(), CREATED))))
            .committed());
    assertTrue(eventOutboxStore().findEventsCreatedBefore(CUTOFF, 10).isEmpty());
    assertEquals(JobStatus.PENDING, store().getJobStatus(job.getId()));
  }

  @Test
  void staleDependencyRollsBackPrimaryAndOutbox() {
    JobEntity job = runningJob();
    JobEntity child = persist(newPendingJob());
    var stale =
        new DependencyTransition(
            child.getId(),
            JobStatus.PAUSED,
            child.getVersion(),
            child.getScheduledTime(),
            JobStatus.PENDING,
            CREATED,
            child.getJobType());
    var plan =
        completionPlan(
            job,
            JobStatus.RUNNING,
            JobStatus.SUCCEEDED,
            null,
            List.of(stale),
            List.of(event(job.getId(), UUID.randomUUID(), CREATED)));
    assertThrows(RuntimeException.class, () -> store().commitCompletion(plan));
    assertTrue(eventOutboxStore().findEventsCreatedBefore(CUTOFF, 10).isEmpty());
    assertEquals(JobStatus.RUNNING, store().getJobStatus(job.getId()));
  }

  @Test
  void batchParentCompletionInsertsEvent() {
    JobEntity parent = persist(newBatchParentJob());
    persistBatch(parent.getId(), 0);
    OutboxEvent event = event(parent.getId(), UUID.randomUUID(), CREATED);
    assertTrue(
        store()
            .commitCompletion(
                completionPlan(
                    parent,
                    JobStatus.PENDING,
                    JobStatus.SUCCEEDED,
                    new BatchCompletion(0, 0, 0),
                    List.of(),
                    List.of(event)))
            .committed());
    assertEquals(
        List.of(OutboxEventRead.decoded(event)),
        eventOutboxStore().findEventsCreatedBefore(CUTOFF, 10));
    assertTrue(batchStore().findBatchById(parent.getId()).orElseThrow().getCompletionProcessed());
  }

  @Test
  void findUsesStrictCutoffLimitAndCreationThenIdOrder() {
    JobEntity job = runningJob();
    // These ids have the same prefix and differ only in their last byte: native SQL Server,
    // binary UUID, PostgreSQL UUID, and Mongo UUID order all agree for this tie-break.
    UUID low = UUID.fromString("018f0000-0000-7000-8000-000000000001");
    UUID high = UUID.fromString("018f0000-0000-7000-8000-000000000002");
    OutboxEvent first = event(job.getId(), high, CREATED);
    OutboxEvent second = event(job.getId(), low, CREATED.plusSeconds(1));
    OutboxEvent third = event(job.getId(), highId(), CREATED.plusSeconds(1));
    OutboxEvent atCutoff = event(job.getId(), UUID.randomUUID(), CUTOFF);
    OutboxEvent later = event(job.getId(), UUID.randomUUID(), CUTOFF.plusSeconds(1));
    assertTrue(
        store()
            .commitCompletion(
                completionPlan(
                    job,
                    JobStatus.RUNNING,
                    JobStatus.SUCCEEDED,
                    null,
                    List.of(),
                    List.of(later, third, atCutoff, second, first)))
            .committed());
    assertEquals(
        List.of(
            OutboxEventRead.decoded(first),
            OutboxEventRead.decoded(second),
            OutboxEventRead.decoded(third)),
        eventOutboxStore().findEventsCreatedBefore(CUTOFF, 10));
    assertEquals(
        List.of(OutboxEventRead.decoded(first), OutboxEventRead.decoded(second)),
        eventOutboxStore().findEventsCreatedBefore(CUTOFF, 2));
    assertTrue(eventOutboxStore().findEventsCreatedBefore(CREATED, 10).isEmpty());
    assertThrows(
        IllegalArgumentException.class,
        () -> eventOutboxStore().findEventsCreatedBefore(CUTOFF, 0));
  }

  /** Replaces the stored event type with an unknown value, bypassing snapshot validation. */
  protected abstract void corruptOutboxEventType(UUID eventId);

  @Test
  void malformedSnapshotPreservesIdAndCanBeDeletedWithoutBlockingLaterRows() {
    JobEntity job = runningJob();
    OutboxEvent malformed = event(job.getId(), UUID.randomUUID(), CREATED);
    OutboxEvent valid = event(job.getId(), UUID.randomUUID(), CREATED.plusSeconds(1));
    assertTrue(
        store()
            .commitCompletion(
                completionPlan(
                    job,
                    JobStatus.RUNNING,
                    JobStatus.SUCCEEDED,
                    null,
                    List.of(),
                    List.of(malformed, valid)))
            .committed());
    corruptOutboxEventType(malformed.eventId());

    var rows = eventOutboxStore().findEventsCreatedBefore(CUTOFF, 10);

    assertEquals(2, rows.size());
    assertEquals(malformed.eventId(), rows.get(0).eventId());
    assertNull(rows.get(0).event());
    assertNotNull(rows.get(0).decodingFailure());
    assertEquals(OutboxEventRead.decoded(valid), rows.get(1));
    assertEquals(1, eventOutboxStore().findEventsCreatedBefore(CUTOFF, 1).size());
    assertEquals(1, eventOutboxStore().deleteEvents(List.of(rows.get(0).eventId())));
    assertEquals(
        List.of(OutboxEventRead.decoded(valid)),
        eventOutboxStore().findEventsCreatedBefore(CUTOFF, 10));
    assertEquals(1, eventOutboxStore().deleteEvents(List.of(valid.eventId())));
    assertTrue(eventOutboxStore().findEventsCreatedBefore(CUTOFF, 10).isEmpty());
  }

  private static UUID highId() {
    return UUID.fromString("018f0000-0000-7000-8000-000000000003");
  }

  @Test
  void deleteIsBoundedAndIgnoresMissingIds() {
    JobEntity job = runningJob();
    OutboxEvent first = event(job.getId(), UUID.randomUUID(), CREATED);
    OutboxEvent second = event(job.getId(), UUID.randomUUID(), CREATED.plusSeconds(1));
    assertTrue(
        store()
            .commitCompletion(
                completionPlan(
                    job,
                    JobStatus.RUNNING,
                    JobStatus.SUCCEEDED,
                    null,
                    List.of(),
                    List.of(first, second)))
            .committed());
    assertEquals(0, eventOutboxStore().deleteEvents(List.of()));
    assertEquals(1, eventOutboxStore().deleteEvents(List.of(first.eventId(), UUID.randomUUID())));
    assertEquals(0, eventOutboxStore().deleteEvents(List.of(first.eventId())));
    assertEquals(
        List.of(OutboxEventRead.decoded(second)),
        eventOutboxStore().findEventsCreatedBefore(CUTOFF, 10));
    List<UUID> tooMany = IntStream.range(0, 501).mapToObj(i -> UUID.randomUUID()).toList();
    assertThrows(IllegalArgumentException.class, () -> eventOutboxStore().deleteEvents(tooMany));
    List<UUID> maximum = new ArrayList<>(tooMany.subList(0, 499));
    maximum.add(second.eventId());
    assertEquals(1, eventOutboxStore().deleteEvents(maximum));
    assertTrue(eventOutboxStore().findEventsCreatedBefore(CUTOFF, 10).isEmpty());
  }

  private JobEntity runningJob() {
    JobEntity job = persist(newPendingJob());
    assertTrue(
        store().compareAndSwapStatus(job.getId(), JobStatus.PENDING, JobStatus.RUNNING, null));
    return job;
  }

  private static OutboxEvent event(UUID jobId, UUID eventId, Instant createdAt) {
    return new OutboxEvent(
        eventId,
        OutboxEventType.JOB_COMPLETED,
        jobId,
        null,
        null,
        null,
        null,
        null,
        CREATED,
        null,
        null,
        0,
        createdAt);
  }
}
