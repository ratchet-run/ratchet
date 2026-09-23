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
package run.ratchet.quarkus.it.tck;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.*;
import run.ratchet.api.event.*;
import run.ratchet.api.exception.RatchetTransientStoreException;
import run.ratchet.ri.core.BatchService;
import run.ratchet.store.dto.BatchProgress;
import run.ratchet.store.spi.*;

@QuarkusTest
@TestProfile(QuarkusCompletionRecoveryTest.CompletionProfile.class)
class QuarkusCompletionRecoveryTest {
  public static class CompletionProfile extends QuarkusRatchetTckProfile {
    @Override
    public java.util.Map<String, String> getConfigOverrides() {
      if ("mongodb".equals(System.getProperty("quarkus.datasource.db-kind"))) {
        // The test decorator generates a runtime subclass of this package-private library bean.
        // Keep the Mongo implementation in that same loader; ordinary profiles remain undecorated.
        return java.util.Map.of(
            "ratchet.it.completion-faults",
            "true",
            "quarkus.class-loading.reloadable-artifacts",
            "run.ratchet:ratchet-store-mongodb");
      }
      return java.util.Map.of("ratchet.it.completion-faults", "true");
    }
  }

  @Inject QuarkusRatchetTckRuntime runtime;
  @Inject JobSchedulerService scheduler;
  @Inject JobCrudStore jobs;
  @Inject JobBulkStore bulk;
  @Inject ArchiveStore archive;
  @Inject BatchStore batches;
  @Inject BatchService batchService;
  @Inject CompletionFaults faults;
  @Inject CompletionEventBoundaryProbe eventBoundary;
  final List<Object> events = new CopyOnWriteArrayList<>();
  final Consumer<Object> listener = events::add;

  @BeforeEach
  void before() {
    faults.reset();
    runtime.clear();
    CompletionProbeJobs.reset();
    events.clear();
    scheduler.addEventListener(listener);
  }

  @AfterEach
  void after() {
    faults.resume.countDown();
    faults.rejectBatchParents.set(false);
    scheduler.removeEventListener(listener);
    runtime.clear();
    faults.reset();
  }

  @Test
  void completionEventsWaitForCommitAndDisappearOnRollback() {
    Object committed = completedEvent();
    eventBoundary.emit(committed, () -> assertFalse(events.contains(committed)), false);
    assertTrue(events.contains(committed));
    Object rolledBack = completedEvent();
    assertThrows(
        IllegalStateException.class,
        () -> eventBoundary.emit(rolledBack, () -> assertFalse(events.contains(rolledBack)), true));
    assertFalse(events.contains(rolledBack));
  }

  @Test
  void failedCompositeRollsBackThenRetriesWithoutReexecutingPayload() throws Exception {
    faults.corruptNextChainCompletion.set(true);
    JobHandle root =
        scheduler.enqueue(CompletionProbeJobs::root).then(CompletionProbeJobs::child).submit();
    assertTrue(
        faults.rolledBack.await(15, TimeUnit.SECONDS),
        "real store rejected the invalid batch mutation");
    assertEquals(JobStatus.RUNNING, jobs.findById(root.id()).orElseThrow().getStatus());
    var expectedChild = faults.failedPlan.dependencies().get(0);
    var child = jobs.findById(expectedChild.jobId()).orElseThrow();
    assertEquals(JobStatus.PENDING, child.getStatus());
    assertEquals(
        expectedChild.expectedScheduledTime(),
        child.getScheduledTime(),
        "dependent unlock rolled back too");
    assertEquals(0, CompletionProbeJobs.children.get());
    assertEquals(0, count(JobCompletedEvent.class, root.id()));
    assertEquals(0, events.stream().filter(ChainStartedEvent.class::isInstance).count());
    faults.resume.countDown();
    await()
        .atMost(Duration.ofSeconds(20))
        .untilAsserted(
            () -> {
              assertEquals(
                  JobStatus.SUCCEEDED, jobs.findById(child.getId()).orElseThrow().getStatus());
              assertEquals(1, count(JobCompletedEvent.class, root.id()));
              assertEquals(1, CompletionProbeJobs.children.get());
            });
    assertEquals(
        1, CompletionProbeJobs.roots.get(), "persistence retry must not rerun the payload");
    assertTrue(
        index(JobCompletedEvent.class, root.id()) < index(ChainStartedEvent.class, child.getId()));
  }

  @Test
  void committedChildSurvivesFailedParentFollowupAndRecoveryFinishesOnce() {
    faults.rejectBatchParents.set(true);
    JobHandle parent =
        scheduler
            .enqueueBatch("recovery")
            .forEach(List.of("one"), CompletionProbeJobs::item)
            .thenOnBatchSuccess(CompletionProbeJobs::callback)
            .submit();
    await().atMost(Duration.ofSeconds(20)).until(() -> faults.rejectedParents.get() > 0);
    var progress = batches.findBatchById(parent.id()).orElseThrow();
    assertEquals(1, progress.getCompletedItems());
    assertFalse(Boolean.TRUE.equals(progress.getCompletionProcessed()));
    assertEquals(JobStatus.PENDING, jobs.findById(parent.id()).orElseThrow().getStatus());
    assertEquals(0, CompletionProbeJobs.callbacks.get());
    assertEquals(0, count(BatchCompletedEvent.class, parent.id()));
    faults.rejectBatchParents.set(false);
    batchService.recoverStuckBatches();
    await()
        .atMost(Duration.ofSeconds(20))
        .untilAsserted(
            () -> {
              assertEquals(
                  JobStatus.SUCCEEDED, jobs.findById(parent.id()).orElseThrow().getStatus());
              assertTrue(batches.findBatchById(parent.id()).orElseThrow().getCompletionProcessed());
              assertEquals(1, CompletionProbeJobs.callbacks.get());
              assertEquals(1, count(BatchCompletedEvent.class, parent.id()));
            });
    batchService.recoverStuckBatches();
    assertEquals(1, batches.findBatchById(parent.id()).orElseThrow().getCompletedItems());
    assertEquals(1, CompletionProbeJobs.children.get());
    assertEquals(1, count(BatchCompletedEvent.class, parent.id()));
  }

  @Test
  void afterCompletionPreservesParentCompletionFailure() {
    faults.rejectBatchParents.set(true);
    JobHandle parent =
        scheduler
            .enqueueBatch("after-completion")
            .forEach(List.of("one"), CompletionProbeJobs::item)
            .submit();
    await().atMost(Duration.ofSeconds(20)).until(() -> faults.rejectedParents.get() > 0);

    var batch = batches.findBatchById(parent.id()).orElseThrow();
    AtomicReference<Throwable> observed =
        eventBoundary.invokeAfterCompletion(
            batchService,
            new BatchProgress(
                parent.id(),
                batch.getTotalItems(),
                batch.getCompletedItems(),
                batch.getFailedItems(),
                batch.getProgressHook()));

    Throwable failure = observed.get();
    assertNotNull(failure);
    assertInstanceOf(RatchetTransientStoreException.class, failure);
    assertEquals("injected parent completion outage", failure.getMessage());
  }

  @Test
  void permanentKeyReturnsOriginalPublicHandleAfterJobPurge() {
    assertPermanentKeyAfterRemoval(false);
  }

  @Test
  void permanentKeyReturnsOriginalPublicHandleAfterArchivePurge() {
    assertPermanentKeyAfterRemoval(true);
  }

  void assertPermanentKeyAfterRemoval(boolean archiveFirst) {
    String key = UUID.randomUUID().toString();
    JobHandle first = scheduler.enqueue(CompletionProbeJobs::root).withIdempotencyKey(key).submit();
    await()
        .atMost(Duration.ofSeconds(20))
        .until(() -> jobs.findById(first.id()).orElseThrow().getStatus() == JobStatus.SUCCEEDED);
    if (archiveFirst) {
      assertEquals(
          1,
          archive.archiveAndDeleteJobsBatch(
              List.of(jobs.findById(first.id()).orElseThrow()), "it", "it"));
      assertEquals(1, archive.purgeArchivedJobs(Instant.now().plusSeconds(60)));
    } else {
      assertEquals(1, bulk.deleteJobsByIds(List.of(first.id())));
    }
    assertTrue(jobs.findById(first.id()).isEmpty());
    JobHandle duplicate =
        scheduler.enqueue(CompletionProbeJobs::child).withIdempotencyKey(key).submit();
    assertEquals(first.id(), duplicate.id());
    assertTrue(jobs.findById(first.id()).isEmpty(), "resubmit must not recreate a purged row");
    assertEquals(1, CompletionProbeJobs.roots.get());
    assertEquals(0, CompletionProbeJobs.children.get());
  }

  Object completedEvent() {
    return new JobCompletedEvent(
        UUID.randomUUID(), null, null, JobType.SINGLE, JobPriority.NORMAL, null, Instant.now(), 0L);
  }

  long count(Class<?> type, UUID id) {
    return events.stream()
        .filter(type::isInstance)
        .map(AbstractJobSchedulerEvent.class::cast)
        .filter(e -> id.equals(e.getJobId()))
        .count();
  }

  int index(Class<?> type, UUID id) {
    for (int i = 0; i < events.size(); i++)
      if (type.isInstance(events.get(i))
          && id.equals(((AbstractJobSchedulerEvent) events.get(i)).getJobId())) return i;
    throw new AssertionError("Missing event " + type);
  }
}
