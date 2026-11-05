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
package run.ratchet.testsuite.batch;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.inject.Inject;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.BatchContext;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobStatus;
import run.ratchet.api.event.BatchCompletedEvent;
import run.ratchet.api.event.BatchCompletingEvent;
import run.ratchet.api.event.JobCompletedEvent;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.testsuite.app.BatchCompletionTracker;
import run.ratchet.testsuite.app.BatchItemProcessor;
import run.ratchet.testsuite.app.FailingJob;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.app.WorkflowBranchTracker;
import run.ratchet.testsuite.observer.EventCapture;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.JobAssertions;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;

/** Validates batch job execution: parent job spawns child items, all complete successfully. */
class BatchExecutionIT extends BaseRatchetIT {

  @Inject private TestJobService jobService;

  @Inject private JobCrudStore jobCrudStore;

  @Inject private EventCapture eventCapture;

  @Deployment
  public static WebArchive createDeployment() {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
    String profile = System.getProperty("testsuite.profile", "wildfly-managed");

    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(profile, dbType)
        .addClasses(
            BatchItemProcessor.class,
            BatchCompletionTracker.class,
            FailingJob.class,
            TestJobService.class,
            WorkflowBranchTracker.class,
            EventCapture.class)
        .addStoreInfrastructure()
        .addBeansXml()
        .build();
  }

  @BeforeEach
  void resetTrackers() {
    BatchItemProcessor.reset();
    BatchCompletionTracker.reset();
    FailingJob.resetCount();
    WorkflowBranchTracker.reset();
    eventCapture.clear();
  }

  @Test
  void duplicateIdempotencyKeyReturnsOriginalAndRunsChildrenOnce() {
    String key = UUID.randomUUID().toString();
    List<String> items = List.of("idempotent-a", "idempotent-b", "idempotent-c");
    JobHandle original =
        jobService
            .enqueueBatch("idempotent-batch")
            .withIdempotencyKey(key)
            .forEach(items, BatchItemProcessor::process)
            .submit();

    JobAssertions.assertBatchSucceeded(jobCrudStore, original, Duration.ofSeconds(30));

    JobHandle duplicate =
        jobService
            .enqueueBatch("idempotent-batch-retry")
            .withIdempotencyKey(key)
            .forEach(items, BatchItemProcessor::process)
            .submit();

    assertEquals(original.id(), duplicate.id());
    assertEquals(Set.copyOf(items), BatchItemProcessor.processedItems());
    assertEquals(items.size(), BatchItemProcessor.executionCount());
  }

  @Test
  void enqueueBatch_shouldProcessAllItems() {
    List<String> items = List.of("a", "b", "c");

    JobHandle handle =
        jobService.enqueueBatch("test-batch").forEach(items, BatchItemProcessor::process).submit();

    JobAssertions.assertBatchSucceeded(jobCrudStore, handle, Duration.ofSeconds(30));
    Set<String> processed = BatchItemProcessor.processedItems();
    assertEquals(3, processed.size());
    assertTrue(processed.containsAll(items));
  }

  @Test
  void batchWithEmptyItemList_shouldCompleteImmediately() {
    JobHandle handle =
        jobService
            .enqueueBatch("empty-batch")
            .forEach(List.of(), BatchItemProcessor::process)
            .submit();

    JobAssertions.assertBatchSucceeded(jobCrudStore, handle, Duration.ofSeconds(15));
    assertEquals(0, BatchItemProcessor.processedCount());
  }

  @Test
  void emptyBatchWithSuccessCallback_shouldCompleteOnSubmitAndRunCallback() {
    JobHandle handle =
        jobService
            .enqueueBatch("empty-success-batch")
            .forEach(List.<String>of(), BatchItemProcessor::process)
            .thenOnBatchSuccess(WorkflowBranchTracker::onSuccess)
            .submit();

    assertEquals(JobStatus.SUCCEEDED, jobCrudStore.getJobStatus(handle.id()));
    await()
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(() -> assertTrue(WorkflowBranchTracker.successBranchFired()));
    assertEquals(0, BatchItemProcessor.processedCount());
  }

  @Test
  void emptyBatchWithFailureCallback_shouldSucceedAndCancelCallback() {
    JobHandle handle =
        jobService
            .enqueueBatch("empty-failure-batch")
            .forEach(List.<String>of(), BatchItemProcessor::process)
            .thenOnBatchFailure(WorkflowBranchTracker::onFailure)
            .submit();

    assertEquals(JobStatus.SUCCEEDED, jobCrudStore.getJobStatus(handle.id()));
    var branches = jobCrudStore.findDependants(handle.id(), JobCrudStore.DEFAULT_PAGE_LIMIT, 0);
    assertEquals(1, branches.size());
    assertEquals(JobStatus.CANCELED, branches.get(0).getStatus());
    assertFalse(WorkflowBranchTracker.failureBranchFired());
  }

  @Test
  void emptyBatches_shouldMatchFullSuccessRateAndZeroFailureConditions() {
    // Workflows select one matching branch per parent; exercise each condition on its own batch.
    JobHandle successRateBatch =
        jobService
            .enqueueBatch("empty-success-rate-batch")
            .forEach(List.<String>of(), BatchItemProcessor::process)
            .thenWhenSuccessRate(1.0, WorkflowBranchTracker::onSuccess)
            .submit();
    JobHandle failureCountBatch =
        jobService
            .enqueueBatch("empty-failure-count-batch")
            .forEach(List.<String>of(), BatchItemProcessor::process)
            .thenWhenFailureCount(0, WorkflowBranchTracker::onConditional)
            .submit();

    assertEquals(JobStatus.SUCCEEDED, jobCrudStore.getJobStatus(successRateBatch.id()));
    assertEquals(JobStatus.SUCCEEDED, jobCrudStore.getJobStatus(failureCountBatch.id()));
    await()
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(
            () -> {
              assertTrue(WorkflowBranchTracker.successBranchFired());
              assertTrue(WorkflowBranchTracker.conditionalBranchFired());
            });
  }

  @Test
  void emptyBatch_shouldPublishBatchAndJobCompletionEventsForParent() {
    JobHandle handle =
        jobService
            .enqueueBatch("empty-events-batch")
            .forEach(List.<String>of(), BatchItemProcessor::process)
            .submit();

    assertEquals(JobStatus.SUCCEEDED, jobCrudStore.getJobStatus(handle.id()));
    await()
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(
            () -> {
              assertEquals(
                  1L,
                  eventCapture.getEvents(BatchCompletingEvent.class).stream()
                      .filter(event -> handle.id().equals(event.getJobId()))
                      .count());
              assertEquals(
                  1L,
                  eventCapture.getEvents(BatchCompletedEvent.class).stream()
                      .filter(event -> handle.id().equals(event.getJobId()))
                      .count());
              var completed =
                  eventCapture.getEvents(JobCompletedEvent.class).stream()
                      .filter(event -> handle.id().equals(event.getJobId()))
                      .toList();
              assertEquals(1, completed.size());
              assertEquals(0L, completed.get(0).getExecutionTimeMs());
            });
  }

  @Test
  void emptyStreamingBatchWithSuccessCallback_shouldCompleteOnSubmitAndRunCallback() {
    JobHandle handle =
        jobService
            .<String>streamingBatch("empty-stream-success")
            .fromStream(Stream.empty())
            .process(BatchItemProcessor::process)
            .thenOnBatchSuccess(WorkflowBranchTracker::onSuccess)
            .start();

    // Check immediately: parent completion must not depend on the batch recovery timer.
    assertEquals(JobStatus.SUCCEEDED, jobCrudStore.getJobStatus(handle.id()));
    await()
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(() -> assertTrue(WorkflowBranchTracker.successBranchFired()));
    assertEquals(0, BatchItemProcessor.processedCount());
  }

  @Test
  void batchWithAllFailingItems_shouldFailParent() {
    List<String> items = List.of("x", "y", "z");

    JobHandle handle =
        jobService
            .enqueueBatch("failing-batch")
            .forEach(
                items,
                item -> {
                  FailingJob.execute();
                })
            .submit();

    JobAssertions.assertBatchTerminated(jobCrudStore, handle, Duration.ofSeconds(30));
    assertEquals(JobStatus.FAILED, jobCrudStore.getJobStatus(handle.id()));
  }

  @Test
  void batchWithMixedItemResults_shouldFailParentAfterTrackingPartialProgress() {
    List<String> items = List.of("ok-1", "fail", "ok-2");

    JobHandle handle =
        jobService
            .enqueueBatch("partially-failing-batch")
            .forEach(items, BatchItemProcessor::failOnBatchFailureItem)
            .onProgress(BatchCompletionTracker::onProgress)
            .submit();

    JobAssertions.assertBatchTerminated(jobCrudStore, handle, Duration.ofSeconds(30));

    assertEquals(JobStatus.FAILED, jobCrudStore.getJobStatus(handle.id()));
    assertEquals(Set.of("ok-1", "ok-2"), BatchItemProcessor.processedItems());
    assertEquals(1, FailingJob.getAttemptCount());

    BatchContext finalSnapshot = finalProgressSnapshot(handle.id());
    assertEquals(items.size(), finalSnapshot.totalItems());
    assertEquals(2, finalSnapshot.completedItems());
    assertEquals(1, finalSnapshot.failedItems());
  }

  private static BatchContext finalProgressSnapshot(UUID batchId) {
    return BatchCompletionTracker.progressSnapshots().stream()
        .filter(snapshot -> batchId.equals(snapshot.batchId()))
        .filter(BatchContext::isComplete)
        .findFirst()
        .orElseThrow(() -> new AssertionError("Expected complete progress snapshot for batch"));
  }
}
