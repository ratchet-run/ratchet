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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.inject.Inject;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.BatchContext;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobStatus;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.testsuite.app.BatchCompletionTracker;
import run.ratchet.testsuite.app.BatchItemProcessor;
import run.ratchet.testsuite.app.FailingJob;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.JobAssertions;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;

/** Validates batch job execution: parent job spawns child items, all complete successfully. */
class BatchExecutionIT extends BaseRatchetIT {

  @Inject private TestJobService jobService;

  @Inject private JobCrudStore jobCrudStore;

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
            TestJobService.class)
        .addStoreInfrastructure()
        .addBeansXml()
        .build();
  }

  @BeforeEach
  void resetTrackers() {
    BatchItemProcessor.reset();
    BatchCompletionTracker.reset();
    FailingJob.resetCount();
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
