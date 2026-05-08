package run.ratchet.testsuite.batch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.inject.Inject;
import java.time.Duration;
import java.util.List;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.BatchContext;
import run.ratchet.api.JobHandle;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.testsuite.app.BatchCompletionTracker;
import run.ratchet.testsuite.app.BatchItemProcessor;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.JobAssertions;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;

/** Validates batch progress callbacks fire with increasing completion counts. */
class BatchProgressIT extends BaseRatchetIT {

  @Inject private TestJobService jobService;

  @Inject private JobCrudStore jobCrudStore;

  @Deployment
  public static WebArchive createDeployment() {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
    String profile = System.getProperty("testsuite.profile", "wildfly-managed");

    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(profile, dbType)
        .addClasses(BatchItemProcessor.class, BatchCompletionTracker.class, TestJobService.class)
        .addStoreInfrastructure()
        .addBeansXml()
        .build();
  }

  @BeforeEach
  void resetTrackers() {
    BatchItemProcessor.reset();
    BatchCompletionTracker.reset();
  }

  @Test
  void batchWithProgressHook_shouldReceiveProgressCallbacks() {
    List<String> items = List.of("item1", "item2", "item3", "item4", "item5");

    JobHandle handle =
        jobService
            .enqueueBatch("progress-batch")
            .forEach(items, BatchItemProcessor::process)
            .onProgress(BatchCompletionTracker::onProgress)
            .submit();

    JobAssertions.assertBatchSucceeded(jobCrudStore, handle, Duration.ofSeconds(30));

    List<BatchContext> snapshots = BatchCompletionTracker.progressSnapshots();
    assertFalse(snapshots.isEmpty(), "Should have received at least one progress callback");
    assertEquals(items.size(), snapshots.size(), "Should receive one progress callback per item");

    for (BatchContext snapshot : snapshots) {
      assertEquals(handle.id(), snapshot.batchId(), "Progress snapshot should belong to batch");
      assertEquals(
          items.size(), snapshot.totalItems(), "Progress snapshot should keep batch total");
      assertEquals(0, snapshot.failedItems(), "Successful batch should not report failed items");
      assertTrue(
          snapshot.completedItems() >= 1 && snapshot.completedItems() <= items.size(),
          "completedItems should be a post-increment count within the batch size");
      assertEquals(
          snapshot.completedItems() == items.size(),
          snapshot.isComplete(),
          "Only the final successful progress snapshot should be complete");
    }

    // Concurrent child jobs are not a public callback-ordering contract. The store-level contract
    // is stronger and more useful here: each atomic child completion produces one distinct
    // post-increment snapshot from 1 through totalItems.
    List<Integer> completedCounts =
        snapshots.stream().map(BatchContext::completedItems).sorted().toList();
    assertEquals(List.of(1, 2, 3, 4, 5), completedCounts);
  }
}
