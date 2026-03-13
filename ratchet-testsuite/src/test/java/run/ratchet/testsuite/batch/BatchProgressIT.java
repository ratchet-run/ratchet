package run.ratchet.testsuite.batch;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import run.ratchet.api.BatchContext;
import run.ratchet.api.JobHandle;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.testsuite.app.BatchCompletionTracker;
import run.ratchet.testsuite.app.BatchItemProcessor;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.JobAssertions;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.List;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
        .addTestInfrastructure()
        .addBeansXml()
        .addPersistenceXml(dbType)
        .addDataSource()
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

    // Verify completedItems increases monotonically
    int previousCompleted = 0;
    for (BatchContext snapshot : snapshots) {
      assertTrue(
          snapshot.completedItems() >= previousCompleted,
          "completedItems should increase monotonically");
      previousCompleted = snapshot.completedItems();
    }
  }
}
