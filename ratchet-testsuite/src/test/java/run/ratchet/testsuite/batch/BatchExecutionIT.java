package run.ratchet.testsuite.batch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import run.ratchet.api.JobHandle;
import run.ratchet.api.JobStatus;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.testsuite.app.BatchItemProcessor;
import run.ratchet.testsuite.app.FailingJob;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.JobAssertions;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
        .addClasses(BatchItemProcessor.class, FailingJob.class, TestJobService.class)
        .addStoreInfrastructure()
        .addBeansXml()
        .build();
  }

  @BeforeEach
  void resetTrackers() {
    BatchItemProcessor.reset();
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
}
