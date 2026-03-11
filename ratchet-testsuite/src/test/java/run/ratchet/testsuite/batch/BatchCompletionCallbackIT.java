package run.ratchet.testsuite.batch;

import static org.junit.jupiter.api.Assertions.assertEquals;

import run.ratchet.api.JobHandle;
import run.ratchet.store.entity.JobStatus;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.testsuite.app.BatchItemProcessor;
import run.ratchet.testsuite.app.SimpleJob;
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

/** Validates that batch completion triggers workflow branches (thenOnBatchSuccess). */
class BatchCompletionCallbackIT extends BaseRatchetIT {

  @Inject private TestJobService jobService;

  @Inject private JobCrudStore jobCrudStore;

  @Deployment
  public static WebArchive createDeployment() {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
    String profile = System.getProperty("testsuite.profile", "wildfly-managed");

    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(profile, dbType)
        .addClasses(BatchItemProcessor.class, SimpleJob.class, TestJobService.class)
        .addTestInfrastructure()
        .addBeansXml()
        .addPersistenceXml(dbType)
        .addDataSource()
        .build();
  }

  @BeforeEach
  void resetTrackers() {
    BatchItemProcessor.reset();
    SimpleJob.resetCount();
  }

  @Test
  void batchWithSuccessCallback_shouldExecuteCallbackAfterBatchCompletes() {
    List<String> items = List.of("x", "y");

    JobHandle handle =
        jobService
            .enqueueBatch("callback-batch")
            .forEach(items, BatchItemProcessor::process)
            .thenOnBatchSuccess(SimpleJob::execute)
            .submit();

    // Wait for the batch parent to complete
    JobAssertions.assertBatchCompleted(jobCrudStore, handle, Duration.ofSeconds(30));

    // The success callback should eventually fire
    JobAssertions.assertJobStatus(jobCrudStore, handle, JobStatus.SUCCEEDED);

    // Give the workflow branch time to execute
    try {
      Thread.sleep(2000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    assertEquals(2, BatchItemProcessor.processedCount());
    assertEquals(1, SimpleJob.getInvocationCount(), "Success callback should have fired once");
  }
}
