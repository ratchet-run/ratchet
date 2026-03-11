package run.ratchet.testsuite.batch;

import static org.junit.jupiter.api.Assertions.assertEquals;

import run.ratchet.api.JobHandle;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.testsuite.app.BatchItemProcessor;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.JobAssertions;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.stream.Stream;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Validates streaming batch processing with chunked item insertion. */
class StreamingBatchIT extends BaseRatchetIT {

  @Inject private TestJobService jobService;

  @Inject private JobCrudStore jobCrudStore;

  @Deployment
  public static WebArchive createDeployment() {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
    String profile = System.getProperty("testsuite.profile", "wildfly-managed");

    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(profile, dbType)
        .addClasses(BatchItemProcessor.class, TestJobService.class)
        .addTestInfrastructure()
        .addBeansXml()
        .addPersistenceXml(dbType)
        .addDataSource()
        .build();
  }

  @BeforeEach
  void resetTrackers() {
    BatchItemProcessor.reset();
  }

  @Test
  void streamingBatch_shouldProcessAllItemsInChunks() {
    Stream<String> items = Stream.of("s1", "s2", "s3", "s4", "s5", "s6");

    JobHandle handle =
        jobService
            .<String>streamingBatch("streaming-test")
            .fromStream(items)
            .process(BatchItemProcessor::process)
            .withChunkSize(2)
            .start();

    JobAssertions.assertBatchCompleted(jobCrudStore, handle, Duration.ofSeconds(30));
    assertEquals(6, BatchItemProcessor.processedCount());
  }
}
