package run.ratchet.testsuite.batch;

import static org.junit.jupiter.api.Assertions.assertEquals;

import jakarta.inject.Inject;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Stream;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobStatus;
import run.ratchet.api.StreamingBatchContext;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.testsuite.app.BatchItemProcessor;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.JobAssertions;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;

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
        .addStoreInfrastructure()
        .addBeansXml()
        .build();
  }

  @BeforeEach
  void resetTrackers() {
    BatchItemProcessor.reset();
  }

  @Test
  void streamingBatch_shouldProcessAllItemsInChunks() {
    Stream<String> items = Stream.of("s1", "s2", "s3", "s4", "s5", "s6");
    List<StreamingBatchContext> progressSnapshots = new CopyOnWriteArrayList<>();

    JobHandle handle =
        jobService
            .<String>streamingBatch("streaming-test")
            .fromStream(items)
            .process(BatchItemProcessor::process)
            .withChunkSize(2)
            .onProgress(progressSnapshots::add)
            .start();

    JobAssertions.assertBatchSucceeded(jobCrudStore, handle, Duration.ofSeconds(30));
    assertEquals(6, BatchItemProcessor.processedCount());
    assertEquals(Set.of("s1", "s2", "s3", "s4", "s5", "s6"), BatchItemProcessor.processedItems());

    assertEquals(
        3, progressSnapshots.size(), "Should report one streaming progress event per chunk");
    assertEquals(
        List.of(2, 4, 6),
        progressSnapshots.stream().map(StreamingBatchContext::processedItems).toList());
    assertEquals(
        List.of(1, 2, 3),
        progressSnapshots.stream().map(StreamingBatchContext::chunksInserted).toList());
  }

  @Test
  void streamingBatch_shouldFailWhenProcessorThrows() {
    Stream<String> items = Stream.of("s1", "s2", "s3", "s4");

    JobHandle handle =
        jobService
            .<String>streamingBatch("streaming-failure-test")
            .fromStream(items)
            .process(BatchItemProcessor::failOnS3)
            .withChunkSize(2)
            .start();

    JobAssertions.assertJobStatus(jobCrudStore, handle, JobStatus.FAILED);
    assertEquals(
        JobStatus.FAILED,
        jobCrudStore.getJobStatus(handle.id()),
        "A failed streaming child should fail the batch parent");
  }
}
