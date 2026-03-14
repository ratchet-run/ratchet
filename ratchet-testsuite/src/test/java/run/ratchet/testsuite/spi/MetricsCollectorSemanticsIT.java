package run.ratchet.testsuite.spi;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.JobHandle;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.testsuite.app.ConfigurableWorkJob;
import run.ratchet.testsuite.app.FailingJob;
import run.ratchet.testsuite.app.RecordingMetricsCollector;
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

/** Verifies that the public MetricsCollector SPI receives correct lifecycle payloads. */
class MetricsCollectorSemanticsIT extends BaseRatchetIT {

  @Inject private TestJobService jobService;

  @Inject private run.ratchet.store.spi.JobCrudStore jobCrudStore;

  @Inject private MetricsCollector metricsCollector;

  @Deployment
  public static WebArchive createDeployment() {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
    String profile = System.getProperty("testsuite.profile", "wildfly-managed");

    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(profile, dbType)
        .addClasses(
            RecordingMetricsCollector.class,
            ConfigurableWorkJob.class,
            FailingJob.class,
            TestJobService.class)
        .addStoreInfrastructure()
        .addBeansXml()
        .build();
  }

  @BeforeEach
  void resetState() {
    RecordingMetricsCollector.reset();
    ConfigurableWorkJob.reset();
    FailingJob.resetCount();
  }

  @Test
  void completedMetrics_shouldReportPositiveExecutionTime() {
    ConfigurableWorkJob.setSleepMs(100);

    JobHandle handle = jobService.enqueue(ConfigurableWorkJob::execute).submit();
    JobAssertions.assertJobCompleted(jobCrudStore, handle);

    await()
        .atMost(Duration.ofSeconds(5))
        .untilAsserted(
            () -> {
              assertTrue(metricsCollector instanceof RecordingMetricsCollector);
              assertEquals(1, RecordingMetricsCollector.completedEvents().size());
              var completed = RecordingMetricsCollector.completedEvents().get(0);
              assertEquals(handle.id(), completed.jobId());
              assertTrue(
                  completed.executionTimeMs() >= 50,
                  "Expected a real execution duration but got " + completed.executionTimeMs());
            });
  }

  @Test
  void failedMetrics_shouldUseOneBasedAttemptsWithoutDuplicateRetryCallbacks() {
    JobHandle handle =
        jobService
            .enqueue(FailingJob::execute)
            .withMaxRetries(1)
            .withBackoff(BackoffPolicy.FIXED, Duration.ofMillis(50))
            .submit();

    JobAssertions.assertJobFailed(jobCrudStore, handle);

    await()
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(
            () -> {
              List<Integer> attempts =
                  RecordingMetricsCollector.failedEvents().stream()
                      .map(RecordingMetricsCollector.FailedMetric::attempt)
                      .toList();
              assertEquals(List.of(1, 2), attempts);
              assertTrue(
                  RecordingMetricsCollector.failedEvents().stream()
                      .allMatch(metric -> metric.jobId() == handle.id()));
            });
  }
}
