package run.ratchet.testsuite.spi;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.inject.Inject;
import java.time.Duration;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.testsuite.app.CountingMetricsCollector;
import run.ratchet.testsuite.app.SimpleJob;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.JobAssertions;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;

/** Validates that a custom {@link MetricsCollector} alternative receives job metrics. */
class CustomMetricsCollectorIT extends BaseRatchetIT {

  @Inject private MetricsCollector metricsCollector;

  @Inject private TestJobService jobService;

  @Inject private JobCrudStore jobCrudStore;

  @Deployment
  public static WebArchive createDeployment() {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
    String profile = System.getProperty("testsuite.profile", "wildfly-managed");

    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(profile, dbType)
        .addClasses(CountingMetricsCollector.class, SimpleJob.class, TestJobService.class)
        .addStoreInfrastructure()
        .addBeansXml()
        .build();
  }

  @BeforeEach
  void resetCounts() {
    CountingMetricsCollector.resetCounts();
    SimpleJob.resetCount();
  }

  @Test
  void customMetricsCollector_shouldReceiveJobMetrics() {
    assertInstanceOf(CountingMetricsCollector.class, metricsCollector);

    var handle = jobService.enqueueNow(SimpleJob::execute);
    JobAssertions.assertJobCompleted(jobCrudStore, handle);

    await()
        .atMost(Duration.ofSeconds(5))
        .untilAsserted(
            () -> {
              assertTrue(
                  CountingMetricsCollector.getStartedCount() >= 1,
                  "Expected jobStarted to be called at least once but was "
                      + CountingMetricsCollector.getStartedCount());
              assertTrue(
                  CountingMetricsCollector.getCompletedCount() >= 1,
                  "Expected jobCompleted to be called at least once but was "
                      + CountingMetricsCollector.getCompletedCount());
            });
  }
}
