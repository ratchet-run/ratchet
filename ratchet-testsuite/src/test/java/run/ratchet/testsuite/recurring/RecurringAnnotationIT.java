package run.ratchet.testsuite.recurring;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertTrue;

import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.app.TestRecurringJobs;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;
import jakarta.inject.Inject;
import java.time.Duration;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Validates that {@code @Recurring} annotated methods are discovered by the CDI extension and
 * scheduled for execution.
 */
class RecurringAnnotationIT extends BaseRatchetIT {

  @Inject private TestJobService jobService;

  @Inject private JobCrudStore jobCrudStore;

  @Deployment
  public static WebArchive createDeployment() {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
    String profile = System.getProperty("testsuite.profile", "wildfly-managed");

    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(profile, dbType)
        .addClasses(TestRecurringJobs.class, TestJobService.class)
        .addTestInfrastructure()
        .addBeansXml()
        .addPersistenceXml(dbType)
        .addDataSource()
        .build();
  }

  @Override
  @BeforeEach
  protected void truncateAll() {
    // Skip table truncation — the @Recurring job registered during CDI startup is the
    // subject under test. Truncating scheduler_job would destroy the recurring master
    // that RecurringJobProcessor registered at deployment time.
  }

  @BeforeEach
  void resetCounts() {
    TestRecurringJobs.resetCounts();
  }

  @Test
  void recurringAnnotation_shouldBeDiscoveredAndScheduled() {
    // The @Recurring(cron = "*/5 * * * * ?") method should fire quickly after deployment.
    await()
        .atMost(Duration.ofSeconds(15))
        .pollInterval(Duration.ofSeconds(1))
        .untilAsserted(
            () ->
                assertTrue(
                    TestRecurringJobs.getEveryMinuteCount() >= 1,
                    "Expected @Recurring method to fire at least once but count was "
                        + TestRecurringJobs.getEveryMinuteCount()));
  }
}
