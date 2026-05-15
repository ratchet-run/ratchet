package run.ratchet.testsuite.recurring;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.inject.Inject;
import java.time.Duration;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobStatus;
import run.ratchet.ri.core.RecurringScheduler;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.testsuite.app.TestCleanupStrategy;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.app.TestRecurringJobs;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;

/**
 * Validates that {@code @Recurring} annotated methods are discovered by the CDI extension and
 * scheduled for execution.
 */
class RecurringAnnotationIT extends BaseRatchetIT {

  private static final String RECURRING_SCHEDULER_LEASE = "recurringScheduler";

  @Inject private JobCrudStore jobCrudStore;

  @Inject private RecurringScheduler recurringScheduler;

  @Inject private TestCleanupStrategy cleanupStrategy;

  @Deployment
  public static WebArchive createDeployment() {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
    String profile = System.getProperty("testsuite.profile", "wildfly-managed");

    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(profile, dbType)
        .addClasses(TestRecurringJobs.class, TestJobService.class)
        .addStoreInfrastructure()
        .addBeansXml()
        .build();
  }

  @BeforeEach
  void resetCounts() {
    TestRecurringJobs.resetCounts();
  }

  @AfterEach
  void stopRecurringScheduler() {
    recurringScheduler.stop();
  }

  @Test
  void recurringAnnotation_shouldBeDiscoveredAndScheduled() {
    // The @Recurring(cron = "*/5 * * * * ?") method should fire repeatedly after deployment.
    await()
        .atMost(Duration.ofSeconds(20))
        .pollInterval(Duration.ofSeconds(1))
        .untilAsserted(
            () ->
                assertTrue(
                    TestRecurringJobs.getEveryFiveSecondsCount() >= 2,
                    "Expected @Recurring method to fire at least twice but count was "
                        + TestRecurringJobs.getEveryFiveSecondsCount()));

    var recurringJob =
        jobCrudStore
            .findActiveByBusinessKey(TestRecurringJobs.EVERY_FIVE_SECONDS_JOB_ID)
            .orElseThrow();
    assertEquals(JobExecutionType.RECURRING, recurringJob.getJobType());
    assertEquals(JobStatus.PENDING, recurringJob.getStatus());
    assertNotNull(recurringJob.getNextFire());
  }

  @Override
  @BeforeEach
  protected void truncateAll() {
    // The @Recurring master is registered during CDI startup, so keep scheduler_job intact.
    // Clear only this singleton lease so residue from a previous deployment cannot block polling.
    cleanupStrategy.deleteSchedulerLock(RECURRING_SCHEDULER_LEASE);
  }
}
