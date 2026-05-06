package run.ratchet.testsuite.recurring;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.inject.Inject;
import java.time.Duration;
import java.time.ZoneOffset;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobHandle;
import run.ratchet.ri.core.RecurringScheduler;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.testsuite.app.CronTestJobs;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;

/** Validates programmatic recurring job scheduling via cron expression. */
class CronScheduleIT extends BaseRatchetIT {

  @Inject private TestJobService jobService;

  @Inject private JobCrudStore jobCrudStore;

  @Inject private RecurringScheduler recurringScheduler;

  @Deployment
  public static WebArchive createDeployment() {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
    String profile = System.getProperty("testsuite.profile", "wildfly-managed");

    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(profile, dbType)
        .addClasses(CronTestJobs.class, TestJobService.class)
        .addStoreInfrastructure()
        .addBeansXml()
        .build();
  }

  @BeforeEach
  void resetTrackers() {
    CronTestJobs.reset();
  }

  @AfterEach
  void stopRecurringScheduler() {
    recurringScheduler.stop();
  }

  @Test
  void scheduleRecurring_shouldFireAtCronRate() {
    // Fire every second (Quartz cron format: sec min hour day month day-of-week)
    JobHandle handle =
        jobService
            .scheduleRecurring("*/1 * * * * ?", ZoneOffset.UTC, CronTestJobs::tick)
            .submit();

    // Wait up to 5 seconds for at least 2 ticks
    await()
        .atMost(Duration.ofSeconds(8))
        .pollInterval(Duration.ofMillis(500))
        .untilAsserted(
            () ->
                assertTrue(
                    CronTestJobs.tickCount() >= 2,
                    "Expected at least 2 ticks but got " + CronTestJobs.tickCount()));
  }
}
