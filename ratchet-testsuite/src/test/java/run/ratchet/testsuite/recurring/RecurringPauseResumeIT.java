package run.ratchet.testsuite.recurring;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import run.ratchet.api.JobHandle;
import run.ratchet.store.entity.JobStatus;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.testsuite.app.CronTestJobs;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.JobAssertions;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;
import jakarta.inject.Inject;
import java.time.Duration;
import java.time.ZoneOffset;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Validates pause/resume behavior for recurring jobs.
 *
 * <p>Tests that pausing a recurring job prevents it from firing and resuming re-enables execution.
 */
class RecurringPauseResumeIT extends BaseRatchetIT {

  @Inject private TestJobService jobService;

  @Inject private JobCrudStore jobCrudStore;

  @Deployment
  public static WebArchive createDeployment() {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
    String profile = System.getProperty("testsuite.profile", "wildfly-managed");

    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(profile, dbType)
        .addClasses(CronTestJobs.class, TestJobService.class)
        .addTestInfrastructure()
        .addBeansXml()
        .addPersistenceXml(dbType)
        .addDataSource()
        .build();
  }

  @BeforeEach
  void resetTrackers() {
    CronTestJobs.reset();
  }

  @Test
  void pauseRecurringJob_shouldStopFiring() {
    // Schedule a recurring job that fires every second
    JobHandle handle =
        jobService
            .scheduleRecurring("*/1 * * * * ?", ZoneOffset.UTC, CronTestJobs::tick)
            .submit();

    // Wait for at least 1 tick to confirm it's running
    await()
        .atMost(Duration.ofSeconds(5))
        .pollInterval(Duration.ofMillis(500))
        .untilAsserted(
            () ->
                assertTrue(
                    CronTestJobs.tickCount() >= 1,
                    "Expected at least 1 tick but got " + CronTestJobs.tickCount()));

    // Pause the recurring job
    boolean paused = jobService.pauseJob(handle.id());
    assertTrue(paused, "Expected pauseJob to return true");

    // Verify the job is in PAUSED status
    JobAssertions.assertJobStatus(jobCrudStore, handle, JobStatus.PAUSED);

    // Record tick count at pause time and verify it remains stable while paused.
    int ticksAtPause = CronTestJobs.tickCount();
    await()
        .during(Duration.ofSeconds(3))
        .atMost(Duration.ofSeconds(5))
        .untilAsserted(
            () ->
                assertEquals(
                    ticksAtPause,
                    CronTestJobs.tickCount(),
                    "Expected no new ticks after pause but got "
                        + (CronTestJobs.tickCount() - ticksAtPause)
                        + " additional ticks"));
  }

  @Test
  void resumePausedRecurringJob_shouldRestartFiring() {
    // Schedule and immediately pause
    JobHandle handle =
        jobService
            .scheduleRecurring("*/1 * * * * ?", ZoneOffset.UTC, CronTestJobs::tick)
            .submit();

    // Wait for the recurring job to be registered
    await()
        .atMost(Duration.ofSeconds(5))
        .pollInterval(Duration.ofMillis(500))
        .untilAsserted(
            () ->
                assertTrue(
                    CronTestJobs.tickCount() >= 1,
                    "Expected at least 1 tick but got " + CronTestJobs.tickCount()));

    // Pause
    assertTrue(jobService.pauseJob(handle.id()));
    JobAssertions.assertJobStatus(jobCrudStore, handle, JobStatus.PAUSED);

    // Record tick count while paused
    int ticksBeforeResume = CronTestJobs.tickCount();

    // Resume
    assertTrue(jobService.resumeJob(handle.id()), "Expected resumeJob to return true");

    // Verify it starts firing again
    await()
        .atMost(Duration.ofSeconds(5))
        .pollInterval(Duration.ofMillis(500))
        .untilAsserted(
            () ->
                assertTrue(
                    CronTestJobs.tickCount() > ticksBeforeResume,
                    "Expected new ticks after resume but count stayed at "
                        + CronTestJobs.tickCount()));
  }
}
