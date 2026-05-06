package run.ratchet.testsuite.core;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.inject.Inject;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobHandle;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.testsuite.app.SimpleJob;
import run.ratchet.testsuite.app.SlowJob;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.JobAssertions;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;

/** Validates cancel behavior: pending and running jobs can be canceled, terminal jobs cannot. */
class JobCancelIT extends BaseRatchetIT {

  @Inject private TestJobService jobService;

  @Inject private JobCrudStore jobCrudStore;

  @Deployment
  public static WebArchive createDeployment() {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
    String profile = System.getProperty("testsuite.profile", "wildfly-managed");

    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(profile, dbType)
        .addClasses(SimpleJob.class, SlowJob.class, TestJobService.class)
        .addStoreInfrastructure()
        .addBeansXml()
        .build();
  }

  @BeforeEach
  void resetJobs() {
    SimpleJob.resetCount();
    SlowJob.reset();
  }

  @Test
  void cancelPendingJob_shouldTransitionToCanceled() {
    // Schedule a job with a long delay so it stays in PENDING
    JobHandle handle = jobService.schedule(Duration.ofHours(1), SimpleJob::execute).submit();

    assertNotNull(handle);
    boolean canceled = jobService.cancelJob(handle.id());
    assertTrue(canceled, "Should be able to cancel a pending job");
    JobAssertions.assertJobCanceled(jobCrudStore, handle);
  }

  @Test
  void cancelAlreadyCompletedJob_shouldReturnFalse() {
    JobHandle handle = jobService.enqueueNow(SimpleJob::execute);

    assertNotNull(handle);
    JobAssertions.assertJobCompleted(jobCrudStore, handle);

    boolean canceled = jobService.cancelJob(handle.id());
    assertFalse(canceled, "Should not be able to cancel a completed job");
  }

  @Test
  void cancelRunningJob_shouldTransitionToCanceled() {
    // SlowJob sleeps for 60 seconds by default — long enough to be running when we cancel
    JobHandle handle = jobService.enqueueNow(SlowJob::execute);

    assertNotNull(handle);

    await()
        .atMost(Duration.ofSeconds(15))
        .pollInterval(100, TimeUnit.MILLISECONDS)
        .until(SlowJob::hasStarted);

    boolean canceled = jobService.cancelJob(handle.id());
    assertTrue(canceled, "Should be able to cancel a running job");

    JobAssertions.assertJobCanceled(jobCrudStore, handle);
  }
}
