package run.ratchet.testsuite.core;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.JobHandle;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.testsuite.app.FailingJob;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.JobAssertions;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;
import jakarta.inject.Inject;
import java.time.Duration;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Validates retry behavior: failure → retry with backoff, configurable retry count. */
class JobRetryIT extends BaseRatchetIT {

  @Inject private TestJobService jobService;

  @Inject private JobCrudStore jobCrudStore;

  @Deployment
  public static WebArchive createDeployment() {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
    String profile = System.getProperty("testsuite.profile", "wildfly-managed");

    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(profile, dbType)
        .addClasses(FailingJob.class, TestJobService.class)
        .addStoreInfrastructure()
        .addBeansXml()
        .build();
  }

  @BeforeEach
  void resetJobs() {
    FailingJob.resetCount();
  }

  @Test
  void failingJob_withRetries_shouldRetryAndFail() {
    JobHandle handle =
        jobService
            .enqueue(FailingJob::execute)
            .withMaxRetries(2)
            .withBackoff(BackoffPolicy.FIXED, Duration.ofMillis(100))
            .submit();

    assertNotNull(handle);
    JobAssertions.assertJobFailed(jobCrudStore, handle);

    // Should have been attempted 3 times (1 initial + 2 retries)
    assertTrue(
        FailingJob.getAttemptCount() >= 3,
        "Expected at least 3 attempts but got " + FailingJob.getAttemptCount());
  }

  @Test
  void failingJob_withNoRetries_shouldFailImmediately() {
    JobHandle handle = jobService.enqueue(FailingJob::execute).submit();

    assertNotNull(handle);
    JobAssertions.assertJobFailed(jobCrudStore, handle);
  }
}
