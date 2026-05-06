package run.ratchet.testsuite.spi;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.inject.Inject;
import java.time.Duration;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.JobHandle;
import run.ratchet.spi.RetryPolicy;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.testsuite.app.FailingJob;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.app.VetoRetryPolicy;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.JobAssertions;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;

/** Validates that a custom {@link RetryPolicy} alternative overrides the default retry behavior. */
class CustomRetryPolicyIT extends BaseRatchetIT {

  @Inject private RetryPolicy retryPolicy;

  @Inject private TestJobService jobService;

  @Inject private JobCrudStore jobCrudStore;

  @Deployment
  public static WebArchive createDeployment() {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
    String profile = System.getProperty("testsuite.profile", "wildfly-managed");

    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(profile, dbType)
        .addClasses(VetoRetryPolicy.class, FailingJob.class, TestJobService.class)
        .addStoreInfrastructure()
        .addBeansXml()
        .build();
  }

  @BeforeEach
  void resetCounts() {
    VetoRetryPolicy.resetCounts();
    FailingJob.resetCount();
  }

  @Test
  void customRetryPolicy_shouldOverrideDefaultBackoff() {
    assertInstanceOf(VetoRetryPolicy.class, retryPolicy);

    JobHandle handle =
        jobService
            .enqueue(FailingJob::execute)
            .withMaxRetries(3)
            .withBackoff(BackoffPolicy.FIXED, Duration.ofMillis(100))
            .submit();

    assertNotNull(handle);
    JobAssertions.assertJobFailed(jobCrudStore, handle);

    // VetoRetryPolicy should have been consulted (at least once)
    assertTrue(
        VetoRetryPolicy.getShouldRetryCount() >= 1,
        "Expected VetoRetryPolicy.shouldRetry to be called but count was "
            + VetoRetryPolicy.getShouldRetryCount());

    // Job should have been attempted only once (no retries due to veto)
    assertEquals(
        1,
        FailingJob.getAttemptCount(),
        "Expected exactly 1 attempt (no retries) but got " + FailingJob.getAttemptCount());
  }
}
