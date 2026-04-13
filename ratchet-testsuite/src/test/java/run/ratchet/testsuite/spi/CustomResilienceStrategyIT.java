package run.ratchet.testsuite.spi;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.JobHandle;
import run.ratchet.spi.ResilienceStrategy;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.testsuite.app.FailingJob;
import run.ratchet.testsuite.app.NoOpResilienceStrategy;
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

/**
 * Validates that a custom {@link ResilienceStrategy} alternative overrides the default circuit
 * breaker behavior.
 */
class CustomResilienceStrategyIT extends BaseRatchetIT {

  @Inject private ResilienceStrategy resilienceStrategy;

  @Inject private TestJobService jobService;

  @Inject private JobCrudStore jobCrudStore;

  @Deployment
  public static WebArchive createDeployment() {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
    String profile = System.getProperty("testsuite.profile", "wildfly-managed");

    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(profile, dbType)
        .addClasses(NoOpResilienceStrategy.class, FailingJob.class, TestJobService.class)
        .addStoreInfrastructure()
        .addBeansXml()
        .build();
  }

  @BeforeEach
  void resetCounts() {
    NoOpResilienceStrategy.resetCounts();
    FailingJob.resetCount();
  }

  @Test
  void customResilienceStrategy_shouldOverrideDefaultCircuitBreaker() {
    assertInstanceOf(NoOpResilienceStrategy.class, resilienceStrategy);
  }

  @Test
  void customResilienceStrategy_serviceAlwaysAvailable() {
    // No-op strategy should always report services as available
    assertTrue(
        resilienceStrategy.isServiceAvailable("any-service"),
        "NoOpResilienceStrategy should always report service as available");
  }

  @Test
  void failingJob_shouldExhaustRetriesWithoutCircuitBreakerTripping() {
    int maxRetries = 3;

    JobHandle handle =
        jobService
            .enqueue(FailingJob::execute)
            .withMaxRetries(maxRetries)
            .withBackoff(BackoffPolicy.FIXED, Duration.ofMillis(100))
            .submit();

    assertNotNull(handle);
    JobAssertions.assertJobFailed(jobCrudStore, handle);

    int expectedAttempts = 1 + maxRetries;
    assertEquals(expectedAttempts, FailingJob.getAttemptCount());

    assertTrue(NoOpResilienceStrategy.getExecuteCount() >= expectedAttempts);
  }
}
