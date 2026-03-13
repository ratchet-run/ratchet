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
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Validates that a custom {@link ResilienceStrategy} alternative overrides the default circuit
 * breaker behavior.
 *
 * <p>Deploys a {@link NoOpResilienceStrategy} that passes through all executions without any
 * circuit breaker protection. A job configured with multiple retries should fail after exhausting
 * all attempts — the circuit breaker should never trip because the no-op strategy has no breaker.
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
        .addTestInfrastructure()
        .addBeansXml()
        .addPersistenceXml(dbType)
        .addDataSource()
        .build();
  }

  @BeforeEach
  void resetCounts() {
    NoOpResilienceStrategy.resetCounts();
    FailingJob.resetCount();
  }

  @Test
  void customResilienceStrategy_shouldOverrideDefaultCircuitBreaker() {
    // Verify CDI selected the custom alternative
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

    // Submit a failing job with retries — NoOpResilienceStrategy has no breaker,
    // so all attempts should proceed without ServiceUnavailableException
    JobHandle handle =
        jobService
            .enqueue(FailingJob::execute)
            .withMaxRetries(maxRetries)
            .withBackoff(BackoffPolicy.FIXED, java.time.Duration.ofMillis(100))
            .submit();

    assertNotNull(handle);
    JobAssertions.assertJobFailed(jobCrudStore, handle);

    // The job should have been attempted 1 (initial) + maxRetries times
    int expectedAttempts = 1 + maxRetries;
    assertEquals(
        expectedAttempts,
        FailingJob.getAttemptCount(),
        "Expected "
            + expectedAttempts
            + " attempts (initial + "
            + maxRetries
            + " retries) but got "
            + FailingJob.getAttemptCount());

    // The NoOpResilienceStrategy.execute() should have been called for each attempt
    assertTrue(
        NoOpResilienceStrategy.getExecuteCount() >= expectedAttempts,
        "Expected NoOpResilienceStrategy.execute() to be called at least "
            + expectedAttempts
            + " times but was called "
            + NoOpResilienceStrategy.getExecuteCount());
  }
}
