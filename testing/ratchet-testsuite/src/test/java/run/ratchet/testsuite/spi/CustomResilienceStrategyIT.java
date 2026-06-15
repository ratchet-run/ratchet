/*
 * Copyright 2026 Ratchet Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
import run.ratchet.spi.ResilienceStrategy;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.testsuite.app.FailingJob;
import run.ratchet.testsuite.app.NoOpResilienceStrategy;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.JobAssertions;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;

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
  void failingJob_shouldExhaustRetriesWithoutCircuitBreakerTripping() {
    assertInstanceOf(NoOpResilienceStrategy.class, resilienceStrategy);

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

    assertEquals(expectedAttempts, NoOpResilienceStrategy.getExecuteCount());
    assertEquals(
        expectedAttempts,
        NoOpResilienceStrategy.executedServices().stream()
            .filter("FailingJob.execute"::equals)
            .count(),
        "Every retry attempt must execute through the custom strategy with the resolved service");
    assertEquals(
        expectedAttempts,
        NoOpResilienceStrategy.checkedServices().stream()
            .filter("FailingJob.execute"::equals)
            .count(),
        "Every retry attempt must ask the custom strategy whether the service is available");
    assertTrue(
        resilienceStrategy.isServiceAvailable("FailingJob.execute"),
        "Custom no-op strategy must remain available after all retry failures");
  }
}
