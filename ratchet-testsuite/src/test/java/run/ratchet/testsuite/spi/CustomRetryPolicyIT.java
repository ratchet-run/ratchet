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

import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
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
        .addClasses(FastRetryPolicy.class, FailingJob.class, TestJobService.class)
        .addStoreInfrastructure()
        .addBeansXml()
        .build();
  }

  @BeforeEach
  void resetCounts() {
    FastRetryPolicy.resetCounts();
    FailingJob.resetCount();
  }

  @Test
  void customRetryPolicy_shouldOverrideDefaultBackoff() {
    assertInstanceOf(FastRetryPolicy.class, retryPolicy);

    JobHandle handle =
        jobService
            .enqueue(FailingJob::execute)
            .withMaxRetries(3)
            .withBackoff(BackoffPolicy.FIXED, Duration.ofMinutes(5))
            .submit();

    assertNotNull(handle);
    JobAssertions.assertJobFailed(jobCrudStore, handle, Duration.ofSeconds(10));

    assertEquals(3, FastRetryPolicy.getShouldRetryCount());
    assertEquals(3, FastRetryPolicy.getDelayCount());
    assertEquals(
        4,
        FailingJob.getAttemptCount(),
        "Expected the policy-provided delay to allow fast retries instead of waiting for "
            + "the job-level five-minute backoff");
  }

  @Alternative
  @Priority(1)
  @ApplicationScoped
  public static class FastRetryPolicy implements RetryPolicy {

    private static final AtomicInteger SHOULD_RETRY_COUNT = new AtomicInteger();
    private static final AtomicInteger DELAY_COUNT = new AtomicInteger();

    static int getShouldRetryCount() {
      return SHOULD_RETRY_COUNT.get();
    }

    static int getDelayCount() {
      return DELAY_COUNT.get();
    }

    static void resetCounts() {
      SHOULD_RETRY_COUNT.set(0);
      DELAY_COUNT.set(0);
    }

    @Override
    public boolean shouldRetry(int attempt, Throwable cause) {
      SHOULD_RETRY_COUNT.incrementAndGet();
      return true;
    }

    @Override
    public Duration getDelay(int attempt) {
      DELAY_COUNT.incrementAndGet();
      return Duration.ofMillis(10);
    }
  }
}
