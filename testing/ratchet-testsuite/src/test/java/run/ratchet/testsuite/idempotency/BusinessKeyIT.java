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
package run.ratchet.testsuite.idempotency;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.inject.Inject;
import java.time.Duration;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobStatus;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.testsuite.app.FailingJob;
import run.ratchet.testsuite.app.SimpleJob;
import run.ratchet.testsuite.app.SlowJob;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.JobAssertions;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;

/** Validates business key deduplication: duplicate active jobs with the same key are rejected. */
class BusinessKeyIT extends BaseRatchetIT {

  @Inject private TestJobService jobService;

  @Inject private JobCrudStore jobCrudStore;

  @Deployment
  public static WebArchive createDeployment() {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
    String profile = System.getProperty("testsuite.profile", "wildfly-managed");

    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(profile, dbType)
        .addClasses(FailingJob.class, SimpleJob.class, SlowJob.class, TestJobService.class)
        .addStoreInfrastructure()
        .addBeansXml()
        .build();
  }

  @BeforeEach
  void resetJobs() {
    FailingJob.resetCount();
    SimpleJob.resetCount();
    SlowJob.reset();
  }

  @Test
  void duplicateBusinessKey_whileActive_shouldBeRejected() {
    // Submit a slow job so it stays active
    SlowJob.setSleepMs(10_000);
    JobHandle first = jobService.enqueue(SlowJob::execute).withBusinessKey("user-123").submit();

    assertNotNull(first);
    JobAssertions.assertJobStatus(jobCrudStore, first, JobStatus.RUNNING);

    // Second submission with same business key while first is active should fail
    IllegalStateException thrown =
        assertThrows(
            IllegalStateException.class,
            () -> jobService.enqueue(SimpleJob::execute).withBusinessKey("user-123").submit(),
            "Should reject duplicate business key while first job is active");
    assertTrue(
        thrown.getMessage().contains("business key"),
        "Duplicate rejection should identify the business key constraint");
  }

  @Test
  void businessKey_afterCompletion_shouldAllowResubmission() {
    String businessKey = "user-456";
    JobHandle first = jobService.enqueue(SimpleJob::execute).withBusinessKey(businessKey).submit();

    JobAssertions.assertJobCompleted(jobCrudStore, first);

    // After first completes, same business key should work
    JobHandle second =
        jobService
            .schedule(Duration.ofMinutes(5), SimpleJob::execute)
            .withBusinessKey(businessKey)
            .submit();

    assertNotNull(second);
    assertActiveBusinessKeyOwner(businessKey, second);
  }

  @Test
  void businessKey_afterFailure_shouldAllowResubmission() {
    String businessKey = "user-789";
    JobHandle first = jobService.enqueue(FailingJob::execute).withBusinessKey(businessKey).submit();

    JobAssertions.assertJobFailed(jobCrudStore, first);

    JobHandle second =
        jobService
            .schedule(Duration.ofMinutes(5), SimpleJob::execute)
            .withBusinessKey(businessKey)
            .submit();

    assertNotNull(second);
    assertActiveBusinessKeyOwner(businessKey, second);
  }

  @Test
  void businessKey_afterCancellation_shouldAllowResubmission() {
    String businessKey = "user-999";
    JobHandle first =
        jobService
            .schedule(Duration.ofMinutes(5), SimpleJob::execute)
            .withBusinessKey(businessKey)
            .submit();

    assertTrue(jobService.cancelJob(first.id()), "Cancellation should transition the first job");
    JobAssertions.assertJobCanceled(jobCrudStore, first);

    JobHandle second =
        jobService
            .schedule(Duration.ofMinutes(5), SimpleJob::execute)
            .withBusinessKey(businessKey)
            .submit();

    assertNotNull(second);
    assertActiveBusinessKeyOwner(businessKey, second);
  }

  private void assertActiveBusinessKeyOwner(String businessKey, JobHandle expectedOwner) {
    var active = jobCrudStore.findActiveByBusinessKey(businessKey);

    assertTrue(active.isPresent(), "Replacement must take ownership of the business key");
    assertEquals(
        expectedOwner.id(),
        active.get().getId(),
        "findActiveByBusinessKey must return the live replacement job");
  }
}
