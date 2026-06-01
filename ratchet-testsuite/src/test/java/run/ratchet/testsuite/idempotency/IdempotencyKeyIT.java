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

import jakarta.inject.Inject;
import java.time.Duration;
import java.util.UUID;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobPriority;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.testsuite.app.FailingJob;
import run.ratchet.testsuite.app.SimpleJob;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.JobAssertions;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;

class IdempotencyKeyIT extends BaseRatchetIT {

  @Inject private TestJobService jobService;

  @Inject private JobCrudStore jobCrudStore;

  @Deployment
  public static WebArchive createDeployment() {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
    String profile = System.getProperty("testsuite.profile", "wildfly-managed");

    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(profile, dbType)
        .addClasses(SimpleJob.class, FailingJob.class, TestJobService.class)
        .addStoreInfrastructure()
        .addBeansXml()
        .build();
  }

  @BeforeEach
  void resetJobs() {
    SimpleJob.resetCount();
    FailingJob.resetCount();
  }

  @Test
  void duplicateIdempotencyKey_shouldReturnExistingHandle() {
    String key = "webhook-delivery-12345";

    JobHandle first = jobService.enqueue(SimpleJob::execute).withIdempotencyKey(key).submit();
    assertNotNull(first);

    // Second submission with same key should return the existing job (idempotent)
    JobHandle second = jobService.enqueue(SimpleJob::execute).withIdempotencyKey(key).submit();
    assertNotNull(second);

    assertEquals(first.id(), second.id(), "Duplicate idempotency key should return same job ID");
    JobAssertions.assertJobCompleted(jobCrudStore, first);
    assertEquals(1, SimpleJob.getInvocationCount());
  }

  @Test
  void idempotencyKey_afterCompletion_shouldReturnExistingHandle() {
    String key = UUID.randomUUID().toString();

    JobHandle first = jobService.enqueue(SimpleJob::execute).withIdempotencyKey(key).submit();
    JobAssertions.assertJobCompleted(jobCrudStore, first);

    JobHandle second = jobService.enqueue(SimpleJob::execute).withIdempotencyKey(key).submit();

    assertNotNull(second);
    assertEquals(
        first.id(),
        second.id(),
        "Completed jobs permanently reserve their idempotency key and return the original job");
    assertEquals(1, SimpleJob.getInvocationCount());
  }

  @Test
  void duplicateIdempotencyKey_afterCompletionWithDifferentTask_shouldReturnExistingHandle() {
    String key = UUID.randomUUID().toString();

    JobHandle first = jobService.enqueue(SimpleJob::execute).withIdempotencyKey(key).submit();
    JobAssertions.assertJobCompleted(jobCrudStore, first);

    JobHandle second =
        jobService
            .enqueue(FailingJob::execute)
            .withIdempotencyKey(key)
            .withPriority(JobPriority.CRITICAL)
            .withTimeout(Duration.ofSeconds(1))
            .withParam("payload", "changed")
            .submit();

    assertNotNull(second);
    assertEquals(
        first.id(),
        second.id(),
        "Duplicate idempotency key must return the original job even when task/config changes");
    assertEquals(1, SimpleJob.getInvocationCount());
    assertEquals(0, FailingJob.getAttemptCount(), "Changed duplicate task must not execute");
  }

  @Test
  void differentIdempotencyKeys_shouldBothExecute() {
    JobHandle first = jobService.enqueue(SimpleJob::execute).withIdempotencyKey("key-a").submit();

    JobHandle second = jobService.enqueue(SimpleJob::execute).withIdempotencyKey("key-b").submit();

    assertNotNull(first);
    assertNotNull(second);

    JobAssertions.assertJobCompleted(jobCrudStore, first);
    JobAssertions.assertJobCompleted(jobCrudStore, second);
    assertEquals(2, SimpleJob.getInvocationCount());
  }
}
