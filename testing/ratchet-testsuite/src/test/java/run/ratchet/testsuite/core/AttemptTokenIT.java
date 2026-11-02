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
package run.ratchet.testsuite.core;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.inject.Inject;
import java.time.Duration;
import java.time.ZoneOffset;
import java.util.List;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.JobHandle;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.RecurringJobStore;
import run.ratchet.testsuite.app.AttemptTokenJob;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.JobAssertions;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;

/** Verifies claim identity and freshness through the real scheduler for each job shape. */
class AttemptTokenIT extends BaseRatchetIT {
  @Inject private TestJobService jobService;
  @Inject private JobCrudStore jobCrudStore;
  @Inject private RecurringJobStore recurringJobStore;

  @Deployment
  public static WebArchive createDeployment() {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
    String profile = System.getProperty("testsuite.profile", "wildfly-managed");
    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(profile, dbType)
        .addClasses(AttemptTokenJob.class, AttemptTokenJob.Attempt.class, TestJobService.class)
        .addStoreInfrastructure()
        .addBeansXml()
        .build();
  }

  @BeforeEach
  void resetJob() {
    AttemptTokenJob.reset();
  }

  @Test
  void retrySupersedesFirstAttemptAndCompletionEndsCurrentAttempt() {
    JobHandle handle =
        jobService
            .enqueue(AttemptTokenJob::retryAndBlock)
            .withMaxRetries(1)
            .withBackoff(BackoffPolicy.FIXED, Duration.ZERO)
            .submit();
    try {
      var second = blocked(0);
      var attempts = AttemptTokenJob.attempts(handle.id());
      assertEquals(2, attempts.size());
      var first = attempts.get(0);
      assertEquals(handle.id(), first.token().jobId());
      assertEquals(handle.id(), second.token().jobId());
      assertTrue(second.token().isNewerThan(first.token()));
      assertFalse(first.context().isCurrentAttempt());
      assertTrue(second.context().isCurrentAttempt());
      second.release().countDown();
      JobAssertions.assertJobCompleted(jobCrudStore, handle);
      assertFalse(second.context().isCurrentAttempt());
    } finally {
      AttemptTokenJob.releaseAll();
    }
  }

  @Test
  void batchChildHasItsOwnCurrentToken() {
    JobHandle parent =
        jobService
            .enqueueBatch("attempt-token-batch")
            .forEach(List.of("child"), AttemptTokenJob::batchItem)
            .submit();
    try {
      var child = blocked(0);
      assertNotEquals(parent.id(), child.context().jobId());
      assertCurrent(child);
      assertTrue(jobCrudStore.findById(child.context().jobId()).isPresent());
      child.release().countDown();
      JobAssertions.assertBatchSucceeded(jobCrudStore, parent, Duration.ofSeconds(30));
    } finally {
      AttemptTokenJob.releaseAll();
    }
  }

  @Test
  void secondChainStepHasItsOwnCurrentToken() {
    JobHandle first =
        jobService.enqueue(AttemptTokenJob::succeed).then(AttemptTokenJob::block).submit();
    try {
      var second = blocked(0);
      var dependants = jobCrudStore.findDependants(first.id(), JobCrudStore.DEFAULT_PAGE_LIMIT, 0);
      assertEquals(1, dependants.size());
      assertEquals(dependants.get(0).getId(), second.token().jobId());
      assertNotEquals(first.id(), second.token().jobId());
      assertCurrent(second);
      second.release().countDown();
      JobAssertions.assertChainCompleted(jobCrudStore, first, 2, Duration.ofSeconds(30));
    } finally {
      AttemptTokenJob.releaseAll();
    }
  }

  @Test
  void workflowBranchHasItsOwnCurrentToken() {
    JobHandle parent =
        jobService.enqueue(AttemptTokenJob::succeed).thenOnSuccess(AttemptTokenJob::block).submit();
    try {
      var branch = blocked(0);
      var dependants = jobCrudStore.findDependants(parent.id(), JobCrudStore.DEFAULT_PAGE_LIMIT, 0);
      assertEquals(1, dependants.size());
      assertEquals(dependants.get(0).getId(), branch.token().jobId());
      assertNotEquals(parent.id(), branch.token().jobId());
      assertCurrent(branch);
      branch.release().countDown();
      JobAssertions.assertJobCompleted(jobCrudStore, branch.token()::jobId);
    } finally {
      AttemptTokenJob.releaseAll();
    }
  }

  @Test
  void recurringFiringsHaveDifferentUnorderedTokens() {
    JobHandle schedule =
        jobService
            .scheduleRecurring("*/1 * * * * ?", ZoneOffset.UTC, AttemptTokenJob::block)
            .submit();
    try {
      var first = blocked(0);
      assertCurrent(first);
      assertNotEquals(schedule.id(), first.token().jobId());
      first.release().countDown();
      JobAssertions.assertJobCompleted(jobCrudStore, first.token()::jobId);
      var second = blocked(1);
      assertCurrent(second);
      assertNotEquals(schedule.id(), second.token().jobId());
      assertNotEquals(first.token().jobId(), second.token().jobId());
      assertThrows(IllegalArgumentException.class, () -> second.token().isNewerThan(first.token()));
      second.release().countDown();
      JobAssertions.assertJobCompleted(jobCrudStore, second.token()::jobId);
    } finally {
      try {
        recurringJobStore.pauseRecurring(schedule.id());
      } finally {
        AttemptTokenJob.releaseAll();
      }
    }
  }

  @Test
  void failureCallbackHasNoAttemptToken() {
    JobHandle handle =
        jobService
            .enqueue(AttemptTokenJob::fail)
            .withMaxRetries(0)
            .onFailure((ctx, failure) -> AttemptTokenJob.recordFailureCallback())
            .submit();
    JobAssertions.assertJobFailed(jobCrudStore, handle);
    await()
        .atMost(Duration.ofSeconds(10))
        .until(() -> AttemptTokenJob.FAILURE_CONTEXT.get() != null);
    var callback = AttemptTokenJob.FAILURE_CONTEXT.get();
    assertTrue(AttemptTokenJob.FAILURE_TOKEN.get().isEmpty());
    assertFalse(AttemptTokenJob.FAILURE_CURRENT.get());
    assertTrue(callback.attemptToken().isEmpty());
    assertFalse(callback.isCurrentAttempt());
  }

  private AttemptTokenJob.Attempt blocked(int index) {
    await().atMost(Duration.ofSeconds(30)).until(() -> AttemptTokenJob.blocked().size() > index);
    return AttemptTokenJob.blocked().get(index);
  }

  private void assertCurrent(AttemptTokenJob.Attempt attempt) {
    assertEquals(attempt.context().jobId(), attempt.token().jobId());
    assertTrue(attempt.context().isCurrentAttempt());
  }
}
