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
package run.ratchet.testsuite.lifecycle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.inject.Inject;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobStatus;
import run.ratchet.ri.core.DrainController;
import run.ratchet.ri.core.internal.Poller;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.testsuite.app.SimpleJob;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.JobAssertions;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;

/**
 * Validates that drain mode blocks and resumes job claims in a real WildFly deployment. Ordering
 * (drain engaged before poller.stop()) is verified separately by {@code
 * RatchetLifecycleShutdownTest} using Mockito InOrder.
 */
class DrainShutdownIT extends BaseRatchetIT {

  @Inject private TestJobService jobService;
  @Inject private JobCrudStore jobCrudStore;
  @Inject private DrainController drainController;
  @Inject private Poller poller;

  @Deployment
  public static WebArchive createDeployment() {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
    String profile = System.getProperty("testsuite.profile", "wildfly-managed");

    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(profile, dbType)
        .addClasses(SimpleJob.class, TestJobService.class)
        .addStoreInfrastructure()
        .addBeansXml()
        .build();
  }

  @BeforeEach
  void resetJobCounter() {
    drainController.setDraining(false);
    SimpleJob.resetCount();
  }

  @Override
  @AfterEach
  protected void cleanupAfterEach() throws Exception {
    try {
      super.cleanupAfterEach();
    } finally {
      drainController.setDraining(false);
      SimpleJob.resetCount();
    }
  }

  @Test
  void drainModeBlocksNewJobClaims() {
    JobHandle handle = null;
    drainController.setDraining(true);
    try {
      handle = jobService.enqueue(SimpleJob::execute).immediate().submit();
      assertNotNull(handle);

      assertDrainedPollDoesNotClaim(handle, SimpleJob.getInvocationCount());
    } finally {
      deleteIfPending(handle);
      drainController.setDraining(false);
    }
  }

  @Test
  void disablingDrainModeResumesJobClaims() {
    assertFalse(drainController.isDraining(), "Drain should be off at start of this test");

    JobHandle handle = jobService.enqueue(SimpleJob::execute).immediate().submit();
    assertNotNull(handle);

    JobAssertions.assertJobCompleted(jobCrudStore, handle);
    assertTrue(
        SimpleJob.getInvocationCount() >= 1,
        "SimpleJob.execute() must have run by the time the job is SUCCEEDED");
  }

  @Test
  void drainModeCanBeReenabledAfterResume() {
    assertFalse(drainController.isDraining(), "Drain should be off at start of this test");

    JobHandle first = jobService.enqueue(SimpleJob::execute).immediate().submit();
    assertNotNull(first);
    JobAssertions.assertJobCompleted(jobCrudStore, first);

    JobHandle blocked = null;
    try {
      drainController.setDraining(true);
      blocked = jobService.enqueue(SimpleJob::execute).immediate().submit();
      assertNotNull(blocked);

      assertDrainedPollDoesNotClaim(blocked, SimpleJob.getInvocationCount());

      drainController.setDraining(false);
      JobAssertions.assertJobCompleted(jobCrudStore, blocked);

      drainController.setDraining(true);
      JobHandle blockedAgain = jobService.enqueue(SimpleJob::execute).immediate().submit();
      assertNotNull(blockedAgain);
      blocked = blockedAgain;

      assertDrainedPollDoesNotClaim(blockedAgain, SimpleJob.getInvocationCount());
    } finally {
      deleteIfPending(blocked);
      drainController.setDraining(false);
    }
  }

  private void assertDrainedPollDoesNotClaim(JobHandle handle, int invocationCountBeforePoll) {
    assertTrue(drainController.isDraining(), "Drain must be active for this assertion");

    poller.tick();

    JobStatus status = jobCrudStore.getJobStatus(handle.id());
    assertEquals(
        JobStatus.PENDING,
        status,
        "Job must remain PENDING while drain mode is active (poller must not claim)");
    assertEquals(
        invocationCountBeforePoll,
        SimpleJob.getInvocationCount(),
        "SimpleJob.execute() count must not change while drain mode blocks claims");
  }

  private void deleteIfPending(JobHandle handle) {
    if (handle != null && jobCrudStore.getJobStatus(handle.id()) == JobStatus.PENDING) {
      jobCrudStore.delete(handle.id());
    }
  }
}
