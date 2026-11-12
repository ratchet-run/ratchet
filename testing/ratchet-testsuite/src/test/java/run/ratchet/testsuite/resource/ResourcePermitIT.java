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
package run.ratchet.testsuite.resource;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.inject.Inject;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobHandle;
import run.ratchet.api.event.JobStartedEvent;
import run.ratchet.ri.core.ResourcePermitService;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionEntity;
import run.ratchet.store.entity.JobExecutionEntity.ExecutionStatus;
import run.ratchet.store.spi.JobAuditStore;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.JobStore;
import run.ratchet.testsuite.app.ResourceTestJob;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.observer.EventCapture;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.JobAssertions;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;

/** Validates resource permit concurrency limiting for jobs tagged with a shared resource. */
class ResourcePermitIT extends BaseRatchetIT {

  @Inject private TestJobService jobService;

  @Inject private JobCrudStore jobCrudStore;

  @Inject private ResourcePermitService resourcePermitService;

  @Inject private JobStore jobStore;

  @Inject private EventCapture eventCapture;

  @Deployment
  public static WebArchive createDeployment() {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
    String profile = System.getProperty("testsuite.profile", "wildfly-managed");

    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(profile, dbType)
        .addClasses(ResourceTestJob.class, TestJobService.class, EventCapture.class)
        .addStoreInfrastructure()
        .addBeansXml()
        .build();
  }

  @BeforeEach
  void resetState() {
    ResourceTestJob.reset();
    eventCapture.clear();
  }

  @Test
  void jobsWithinPermitLimit_shouldAllComplete() {
    resourcePermitService.configureResource("test-res", 3, 1000, "test");

    List<JobHandle> handles = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      handles.add(jobService.enqueue(ResourceTestJob::execute).withResource("test-res").submit());
    }

    // Wait for all jobs to complete
    await()
        .atMost(Duration.ofSeconds(30))
        .pollInterval(Duration.ofMillis(500))
        .until(() -> ResourceTestJob.getCompletedCount() >= 3);

    for (JobHandle handle : handles) {
      JobAssertions.assertJobCompleted(jobCrudStore, handle);
    }

    assertTrue(
        ResourceTestJob.getMaxConcurrentSeen() >= 2,
        "Permit limit of 3 should allow overlapping execution");
    assertTrue(
        ResourceTestJob.getMaxConcurrentSeen() <= 3,
        "Max concurrent should be <= 3 (permit limit) but was "
            + ResourceTestJob.getMaxConcurrentSeen());
  }

  @Test
  void jobsExceedingPermitLimit_shouldBeConcurrencyLimited() {
    resourcePermitService.configureResource("limited-res", 2, 500, "test");

    List<JobHandle> handles = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      handles.add(
          jobService.enqueue(ResourceTestJob::execute).withResource("limited-res").submit());
    }

    // Wait for all jobs to complete — longer timeout since jobs get rescheduled
    await()
        .atMost(Duration.ofSeconds(60))
        .pollInterval(Duration.ofMillis(500))
        .until(() -> ResourceTestJob.getCompletedCount() >= 5);

    for (JobHandle handle : handles) {
      JobAssertions.assertJobCompleted(jobCrudStore, handle);
    }

    // Critical invariant: never more than 2 concurrent
    assertTrue(
        ResourceTestJob.getMaxConcurrentSeen() <= 2,
        "Max concurrent should be <= 2 (permit limit) but was "
            + ResourceTestJob.getMaxConcurrentSeen());

    // Waiting for a permit is not an attempt: each job records one execution and one start.
    for (JobHandle handle : handles) {
      assertOneSucceededAttempt(handle);
    }
  }

  @Test
  void jobWaitingForPermit_recordsNoAttemptWhileItWaits() {
    resourcePermitService.configureResource("queue-res", 1, 200, "test");
    ResourceTestJob.closeGate();
    try {
      JobHandle holder =
          jobService.enqueue(ResourceTestJob::executeGated).withResource("queue-res").submit();
      await()
          .atMost(Duration.ofSeconds(30))
          .pollInterval(Duration.ofMillis(100))
          .until(() -> ResourceTestJob.getGatedRunning() == 1);

      JobHandle waiter =
          jobService.enqueue(ResourceTestJob::execute).withResource("queue-res").submit();
      await()
          .atMost(Duration.ofSeconds(30))
          .pollInterval(Duration.ofMillis(100))
          .until(
              () ->
                  "Waiting for resource: queue-res"
                      .equals(
                          jobCrudStore
                              .findById(waiter.id())
                              .map(JobEntity::getLastError)
                              .orElse(null)));

      List<JobExecutionEntity> executions = executions(waiter);
      assertTrue(executions.isEmpty(), "a waiting job has no executions, found " + executions);
      assertEquals(0, starts(waiter), "a waiting job has no JobStartedEvent");

      ResourceTestJob.openGate();
      JobAssertions.assertJobCompleted(jobCrudStore, holder);
      JobAssertions.assertJobCompleted(jobCrudStore, waiter);
      assertOneSucceededAttempt(holder);
      assertOneSucceededAttempt(waiter);
    } finally {
      ResourceTestJob.openGate();
    }
  }

  @Test
  void jobWithUnconfiguredResource_shouldFailHard() {
    JobHandle handle =
        jobService.enqueue(ResourceTestJob::execute).withResource("missing-resource").submit();

    JobAssertions.assertJobFailed(jobCrudStore, handle);
    assertEquals(0, ResourceTestJob.getCompletedCount(), "Job body should not execute");
  }

  private void assertOneSucceededAttempt(JobHandle handle) {
    await()
        .atMost(Duration.ofSeconds(10))
        .pollInterval(Duration.ofMillis(200))
        .untilAsserted(
            () -> {
              List<JobExecutionEntity> executions = executions(handle);
              assertEquals(1, executions.size(), "executions of job " + handle.id());
              assertEquals(ExecutionStatus.SUCCEEDED, executions.get(0).getStatus());
            });
    assertEquals(1, starts(handle), "JobStartedEvents for job " + handle.id());
  }

  private List<JobExecutionEntity> executions(JobHandle handle) {
    return jobStore
        .capability(JobAuditStore.class)
        .orElseThrow()
        .findExecutionsByJobId(handle.id(), 100, 0);
  }

  private long starts(JobHandle handle) {
    return eventCapture.getEvents(JobStartedEvent.class).stream()
        .filter(event -> handle.id().equals(event.getJobId()))
        .count();
  }
}
