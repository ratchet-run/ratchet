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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.inject.Inject;
import jakarta.transaction.Status;
import jakarta.transaction.UserTransaction;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.api.JobStatus;
import run.ratchet.api.ResourceLimit;
import run.ratchet.api.event.JobStartedEvent;
import run.ratchet.api.exception.ResourceNotConfiguredException;
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

  @Inject private JobSchedulerService scheduler;

  @Inject private UserTransaction utx;

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
    setLimit("test-res", 3, 1000);

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
    setLimit("limited-res", 2, 500);

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
    setLimit("queue-res", 1, 200);
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
  void freedPermit_shouldGoToTheWaiterThatWaitedFirst() {
    // The first waiter parks with a long retry delay. The delay is then shortened, so the second
    // waiter re-checks often. Both wait behind the holder, and the first waited longer.
    setLimit("order-res", 1, 30_000);
    ResourceTestJob.closeGate();
    try {
      JobHandle holder =
          jobService.enqueue(ResourceTestJob::executeGated).withResource("order-res").submit();
      awaitGatedRunning();

      JobHandle first =
          jobService.enqueue(ResourceTestJob::execute).withResource("order-res").submit();
      awaitWaiting(first, "order-res");

      setLimit("order-res", 1, 200);
      JobHandle second =
          jobService.enqueue(ResourceTestJob::execute).withResource("order-res").submit();
      awaitWaiting(second, "order-res");
      assertParkedWithCheckAhead(first);

      ResourceTestJob.openGate();
      JobAssertions.assertJobCompleted(jobCrudStore, holder);
      await()
          .atMost(Duration.ofSeconds(60))
          .pollInterval(Duration.ofMillis(100))
          .until(() -> starts(first) + starts(second) > 0);

      assertEquals(
          first.id(),
          startedJobIds(first, second).get(0),
          "the job that waited first should take the freed permit first");

      JobAssertions.assertJobCompleted(jobCrudStore, first);
      JobAssertions.assertJobCompleted(jobCrudStore, second);
    } finally {
      ResourceTestJob.openGate();
    }
  }

  @Test
  void freedPermit_shouldStartTheWaiterWithoutWaitingOutItsRetryDelay() {
    setLimit("wake-res", 1, 30_000);
    ResourceTestJob.closeGate();
    try {
      JobHandle holder =
          jobService.enqueue(ResourceTestJob::executeGated).withResource("wake-res").submit();
      awaitGatedRunning();

      JobHandle waiter =
          jobService.enqueue(ResourceTestJob::execute).withResource("wake-res").submit();
      awaitWaiting(waiter, "wake-res");
      assertParkedWithCheckAhead(waiter);

      ResourceTestJob.openGate();
      JobAssertions.assertJobCompleted(jobCrudStore, holder);
      // The waiter's next check is about 30 seconds away. Freeing the permit should start it now.
      await()
          .alias("waiter starts soon after the holder frees the permit")
          .atMost(Duration.ofSeconds(5))
          .pollInterval(Duration.ofMillis(100))
          .until(() -> starts(waiter) == 1);

      JobAssertions.assertJobCompleted(jobCrudStore, waiter);
    } finally {
      ResourceTestJob.openGate();
    }
  }

  @Test
  void submitWithUnconfiguredResource_shouldThrowAndWriteNoJob() {
    // Idempotency keys are at most 36 characters.
    String key = UUID.randomUUID().toString();

    ResourceNotConfiguredException exception =
        assertThrows(
            ResourceNotConfiguredException.class,
            () ->
                jobService
                    .enqueue(ResourceTestJob::execute)
                    .withResource("missing-resource")
                    .withIdempotencyKey(key)
                    .submit());

    assertEquals("missing-resource", exception.resourceName());
    assertTrue(jobCrudStore.findOriginalJobIdByIdempotencyKey(key).isEmpty(), "no job written");
    assertEquals(0, ResourceTestJob.getCompletedCount(), "Job body should not execute");
  }

  @Test
  void unknownResourceSubmit_shouldLeaveTheCallerTransactionCommittable() throws Exception {
    JobHandle handle;
    utx.begin();
    try {
      // A limit set earlier in the same transaction is visible to the submit check.
      setLimit("tx-res", 1, 200);
      handle = jobService.enqueue(ResourceTestJob::execute).withResource("tx-res").submit();
      assertThrows(
          ResourceNotConfiguredException.class,
          () -> jobService.enqueue(ResourceTestJob::execute).withResource("tx-missing").submit());
      assertEquals(
          Status.STATUS_ACTIVE,
          utx.getStatus(),
          "a missing limit must not mark the caller's transaction rollback-only");
    } catch (Throwable failure) {
      utx.rollback();
      throw failure;
    }
    utx.commit();

    JobAssertions.assertJobCompleted(jobCrudStore, handle);
    assertEquals(
        Optional.of(new ResourceLimit("tx-res", 1, Duration.ofMillis(200), "test")),
        scheduler.findResourceLimit("tx-res"));
  }

  @Test
  void setResourceLimit_shouldBeReadBackByFindAndList() {
    scheduler.setResourceLimit(
        ResourceLimit.of(" read-back ", 2)
            .withRetryDelay(Duration.ofMillis(750))
            .withDescription("Read back"));

    ResourceLimit expected = new ResourceLimit("read-back", 2, Duration.ofMillis(750), "Read back");
    assertEquals(Optional.of(expected), scheduler.findResourceLimit("read-back"));
    assertTrue(scheduler.listResourceLimits().contains(expected), "list contains " + expected);
  }

  private void setLimit(String resource, int maxConcurrent, long retryDelayMs) {
    scheduler.setResourceLimit(
        ResourceLimit.of(resource, maxConcurrent)
            .withRetryDelay(Duration.ofMillis(retryDelayMs))
            .withDescription("test"));
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

  /** Ids of the given jobs that have started, in start order. */
  private List<UUID> startedJobIds(JobHandle... handles) {
    Set<UUID> ids = Arrays.stream(handles).map(JobHandle::id).collect(Collectors.toSet());
    return eventCapture.getEvents(JobStartedEvent.class).stream()
        .filter(event -> ids.contains(event.getJobId()))
        .sorted(Comparator.comparing(JobStartedEvent::getTimestamp))
        .map(JobStartedEvent::getJobId)
        .toList();
  }

  private void awaitGatedRunning() {
    await()
        .atMost(Duration.ofSeconds(30))
        .pollInterval(Duration.ofMillis(100))
        .until(() -> ResourceTestJob.getGatedRunning() == 1);
  }

  /**
   * The waiter is parked and its own next check is still well ahead, so only a wake-up can start
   * it.
   */
  private void assertParkedWithCheckAhead(JobHandle handle) {
    JobEntity job = jobCrudStore.findById(handle.id()).orElseThrow();
    assertEquals(JobStatus.PENDING, job.getStatus(), "waiter " + handle.id() + " status");
    assertTrue(
        job.getScheduledTime().isAfter(Instant.now().plusSeconds(10)),
        "waiter "
            + handle.id()
            + " next check should be over 10 s away, was "
            + job.getScheduledTime());
  }

  private void awaitWaiting(JobHandle handle, String resource) {
    await()
        .atMost(Duration.ofSeconds(30))
        .pollInterval(Duration.ofMillis(100))
        .until(
            () ->
                ("Waiting for resource: " + resource)
                    .equals(
                        jobCrudStore
                            .findById(handle.id())
                            .map(JobEntity::getLastError)
                            .orElse(null)));
  }
}
