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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.inject.Inject;
import jakarta.transaction.Status;
import jakarta.transaction.TransactionSynchronizationRegistry;
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
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.junit.jupiter.api.function.Executable;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.api.JobStatus;
import run.ratchet.api.ResourceLimit;
import run.ratchet.api.event.JobStartedEvent;
import run.ratchet.api.exception.ResourceNotConfiguredException;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionEntity;
import run.ratchet.store.entity.JobExecutionEntity.ExecutionStatus;
import run.ratchet.store.spi.BatchStore;
import run.ratchet.store.spi.JobAuditStore;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.JobStore;
import run.ratchet.testsuite.app.BatchItemProcessor;
import run.ratchet.testsuite.app.ResourceTestJob;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.app.TestTransactionRunner;
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

  @Inject private TestTransactionRunner txRunner;

  @Inject private JobStore jobStore;

  @Inject private BatchStore batchStore;

  @Inject private EventCapture eventCapture;

  @Deployment
  public static WebArchive createDeployment() {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
    String profile = System.getProperty("testsuite.profile", "wildfly-managed");

    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(profile, dbType)
        .addClasses(
            ResourceTestJob.class,
            TestJobService.class,
            EventCapture.class,
            BatchItemProcessor.class)
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
      assertWaiterStartsSoonAfterHolderFinishes("wake-res", holder);
    } finally {
      ResourceTestJob.openGate();
    }
  }

  @Test
  // The MongoDB store has no TransactionalCompletionStore, so it cannot run a @TransactionalJob.
  @DisabledIfSystemProperty(named = "ratchet.test.db.type", matches = "mongodb")
  void transactionalHolder_shouldStartTheWaiterWithoutWaitingOutItsRetryDelay() {
    setLimit("tx-wake-res", 1, 30_000);
    ResourceTestJob.closeGate();
    try {
      JobHandle holder =
          jobService
              .enqueue(ResourceTestJob::executeGatedTransactional)
              .withResource("tx-wake-res")
              .submit();
      assertWaiterStartsSoonAfterHolderFinishes("tx-wake-res", holder);
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
  // MongoDB writes commit in their own session, so a JTA rollback cannot undo them.
  @DisabledIfSystemProperty(named = "ratchet.test.db.type", matches = "mongodb")
  void nestedMissingLimitAfterTheBatchWrites_shouldCommitNothing() {
    String key = UUID.randomUUID().toString();
    AtomicReference<UUID> parentId = new AtomicReference<>();

    RuntimeException failure =
        assertThrows(
            RuntimeException.class, () -> startStreamWithNestedMissingLimit(key, 1, parentId));

    assertTrue(causedBy(failure, ResourceNotConfiguredException.class), failure.toString());
    UUID parent = parentId.get();
    assertNotNull(parent, "the first chunk is written before the nested submit fails");
    assertTrue(jobCrudStore.findById(parent).isEmpty(), "the parent job must not commit");
    assertTrue(batchStore.findBatchById(parent).isEmpty(), "the batch row must not commit");
    assertTrue(
        jobCrudStore.findDependants(parent, JobCrudStore.DEFAULT_PAGE_LIMIT, 0).isEmpty(),
        "the first chunk's child job must not commit");
    assertTrue(
        jobCrudStore.findOriginalJobIdByIdempotencyKey(key).isEmpty(),
        "the idempotency key must not commit");
  }

  @Test
  void nestedMissingLimitAfterTheBatchWrites_shouldMarkTheCallerTransactionRollbackOnly() {
    // Two items per chunk, so no child is written. MongoDB keeps its writes after the rollback,
    // and a lone child there would run under a batch whose total is never set.
    CallerOutcome outcome =
        inCallerTransaction(
            () ->
                startStreamWithNestedMissingLimit(
                    UUID.randomUUID().toString(), 2, new AtomicReference<>()));

    assertTrue(
        causedBy(outcome.failure(), ResourceNotConfiguredException.class),
        outcome.failure().toString());
    assertEquals(Status.STATUS_MARKED_ROLLBACK, outcome.status());
  }

  @Test
  // MongoDB writes commit in their own session, so a JTA rollback cannot undo them.
  @DisabledIfSystemProperty(named = "ratchet.test.db.type", matches = "mongodb")
  void ownMissingLimitThenFailingCloseHandler_shouldCommitNothing() {
    String closeKey = UUID.randomUUID().toString();
    AtomicReference<JobHandle> closeSubmit = new AtomicReference<>();

    RuntimeException failure =
        assertThrows(
            RuntimeException.class, () -> startStreamWithFailingClose(closeKey, closeSubmit));

    assertNotNull(closeSubmit.get(), "the close handler submits a job before it fails");
    assertTrue(
        jobCrudStore.findOriginalJobIdByIdempotencyKey(closeKey).isEmpty(),
        "the job the close handler submitted must not commit");
    assertOwnAndCloseFailures(failure);
  }

  @Test
  void ownMissingLimitThenFailingCloseHandler_shouldMarkTheCallerTransactionRollbackOnly() {
    AtomicReference<JobHandle> closeSubmit = new AtomicReference<>();

    CallerOutcome outcome =
        inCallerTransaction(
            () -> startStreamWithFailingClose(UUID.randomUUID().toString(), closeSubmit));

    assertNotNull(closeSubmit.get(), "the close handler submits a job before it fails");
    assertEquals(Status.STATUS_MARKED_ROLLBACK, outcome.status());
    assertOwnAndCloseFailures(outcome.failure());
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

  /**
   * Starts a two-item streaming batch. The second item is read after the parent and batch rows are
   * written, and reading it submits a job that names a resource with no limit. With a chunk size of
   * one, the first chunk is written too, and its progress report saves the parent id first.
   */
  private JobHandle startStreamWithNestedMissingLimit(
      String key, int chunkSize, AtomicReference<UUID> parentId) {
    Stream<String> items =
        Stream.of("nested-1", "nested-2")
            .peek(
                item -> {
                  if (item.equals("nested-2")) {
                    jobService
                        .enqueue(ResourceTestJob::execute)
                        .withResource("nested-missing")
                        .submit();
                  }
                });
    return jobService
        .<String>streamingBatch("nested-missing-limit")
        .withIdempotencyKey(key)
        .fromStream(items)
        .process(BatchItemProcessor::process)
        .withChunkSize(chunkSize)
        .onProgress(progress -> parentId.set(progress.batchId()))
        .start();
  }

  /**
   * Starts a streaming batch whose child resource has no limit, so its own check fails before the
   * stream is read. Closing the stream then submits a delayed job, saved to {@code closeSubmit},
   * and a job that names a second resource with no limit.
   */
  private JobHandle startStreamWithFailingClose(
      String closeKey, AtomicReference<JobHandle> closeSubmit) {
    Stream<String> items =
        Stream.of("close-1")
            .onClose(
                () -> {
                  closeSubmit.set(
                      jobService
                          .schedule(Duration.ofHours(1), ResourceTestJob::execute)
                          .withIdempotencyKey(closeKey)
                          .submit());
                  jobService
                      .enqueue(ResourceTestJob::execute)
                      .withResource("close-nested-missing")
                      .submit();
                });
    return jobService
        .<String>streamingBatch("close-missing-limit")
        .fromStream(items)
        .process(BatchItemProcessor::process)
        .withResource("close-outer-missing")
        .start();
  }

  /** The submit's own missing limit is the cause, and the close handler's failure stays on it. */
  private static void assertOwnAndCloseFailures(RuntimeException failure) {
    assertInstanceOf(IllegalStateException.class, failure, failure.toString());
    ResourceNotConfiguredException own =
        assertInstanceOf(ResourceNotConfiguredException.class, failure.getCause());
    assertEquals("close-outer-missing", own.resourceName());
    assertTrue(
        Arrays.stream(own.getSuppressed())
            .anyMatch(
                suppressed ->
                    suppressed instanceof ResourceNotConfiguredException nested
                        && nested.resourceName().equals("close-nested-missing")),
        Arrays.toString(own.getSuppressed()));
  }

  /** How the submit failed, and the caller transaction's status right after the failure. */
  private record CallerOutcome(RuntimeException failure, int status) {}

  /**
   * Runs the submit in a caller transaction that is rolled back afterwards. The transaction comes
   * from an interceptor, not a bare UserTransaction. On GlassFish and Payara, a submit that calls
   * the scheduler again from inside itself (here, from its own stream) leaves the request unable to
   * call UserTransaction methods: the {@code @Transactional} interceptor keeps the state it
   * restores in one field per bean, and the nested call overwrites it.
   */
  private CallerOutcome inCallerTransaction(Executable submit) {
    AtomicReference<CallerOutcome> outcome = new AtomicReference<>();
    txRunner.runRollbackOnly(
        () -> {
          RuntimeException failure = assertThrows(RuntimeException.class, submit);
          outcome.set(new CallerOutcome(failure, transactionStatus()));
        });
    return outcome.get();
  }

  private static int transactionStatus() {
    try {
      TransactionSynchronizationRegistry registry =
          InitialContext.doLookup("java:comp/TransactionSynchronizationRegistry");
      return registry.getTransactionStatus();
    } catch (NamingException e) {
      throw new IllegalStateException(e);
    }
  }

  private static boolean causedBy(Throwable failure, Class<? extends Throwable> type) {
    for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
      if (type.isInstance(cause)) {
        return true;
      }
    }
    return false;
  }

  private void setLimit(String resource, int maxConcurrent, long retryDelayMs) {
    scheduler.setResourceLimit(
        ResourceLimit.of(resource, maxConcurrent)
            .withRetryDelay(Duration.ofMillis(retryDelayMs))
            .withDescription("test"));
  }

  private void assertWaiterStartsSoonAfterHolderFinishes(String resource, JobHandle holder) {
    awaitGatedRunning();

    JobHandle waiter = jobService.enqueue(ResourceTestJob::execute).withResource(resource).submit();
    awaitWaiting(waiter, resource);
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
