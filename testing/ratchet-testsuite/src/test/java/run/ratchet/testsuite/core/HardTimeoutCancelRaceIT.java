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
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.BusinessKeyHolder;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.api.JobStatus;
import run.ratchet.api.JobType;
import run.ratchet.ri.core.internal.JobTask;
import run.ratchet.ri.core.internal.JobTimeoutHandler;
import run.ratchet.store.entity.JobExecutionEntity;
import run.ratchet.store.entity.JobExecutionEntity.ExecutionStatus;
import run.ratchet.store.spi.JobAuditStore;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.JobStore;
import run.ratchet.testsuite.app.CancellableJob;
import run.ratchet.testsuite.app.SimpleJob;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.CoreOnlyStoreExtension;
import run.ratchet.testsuite.util.JobAssertions;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;
import run.ratchet.testsuite.util.StoreCallGate;

/**
 * Races a user cancel against the hard-timeout watchdog on a real server and database.
 *
 * <p>The watchdog interrupts the body at its deadline. Store-call gates place the cancel in four
 * windows:
 *
 * <ol>
 *   <li>Before the worker reads the status, so the worker sees the cancel itself.
 *   <li>After the worker saved FAILED and before it released its hold, with the watchdog reading
 *       CANCELED before the worker ends, so the worker finishes the cancel. The test waits until
 *       the watchdog has left the cancel to the worker before it releases the worker.
 *   <li>After the worker's {@code call()} returned and before the watchdog reads the row, so the
 *       watchdog finishes the cancel.
 *   <li>After the watchdog read the row RUNNING and before its retry write, which the store
 *       rejects.
 * </ol>
 */
class HardTimeoutCancelRaceIT extends BaseRatchetIT {
  private static final Duration TIMEOUT = Duration.ofSeconds(3);

  @Inject private JobSchedulerService scheduler;
  @Inject private JobCrudStore store;
  @Inject private JobAuditStore audit;
  @Inject private JobStore jobStore;
  @Inject private Instance<Object> beans;

  @Deployment
  public static WebArchive createDeployment() {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(System.getProperty("testsuite.profile", "wildfly-managed"), dbType)
        .addClasses(CancellableJob.class, SimpleJob.class)
        .addStoreInfrastructure()
        // Included only for the shared capability list, not as a service provider.
        .addClasses(CoreOnlyStoreExtension.class)
        .addStoreCallGate()
        // The watchdog must own the timeout rather than a cooperative stop.
        .addResource(
            RatchetArchiveBuilder.testRuntimeConfigContent(
                dbType, Map.of("ratchet.timeout.cancellation-grace-seconds", "0")),
            "ratchet-testsuite.properties")
        .addBeansXml()
        .build();
  }

  @BeforeEach
  void resetJob() {
    CancellableJob.reset();
  }

  @AfterEach
  void releaseGates() {
    try {
      StoreCallGate.reset(Duration.ofSeconds(30));
    } finally {
      CancellableJob.release();
    }
  }

  @Test
  void cancelBeforeTheWorkerReadsTheStatus_endsCanceledAndFreesTheKey() {
    String key = "timeout-worker-read-cancel-" + UUID.randomUUID();
    StoreCallGate.Pause watchdog = StoreCallGate.pauseNext("findById", JobTimeoutHandler.class);
    // The post-body read runs inside handleFailure; the read before the body does not.
    StoreCallGate.Pause worker =
        StoreCallGate.pauseNext("wasCanceledUnderClaim", JobTask.class, "handleFailure");
    JobHandle job = submitTimingOut(key);

    awaitWorkerAndWatchdogParked(worker, watchdog, job);
    int attempts = store.findById(job.id()).orElseThrow().getAttempts();
    cancelWhileTheWatchdogWaits(key, job);
    worker.release();
    awaitWorkerReturned();
    assertTrue(
        StoreCallGate.returnedOn(job.id(), "releaseBusinessKeyHold", worker.thread(), true),
        "The worker's own hold release must delete the cancel's hold");
    assertEquals(
        List.of(ExecutionStatus.CANCELED),
        executionStatuses(job),
        "The worker must record its own cancellation before the watchdog resumes");
    assertTrue(
        scheduler.findBusinessKeyHolder(key).isEmpty(),
        "The worker's hold release must free the key before the watchdog resumes");
    watchdog.release();

    assertCancelFinished(key, job);
    assertNoRetryWritesAndUnchangedAttempts(job, attempts);
  }

  @Test
  void cancelWhileTheWorkerReleasesItsHold_endsCanceledAndFreesTheKey() {
    String key = "timeout-worker-release-cancel-" + UUID.randomUUID();
    StoreCallGate.Pause worker = StoreCallGate.pauseNext("releaseBusinessKeyHold", JobTask.class);
    StoreCallGate.Pause watchdog = StoreCallGate.pauseNext("findById", JobTimeoutHandler.class);
    JobHandle job = submitTimingOut(key);

    awaitWorkerAndWatchdogParked(worker, watchdog, job);
    assertEquals(
        List.of(ExecutionStatus.FAILED),
        executionStatuses(job),
        "The worker must save its execution before releasing its hold");
    int attempts = store.findById(job.id()).orElseThrow().getAttempts();
    cancelWhileTheWatchdogWaits(key, job);
    watchdog.release();
    watchdog.awaitReturned(Duration.ofSeconds(15));
    awaitLeft(watchdog.thread(), JobTimeoutHandler.class);
    worker.release();
    worker.awaitReturned(Duration.ofSeconds(15));
    assertTrue(
        StoreCallGate.returnedOn(job.id(), "releaseBusinessKeyHold", worker.thread(), true),
        "The worker's own hold release must delete the cancel's hold");

    assertCancelFinished(key, job);
    assertFalse(
        StoreCallGate.calledOn(job.id(), "releaseBusinessKeyHold", watchdog.thread()),
        "The watchdog must leave the cancel to the worker that was still running");
    assertNoRetryWritesAndUnchangedAttempts(job, attempts);
  }

  @Test
  void cancelThatTheWatchdogReadsAfterTheWorkerEnded_endsCanceledAndFreesTheKey() {
    String key = "timeout-read-cancel-" + UUID.randomUUID();
    StoreCallGate.Pause read = StoreCallGate.pauseNext("findById", JobTimeoutHandler.class);
    JobHandle job = submitTimingOut(key);
    StoreCallGate.Watch release = StoreCallGate.watch("releaseBusinessKeyHold", job.id());

    awaitWorkerLeftTheTimeoutToTheWatchdog(read, release, job);
    cancelWhileTheWatchdogWaits(key, job);
    read.release();

    assertCancelFinished(key, job);
    assertFalse(
        StoreCallGate.called(job.id(), "incrementRetryAttempt"),
        "The watchdog must not write a retry for a job it read as CANCELED");
    assertFalse(
        StoreCallGate.called(job.id(), "scheduleJobRetry"),
        "The watchdog must not reschedule a job it read as CANCELED");
  }

  @Test
  void cancelThatMakesTheWatchdogsRetryWriteMiss_endsCanceledAndFreesTheKey() {
    String key = "timeout-write-cancel-" + UUID.randomUUID();
    StoreCallGate.Pause write =
        StoreCallGate.pauseNext("incrementRetryAttempt", JobTimeoutHandler.class);
    JobHandle job = submitTimingOut(key);
    StoreCallGate.Watch release = StoreCallGate.watch("releaseBusinessKeyHold", job.id());

    awaitWorkerLeftTheTimeoutToTheWatchdog(write, release, job);
    int attempts = store.findById(job.id()).orElseThrow().getAttempts();
    cancelWhileTheWatchdogWaits(key, job);
    write.release();

    assertCancelFinished(key, job);
    assertEquals(
        attempts,
        store.findById(job.id()).orElseThrow().getAttempts(),
        "The store must reject the watchdog's retry increment on a CANCELED row");
    assertFalse(
        StoreCallGate.called(job.id(), "scheduleJobRetry"),
        "The rejected retry increment must not be followed by a reschedule");
  }

  @Test
  void gateSeesStoreCallsAndKeepsEveryCapabilityTheStoreAdvertises() {
    UUID unknown = UUID.randomUUID();
    assertTrue(store.findById(unknown).isEmpty());
    assertEquals(
        List.of("findById"),
        StoreCallGate.calls(unknown),
        "The gate must see calls on the store bean");
    for (Class<?> capability : CoreOnlyStoreExtension.optionalCapabilities()) {
      assertEquals(
          jobStore.capability(capability).isPresent(),
          beans.select(capability).isResolvable(),
          capability.getSimpleName()
              + " must stay injectable exactly when the store advertises it");
    }
  }

  private JobHandle submitTimingOut(String key) {
    JobHandle job =
        scheduler
            .enqueue(CancellableJob::execute)
            .withBusinessKey(key)
            .withTimeout(TIMEOUT)
            .withMaxRetries(1)
            .submit();
    await().atMost(Duration.ofSeconds(15)).untilTrue(CancellableJob.STARTED);
    Thread worker = CancellableJob.WORKER.get();
    assertNotNull(worker);
    assertTrue(
        Arrays.stream(worker.getStackTrace())
            .anyMatch(frame -> frame.getClassName().equals(JobTask.class.getName())),
        "The body must run inside JobTask.call() on the recorded worker thread");
    assertFalse(CancellableJob.ENDED.get(), "The body must still be running");
    return job;
  }

  /** The watchdog is parked, and the worker saved its execution FAILED and returned from call(). */
  private void awaitWorkerLeftTheTimeoutToTheWatchdog(
      StoreCallGate.Pause watchdog, StoreCallGate.Watch release, JobHandle job) {
    watchdog.awaitParked(Duration.ofSeconds(30));
    assertEquals(job.id(), watchdog.jobId(), "The pause must hold this job's watchdog");
    release.awaitCalls(1, Duration.ofSeconds(15));
    awaitWorkerReturned();
    assertEquals(
        List.of(ExecutionStatus.FAILED),
        executionStatuses(job),
        "The worker must leave the outcome to the watchdog");
  }

  private void awaitWorkerAndWatchdogParked(
      StoreCallGate.Pause worker, StoreCallGate.Pause watchdog, JobHandle job) {
    worker.awaitParked(Duration.ofSeconds(30));
    assertEquals(job.id(), worker.jobId(), "The pause must hold this job's worker");
    assertSame(CancellableJob.WORKER.get(), worker.thread());
    watchdog.awaitParked(Duration.ofSeconds(30));
    assertEquals(job.id(), watchdog.jobId(), "The pause must hold this job's watchdog");
  }

  private void awaitWorkerReturned() {
    Thread worker = CancellableJob.WORKER.get();
    assertNotNull(worker);
    awaitLeft(worker, JobTask.class);
  }

  private void awaitLeft(Thread thread, Class<?> type) {
    await()
        .atMost(Duration.ofSeconds(15))
        .until(
            () ->
                Arrays.stream(thread.getStackTrace())
                    .noneMatch(frame -> frame.getClassName().equals(type.getName())));
  }

  private void assertNoRetryWritesAndUnchangedAttempts(JobHandle job, int attempts) {
    assertFalse(
        StoreCallGate.called(job.id(), "incrementRetryAttempt"),
        "The watchdog must not write a retry for a job it read as CANCELED");
    assertFalse(
        StoreCallGate.called(job.id(), "scheduleJobRetry"),
        "The watchdog must not reschedule a job it read as CANCELED");
    assertEquals(
        attempts,
        store.findById(job.id()).orElseThrow().getAttempts(),
        "The cancel must leave the attempt count unchanged");
  }

  /** The cancel commits while the job row is still RUNNING, so it places a hold on the key. */
  private void cancelWhileTheWatchdogWaits(String key, JobHandle job) {
    assertTrue(scheduler.cancelJob(job.id()));
    assertEquals(
        new BusinessKeyHolder(job.id(), JobType.SINGLE, JobStatus.CANCELED),
        scheduler.findBusinessKeyHolder(key).orElseThrow());
  }

  /** Both checks run and report, so a regression shows each part that broke. */
  private void assertCancelFinished(String key, JobHandle job) {
    assertAll(
        // HeldKeyReleases retries a failed release after 5 s, so that retry must not pass this
        // check.
        () ->
            await()
                .atMost(Duration.ofSeconds(3))
                .until(() -> scheduler.findBusinessKeyHolder(key).isEmpty()),
        () ->
            await()
                .atMost(Duration.ofSeconds(10))
                .untilAsserted(
                    () -> assertEquals(List.of(ExecutionStatus.CANCELED), executionStatuses(job))));
    JobAssertions.assertJobCanceled(store, job);
    assertNotNull(
        scheduler.schedule(Duration.ofHours(1), SimpleJob::execute).withBusinessKey(key).submit(),
        "The key must be free for a new job");
  }

  private List<ExecutionStatus> executionStatuses(JobHandle job) {
    return audit.findExecutionsByJobId(job.id(), 10, 0).stream()
        .map(JobExecutionEntity::getStatus)
        .toList();
  }
}
