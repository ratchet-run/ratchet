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
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.inject.Inject;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.ri.core.internal.JobTask;
import run.ratchet.store.entity.JobExecutionEntity;
import run.ratchet.store.entity.JobExecutionEntity.ExecutionStatus;
import run.ratchet.store.spi.JobAuditStore;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.testsuite.app.CheckpointJob;
import run.ratchet.testsuite.app.CheckpointTransactions;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.JobAssertions;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;
import run.ratchet.testsuite.util.StoreCallGate;

/** A transient worker retry write must leave the job runnable with exactly one charged retry. */
class FailedRescheduleRetryIT extends BaseRatchetIT {
  @Inject private JobSchedulerService scheduler;
  @Inject private JobCrudStore store;
  @Inject private JobAuditStore audit;

  @Deployment
  public static WebArchive createDeployment() {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(System.getProperty("testsuite.profile", "wildfly-managed"), dbType)
        .addClasses(CheckpointJob.class, CheckpointTransactions.class)
        .addStoreInfrastructure()
        .addStoreCallGate()
        .addResource(
            RatchetArchiveBuilder.testRuntimeConfigContent(
                dbType, Map.of("ratchet.timeout.cancellation-grace-seconds", "0")),
            "ratchet-testsuite.properties")
        .addBeansXml()
        .build();
  }

  @BeforeEach
  void resetJob() {
    CheckpointJob.reset();
  }

  @AfterEach
  void resetGate() {
    StoreCallGate.reset(Duration.ofSeconds(30));
  }

  @Test
  void failedWorkerRescheduleIsRetriedWithoutAnotherCharge() {
    StoreCallGate.Fault fault = StoreCallGate.failNext("scheduleJobRetry", JobTask.class);
    StoreCallGate.Pause retry = StoreCallGate.pauseNext("scheduleJobRetry", JobTask.class);
    JobHandle job =
        scheduler
            .enqueue(CheckpointJob::retryCommitted)
            .withMaxRetries(1)
            .withBackoff(BackoffPolicy.FIXED, Duration.ofMillis(100))
            .submit();
    StoreCallGate.Watch retries = StoreCallGate.watch("scheduleJobRetry", job.id());

    fault.awaitFired(Duration.ofSeconds(30));
    assertEquals(job.id(), fault.jobId());
    assertNotSame(Thread.currentThread(), fault.thread());
    // OutcomeWriteRetries first retries inline on the worker after the injected failure.
    retry.awaitParked(Duration.ofSeconds(30));
    assertEquals(job.id(), retry.jobId());
    assertEquals(1, retries.count(), "The first retry write must have thrown");
    assertTrue(StoreCallGate.calledOn(job.id(), "scheduleJobRetry", fault.thread()));
    assertFalse(StoreCallGate.returnedOn(job.id(), "scheduleJobRetry", fault.thread(), true));
    assertFalse(StoreCallGate.returnedOn(job.id(), "scheduleJobRetry", fault.thread(), null));
    retry.release();
    retry.awaitReturned(Duration.ofSeconds(15));
    assertTrue(StoreCallGate.returnedOn(job.id(), "scheduleJobRetry", retry.thread(), true));
    // Includes the second body invocation.
    JobAssertions.assertJobCompleted(store, job, Duration.ofSeconds(45));
    // AbstractJobRetryStoreContract.scheduleJobRetryRejectsStaleSave stores 1 after one retry.
    // Attempts count retry charges, so the initial run plus its retry must still store only 1.
    assertEquals(1, store.findById(job.id()).orElseThrow().getAttempts());
    assertEquals(2, CheckpointJob.OBSERVED.get(job.id()).size());
    assertEquals(2, retries.count(), "One retry write threw and the second returned true");
    assertEquals(
        2, StoreCallGate.calls(job.id()).stream().filter("scheduleJobRetry"::equals).count());

    await()
        .atMost(Duration.ofSeconds(15))
        .untilAsserted(
            () -> {
              List<ExecutionStatus> statuses =
                  audit.findExecutionsByJobId(job.id(), 10, 0).stream()
                      .map(JobExecutionEntity::getStatus)
                      .toList();
              // The first body invocation failed and its retry succeeded.
              assertEquals(2, statuses.size());
              assertEquals(1, statuses.stream().filter(ExecutionStatus.FAILED::equals).count());
              assertEquals(1, statuses.stream().filter(ExecutionStatus.SUCCEEDED::equals).count());
            });
  }
}
