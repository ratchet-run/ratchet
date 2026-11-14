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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.inject.Inject;
import java.time.Duration;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobHandle;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.JobTerminalStore;
import run.ratchet.testsuite.app.CancellableJob;
import run.ratchet.testsuite.app.CooperativeJob;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.app.TestTransactionRunner;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.JobAssertions;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;

/** Verifies cooperative cancellation has the hard-timeout outcome without an interrupt. */
class CooperativeCancellationIT extends BaseRatchetIT {
  @Inject private TestJobService jobService;
  @Inject private JobTerminalStore terminalStore;
  @Inject private TestTransactionRunner transactions;
  @Inject private JobCrudStore jobCrudStore;
  private JobHandle cancellable;

  @Deployment
  public static WebArchive createDeployment() {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
    String profile = System.getProperty("testsuite.profile", "wildfly-managed");
    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(profile, dbType)
        .addClasses(
            CooperativeJob.class,
            CancellableJob.class,
            TestJobService.class,
            CooperativeCancellationOptionsProducer.class)
        .addStoreInfrastructure()
        .addBeansXml()
        .build();
  }

  @BeforeEach
  void resetJob() {
    CooperativeJob.reset();
    CancellableJob.reset();
  }

  @AfterEach
  void cleanupCancellable() {
    CancellableJob.release();
    if (cancellable != null) {
      jobService.cancelJob(cancellable.id());
      await().atMost(Duration.ofSeconds(15)).untilTrue(CancellableJob.ENDED);
    }
  }

  @Test
  void requestedStopFailsAsTimeoutWithoutInterruptAndCallsOnFailureOnce() {
    // A cooperative timeout fails once without interrupting the body.
    JobHandle handle =
        jobService
            .enqueue(CooperativeJob::execute)
            .withTimeout(Duration.ofSeconds(6))
            .withMaxRetries(0)
            .onFailure((ctx, failure) -> CooperativeJob.recordFailureCallback())
            .submit();
    JobAssertions.assertJobFailed(jobCrudStore, handle);
    assertTrue(CooperativeJob.STARTED.get());
    assertTrue(CooperativeJob.SAW_REQUEST.get());
    assertTrue(CooperativeJob.STOPPED_COOPERATIVELY.get());
    assertFalse(CooperativeJob.INTERRUPTED.get());
    assertNotNull(CooperativeJob.DEADLINE.get());
    await()
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(() -> assertEquals(1, CooperativeJob.FAILURE_CALLBACKS.get()));
    await()
        .during(Duration.ofSeconds(2))
        .atMost(Duration.ofSeconds(5))
        .untilAsserted(() -> assertEquals(1, CooperativeJob.FAILURE_CALLBACKS.get()));
    assertTrue(
        jobCrudStore
            .findById(handle.id())
            .orElseThrow()
            .getLastError()
            .contains("Hard timeout exceeded"));
  }

  @Test
  void cancelOfRunningJobStopsTheBodyAndEndsCanceled() {
    // A local cancel stops the body without retrying or calling its failure callback.
    startCancellable();
    int attempts = jobCrudStore.findById(cancellable.id()).orElseThrow().getAttempts();
    assertTrue(jobService.cancelJob(cancellable.id()));
    await().atMost(Duration.ofSeconds(5)).untilTrue(CancellableJob.SAW_REQUEST);
    await().atMost(Duration.ofSeconds(15)).untilTrue(CancellableJob.ENDED);
    JobAssertions.assertJobCanceled(jobCrudStore, cancellable);
    await()
        .during(Duration.ofSeconds(2))
        .atMost(Duration.ofSeconds(5))
        .untilAsserted(
            () -> {
              assertEquals(0, CancellableJob.FAILURE_CALLBACKS.get());
              assertFalse(CancellableJob.INTERRUPTED.get());
              assertFalse(CancellableJob.COMPLETED.get());
              assertEquals(
                  attempts, jobCrudStore.findById(cancellable.id()).orElseThrow().getAttempts());
            });
  }

  @Test
  void cancelWrittenOutsideThisNodeReachesTheBodyThroughTheNodeTick() {
    // The node tick relays a store cancellation without a local cancellation event.
    startCancellable();
    assertTrue(transactions.call(() -> terminalStore.cancelRunningJob(cancellable.id(), null)));
    await().atMost(Duration.ofSeconds(20)).untilTrue(CancellableJob.SAW_REQUEST);
    await().atMost(Duration.ofSeconds(15)).untilTrue(CancellableJob.ENDED);
    JobAssertions.assertJobCanceled(jobCrudStore, cancellable);
    await()
        .during(Duration.ofSeconds(2))
        .atMost(Duration.ofSeconds(5))
        .untilAsserted(() -> assertEquals(0, CancellableJob.FAILURE_CALLBACKS.get()));
  }

  private void startCancellable() {
    cancellable =
        jobService
            .enqueue(CancellableJob::execute)
            .withTimeout(Duration.ofSeconds(120))
            .withMaxRetries(3)
            .onFailure((ctx, failure) -> CancellableJob.recordFailureCallback())
            .submit();
    await().atMost(Duration.ofSeconds(15)).untilTrue(CancellableJob.STARTED);
  }
}
