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
package run.ratchet.testsuite.chain;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.inject.Inject;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobStatus;
import run.ratchet.api.event.ChainFailedEvent;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.testsuite.app.CancellableJob;
import run.ratchet.testsuite.app.ChainStepTracker;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.JobAssertions;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;

/** Validates linear chain execution: A → B → C in order. */
class LinearChainIT extends BaseRatchetIT {

  @Inject private TestJobService jobService;

  @Inject private JobCrudStore jobCrudStore;

  @Deployment
  public static WebArchive createDeployment() {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
    String profile = System.getProperty("testsuite.profile", "wildfly-managed");

    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(profile, dbType)
        .addClasses(ChainStepTracker.class, CancellableJob.class, TestJobService.class)
        .addStoreInfrastructure()
        .addBeansXml()
        .build();
  }

  @BeforeEach
  void resetTrackers() {
    ChainStepTracker.reset();
    CancellableJob.reset();
  }

  @Test
  void linearChain_shouldExecuteStepsInOrder() {
    // The chain executes each step in order.
    JobHandle handle =
        jobService
            .enqueue(ChainStepTracker::stepA)
            .then(ChainStepTracker::stepB)
            .then(ChainStepTracker::stepC)
            .submit();

    JobAssertions.assertChainCompleted(jobCrudStore, handle, 3, Duration.ofSeconds(30));

    List<String> order = ChainStepTracker.executionOrder();
    assertEquals(List.of("A", "B", "C"), order, "Chain steps should execute in order");
  }

  @Test
  void linearChain_shouldStopAfterStepFailure() {
    // A failing step cancels the remaining chain.
    JobHandle handle =
        jobService
            .enqueue(ChainStepTracker::stepA)
            .withMaxRetries(0)
            .then(ChainStepTracker::stepBThenFail)
            .then(ChainStepTracker::stepC)
            .submit();

    JobAssertions.assertJobCompleted(jobCrudStore, handle, Duration.ofSeconds(30));

    AtomicReference<JobEntity> failedStep = new AtomicReference<>();
    await()
        .atMost(Duration.ofSeconds(30))
        .pollInterval(Duration.ofMillis(500))
        .untilAsserted(
            () -> {
              List<JobEntity> dependants =
                  jobCrudStore.findDependants(handle.id(), JobCrudStore.DEFAULT_PAGE_LIMIT, 0);
              assertEquals(1, dependants.size(), "First chain step should have one dependant");
              JobEntity stepB = dependants.get(0);
              assertEquals(JobStatus.FAILED, stepB.getStatus(), "Failing chain step should fail");
              failedStep.set(stepB);
            });

    await()
        .atMost(Duration.ofSeconds(30))
        .pollInterval(Duration.ofMillis(500))
        .untilAsserted(
            () -> {
              List<JobEntity> dependants =
                  jobCrudStore.findDependants(
                      failedStep.get().getId(), JobCrudStore.DEFAULT_PAGE_LIMIT, 0);
              assertEquals(1, dependants.size(), "Failed chain step should have one dependant");
              assertEquals(
                  JobStatus.CANCELED,
                  dependants.get(0).getStatus(),
                  "Downstream chain step should be canceled after a failure");
            });

    assertEquals(
        List.of("A", "B"),
        ChainStepTracker.executionOrder(),
        "Chain execution should stop before later steps after a failure");
  }

  @Test
  void cancelOfRunningChainStepCancelsTheNextStepAtOnceAndFailsTheChainOnce() {
    // Canceling the running step cancels its child and publishes one chain failure.
    CancellableJob.HOLD_AFTER_REQUEST = true;
    var scheduler = jobService.getScheduler();
    AtomicReference<UUID> chainId = new AtomicReference<>();
    AtomicInteger failures = new AtomicInteger();
    Consumer<Object> listener =
        event -> {
          if (event instanceof ChainFailedEvent failed
              && failed.getParentJobId().equals(chainId.get())) failures.incrementAndGet();
        };
    scheduler.addEventListener(listener);
    JobHandle first = null;
    try {
      first =
          jobService
              .enqueue(CancellableJob::execute)
              .withTimeout(Duration.ofSeconds(120))
              .then(ChainStepTracker::stepB)
              .submit();
      chainId.set(first.id());
      await().atMost(Duration.ofSeconds(15)).untilTrue(CancellableJob.STARTED);
      assertTrue(scheduler.cancelJob(first.id()));
      await().atMost(Duration.ofSeconds(5)).untilTrue(CancellableJob.SAW_REQUEST);
      await()
          .atMost(Duration.ofSeconds(15))
          .untilAsserted(
              () -> {
                var children =
                    jobCrudStore.findDependants(chainId.get(), JobCrudStore.DEFAULT_PAGE_LIMIT, 0);
                assertEquals(1, children.size());
                assertEquals(JobStatus.CANCELED, children.get(0).getStatus());
              });
      assertTrue(ChainStepTracker.executionOrder().isEmpty());
      assertFalse(CancellableJob.ENDED.get(), "The first body must still be waiting on the gate");
      CancellableJob.release();
      await().atMost(Duration.ofSeconds(15)).untilTrue(CancellableJob.ENDED);
      JobAssertions.assertJobCanceled(jobCrudStore, first);
      await()
          .during(Duration.ofSeconds(3))
          .atMost(Duration.ofSeconds(10))
          .untilAsserted(
              () -> {
                assertEquals(1, failures.get());
                assertTrue(ChainStepTracker.executionOrder().isEmpty());
              });
    } finally {
      CancellableJob.release();
      try {
        if (first != null) {
          scheduler.cancelJob(first.id());
          await().atMost(Duration.ofSeconds(15)).untilTrue(CancellableJob.ENDED);
        }
      } finally {
        scheduler.removeEventListener(listener);
      }
    }
  }
}
